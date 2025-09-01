#!/usr/bin/env python3
import argparse, hashlib, json, logging, os, re, signal, sys, time, yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx, feedparser
from goose3 import Goose
import trafilatura
from sqlalchemy import (
    create_engine, text, MetaData, Table, Column,
    Integer, BigInteger, String, Text, DateTime, Boolean, UniqueConstraint
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import select

# -------------------------
# Config & constants
# -------------------------
USER_AGENT = "rss-collector/0.2 (+httpx)"
DEFAULT_DB_URL = os.getenv("DATABASE_URL", "sqlite:///collector.db")
HTML_DIR = Path(os.getenv("HTML_DIR", "./data/html")).resolve()
MIN_HOST_INTERVAL = float(os.getenv("MIN_HOST_INTERVAL", "1.0"))
MAX_BODY_CHARS = int(os.getenv("MAX_BODY_CHARS", "120000"))
REQUEST_TIMEOUT = 15.0
MAX_RETRIES = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("rss-collector")

STOP = False
goose = Goose()

# -------------------------
# DB schema
# -------------------------
metadata = MetaData()

feeds_table = Table(
    "feeds", metadata,
    Column("id", Integer, primary_key=True),
    Column("outlet", String(255), nullable=False),
    Column("url", Text, nullable=False, unique=True),
    Column("active", Boolean, default=True),
    Column("etag", String(255)),
    Column("last_modified", String(255)),
    Column("last_checked", DateTime(timezone=True)),
)

articles_table = Table(
    "articles", metadata,
    Column("id", BigInteger, primary_key=True),
    Column("outlet", String(255), nullable=False),
    Column("feed_id", Integer),
    Column("url", Text, nullable=False),
    Column("canonical_url", Text, nullable=False),
    Column("title", Text),
    Column("summary", Text),
    Column("published_at", DateTime(timezone=True)),
    Column("author", String(255)),
    Column("html_sha256", String(64)),
    Column("html_path", Text),
    Column("body", Text),
    Column("hash_sha256", String(64), nullable=False),
    Column("created_at", DateTime(timezone=True), default=datetime.now(timezone.utc)),
    UniqueConstraint("canonical_url", name="uix_canonical"),
)

# -------------------------
# Helpers
# -------------------------
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def sha256_hexd(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def canonicalize_url(url: str) -> str:
    return url.split("#", 1)[0].split("?", 1)[0].rstrip("/")

def guess_lead(text: str, max_len: int = 240) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return (text[:max_len] + ("…" if len(text) > max_len else ""))

def write_html_local(html: str) -> Tuple[str, str]:
    HTML_DIR.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(html.encode("utf-8", errors="ignore")).hexdigest()
    path = HTML_DIR / f"{digest}.html"
    if not path.exists():
        path.write_text(html, encoding="utf-8", errors="ignore")
    return digest, str(path)

# -------------------------
# Repo
# -------------------------
class Repo:
    def __init__(self, engine: Engine):
        self.engine = engine
        metadata.create_all(self.engine)

    def upsert_feed(self, outlet: str, url: str):
        with self.engine.begin() as conn:
            row = conn.execute(select(feeds_table.c.id).where(feeds_table.c.url == url)).fetchone()
            if row is None:
                conn.execute(feeds_table.insert().values(outlet=outlet, url=url, active=True))

    def list_active_feeds(self) -> List[dict]:
        with self.engine.begin() as conn:
            rows = conn.execute(select(feeds_table).where(feeds_table.c.active == True)).mappings().all()
            return [dict(r) for r in rows]

    def update_feed_state(self, feed_id: int, etag: Optional[str], last_modified: Optional[str]):
        with self.engine.begin() as conn:
            conn.execute(
                feeds_table.update().where(feeds_table.c.id == feed_id).values(
                    etag=etag, last_modified=last_modified, last_checked=utcnow()
                )
            )

    def insert_article(self, feed_id: int, outlet: str, url: str, title: str,
                        summary: str, published_at: Optional[datetime], author: Optional[str],
                        html_sha256: Optional[str], html_path: Optional[str], body: Optional[str]) -> Optional[int]:
        canonical = canonicalize_url(url)
        # body도 해시 키에 포함시켜서 업데이트 감지
        content_key = f"{canonical}|{body[:200] if body else ''}"
        hash_key = sha256_hexd(content_key)
        if body and len(body) > MAX_BODY_CHARS:
            body = body[:MAX_BODY_CHARS]
        with self.engine.begin() as conn:
            try:
                res = conn.execute(articles_table.insert().values(
                    outlet=outlet, feed_id=feed_id, url=url, canonical_url=canonical,
                    title=title, summary=summary, published_at=published_at, author=author,
                    html_sha256=html_sha256, html_path=html_path, body=body, hash_sha256=hash_key,
                    created_at=utcnow()
                ))
                return res.inserted_primary_key[0]
            except IntegrityError:
                return None

# -------------------------
# Extraction
# -------------------------
def extract_body(html: str, url: str) -> Tuple[Optional[str], Optional[str]]:
    # 1) trafilatura
    try:
        json_str = trafilatura.extract(html, output="json", include_comments=False, include_tables=False)
        if json_str:
            data = json.loads(json_str)
            text = data.get("text")
            title = data.get("title")
            if text and len(text.strip()) > 200:
                text = re.sub(r"\s+", " ", text).strip()
                return title, text
    except Exception as e:
        logger.warning("trafilatura failed: %s", e)

    # 2) goose3
    try:
        article = goose.extract(raw_html=html, url=url)
        if article.cleaned_text and len(article.cleaned_text.strip()) > 200:
            text = re.sub(r"\s+", " ", article.cleaned_text).strip()
            return article.title, text
    except Exception as e:
        logger.warning("goose3 failed: %s", e)

    return None, None

# -------------------------
# Core
# -------------------------
def fetch_and_process_feed(client: httpx.Client, repo: Repo, feed_row: dict):
    feed_id = feed_row["id"]
    outlet = feed_row["outlet"]
    feed_url = feed_row["url"]

    logger.info("Fetch feed %s (%s)", outlet, feed_url)
    resp = client.get(feed_url, headers={"User-Agent": USER_AGENT})
    if resp.status_code != 200:
        logger.warning("Feed fetch failed %s", feed_url)
        return

    parsed = feedparser.parse(resp.content)
    entries = parsed.entries or []
    logger.info("Feed entries: %d", len(entries))

    for e in entries:
        link = e.get("link") or e.get("id")
        if not link:
            continue
        title = (e.get("title") or "").strip()
        author = (e.get("author") or "").strip() if e.get("author") else None
        summary = (e.get("summary") or "").strip() if e.get("summary") else ""
        published_at = None
        for k in ("published_parsed", "updated_parsed"):
            if getattr(e, k, None):
                try:
                    tm = getattr(e, k)
                    published_at = datetime(*tm[:6], tzinfo=timezone.utc)
                    break
                except Exception:
                    pass

        try:
            page = client.get(link, headers={"User-Agent": USER_AGENT})
        except Exception as ex:
            logger.warning("Article fetch failed: %s err=%s", link, ex)
            continue

        html = page.text
        extracted_title, body = extract_body(html, link)
        if not body:
            body = summary or title

        html_sha, html_path = write_html_local(html)
        final_title = extracted_title or title or ""
        final_summary = guess_lead(body or summary or final_title)

        article_id = repo.insert_article(
            feed_id=feed_id, outlet=outlet, url=link,
            title=final_title, summary=final_summary,
            published_at=published_at, author=author,
            html_sha256=html_sha, html_path=html_path, body=body,
        )
        if article_id:
            logger.info("Saved article id=%s title=%s", article_id, final_title[:80])
        else:
            logger.info("Duplicate skipped url=%s", link)

# -------------------------
# CLI
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds", type=str, default=None, help="feeds.yaml path")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval", type=int, default=300)
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    engine = create_engine(DEFAULT_DB_URL, future=True)
    repo = Repo(engine)

    feeds = []
    if args.feeds and Path(args.feeds).exists():
        data = yaml.safe_load(Path(args.feeds).read_text(encoding="utf-8")) or {}
        feeds = [(f.get("outlet", "unknown"), f["url"]) for f in data.get("feeds", []) if f.get("url")]
    else:
        feeds = [("연합뉴스", "https://www.yna.co.kr/rss/news.xml")]

    for outlet, url in feeds:
        repo.upsert_feed(outlet, url)

    client = httpx.Client(timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})

    def run_once():
        rows = repo.list_active_feeds()
        for row in rows:
            try:
                fetch_and_process_feed(client, repo, row)
            except Exception as e:
                logger.exception("Feed error %s: %s", row.get("url"), e)

    run_once() if args.once else run_once()

    client.close()

if __name__ == "__main__":
    main()
