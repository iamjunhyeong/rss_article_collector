#!/usr/bin/env python3
import argparse, yaml, httpx, feedparser, re, json, hashlib, logging, csv
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Tuple
import pandas as pd
from goose3 import Goose
from bs4 import BeautifulSoup
import trafilatura

# -------------------------
# 공통 설정
# -------------------------
USER_AGENT = "rss-collector/0.2 (+httpx)"
REQUEST_TIMEOUT = 15.0
MAX_BODY_CHARS = 10000   # 🔥 본문은 1500자로 제한

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("crawler")

FEEDS_CSV = "feeds.csv"
ARTICLES_CSV = "articles.csv"

goose = Goose()

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_hexd(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="ignore")).hexdigest()

def canonicalize_url(url: str) -> str:
    return url.split("#", 1)[0].split("?", 1)[0].rstrip("/")

def guess_lead(text: str, max_len: int = 240) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return (text[:max_len] + ("…" if len(text) > max_len else ""))

# -------------------------
# CSV Repo
# -------------------------
class CsvRepo:
    def __init__(self):
        if not Path(FEEDS_CSV).exists():
            pd.DataFrame(columns=[
                "id", "outlet", "url", "active", "etag", "last_modified", "last_checked"
            ]).to_csv(FEEDS_CSV, index=False)

        if not Path(ARTICLES_CSV).exists():
            pd.DataFrame(columns=[
                "id", "outlet", "feed_id", "url", "canonical_url", "title", "summary",
                "published_at", "author", "html_sha256", "html_path", "body",
                "hash_sha256", "created_at"
            ]).to_csv(ARTICLES_CSV, index=False)

    def _next_id(self, path: str) -> int:
        df = pd.read_csv(path)
        if df.empty:
            return 1
        return (pd.to_numeric(df["id"], errors="coerce").max() + 1)

    def upsert_feed(self, outlet: str, url: str):
        df = pd.read_csv(FEEDS_CSV)
        if url in df["url"].values:
            return
        new_id = self._next_id(FEEDS_CSV)
        new_row = {
            "id": new_id, "outlet": outlet, "url": url, "active": True,
            "etag": "", "last_modified": "", "last_checked": utcnow()
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(FEEDS_CSV, index=False)

    def list_active_feeds(self) -> List[dict]:
        df = pd.read_csv(FEEDS_CSV)
        df = df[df["active"].astype(str) == "True"]
        return df.to_dict(orient="records")

    def insert_article(self, feed_id: int, outlet: str, url: str, title: str,
                       summary: str, published_at: Optional[str], author: Optional[str],
                       html_sha256: Optional[str], html_path: Optional[str], body: Optional[str]) -> Optional[int]:
        canonical = canonicalize_url(url)
        content_key = f"{canonical}|{body[:200] if body else ''}"
        hash_key = sha256_hexd(content_key)

        df = pd.read_csv(ARTICLES_CSV)
        if canonical in df["canonical_url"].values:
            return None

        new_id = self._next_id(ARTICLES_CSV)
        if body and len(body) > MAX_BODY_CHARS:
            body = body[:MAX_BODY_CHARS]

        new_row = {
            "id": new_id,
            "outlet": outlet,
            "feed_id": feed_id,
            "url": url,
            "canonical_url": canonical,
            "title": title,
            "summary": summary,
            "published_at": published_at or "",
            "author": author or "",
            "html_sha256": html_sha256 or "",
            "html_path": html_path or "",
            "body": body or "",
            "hash_sha256": hash_key,
            "created_at": utcnow(),
        }
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        df.to_csv(ARTICLES_CSV, index=False, quoting=csv.QUOTE_ALL, escapechar='\\')
        return new_id

# -------------------------
# 본문 추출기
# -------------------------
def extract_body(html: str, url: str) -> Tuple[Optional[str], Optional[str]]:
    # 연합뉴스 전용 파서
    try:
        soup = BeautifulSoup(html, "html.parser")
        body_div = soup.select_one("div.story-news.article")
        if body_div:
            text = body_div.get_text(" ", strip=True)
            text = re.sub(r"\s+", " ", text).strip()
            text = re.sub(r"\([^)]+기자\)", "", text)
            text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+", "", text)
            text = re.sub(r"무단 전재.*", "", text)
            title_tag = soup.select_one("h1.tit") or soup.select_one("h1")
            title = title_tag.get_text(" ", strip=True) if title_tag else None
            return title, text[:MAX_BODY_CHARS]
    except Exception as e:
        logger.warning("YNA parser failed: %s", e)

    # trafilatura fallback
    try:
        json_str = trafilatura.extract(html, output="json", include_comments=False, include_tables=False)
        if json_str:
            data = json.loads(json_str)
            text = data.get("text")
            title = data.get("title")
            if text:
                text = re.sub(r"\s+", " ", text).strip()
                return title, text[:MAX_BODY_CHARS]
    except Exception as e:
        logger.warning("trafilatura failed: %s", e)

    # goose3 fallback
    try:
        article = goose.extract(raw_html=html, url=url)
        if article.cleaned_text:
            text = re.sub(r"\s+", " ", article.cleaned_text).strip()
            return article.title, text[:MAX_BODY_CHARS]
    except Exception as e:
        logger.warning("goose3 failed: %s", e)

    return None, None

# -------------------------
# RSS → 기사 수집
# -------------------------
def fetch_and_process_feed(client: httpx.Client, repo: CsvRepo, feed_row: dict, limit: int = None):
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

    for i, e in enumerate(entries):
        if limit and i >= limit:
            break
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
                    published_at = datetime(*tm[:6], tzinfo=timezone.utc).isoformat()
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

        final_title = extracted_title or title or ""
        final_summary = guess_lead(body or summary or final_title)

        article_id = repo.insert_article(
            feed_id=feed_id, outlet=outlet, url=link,
            title=final_title, summary=final_summary,
            published_at=published_at, author=author,
            html_sha256="", html_path="", body=body,
        )
        if article_id:
            logger.info("Saved article id=%s title=%s", article_id, final_title[:80])
        else:
            logger.info("Duplicate skipped url=%s", link)

# -------------------------
# 메인
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feeds", type=str, default=None, help="feeds.yaml path")
    parser.add_argument("--limit", type=int, default=10, help="기사 개수 제한")
    args = parser.parse_args()

    repo = CsvRepo()

    # junpyopark 스타일: feeds.yaml에 여러 RSS 정의
    feeds = []
    if args.feeds and Path(args.feeds).exists():
        data = yaml.safe_load(Path(args.feeds).read_text(encoding="utf-8")) or {}
        feeds = [(f.get("outlet", "unknown"), f["url"]) for f in data.get("feeds", []) if f.get("url")]
    else:
        feeds = [
            ("연합뉴스-전체", "https://www.yna.co.kr/rss/news.xml"),
            ("연합뉴스-정치", "https://www.yna.co.kr/rss/politics.xml"),
            ("연합뉴스-경제", "https://www.yna.co.kr/rss/economy.xml"),
        ]

    for outlet, url in feeds:
        repo.upsert_feed(outlet, url)

    client = httpx.Client(timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    rows = repo.list_active_feeds()

    for row in rows:
        try:
            fetch_and_process_feed(client, repo, row, limit=args.limit)
        except Exception as e:
            logger.exception("Feed error %s: %s", row.get("url"), e)

    client.close()

if __name__ == "__main__":
    main()
