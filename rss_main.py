import pandas as pd
import feedparser
import requests
from goose3 import Goose
from bs4 import BeautifulSoup
from datetime import datetime, UTC
import re
import sqlalchemy as sa
import json
import os

from dotenv import load_dotenv
load_dotenv()  # .env 파일 자동 로드

DEFAULT_DB_URL = os.getenv("DATABASE_URL", "sqlite:///collector.db")
engine = sa.create_engine(
    DEFAULT_DB_URL,
    future=True,
    pool_pre_ping=True,
    pool_recycle=3600
)

# -------------------------
# DB에 저장
1
# -------------------------
import json

def save_articles(rows):
    with engine.begin() as conn:
        for row in rows:
            # 기사 저장
            result = conn.execute(sa.text("""
                INSERT INTO news (outlet, feed_url, title, summary, link, published, crawled_at)
                VALUES (:outlet, :feed_url, :title, :summary, :link, :published, :crawled_at)
                ON CONFLICT (link) DO NOTHING
                RETURNING id
            """), {
                "outlet": row["outlet"],
                "feed_url": row["feed_url"],
                "title": row["title"],
                "summary": row["summary"],
                "link": row["link"],
                "published": row["published"],
                "crawled_at": row["crawled_at"],
            })

            article_id = result.scalar_one_or_none()
            if not article_id:
                # 이미 존재하는 경우 id 가져오기
                article_id = conn.execute(sa.text("""
                    SELECT id FROM news WHERE link = :link
                """), {"link": row["link"]}).scalar_one()

            # 이미지 저장 (주의: FK는 news_id)
            for img in row.get("images", []):
                conn.execute(sa.text("""
                    INSERT INTO news_image (news_id, src, alt)
                    VALUES (:news_id, :src, :alt)
                    ON CONFLICT DO NOTHING
                """), {
                    "news_id": article_id,
                    "src": img.get("src"),
                    "alt": img.get("alt", "")
                })


# -------------------------
# RSS 피드 리스트
# -------------------------
rss_list = [
    ("국민일보", "https://rss.kmib.co.kr/rss/rss.xml"),
    ("경향신문", "https://www.khan.co.kr/rss/rssdata/total_news.xml"),
]

goose = Goose()

# -------------------------
# 본문 추출기
# -------------------------
def extract_body_and_images(html: str, outlet: str):
    soup = BeautifulSoup(html, "html.parser")

    if outlet == "국민일보":
        body_div = soup.select_one("div#articleBody")

    elif outlet == "경향신문":
        body_div = soup.select_one("div.art_body")

    else:
        body_div = None

    if not body_div:
        return "", []

    texts = []
    images = []

    for elem in body_div.find_all(["p", "img"]):
        if elem.name == "p":
            text = elem.get_text(" ", strip=True)
            if text:
                texts.append(text)
        elif elem.name == "img":
            src = elem.get("src")
            alt = elem.get("alt", "")
            if src:
                images.append({"src": src, "alt": alt})

    text = " ".join(texts)

    # 불필요한 패턴 제거
    text = re.sub(r"\([^)]+기자\)", "", text)  # (홍길동 기자)
    text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+", "", text)  # 이메일
    text = re.sub(r"무단 전재.*", "", text)  # 저작권 문구
    text = re.sub(r"\s+", " ", text).strip()  # 공백 정리

    return text, images

# -------------------------
# 크롤링 (한번 실행당 10개)
# -------------------------
rows = []
max_articles = 10
count = 0

for outlet, feed_url in rss_list:
    if count >= max_articles:
        break
    feed = feedparser.parse(feed_url)
    for entry in feed.entries:
        if count >= max_articles:
            break

        link = entry.get("link")
        title = re.sub(r"\s+", " ", entry.get("title", "").strip())
        summary = re.sub(r"\s+", " ", entry.get("summary", "").strip())
        published = entry.get("published", "")

        body = ""
        try:
            resp = requests.get(link, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            # 기본 BeautifulSoup 전용 파서
            body, images = extract_body_and_images(resp.text, outlet)
            if not body:
                article = goose.extract(raw_html=resp.text, url=link)
                body = article.cleaned_text.strip() if article.cleaned_text else ""
        except Exception as e:
            print(f"[{outlet}] 기사 크롤링 실패: {e}")

        rows.append({
            "outlet": outlet,
            "feed_url": feed_url,
            "title": title,
            "link": link,
            "summary": summary,
            "published": published,
            "body": body,
            "images": images,  # ← 이미지 따로 저장
            "crawled_at": datetime.now(UTC).isoformat()
        })
        count += 1

# -------------------------
# 저장
# -------------------------
df = pd.DataFrame(rows)
df.to_csv("rss_news.csv", index=False, encoding="utf-8-sig")
print("총 수집 기사:", len(df))

# DB 저장
save_articles(rows)
print("DB 저장 완료")