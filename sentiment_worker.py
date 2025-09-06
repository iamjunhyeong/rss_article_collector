import os, json, time
import sqlalchemy as sa
from openai import OpenAI
from prometheus_client import Counter, start_http_server

from dotenv import load_dotenv
load_dotenv()  # .env 파일 자동 로드

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DB_URL = os.getenv("DATABASE_URL", "sqlite:///collector.db")

engine = sa.create_engine(
    DB_URL,
    future=True,
    pool_pre_ping=True,       # ✅ 끊어진 커넥션 자동 감지
    pool_recycle=3600         # ✅ 1시간마다 커넥션 재활용
)
client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# Prometheus Counters
processed_counter = Counter("articles_processed_total", "Total articles processed")
success_counter   = Counter("articles_success_total", "Articles successfully tagged")
fail_counter      = Counter("articles_fail_total", "Articles failed to tag")

# Start Prometheus metrics server on port 8000
start_http_server(8000)

PROMPT_TEMPLATE = """
다음 뉴스 기사를 읽고 반드시 **유효한 JSON만** 출력하세요.
추가 텍스트나 설명은 절대 쓰지 마세요.

## 출력 형식 (필수, 누락 금지)
{{
  "category": "카테고리1",  # 반드시 하나만
  "sentiment": "감정라벨",
  "political_orientation": "PROGRESSIVE | CONSERVATIVE | MODERATE",
  "confidence": 0.0~1.0,
  "rationale": "근거 설명 (1~2문장)",
  "summary": "기사 요약 (본문을 3~4문장으로 압축)"
}}

## 카테고리 후보 (반드시 이 중에서 하나만 선택, 영어 코드 사용)
- politics: Politics (Government, policy, diplomacy, elections)
- society: Society (Incidents, accidents, labor, education, crime)
- economy: Economy (Business, finance, industry, trade, stock, prices)
- international: International (International relations, war, diplomacy, overseas events)
- culture: Culture (Movies, drama, entertainment, tradition)
- sports: Sports (Baseball, soccer, basketball, Olympics, game results)
- it_science: IT & Science (Technology, science, internet, AI)

## 감정 후보 (반드시 이 중에서만 선택)
- hope_encourage: 성취, 희망, 승리, 격려, 긍정적 메시지
- anger_criticism: 분노, 비판, 불만, 논란
- anxiety_crisis: 불안, 위기, 공포, 갈등
- sad_shock: 슬픔, 충격, 재난, 사고, 패배
- neutral_factual: 중립적, 단순 사실 전달
- fun_interest: 재미, 흥미, 유쾌

## 정치 성향 후보 (반드시 이 중에서 하나만 선택)
- PROGRESSIVE
- CONSERVATIVE
- MODERATE
- NONE (해당 없음, 예: 스포츠, 연예 기사)

---

## 예시 1
제목: "태권도 배준서, 그랑프리 챌린지 우승…5초 남기고 역전 드라마"
본문: "한국 태권도 선수가 경기 종료 직전 역전승으로 금메달을 차지했다."
출력:
{{"category": "sports", "sentiment": "hope_encourage", "political_orientation": "NONE", "confidence": 0.9, "rationale": "스포츠 경기에서의 극적인 승리를 전달하는 긍정적 기사이다."}}

## 예시 2
제목: "여자농구 챔피언 BNK, 개막전서 후지쓰에 10점 차 패배"
본문: "BNK가 개막전에서 후지쓰에 패배했다."
출력:
{{"category": "sports", "sentiment": "sad_shock", "political_orientation": "NONE", "confidence": 0.85, "rationale": "스포츠 경기의 패배 소식을 전하며 실망과 아쉬움을 드러낸다."}}

## 예시 3
제목: "대통령, 강릉 일원 재난사태 선포"
본문: "가뭄 피해 확산을 막기 위해 강릉에 재난 사태가 선포됐다."
출력:
{{"category": "politics", "sentiment": "neutral_factual", "political_orientation": "CONSERVATIVE", "confidence": 0.8, "rationale": "정부의 공식 발표를 전달하는 사실 중심 기사이다."}}

## 예시 4
제목: "삼성전자, 2분기 영업이익 12조 달성"
본문: "삼성전자가 2분기에 12조 원의 영업이익을 기록했다."
출력:
{{"category": "economy", "sentiment": "hope_encourage", "political_orientation": "NONE", "confidence": 0.88, "rationale": "긍정적인 실적 발표로 희망적인 분위기를 전달한다."}}

## 예시 5
제목: "북, 러시아에 병력 파견 결정"
본문: "북한이 러시아와의 조약 체결 직후 러시아에 병력을 파견하기로 했다."
출력:
{{"category": "international", "sentiment": "anxiety_crisis", "political_orientation": "CONSERVATIVE", "confidence": 0.85, "rationale": "국제 갈등과 군사 파병 소식으로 불안과 위기감을 조성한다."}}

## 예시 6
제목: "유명 배우 신작 영화 개봉 첫날 매진"
본문: "유명 배우의 신작 영화가 개봉 첫날 매진을 기록했다."
출력:
{{"category": "culture", "sentiment": "hope_encourage", "political_orientation": "NONE", "confidence": 0.9, "rationale": "문화 콘텐츠의 성공을 다룬 긍정적인 기사이다."}}

## 예시 7
제목: "국회, 전기요금 인상 두고 여야 격렬한 공방"
본문: "전기요금 인상안을 두고 여야가 국회 본회의에서 날 선 공방을 벌였다. 일부 의원들은 정부 정책을 강하게 비판했다."
출력:
{{"category": "politics", "sentiment": "anger_criticism", "political_orientation": "PROGRESSIVE", "confidence": 0.9, "rationale": "정부 정책을 둘러싼 정치적 갈등과 비판을 중심으로 다룬 기사이다."}}

## 예시 8
제목: "강아지가 스케이트보드 타고 도심 질주…시민들 웃음"
본문: "서울 도심에서 한 반려견이 스케이트보드를 타고 거리를 달려 시민들의 눈길을 사로잡았다. 현장에서는 웃음과 환호가 이어졌다."
출력:
{{"category": "society", "sentiment": "fun_interest", "political_orientation": "NONE", "confidence": 0.92, "rationale": "재미있고 흥미로운 사회적 장면을 전달하는 기사이다."}}

---

## 실제 분류할 기사
제목: {title}
본문: {body}

출력은 반드시 JSON만!
"""

def classify_article(article):
    prompt = PROMPT_TEMPLATE.format(
        title=article["title"], 
        body=str(article.get("body", ""))[:1500]
    )
    # print(article["title"])
    resp = client.responses.create(
        model="gpt-4o-mini",
        input=prompt,
        temperature=0.2,
        max_output_tokens=800,
    )
    text = resp.output_text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        print(f"⚠️ JSON decode 실패. 원본 응답:\n{text}")
        # fallback: JSON 추출만 시도
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            try:
                return json.loads(text[start:end+1])
            except:
                pass
        raise

import pandas as pd

import pandas as pd
import sqlalchemy as sa
import json, time, traceback

# DB 연결
engine = sa.create_engine(DB_URL, future=True, pool_pre_ping=True, pool_recycle=3600)

# 1) DB에서 sentiment NULL 기사 가져오기
def fetch_unlabeled_from_db(limit=10):
    with engine.begin() as conn:
        rows = conn.execute(sa.text("""
            SELECT id, link, title, summary
            FROM news
            WHERE sentiment IS NULL
            ORDER BY crawled_at DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]

# 2) CSV에서 body 보강하기
def attach_body(rows, csv_path="rss_news.csv"):
    df = pd.read_csv(csv_path)
    df = df.set_index("link")  # 빠른 lookup
    enriched = []
    for row in rows:
        link = row["link"]
        if link in df.index:
            row["body"] = str(df.loc[link, "body"])
        else:
            row["body"] = ""
        enriched.append(row)
    return enriched

# 3) 감정분석 결과 저장하기
def save_tag(article_link, tag):
    sentiment = tag.get("sentiment", "neutral_factual")

    # ✅ sentiment → emotion 매핑
    sentiment_to_emotion = {
        "hope_encourage": "POSITIVE",
        "fun_interest": "POSITIVE",
        "neutral_factual": "NEUTRAL",
        "anger_criticism": "NEGATIVE",
        "sad_shock": "NEGATIVE",
        "anxiety_crisis": "NEGATIVE",
    }
    emotion = sentiment_to_emotion.get(sentiment.lower(), "NEUTRAL")

    with engine.begin() as conn:
        conn.execute(sa.text("""
            UPDATE news
            SET category              = :cat,
                sentiment             = :sent,
                emotion               = :emo,   -- 상위 감정
                confidence            = :conf,
                rationale             = :rat,
                summary               = :sum,
                political_orientation = :pol,   -- ✅ 추가
                tagged_at             = now()
            WHERE link = :link
        """), {
            "link": article_link,
            # ✅ enum 상수와 맞추기 위해 대문자로 변환
            "cat": tag.get("category", "").upper(),
            "sent": sentiment.upper(),
            "emo": emotion,  # 이미 POSITIVE/NEGATIVE/NEUTRAL
            "conf": float(tag.get("confidence", 0.0) or 0.0),
            "rat": tag.get("rationale", ""),
            "sum": tag.get("summary", ""),
            "pol": tag.get("political_orientation", "MODERATE").upper()  # 기본값 MODERATE
        })



def fetch_unlabeled_from_csv(limit=10):
    df = pd.read_csv("rss_news.csv")
    # 아직 감정분석 안 된 것만 필터링하려면 article_tag 테이블과 조인 대신, 임시로 전부 다 가져오기
    rows = df.head(limit).to_dict(orient="records")
    return rows


def fetch_unlabeled(limit=10):
    with engine.begin() as conn:
        rows = conn.execute(sa.text("""
            SELECT n.id, n.title, n.summary, n.link
            FROM news n
            WHERE n.sentiment IS NULL
            ORDER BY n.crawled_at DESC
            LIMIT :limit
        """), {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]



import traceback

def main():
    while True:
        # 1. DB에서 미분석 기사 조회
        unlabeled = fetch_unlabeled_from_db(100)
        if not unlabeled:
            print("No new articles. sleep…")
            time.sleep(30)
            break

        # 2. CSV에서 body 붙이기
        rows = attach_body(unlabeled)

        # 3. 감정분석 실행
        for row in rows:
            try:
                tag = classify_article(row)  # LLM 호출
                save_tag(row["link"], tag)  # DB 업데이트
                print(f"✔ tagged {row['link']} → {tag.get('sentiment')}")
            except Exception as e:
                print(f"❌ error tagging {row['link']}: {e}")
                traceback.print_exc()

        break

if __name__ == "__main__":
    main()
