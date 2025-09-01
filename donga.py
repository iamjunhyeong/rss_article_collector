import requests, re
from bs4 import BeautifulSoup

def parse_donga(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=10)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    # 바깥 div.article_txt 말고, 안쪽 모든 div.article_txt의 <p> 찾기
    paras = soup.select("div.article_txt p")
    text = " ".join([p.get_text(" ", strip=True) for p in paras])

    # 후처리
    text = re.sub(r"\([^)]+기자\)", "", text)
    text = re.sub(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+", "", text)
    text = re.sub(r"무단 전재.*", "", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


# ---------------- 테스트 ----------------
if __name__ == "__main__":
    url = "https://www.donga.com/news/Economy/article/all/20250901/132290869/2"
    body = parse_donga(url)
    print("본문 길이:", len(body))
    print("앞 500자:", body[:500])
