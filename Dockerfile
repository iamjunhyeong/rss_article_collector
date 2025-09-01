FROM python:3.11-slim

# 기본 툴 설치
RUN apt-get update && apt-get install -y \
    build-essential curl git libxml2 libxslt1.1 libxslt1-dev libxml2-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 기본 CMD는 collector 실행 (워커는 override)
CMD ["python", "rss_collector.py", "--feeds", "feeds.yaml", "--loop", "--interval", "300"]
