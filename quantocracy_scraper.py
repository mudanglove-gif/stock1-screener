"""
Quantocracy 전체 아카이브 스크래퍼
- 149페이지 × 50개 = ~7,450개 글 수집
- 키워드 기반 퀀트 전략 자동 태깅
- SQLite DB 저장
"""

import re
import sqlite3
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

DB_PATH = "quantocracy.db"
BASE_URL = "https://quantocracy.com/"
HEADERS = {"User-Agent": "Mozilla/5.0 (personal quant research tool)"}
DELAY = 2  # 페이지 간 딜레이 (초)

# ── 전략 카테고리 키워드 ──
STRATEGY_KEYWORDS = {
    "momentum": [
        "momentum", "trend following", "trend-following", "cross-section",
        "winner", "loser", "relative strength", "price momentum",
        "time series momentum", "dual momentum", "absolute momentum",
    ],
    "value": [
        "value", "PBR", "PER", "P/E", "P/B", "CAPE", "cheap", "expensive",
        "book-to-market", "earnings yield", "dividend yield", "deep value",
    ],
    "volatility": [
        "volatility", "VIX", "options", "put", "call", "straddle",
        "implied volatility", "realized volatility", "variance",
        "selling volatility", "vol surface",
    ],
    "ml_ai": [
        "machine learning", "deep learning", "neural", "transformer",
        "LLM", "ChatGPT", "GPT", "AI", "random forest", "gradient boosting",
        "reinforcement learning", "NLP", "natural language",
    ],
    "factor": [
        "factor", "size", "quality", "profitability", "anomaly",
        "low volatility", "beta", "multi-factor", "factor investing",
        "Fama-French", "smart beta",
    ],
    "macro": [
        "macro", "GDP", "inflation", "interest rate", "yield curve",
        "Fed", "central bank", "recession", "business cycle",
        "regime", "economic",
    ],
    "portfolio": [
        "portfolio", "allocation", "risk parity", "diversification",
        "Markowitz", "rebalancing", "asset allocation", "60/40",
        "equal weight", "Kelly",
    ],
    "mean_reversion": [
        "mean reversion", "pairs", "cointegration", "spread",
        "stat arb", "statistical arbitrage", "reversal",
        "contrarian", "overshoot",
    ],
    "sentiment": [
        "sentiment", "text mining", "news", "social media",
        "Twitter", "Reddit", "earnings call", "10-K",
    ],
    "technical": [
        "technical analysis", "RSI", "MACD", "moving average",
        "Bollinger", "support", "resistance", "breakout",
        "candlestick", "chart pattern", "golden cross",
        "death cross", "ichimoku",
    ],
    "crypto": [
        "crypto", "bitcoin", "ethereum", "blockchain",
        "DeFi", "token", "stablecoin",
    ],
    "risk": [
        "risk management", "drawdown", "tail risk", "black swan",
        "stop loss", "position sizing", "risk-adjusted",
        "Sharpe", "Sortino", "max drawdown",
    ],
}


def init_db():
    """SQLite DB 초기화"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            source TEXT,
            url TEXT UNIQUE,
            description TEXT,
            published_at TEXT,
            collected_at TEXT DEFAULT CURRENT_TIMESTAMP,
            page INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS strategy_tags (
            article_id INTEGER,
            tag TEXT,
            FOREIGN KEY (article_id) REFERENCES articles(id)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON articles(url)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tag ON strategy_tags(tag)")
    conn.commit()
    return conn


def auto_tag(title, description):
    """키워드 기반 자동 태깅"""
    text = (title + " " + description).lower()
    tags = []
    for category, keywords in STRATEGY_KEYWORDS.items():
        if any(kw.lower() in text for kw in keywords):
            tags.append(category)
    return tags


def scrape_page(page_num):
    """한 페이지의 모든 글 수집"""
    url = f"{BASE_URL}?pg={page_num}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        print(f"  Page {page_num} 요청 실패: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = []

    for entry in soup.select(".qo-entry"):
        title_el = entry.select_one(".qo-title")
        desc_el = entry.select_one(".qo-description")
        extras_el = entry.select_one(".qo-extras")

        if not title_el:
            continue

        full_title = title_el.get_text(strip=True)

        # 출처 추출: "제목 [블로그명]" 패턴
        source_match = re.search(r"\[([^\]]+)\]$", full_title)
        source = source_match.group(1) if source_match else "Unknown"
        clean_title = re.sub(r"\s*\[[^\]]+\]$", "", full_title).strip()

        # 날짜 파싱
        date_str = extras_el.get_text(strip=True) if extras_el else ""
        date_match = re.search(
            r"(\d{1,2}\s+\w+\s+\d{4},\s+\d{1,2}:\d{2}[ap]m)", date_str
        )
        parsed_date = None
        if date_match:
            try:
                parsed_date = datetime.strptime(
                    date_match.group(1), "%d %b %Y, %I:%M%p"
                ).isoformat()
            except ValueError:
                pass

        href = title_el.get("href", "")
        description = desc_el.get_text(strip=True)[:500] if desc_el else ""

        articles.append({
            "title": clean_title,
            "source": source,
            "url": href,
            "description": description,
            "published_at": parsed_date,
            "page": page_num,
        })

    return articles


def save_articles(conn, articles):
    """DB에 저장 (중복 URL 스킵)"""
    saved = 0
    for art in articles:
        try:
            cursor = conn.execute(
                """INSERT OR IGNORE INTO articles
                   (title, source, url, description, published_at, page)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (art["title"], art["source"], art["url"],
                 art["description"], art["published_at"], art["page"]),
            )
            if cursor.rowcount > 0:
                saved += 1
                article_id = cursor.lastrowid
                # 자동 태깅
                tags = auto_tag(art["title"], art["description"])
                for tag in tags:
                    conn.execute(
                        "INSERT INTO strategy_tags (article_id, tag) VALUES (?, ?)",
                        (article_id, tag),
                    )
        except Exception:
            pass
    conn.commit()
    return saved


def run_scraper(max_pages=149):
    """전체 스크래핑 실행"""
    conn = init_db()
    total_saved = 0

    print(f"Quantocracy 스크래핑 시작 (최대 {max_pages}페이지)")

    for pg in range(1, max_pages + 1):
        articles = scrape_page(pg)
        if not articles:
            print(f"  Page {pg}: 글 없음 — 마지막 페이지 도달")
            break

        saved = save_articles(conn, articles)
        total_saved += saved
        print(f"  Page {pg}: {len(articles)}개 수집, {saved}개 신규 저장")

        time.sleep(DELAY)

    # 통계 출력
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"\n완료! 총 {total}개 글 DB 저장 (이번 실행: {total_saved}개 신규)")

    # 카테고리별 통계
    print("\n카테고리별 분포:")
    rows = conn.execute(
        "SELECT tag, COUNT(*) as cnt FROM strategy_tags GROUP BY tag ORDER BY cnt DESC"
    ).fetchall()
    for tag, cnt in rows:
        print(f"  {tag}: {cnt}개")

    conn.close()


if __name__ == "__main__":
    run_scraper()
