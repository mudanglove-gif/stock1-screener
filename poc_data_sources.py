"""
Phase 2-pre 데이터 소스 PoC
- 08:50 morning_check.py 구현 전 각 데이터 소스 가용성 검증
- 사용: py -3.12 poc_data_sources.py
"""

import os
import sys
from datetime import datetime, timedelta

print("=" * 60)
print("Stock1 데이터 소스 PoC")
print(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 60)

# ──────────────────────────────────────────────────
# 1. yfinance — US 증시
# ──────────────────────────────────────────────────
print("\n[1] yfinance US 증시")
try:
    import yfinance as yf
    tickers = yf.download(["^GSPC", "^IXIC", "^DJI"], period="3d", progress=False)
    closes = tickers["Close"]
    sp500_today = closes["^GSPC"].iloc[-1]
    sp500_prev = closes["^GSPC"].iloc[-2]
    nasdaq_today = closes["^IXIC"].iloc[-1]
    nasdaq_prev = closes["^IXIC"].iloc[-2]
    print(f"  ✅ S&P500: {sp500_today:.2f} ({(sp500_today/sp500_prev-1)*100:+.2f}%)")
    print(f"  ✅ Nasdaq: {nasdaq_today:.2f} ({(nasdaq_today/nasdaq_prev-1)*100:+.2f}%)")
    print(f"  → 결론: 사용 가능, requirements에 yfinance 추가")
except ImportError:
    print("  ⚠ yfinance 미설치 → pip install yfinance")
except Exception as e:
    print(f"  ❌ 실패: {e}")

# ──────────────────────────────────────────────────
# 2. FRED VIX — 타이밍 확인
# ──────────────────────────────────────────────────
print("\n[2] FRED VIX 타이밍")
try:
    from fredapi import Fred
    fred = Fred(api_key="d41ee4f2e4718a0e25f8dfabaabe3ec4")
    vix = fred.get_series("VIXCLS").dropna()
    latest_date = vix.index[-1]
    latest_value = vix.iloc[-1]
    delay_days = (datetime.now() - latest_date).days
    print(f"  ✅ VIX 최신: {latest_date.strftime('%Y-%m-%d')} = {latest_value:.2f}")
    print(f"  지연: {delay_days}일")
    if delay_days <= 1:
        print(f"  → 결론: 08:50 KST 기준 전일 데이터 가용 ✅")
    else:
        print(f"  ⚠ 1일 이상 지연 — 실시간 사용 시 주의")
except Exception as e:
    print(f"  ❌ 실패: {e}")

# ──────────────────────────────────────────────────
# 3. 네이버 야간선물
# ──────────────────────────────────────────────────
print("\n[3] 네이버 금융 야간선물")
try:
    import requests
    from bs4 import BeautifulSoup
    # 코스피200 선물 메인
    url = "https://finance.naver.com/sise/sise_index.naver?code=KPI200"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")
    no_today = soup.select_one("#now_value")
    if no_today:
        print(f"  ✅ KPI200: {no_today.get_text(strip=True)}")
    else:
        print(f"  ⚠ KPI200 파싱 실패 (CSS 셀렉터 변경 가능성)")

    # 코스피200 선물 상세 (야간 거래 포함)
    url2 = "https://finance.naver.com/sise/sise_index_day.naver?code=KPI200"
    resp2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    print(f"  네이버 응답 코드: {resp2.status_code}")
except Exception as e:
    print(f"  ❌ 실패: {e}")

# ──────────────────────────────────────────────────
# 4. Investing.com 야간선물
# ──────────────────────────────────────────────────
print("\n[4] Investing.com KOSPI200 선물")
try:
    import requests
    from bs4 import BeautifulSoup
    url = "https://kr.investing.com/indices/kospi-200-futures"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9",
    }
    resp = requests.get(url, headers=headers, timeout=10)
    print(f"  응답 코드: {resp.status_code}")
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, "html.parser")
        # data-test 속성 사용
        price_el = soup.select_one("[data-test='instrument-price-last']")
        if price_el:
            print(f"  ✅ 선물가: {price_el.get_text(strip=True)}")
        else:
            print(f"  ⚠ 가격 셀렉터 미발견 (Investing.com이 동적 렌더링 사용 가능성)")
    elif resp.status_code == 403:
        print(f"  ⚠ 403 Forbidden — User-Agent 차단, 다른 방법 필요")
except Exception as e:
    print(f"  ❌ 실패: {e}")

# ──────────────────────────────────────────────────
# 5. 네이버 외국인 선물 매매동향
# ──────────────────────────────────────────────────
print("\n[5] 네이버 외국인 선물 매매동향")
try:
    import requests
    from bs4 import BeautifulSoup
    url = "https://finance.naver.com/sise/investorDealTrendDay.naver"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    print(f"  응답 코드: {resp.status_code}")
    if resp.status_code == 200:
        soup = BeautifulSoup(resp.text, "html.parser")
        # 테이블 존재 확인
        tables = soup.find_all("table")
        print(f"  테이블 발견: {len(tables)}개")
        if tables:
            print(f"  → 추가 파싱 작업 필요 (선물/외국인 컬럼 식별)")
except Exception as e:
    print(f"  ❌ 실패: {e}")

# ──────────────────────────────────────────────────
# 6. KIS 분봉 API (Phase 1 분봉 최적화용)
# ──────────────────────────────────────────────────
print("\n[6] KIS 분봉 API 가용성")
print("  ℹ KIS OpenAPI 분봉 TR: FHKST03010200 (주식분봉조회)")
print("  ℹ 제공 기간: 최근 30거래일 (1분/3분/5분/10분/15분/30분/60분)")
print("  ℹ 1회 최대 30건 응답 → 페이징 필요")
print("  → Phase 1 분봉 최적화 가능 (30일치 충분)")
print("  → 실제 호출 테스트는 KIS 키 필요, 앱에서 검증")

# ──────────────────────────────────────────────────
# 7. NXT 프리마켓
# ──────────────────────────────────────────────────
print("\n[7] NXT 프리마켓 데이터")
print("  ℹ NXT (Next Trade): 2025년 3월 출범")
print("  ℹ KIS OpenAPI NXT 지원: 2025년 하반기 추가됨")
print("  ℹ TR ID: NXT 종목 시세는 정규 종목코드 + 거래소 구분 N")
print("  ⚠ 유동성 낮음, 체결 없는 종목 다수 → 보조 지표로만 활용")
print("  → 실제 검증은 KIS 키 + NXT 종목코드 매핑 필요")

# ──────────────────────────────────────────────────
# 8. 네이버 환율 (이미 검증됨)
# ──────────────────────────────────────────────────
print("\n[8] 네이버 환율 (검증됨)")
try:
    import requests
    from bs4 import BeautifulSoup
    url = "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
    soup = BeautifulSoup(resp.text, "html.parser")
    rate = soup.select_one(".no_today")
    if rate:
        print(f"  ✅ USD/KRW: {rate.get_text(strip=True)}")
except Exception as e:
    print(f"  ❌ 실패: {e}")

print("\n" + "=" * 60)
print("PoC 완료")
print("=" * 60)
