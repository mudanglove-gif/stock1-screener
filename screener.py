"""
Stock1 기술적 스크리너 v4
- FinanceDataReader로 KOSPI/KOSDAQ 전 종목 OHLCV 수집
- pandas-ta로 기술적 지표 계산
- 복합 스코어링 + 수급/펀더멘탈 필터
- ATR 기반 손절/목표가 산출
- Quantocracy 전략 인사이트 연동
"""

import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta
import requests
import FinanceDataReader as fdr

LOOKBACK_DAYS = 400
NAVER_API_DELAY = 0.1
FRED_API_KEY = "d41ee4f2e4718a0e25f8dfabaabe3ec4"
ECOS_API_KEY = "SA1KDIVJJYNNKZRW1K6Z"


def get_korea_macro():
    """네이버 금융 + ECOS로 한국 매크로 데이터 수집"""
    usd_krw = 0
    base_rate = 0

    # 1. 원/달러 환율: 네이버 금융 (실시간)
    try:
        from bs4 import BeautifulSoup
        url = "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        rate_text = soup.select_one(".no_today").get_text(strip=True)
        usd_krw = float(rate_text.replace("원", "").replace(",", "").strip())
    except Exception as e:
        print(f"네이버 환율 조회 실패: {e}")

    # 2. 기준금리: ECOS API (지연 허용)
    try:
        url2 = f"https://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/1/722Y001/D/20250101/20261231/0101000"
        resp2 = requests.get(url2, timeout=10)
        rows2 = resp2.json().get("StatisticSearch", {}).get("row", [])
        base_rate = float(rows2[-1]["DATA_VALUE"]) if rows2 else 0
    except Exception as e:
        print(f"ECOS 기준금리 조회 실패: {e}")

    return {"usd_krw": usd_krw, "base_rate": base_rate}


def get_market_regime():
    """FRED + ECOS로 시장 국면 판별"""
    vix = 0
    us10y = 0
    usd_krw = 0
    base_rate = 0

    # FRED (미국)
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        vix = float(fred.get_series("VIXCLS").dropna().iloc[-1])
        us10y = float(fred.get_series("DGS10").dropna().iloc[-1])
    except Exception as e:
        print(f"FRED API 조회 실패: {e}")

    # 한국 매크로 (네이버 환율 + ECOS 기준금리)
    korea = get_korea_macro()
    usd_krw = korea["usd_krw"]
    base_rate = korea["base_rate"]

    # 복합 레짐 판별 (VIX + US10Y + 환율)
    risk_score = 0
    risk_factors = []

    # VIX 기반
    if vix > 30:
        risk_score += 3
        risk_factors.append(f"VIX {vix:.1f} 공포")
    elif vix > 25:
        risk_score += 2
        risk_factors.append(f"VIX {vix:.1f} 경계")
    elif vix > 20:
        risk_score += 1
        risk_factors.append(f"VIX {vix:.1f} 주의")

    # US10Y 기반 (금리 부담)
    if us10y > 5.0:
        risk_score += 2
        risk_factors.append(f"US10Y {us10y:.1f}% 고금리")
    elif us10y > 4.5:
        risk_score += 1
        risk_factors.append(f"US10Y {us10y:.1f}% 금리부담")

    # 환율 기반 (외국인 매도 압력)
    if usd_krw > 1450:
        risk_score += 2
        risk_factors.append(f"원/달러 {usd_krw:.0f} 급등")
    elif usd_krw > 1350:
        risk_score += 1
        risk_factors.append(f"원/달러 {usd_krw:.0f} 약세")

    if risk_score >= 5:
        regime = "risk_off"
        score_adj = 20
    elif risk_score >= 3:
        regime = "caution"
        score_adj = 10
    elif risk_score == 0:
        regime = "risk_on"
        score_adj = -5
    else:
        regime = "neutral"
        score_adj = 0

    desc = f"{regime} (위험점수:{risk_score}/7, {', '.join(risk_factors) if risk_factors else '안정'})"

    # 환율 방향 (수출주/내수주 가중 참고)
    fx_direction = "원화약세" if usd_krw > 1400 else "원화강세" if usd_krw < 1200 else "보통"

    print(f"시장 국면: {regime} - {desc}")
    print(f"  VIX: {vix:.1f}, US10Y: {us10y:.2f}%, 원/달러: {usd_krw:.1f}, 기준금리: {base_rate}%, 환율방향: {fx_direction}")

    return {
        "regime": regime,
        "description": desc,
        "vix": round(vix, 2),
        "us10y": round(us10y, 2),
        "usd_krw": round(usd_krw, 1),
        "base_rate": base_rate,
        "fx_direction": fx_direction,
        "score_adjustment": score_adj,
    }


ATTENTION_HISTORY_PATH = os.path.join(os.path.dirname(__file__), "docs", "attention_history.json")
PERFORMANCE_PATH = os.path.join(os.path.dirname(__file__), "docs", "performance.json")


def load_attention_history():
    """히스토리 파일 로드 (없으면 빈 dict)"""
    try:
        with open(ATTENTION_HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_attention_history(history):
    """히스토리 파일 저장 (최근 20거래일만 유지)"""
    for code in history:
        history[code] = history[code][-20:]
    with open(ATTENTION_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False)


def get_naver_attention(code):
    """네이버 종목토론방 page=1 스냅샷으로 관심도(posts_per_hour) 측정"""
    try:
        url = f"https://finance.naver.com/item/board.naver?code={code}&page=1"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if resp.status_code != 200:
            return 0
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")
        dates = []
        for span in soup.select("span.tah.p10.gray03"):
            text = span.get_text(strip=True)
            try:
                dt = datetime.strptime(text, "%Y.%m.%d %H:%M")
                dates.append(dt)
            except ValueError:
                continue
        if len(dates) < 2:
            return 0
        newest = max(dates)
        oldest = min(dates)
        hours_diff = max((newest - oldest).total_seconds() / 3600, 0.01)
        return round(len(dates) / hours_diff, 1)
    except Exception:
        return 0


def calc_attention_surge(code, current_rate, history):
    """히스토리 대비 급증 비율 계산. 5거래일 평균 대비 배수 반환"""
    records = history.get(code, [])
    if len(records) < 3:
        return 0, 0  # 히스토리 부족 → 판단 보류
    avg = sum(r["rate"] for r in records) / len(records)
    if avg < 0.01:
        return 0, 0
    ratio = round(current_rate / avg, 1)
    return ratio, round(avg, 1)


NEGATIVE_KEYWORDS = ["유상증자", "전환사채", "신주인수권", "감자", "상장폐지", "관리종목", "횡령", "배임", "분식"]


def check_negative_disclosure(code):
    """네이버 금융 공시에서 최근 네거티브 키워드 체크"""
    try:
        from bs4 import BeautifulSoup
        url = f"https://finance.naver.com/item/news.naver?code={code}"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        soup = BeautifulSoup(resp.text, "html.parser")
        titles = [a.get_text(strip=True) for a in soup.select("td.title a")][:10]
        for title in titles:
            for kw in NEGATIVE_KEYWORDS:
                if kw in title:
                    return True, title
    except Exception:
        pass
    return False, ""


def get_fundamental_data(code):
    """네이버 증권 API에서 PER/PBR/외국인/배당 등 수집"""
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/integration"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        result = {}
        for info in data.get("totalInfos", []):
            key = info.get("key", "")
            val = info.get("value", "")
            if key == "PER":
                result["per"] = float(val.replace("배", "").replace(",", "").strip()) if val else None
            elif key == "PBR":
                result["pbr"] = float(val.replace("배", "").replace(",", "").strip()) if val else None
            elif key == "외인소진율":
                result["foreign_ratio"] = float(val.replace("%", "").replace(",", "").strip()) if val else None
            elif key == "배당수익률":
                result["dividend_yield"] = float(val.replace("%", "").replace(",", "").strip()) if val else None
            elif key == "EPS":
                result["eps"] = float(val.replace("원", "").replace(",", "").strip()) if val else None
            elif key == "ROE":
                result["roe"] = float(val.replace("%", "").replace(",", "").strip()) if val else None
        return result
    except Exception:
        return {}
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "signals.json")
DM_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "dual_momentum_portfolio.json")
MIN_PRICE = 1000
MIN_VOLUME = 10000
MIN_DAYS = 60


def get_all_tickers():
    tickers = []
    for market in ["KOSPI", "KOSDAQ"]:
        listing = fdr.StockListing(market)
        for _, row in listing.iterrows():
            code = row.get("Code", "")
            name = row.get("Name", "")
            if code and name and len(code) == 6:
                tickers.append({"code": code, "name": name, "market": market})
    print(f"총 {len(tickers)}개 종목 수집")
    return tickers


def get_ohlcv(code, start, end):
    try:
        df = fdr.DataReader(code, start, end)
        if df is None or df.empty:
            return None
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        df = df[["open", "high", "low", "close", "volume"]]
        df = df[df["volume"] > 0]
        return df
    except Exception:
        return None


def calc_indicators(df):
    if df is None or len(df) < MIN_DAYS:
        return None

    # 이동평균
    df["ma5"] = ta.sma(df["close"], length=5)
    df["ma10"] = ta.sma(df["close"], length=10)
    df["ma20"] = ta.sma(df["close"], length=20)
    df["ma50"] = ta.sma(df["close"], length=50)
    df["ma60"] = ta.sma(df["close"], length=60)
    df["ma120"] = ta.sma(df["close"], length=120)
    df["ma150"] = ta.sma(df["close"], length=150)
    df["ma200"] = ta.sma(df["close"], length=200)

    # RSI
    df["rsi14"] = ta.rsi(df["close"], length=14)
    df["rsi2"] = ta.rsi(df["close"], length=2)

    # MACD
    macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd_df is not None:
        df["macd_line"] = macd_df.iloc[:, 0]
        df["macd_signal"] = macd_df.iloc[:, 1]
        df["macd_hist"] = macd_df.iloc[:, 2]

    # 볼린저 밴드
    bb = ta.bbands(df["close"], length=20, std=2)
    if bb is not None:
        df["bb_upper"] = bb.iloc[:, 2]
        df["bb_mid"] = bb.iloc[:, 1]
        df["bb_lower"] = bb.iloc[:, 0]

    # ATR (Quantocracy 검증: 손절/목표 산출용)
    df["atr14"] = ta.atr(df["high"], df["low"], df["close"], length=14)

    # ADX (추세 강도)
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=14)
    if adx_df is not None:
        df["adx"] = adx_df.iloc[:, 0]

    # 스토캐스틱
    stoch = ta.stoch(df["high"], df["low"], df["close"], k=14, d=3)
    if stoch is not None:
        df["stoch_k"] = stoch.iloc[:, 0]
        df["stoch_d"] = stoch.iloc[:, 1]

    # OBV
    df["obv"] = ta.obv(df["close"], df["volume"])

    # 거래량 이동평균
    df["vol_ma5"] = ta.sma(df["volume"].astype(float), length=5)
    df["vol_ma20"] = ta.sma(df["volume"].astype(float), length=20)
    df["vol_ma50"] = ta.sma(df["volume"].astype(float), length=50)

    # 모멘텀 (Quantocracy 검증: 12-1개월 모멘텀)
    if len(df) >= 252:
        df["mom_12m"] = df["close"].pct_change(252)
    if len(df) >= 21:
        df["mom_1m"] = df["close"].pct_change(21)

    # === v3 추가 지표 ===

    # Donchian Channel (Quantocracy 7건 — 터틀 트레이딩)
    df["donchian_upper"] = df["high"].rolling(window=20).max()
    df["donchian_lower"] = df["low"].rolling(window=20).min()
    df["donchian55_upper"] = df["high"].rolling(window=55).max()
    df["ema20"] = ta.ema(df["close"], length=20)

    # MDD (Quantocracy 125건 — 리스크 필터)
    rolling_max = df["close"].rolling(window=60, min_periods=1).max()
    df["mdd_60"] = (df["close"] - rolling_max) / rolling_max

    # 52주 최고/최저 (Quantocracy 29건)
    if len(df) >= 252:
        df["high_52w"] = df["high"].rolling(window=252).max()
        df["low_52w"] = df["low"].rolling(window=252).min()

    # 연속 상승/하락 일수 (Quantocracy 7건)
    df["daily_return"] = df["close"].pct_change()
    consecutive = []
    count = 0
    for ret in df["daily_return"]:
        if pd.notna(ret):
            if ret > 0:
                count = count + 1 if count > 0 else 1
            elif ret < 0:
                count = count - 1 if count < 0 else -1
            else:
                count = 0
        consecutive.append(count)
    df["consecutive_days"] = consecutive

    # ROC (Quantocracy 276건)
    df["roc_10"] = ta.roc(df["close"], length=10)

    return df


def score_stock(df, fundamental=None, attention=0):
    """복합 스코어링 (기술 40% + 수급 30% + 펀더멘탈 20% + 매크로 10%)"""
    if df is None or len(df) < MIN_DAYS:
        return 0, []

    last = df.iloc[-1]
    prev = df.iloc[-2]
    score = 0
    reasons = []
    fund = fundamental or {}

    # 1. 추세 (Trend Following — Quantocracy 236회 언급)
    if pd.notna(last.get("ma200")) and last["close"] > last["ma200"]:
        score += 15
        reasons.append("200일선 위 (추세 상승)")
    if pd.notna(last.get("ma20")) and pd.notna(last.get("ma60")):
        if last["ma5"] > last["ma20"] > last["ma60"]:
            score += 10
            reasons.append("이평선 정배열")

    # 2. 모멘텀 (Dual Momentum — Quantocracy 64회)
    if pd.notna(last.get("mom_12m")) and pd.notna(last.get("mom_1m")):
        if last["mom_12m"] > 0 and last["mom_1m"] > 0:
            score += 10
            reasons.append(f"듀얼모멘텀 양호 (12M:{last['mom_12m']:.1%}, 1M:{last['mom_1m']:.1%})")

    # 3. RSI 구간
    if pd.notna(last.get("rsi14")):
        if 40 <= last["rsi14"] <= 60:
            score += 5
            reasons.append(f"RSI 중립구간 ({last['rsi14']:.0f})")
        elif 30 <= last["rsi14"] < 40:
            score += 8
            reasons.append(f"RSI 과매도 근접 ({last['rsi14']:.0f})")

    # 4. MACD 골든크로스
    if pd.notna(last.get("macd_hist")) and pd.notna(prev.get("macd_hist")):
        if prev["macd_hist"] < 0 and last["macd_hist"] > 0:
            score += 12
            reasons.append("MACD 골든크로스")
        elif last["macd_hist"] > prev["macd_hist"] and last["macd_hist"] > 0:
            score += 5
            reasons.append("MACD 히스토그램 확대")

    # 5. 거래량 확인 (Quantocracy 76회)
    if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0:
        vol_ratio = last["volume"] / last["vol_ma20"]
        if vol_ratio > 2.0:
            score += 10
            reasons.append(f"거래량 급증 ({vol_ratio:.0f}배)")
        elif vol_ratio > 1.5:
            score += 5
            reasons.append(f"거래량 증가 ({vol_ratio:.1f}배)")

    # 6. ADX 추세 강도
    if pd.notna(last.get("adx")):
        if last["adx"] > 25:
            score += 8
            reasons.append(f"추세 강함 (ADX:{last['adx']:.0f})")

    # 7. 볼린저밴드
    if pd.notna(last.get("bb_lower")) and pd.notna(last.get("bb_upper")):
        bb_width = (last["bb_upper"] - last["bb_lower"]) / last["bb_mid"] if last["bb_mid"] > 0 else 0
        if bb_width < 0.05:
            score += 7
            reasons.append("볼린저 스퀴즈 (변동성 축소 → 이탈 임박)")

    # 8. 스토캐스틱
    if pd.notna(last.get("stoch_k")) and pd.notna(prev.get("stoch_k")):
        if prev["stoch_k"] < 20 and last["stoch_k"] > prev["stoch_k"]:
            score += 8
            reasons.append(f"스토캐스틱 과매도 반등 (%K:{last['stoch_k']:.0f})")

    # === v3 추가 팩터 ===

    # 9. MDD 리스크 필터 (Quantocracy 125건)
    if pd.notna(last.get("mdd_60")):
        mdd = abs(last["mdd_60"])
        if mdd > 0.30:
            score -= 10
            reasons.append(f"⚠ 60일 MDD {mdd:.0%} (고위험)")
        elif mdd < 0.10:
            score += 5
            reasons.append(f"MDD 양호 ({mdd:.0%})")

    # 10. 연속 패턴 (Quantocracy 7건)
    consec = last.get("consecutive_days", 0)
    if isinstance(consec, (int, float)) and pd.notna(consec):
        if consec <= -3:
            score += 6
            reasons.append(f"3일+ 연속 하락 후 반등 기대")
        elif consec >= 5:
            score -= 5
            reasons.append(f"⚠ {int(consec)}일 연속 상승 (과열)")

    # 11. 52주 신고가 근접 (Quantocracy 29건)
    if pd.notna(last.get("high_52w")):
        dist_high = (last["high_52w"] - last["close"]) / last["high_52w"]
        if dist_high < 0.03:
            score += 7
            reasons.append("52주 신고가 근접 (3% 이내)")

    # 12. ROC 모멘텀 (Quantocracy 276건)
    if pd.notna(last.get("roc_10")):
        if 2 < last["roc_10"] < 15:
            score += 5
            reasons.append(f"ROC 양호 ({last['roc_10']:.1f}%)")

    # 13. 거래량 급감 (경고)
    if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0:
        vol_ratio = last["volume"] / last["vol_ma20"]
        if vol_ratio < 0.5:
            score -= 5
            reasons.append(f"⚠ 거래량 급감 ({vol_ratio:.0%})")

    # === 펀더멘탈 팩터 (레이어 2) ===

    # 14. PER 적정 구간 (밸류)
    per = fund.get("per")
    if per is not None and per > 0:
        if per < 15:
            score += 8
            reasons.append(f"PER 저평가 ({per:.1f}배)")
        elif per > 50:
            score -= 5
            reasons.append(f"⚠ PER 고평가 ({per:.0f}배)")

    # 15. PBR 저평가
    pbr = fund.get("pbr")
    if pbr is not None:
        if 0 < pbr < 1.0:
            score += 6
            reasons.append(f"PBR 자산가치 이하 ({pbr:.2f}배)")

    # 16. 외국인 보유비율
    foreign = fund.get("foreign_ratio")
    if foreign is not None:
        if foreign > 20:
            score += 5
            reasons.append(f"외국인 보유 {foreign:.1f}%")

    # 17. 배당수익률
    div_yield = fund.get("dividend_yield")
    if div_yield is not None and div_yield > 2.0:
        score += 4
        reasons.append(f"배당 {div_yield:.1f}%")

    # 18. EPS 양수 (흑자 필터)
    eps = fund.get("eps")
    if eps is not None:
        if eps > 0:
            score += 3
            reasons.append("EPS 흑자")
        else:
            score -= 8
            reasons.append("⚠ EPS 적자")

    # 19. 관심도 + 시그널 동시 발생 가산
    if isinstance(attention, tuple):
        surge_ratio, avg_rate = attention
        if surge_ratio >= 3:
            score += 5
            reasons.append(f"관심급증 {surge_ratio}배 + 시그널 동시")

    # 20. 네거티브 공시 감점
    neg_flag = fund.get("_negative_disclosure")
    if neg_flag:
        score -= 15
        reasons.append(f"⚠ 네거티브 공시: {fund.get('_negative_title', '')[:20]}")

    return score, reasons


def calc_atr_targets(df):
    """ATR 기반 진입/손절/목표 (ADX 강도에 따라 배수 가변)"""
    last = df.iloc[-1]
    atr = last.get("atr14", 0)
    adx = last.get("adx", 0)
    close = last["close"]

    if not pd.notna(atr) or atr == 0:
        return int(close), int(close * 0.95), int(close * 1.10)

    # ADX 기반 배수 가변: 강한 추세일수록 넓게
    if pd.notna(adx) and adx > 40:
        sl_mult, tp_mult = 1.5, 3.5  # 강한 추세: 좁은 손절, 넓은 목표 (1:2.3)
    elif pd.notna(adx) and adx > 25:
        sl_mult, tp_mult = 2.0, 3.0  # 보통 추세 (1:1.5)
    else:
        sl_mult, tp_mult = 2.5, 2.5  # 횡보: 넓은 손절, 좁은 목표 (1:1.0)

    entry = int(close)
    stop_loss = int(close - sl_mult * atr)
    target = int(close + tp_mult * atr)

    return entry, stop_loss, target


def calc_hurst_exponent(prices, min_chunk=8):
    """R/S 분석으로 Hurst Exponent 계산. H<0.5 mean-reverting, H>0.5 trending."""
    log_returns = np.diff(np.log(prices))
    n = len(log_returns)
    if n < min_chunk * 2:
        return None
    chunk_sizes = [s for s in [n // 2, n // 4, n // 8, n // 16] if s >= min_chunk]
    if len(chunk_sizes) < 2:
        return None
    rs_means = []
    for size in chunk_sizes:
        rs_list = []
        for start in range(0, n - size + 1, size):
            chunk = log_returns[start:start + size]
            mean_c = chunk.mean()
            deviate = np.cumsum(chunk - mean_c)
            R = deviate.max() - deviate.min()
            S = chunk.std(ddof=1)
            if S > 0:
                rs_list.append(R / S)
        if rs_list:
            rs_means.append((size, np.mean(rs_list)))
    if len(rs_means) < 2:
        return None
    log_sizes = np.log([x[0] for x in rs_means])
    log_rs = np.log([x[1] for x in rs_means])
    H = np.polyfit(log_sizes, log_rs, 1)[0]
    return H


def find_swing_points(df, max_window=10):
    """스윙 하이/로우 탐지 (강도 1~4)"""
    highs, lows = [], []
    n = len(df)
    strength_windows = [3, 5, 10, 20]
    for i in range(max_window, n - max_window):
        for si, w in enumerate(strength_windows, 1):
            if i - w < 0 or i + w >= n:
                break
            h_slice = df["high"].iloc[i - w:i + w + 1]
            if df["high"].iloc[i] == h_slice.max():
                highs.append((i, df["high"].iloc[i], si))
            else:
                break
        for si, w in enumerate(strength_windows, 1):
            if i - w < 0 or i + w >= n:
                break
            l_slice = df["low"].iloc[i - w:i + w + 1]
            if df["low"].iloc[i] == l_slice.min():
                lows.append((i, df["low"].iloc[i], si))
            else:
                break
    # 중복 제거: 같은 인덱스에 여러 강도면 최고 강도만
    h_dict, l_dict = {}, {}
    for idx, price, strength in highs:
        if idx not in h_dict or strength > h_dict[idx][1]:
            h_dict[idx] = (price, strength)
    for idx, price, strength in lows:
        if idx not in l_dict or strength > l_dict[idx][1]:
            l_dict[idx] = (price, strength)
    swing_highs = [(idx, p, s) for idx, (p, s) in sorted(h_dict.items()) if s >= 2]
    swing_lows = [(idx, p, s) for idx, (p, s) in sorted(l_dict.items()) if s >= 2]
    return swing_highs, swing_lows


def fit_downtrend_line(df, swing_highs, lookback):
    """하락 추세선 피팅 (극값 연결 방식, 최소 3접점)"""
    n = len(df)
    start_idx = n - lookback
    candidates = [(i, p, s) for i, p, s in swing_highs if i >= start_idx and i < n - 1]
    if len(candidates) < 3:
        return None
    # 하락 추세 고점들 찾기: 시간순으로 하락하는 고점 시퀀스
    desc_pivots = [candidates[0]]
    for c in candidates[1:]:
        if c[1] < desc_pivots[-1][1]:
            desc_pivots.append(c)
    if len(desc_pivots) < 3:
        return None
    # 극값 연결: 첫 고점 ~ 마지막 고점
    i0, p0 = desc_pivots[0][0], desc_pivots[0][1]
    i_last, p_last = desc_pivots[-1][0], desc_pivots[-1][1]
    span = i_last - i0
    if span < 15:
        return None
    slope = (p_last - p0) / span  # 가격/일 단위
    slope_pct = slope / p0  # %/일 단위
    if not (-0.005 <= slope_pct <= -0.0005):
        return None
    # 접점 검증: 추세선 ±1% 이내에 위치한 피봇 수
    def trendline_at(idx):
        return p0 + slope * (idx - i0)
    touches = []
    for idx, price, strength in candidates:
        tv = trendline_at(idx)
        if tv > 0 and abs(price - tv) / tv <= 0.01:
            touches.append((idx, price, strength))
    if len(touches) < 3:
        return None
    # 미터치 조건: 추세선 생성 기간 중 종가가 추세선 위로 돌파한 적 없어야 함
    for k in range(i0, i_last + 1):
        tv = trendline_at(k)
        if df["close"].iloc[k] > tv * 1.015:
            return None
    return {
        "start_idx": i0, "start_price": p0,
        "end_idx": i_last, "end_price": p_last,
        "slope": slope, "slope_pct": slope_pct,
        "touches": touches, "span": span,
        "trendline_at": trendline_at,
    }


def find_triangles(df, swing_highs, swing_lows, lookback):
    """삼각수렴 탐지 (상승삼각형 / 대칭삼각형)"""
    n = len(df)
    start_idx = n - lookback
    highs = [(i, p, s) for i, p, s in swing_highs if i >= start_idx and i < n - 1]
    lows = [(i, p, s) for i, p, s in swing_lows if i >= start_idx and i < n - 1]
    if len(highs) < 2 or len(lows) < 2:
        return None
    first_h_idx = highs[0][0]
    first_l_idx = lows[0][0]
    pattern_start = min(first_h_idx, first_l_idx)
    duration = n - 1 - pattern_start
    if duration < 20 or duration > 120:
        return None
    # 상단선/하단선 기울기 (선형 회귀)
    h_indices = np.array([h[0] for h in highs], dtype=float)
    h_prices = np.array([h[1] for h in highs], dtype=float)
    l_indices = np.array([l[0] for l in lows], dtype=float)
    l_prices = np.array([l[1] for l in lows], dtype=float)
    if len(h_indices) < 2 or len(l_indices) < 2:
        return None
    h_slope = np.polyfit(h_indices, h_prices, 1)[0]
    l_slope = np.polyfit(l_indices, l_prices, 1)[0]
    # 수렴 확인: 상단 하락 or 수평, 하단 상승
    converging = h_slope <= l_slope
    if not converging:
        return None
    # 패턴 분류
    h_slope_pct = h_slope / h_prices[0] if h_prices[0] > 0 else 0
    l_slope_pct = l_slope / l_prices[0] if l_prices[0] > 0 else 0
    # 상승 삼각형: 상단 수평(-0.05%~+0.05%/일), 하단 상승
    if abs(h_slope_pct) < 0.0005 and l_slope_pct > 0.0002:
        pattern_type = "ascending_triangle"
    # 대칭 삼각형: 상단 하락, 하단 상승
    elif h_slope_pct < -0.0002 and l_slope_pct > 0.0002:
        pattern_type = "symmetrical_triangle"
    else:
        return None
    # 상단선 오늘 값 계산
    resistance_today = h_prices[0] + h_slope * (n - 1 - h_indices[0])
    if resistance_today <= 0:
        return None
    return {
        "type": pattern_type,
        "start_idx": pattern_start,
        "duration": duration,
        "resistance_today": resistance_today,
        "h_slope": h_slope, "l_slope": l_slope,
        "h_count": len(highs), "l_count": len(lows),
        "h_slope_pct": h_slope_pct, "l_slope_pct": l_slope_pct,
    }


def check_signals(df, score, reasons):
    """시그널 카테고리 분류"""
    if df is None or len(df) < 25:
        return []
    last = df.iloc[-1]
    prev = df.iloc[-2]
    signals = []

    # 골든크로스 (MID: MA20×MA60 품질 필터)
    if len(df) >= 62:
        # 1. 크로스 감지 (최근 5일 이내)
        cross_idx = None
        for i in range(-5, 0):
            p, c = df.iloc[i - 1], df.iloc[i]
            if (pd.notna(p.get("ma20")) and pd.notna(p.get("ma60")) and
                    pd.notna(c.get("ma20")) and pd.notna(c.get("ma60"))):
                if p["ma20"] <= p["ma60"] and c["ma20"] > c["ma60"]:
                    cross_idx = i
                    break

        if cross_idx is not None:
            # 2. 크로스 품질 체크
            ma20_now = last.get("ma20")
            ma20_20ago = df["ma20"].iloc[-21] if len(df) >= 21 else None
            slope = ((ma20_now - ma20_20ago) / ma20_20ago) if (pd.notna(ma20_now) and pd.notna(ma20_20ago) and ma20_20ago > 0) else 0
            slope_ok = slope >= 0.010

            cross_row = df.iloc[cross_idx]
            gap_pct = (cross_row["ma20"] - cross_row["ma60"]) / cross_row["ma60"] if cross_row.get("ma60", 0) > 0 else 0
            gap_ok = gap_pct >= 0.003

            # 3. 데드크로스 기간 (크로스 전 최소 20일)
            dead_days = 0
            start_j = max(0, len(df) + cross_idx - 1 - 60)
            for j in range(len(df) + cross_idx - 2, start_j - 1, -1):
                row_j = df.iloc[j]
                if pd.notna(row_j.get("ma20")) and pd.notna(row_j.get("ma60")) and row_j["ma20"] < row_j["ma60"]:
                    dead_days += 1
                else:
                    break
            pre_condition_ok = dead_days >= 10

            # 4. 최근 60일 이내 골든크로스 반복 배제
            repeat = False
            for j in range(len(df) + cross_idx - 2, max(0, len(df) - 61) - 1, -1):
                if j < 1:
                    break
                p2, c2 = df.iloc[j - 1], df.iloc[j]
                if (pd.notna(p2.get("ma20")) and pd.notna(p2.get("ma60")) and
                        pd.notna(c2.get("ma20")) and pd.notna(c2.get("ma60"))):
                    if p2["ma20"] <= p2["ma60"] and c2["ma20"] > c2["ma60"]:
                        repeat = True
                        break

            # 5. MA60 방향 체크 (하락 중이면 배제)
            ma60_now = last.get("ma60")
            ma60_20ago = df["ma60"].iloc[-21] if len(df) >= 21 else None
            ma60_declining = (pd.notna(ma60_now) and pd.notna(ma60_20ago) and ma60_now < ma60_20ago)

            # 6. 거래량 조건
            avg_vol_50 = last.get("vol_ma50", 0) or 0
            rvol = last["volume"] / avg_vol_50 if avg_vol_50 > 0 else 0
            volume_ok = rvol >= 1.2

            cross_abs = len(df) + cross_idx
            vol_around = df["volume"].iloc[max(0, cross_abs - 2):min(len(df), cross_abs + 3)].mean()
            vol_accumulation = (vol_around > avg_vol_50) if avg_vol_50 > 0 else False

            # 7. 추세 컨텍스트
            ma200 = last.get("ma200")
            above_ma200 = pd.notna(ma200) and last["close"] > ma200
            high_52w = last.get("high_52w")
            low_52w = last.get("low_52w")
            pos_52w = 0.0
            if pd.notna(high_52w) and pd.notna(low_52w) and high_52w > low_52w:
                pos_52w = (last["close"] - low_52w) / (high_52w - low_52w)
            position_ok = pos_52w >= 0.25

            # 8. 배제 조건
            upper_limit_gc = (last["close"] / prev["close"] - 1 > 0.295) if prev["close"] > 0 else False
            bearish_gc = last["close"] < last["open"]
            close_10ago = df["close"].iloc[-11] if len(df) >= 11 else None
            recent_surge = (pd.notna(close_10ago) and close_10ago > 0 and (last["close"] - close_10ago) / close_10ago > 0.15)

            all_pass = (slope_ok and gap_ok and pre_condition_ok and not repeat
                        and not ma60_declining and volume_ok
                        and not upper_limit_gc and not bearish_gc and not recent_surge)

            if all_pass:
                # 스코어링 (0~100점, 60점 이상 발동)
                s_slope = min(slope / 0.08, 1.0) * 15
                s_rvol = min(rvol / 4.0, 1.0) * 15
                s_dead = min(dead_days / 40, 1.0) * 15
                s_gap = min(gap_pct / 0.02, 1.0) * 10
                s_vol_acc = 10 if vol_accumulation else 0
                s_ma200 = 10 if above_ma200 else 0
                s_pos52 = min(pos_52w / 0.7, 1.0) * 15
                # 변동성 수축: 최근 10일 ATR < 60일 ATR * 0.8
                atr_now = df["atr14"].iloc[-10:].mean() if pd.notna(last.get("atr14")) else None
                atr_60 = df["atr14"].iloc[-60:].mean() if len(df) >= 60 else None
                s_atr = 10 if (atr_now is not None and atr_60 is not None and atr_60 > 0 and atr_now < atr_60 * 0.8) else 0
                gc_score = int(s_slope + s_rvol + s_dead + s_gap + s_vol_acc + s_ma200 + s_pos52 + s_atr)
                if gc_score >= 55:
                    ma20_v = int(last["ma20"]) if pd.notna(last.get("ma20")) else "?"
                    ma60_v = int(last["ma60"]) if pd.notna(last.get("ma60")) else "?"
                    signals.append(("golden_cross", f"MA20({ma20_v})×MA60({ma60_v}), RVOL {rvol:.1f}배, 강도 {gc_score}점"))

    # 추세돌파 (하락추세선 + 삼각수렴 품질 필터)
    if len(df) >= 62:
        swing_highs, swing_lows = find_swing_points(df)
        avg_vol_50_bo = last.get("vol_ma50", 0) or 0
        rvol_bo = last["volume"] / avg_vol_50_bo if avg_vol_50_bo > 0 else 0
        trading_val = last["close"] * last["volume"]

        # 공통 배제 조건
        upper_limit_bo = (last["close"] / prev["close"] - 1 > 0.295) if prev["close"] > 0 else False
        bearish_bo = last["close"] < last["open"]
        candle_range_bo = last["high"] - last["low"]
        body_bo = abs(last["close"] - last["open"])
        doji_bo = (body_bo / candle_range_bo < 0.3) if candle_range_bo > 0 else True
        gap_pct_bo = (last["open"] - prev["close"]) / prev["close"] if prev["close"] > 0 else 0
        intraday_drop_bo = (last["close"] - last["open"]) / last["open"] < -0.025 if last["open"] > 0 else False
        high_52w_bo = last.get("high_52w")
        low_52w_bo = last.get("low_52w")
        pos_52w_bo = 0.0
        if pd.notna(high_52w_bo) and pd.notna(low_52w_bo) and high_52w_bo > low_52w_bo:
            pos_52w_bo = (last["close"] - low_52w_bo) / (high_52w_bo - low_52w_bo)
        # 52주 저점 대비 15% 이상, 52주 고점 2% 이내면 전고점돌파로 분류
        pos_ok_bo = (pd.notna(low_52w_bo) and low_52w_bo > 0 and last["close"] >= low_52w_bo * 1.15)
        not_near_ath = not (pd.notna(high_52w_bo) and high_52w_bo > 0 and last["close"] > high_52w_bo * 1.02)

        basic_exclude = (upper_limit_bo or bearish_bo or doji_bo or gap_pct_bo > 0.07
                         or intraday_drop_bo or not pos_ok_bo or not not_near_ath
                         or rvol_bo < 1.5 or trading_val < 5_000_000_000)

        if not basic_exclude:
            best_breakout = None
            best_score_bo = 0

            # --- A. 하락 추세선 돌파 ---
            for lb in [60, 120, 250]:
                if len(df) < lb:
                    continue
                tl = fit_downtrend_line(df, swing_highs, lb)
                if tl is None:
                    continue
                tv_today = tl["trendline_at"](len(df) - 1)
                tv_yesterday = tl["trendline_at"](len(df) - 2)
                if tv_today <= 0:
                    continue
                # 돌파 조건: 종가 > 추세선 +1.5%, 전일 종가 < 추세선
                breakout_ok = last["close"] > tv_today * 1.015
                prev_below = prev["close"] < tv_yesterday
                if not (breakout_ok and prev_below):
                    continue
                # 10일 내 동일 추세선 돌파 배제
                recent_break = False
                for k in range(max(0, len(df) - 11), len(df) - 2):
                    if df["close"].iloc[k] > tl["trendline_at"](k) * 1.015:
                        recent_break = True
                        break
                if recent_break:
                    continue
                # 거래량 수축 확인 (후반부 10일 vs 전체 기간 평균)
                pattern_start = tl["start_idx"]
                pattern_vol = df["volume"].iloc[pattern_start:len(df) - 1]
                vol_contraction = 1.0
                if len(pattern_vol) >= 15:
                    vol_tail = pattern_vol.iloc[-10:].mean()
                    vol_all = pattern_vol.mean()
                    vol_contraction = vol_tail / vol_all if vol_all > 0 else 1.0
                # 스코어링
                touch_score = min(len(tl["touches"]) / 5, 1.0) * 10
                touch_strength = sum(s for _, _, s in tl["touches"]) / len(tl["touches"])
                strength_score = min(touch_strength / 3.0, 1.0) * 5
                span_score = min(tl["span"] / 120, 1.0) * 5
                s_tl_quality = touch_score + strength_score + span_score  # /20
                bo_pct = last["close"] / tv_today - 1.015
                s_bo_pct = min(max(bo_pct, 0) / 0.05, 1.0) * 15
                s_rvol_bo = min(rvol_bo / 3.0, 1.0) * 15
                s_vol_contr = 15 if vol_contraction < 0.8 else (7 if vol_contraction < 1.0 else 0)
                tf_label = "단기" if lb <= 60 else ("중기" if lb <= 120 else "장기")
                s_pattern_dur = min(tl["span"] / 120, 1.0) * 10
                s_pattern_type = 8  # 하락추세선 = 0.8 * 10
                ma200_bo = last.get("ma200")
                above_ma200_bo = pd.notna(ma200_bo) and last["close"] > ma200_bo
                ma50_bo = last.get("ma50")
                s_ma200_bo = 10 if (above_ma200_bo and pd.notna(ma50_bo) and ma50_bo > ma200_bo) else (5 if above_ma200_bo else 0)
                s_market = 5  # 시장 추세는 별도 데이터 필요, 기본 중립
                bo_score = int(s_tl_quality + s_bo_pct + s_rvol_bo + s_vol_contr + s_pattern_dur + s_pattern_type + s_ma200_bo + s_market)
                if bo_score >= 60 and bo_score > best_score_bo:
                    early_rev = not above_ma200_bo
                    flag_str = " [추세전환초기]" if early_rev else ""
                    best_score_bo = bo_score
                    best_breakout = ("breakout", f"하락추세선({tf_label},{tl['span']}일) 돌파 +{(last['close']/tv_today-1)*100:.1f}%, RVOL {rvol_bo:.1f}배, 강도 {bo_score}점{flag_str}")

            # --- B. 삼각수렴 돌파 ---
            for lb in [60, 120]:
                if len(df) < lb:
                    continue
                tri = find_triangles(df, swing_highs, swing_lows, lb)
                if tri is None:
                    continue
                res_today = tri["resistance_today"]
                # 상단선 전일 값 근사
                res_yesterday = res_today - tri["h_slope"]
                if res_today <= 0:
                    continue
                breakout_ok_t = last["close"] > res_today * 1.015
                prev_below_t = prev["close"] < res_yesterday
                if not (breakout_ok_t and prev_below_t):
                    continue
                # 거래량 수축 확인
                pat_start_t = tri["start_idx"]
                pattern_vol_t = df["volume"].iloc[pat_start_t:len(df) - 1]
                vol_contr_t = 1.0
                if len(pattern_vol_t) >= 15:
                    vol_tail_t = pattern_vol_t.iloc[-10:].mean()
                    vol_all_t = pattern_vol_t.mean()
                    vol_contr_t = vol_tail_t / vol_all_t if vol_all_t > 0 else 1.0
                # 스코어링
                total_pivots = tri["h_count"] + tri["l_count"]
                s_tl_q_t = min(total_pivots / 8, 1.0) * 20
                bo_pct_t = last["close"] / res_today - 1.015
                s_bo_t = min(max(bo_pct_t, 0) / 0.05, 1.0) * 15
                s_rvol_t = min(rvol_bo / 3.0, 1.0) * 15
                s_vc_t = 15 if vol_contr_t < 0.8 else (7 if vol_contr_t < 1.0 else 0)
                s_dur_t = min(tri["duration"] / 120, 1.0) * 10
                s_type_t = 10 if tri["type"] == "ascending_triangle" else 9  # 상승 > 대칭
                ma200_t = last.get("ma200")
                above_ma200_t = pd.notna(ma200_t) and last["close"] > ma200_t
                ma50_t = last.get("ma50")
                s_ma200_t = 10 if (above_ma200_t and pd.notna(ma50_t) and ma50_t > ma200_t) else (5 if above_ma200_t else 0)
                s_mkt_t = 5
                tri_score = int(s_tl_q_t + s_bo_t + s_rvol_t + s_vc_t + s_dur_t + s_type_t + s_ma200_t + s_mkt_t)
                type_label = "상승삼각형" if tri["type"] == "ascending_triangle" else "대칭삼각형"
                tf_label_t = "단기" if lb <= 60 else "중기"
                if tri_score >= 60 and tri_score > best_score_bo:
                    best_score_bo = tri_score
                    best_breakout = ("breakout", f"{type_label}({tf_label_t},{tri['duration']}일) 돌파 +{(last['close']/res_today-1)*100:.1f}%, RVOL {rvol_bo:.1f}배, 강도 {tri_score}점")

            if best_breakout:
                signals.append(best_breakout)

    # 눌림목 (SHALLOW/STANDARD/DEEP 품질 필터)
    if len(df) >= 62 and pd.notna(last.get("ma20")) and pd.notna(last.get("ma50")) and pd.notna(last.get("ma200")):
        pb_configs = [
            # (type, lookback, min_up%, min_dd%, max_dd%, min_dur, max_dur, 1st_ma, 2nd_ma, tol%, min_score)
            ("SHALLOW", 30, 0.07, 0.03, 0.10, 3, 10, "ma10", "ma20", 0.02, 55),
            ("STANDARD", 60, 0.12, 0.05, 0.15, 5, 20, "ma20", "ma50", 0.025, 60),
            ("DEEP", 120, 0.20, 0.10, 0.25, 10, 40, "ma50", "ma200", 0.03, 65),
        ]
        # T0 공통 배제
        pb_upper_limit = (last["close"] / prev["close"] - 1 > 0.295) if prev["close"] > 0 else False
        pb_bearish = last["close"] < last["open"]
        pb_low52 = last.get("low_52w")
        pb_near_52low = pd.notna(pb_low52) and pb_low52 > 0 and last["close"] < pb_low52 * 1.20
        if not pb_upper_limit and not pb_bearish and not pb_near_52low:
            best_pb = None
            best_pb_score = 0
            for pb_type, lb, min_up, min_dd, max_dd, min_dur, max_dur, ma1_key, ma2_key, tol, min_sc in pb_configs:
                if len(df) < lb + 20:
                    continue
                # --- 선행 상승 추세 ---
                # 직전 lb일 내 최고점/최저점
                window = df.iloc[-(lb + 1):-1]
                if len(window) < lb:
                    continue
                recent_high = window["close"].max()
                rh_pos = window["close"].idxmax()
                rh_iloc = df.index.get_loc(rh_pos) if rh_pos in df.index else None
                if rh_iloc is None:
                    continue
                # rh_iloc 이전 구간에서 최저점 찾기
                pre_rh = df.iloc[max(0, rh_iloc - lb):rh_iloc + 1]
                if len(pre_rh) < 5:
                    continue
                prior_low = pre_rh["close"].min()
                if prior_low <= 0:
                    continue
                prior_up_pct = (recent_high - prior_low) / prior_low
                if prior_up_pct < min_up:
                    continue
                # 정배열 확인 (고점 시점 직전 5일 평균)
                align_start = max(0, rh_iloc - 5)
                align_slice = df.iloc[align_start:rh_iloc + 1]
                ma20_avg = align_slice["ma20"].mean() if "ma20" in align_slice else None
                ma50_avg = align_slice["ma50"].mean() if "ma50" in align_slice else None
                ma200_avg = align_slice["ma200"].mean() if "ma200" in align_slice else None
                if not (pd.notna(ma20_avg) and pd.notna(ma50_avg) and pd.notna(ma200_avg)):
                    continue
                if not (ma20_avg > ma50_avg > ma200_avg):
                    continue
                # MA200 상승 중
                if len(df) >= 26:
                    ma200_recent = df["ma200"].iloc[-6] if pd.notna(df["ma200"].iloc[-6]) else None
                    ma200_20ago = df["ma200"].iloc[-26] if pd.notna(df["ma200"].iloc[-26]) else None
                    if not (ma200_recent is not None and ma200_20ago is not None and ma200_recent > ma200_20ago):
                        continue
                # 52주 고점 90% 이상
                high_52w_pb = last.get("high_52w")
                if pd.notna(high_52w_pb) and high_52w_pb > 0 and recent_high < high_52w_pb * 0.90:
                    continue
                # --- 조정 구간 분석 ---
                pullback_start = rh_iloc + 1
                pullback_end = len(df) - 1  # T0 포함
                if pullback_end <= pullback_start:
                    continue
                pb_slice = df.iloc[pullback_start:pullback_end + 1]
                pb_dur = len(pb_slice)
                if pb_dur < min_dur or pb_dur > max_dur:
                    continue
                pb_low = pb_slice["low"].min()
                drawdown = (recent_high - pb_low) / recent_high
                if drawdown < min_dd or drawdown > max_dd:
                    continue
                # 건강한 조정 체크
                down_candles = (pb_slice["close"] < pb_slice["open"]).sum()
                down_ratio = down_candles / pb_dur
                if down_ratio > 0.70:
                    continue
                # 연속 음봉 5일 이상 배제
                max_consec = 0
                cur_consec = 0
                for _, r in pb_slice.iterrows():
                    if r["close"] < r["open"]:
                        cur_consec += 1
                        max_consec = max(max_consec, cur_consec)
                    else:
                        cur_consec = 0
                if max_consec >= 5:
                    continue
                # 단일 -7% 급락 배제
                pb_daily_ret = pb_slice["close"].pct_change()
                if (pb_daily_ret < -0.07).any():
                    continue
                # 장중 낙폭 반등 비율
                pb_ranges = pb_slice["high"] - pb_slice["low"]
                pb_close_pos = (pb_slice["close"] - pb_slice["low"]) / pb_ranges.replace(0, np.nan)
                avg_close_pos = pb_close_pos.mean() if pb_close_pos.notna().any() else 0
                if avg_close_pos < 0.4:
                    continue
                # MA200 이탈 배제
                if pd.notna(pb_slice.get("ma200")).all():
                    ma200_breaks = (pb_slice["close"] < pb_slice["ma200"]).sum()
                    if ma200_breaks > 0:
                        continue
                # 거래량 수축 확인
                avg_vol_50_pb = last.get("vol_ma50", 0) or 0
                pb_avg_vol = pb_slice["volume"].mean()
                vol_dry_ratio = pb_avg_vol / avg_vol_50_pb if avg_vol_50_pb > 0 else 1.0
                # 조정기 거래량 증가 배제 (분배 매도)
                if vol_dry_ratio > 1.15:
                    continue
                # --- 지지 영역 ---
                ma1_val = last.get(ma1_key)
                ma2_val = last.get(ma2_key)
                support_type = None
                support_val = None
                if pd.notna(ma1_val) and ma1_val > 0 and abs(pb_low - ma1_val) / ma1_val <= tol:
                    support_type = ma1_key.upper()
                    support_val = ma1_val
                elif pd.notna(ma2_val) and ma2_val > 0 and abs(pb_low - ma2_val) / ma2_val <= tol:
                    support_type = ma2_key.upper()
                    support_val = ma2_val
                if support_type is None:
                    continue
                # 피보나치 되돌림
                fib_ratio = (recent_high - last["close"]) / (recent_high - prior_low) if recent_high > prior_low else 0
                near_fib = None
                if abs(fib_ratio - 0.382) < 0.05:
                    near_fib = "FIB_382"
                elif abs(fib_ratio - 0.500) < 0.05:
                    near_fib = "FIB_500"
                elif abs(fib_ratio - 0.618) < 0.05:
                    near_fib = "FIB_618"
                # --- 반등 확증 (T0) ---
                # 양봉 + 시가 대비 1.5% 이상
                body_pct = (last["close"] - last["open"]) / last["open"] if last["open"] > 0 else 0
                bullish_candle = body_pct >= 0.015
                # 망치형
                t0_range = last["high"] - last["low"]
                t0_body = abs(last["close"] - last["open"])
                lower_shadow = min(last["close"], last["open"]) - last["low"]
                hammer = (t0_range > 0 and lower_shadow / t0_range >= 0.6 and t0_body / t0_range >= 0.3
                          and last["close"] > last["open"])
                # 장악형
                engulfing = (prev["close"] < prev["open"] and last["close"] > last["open"]
                             and last["close"] > prev["open"] and last["open"] < prev["close"])
                # 이평선 재돌파
                ma_reclaim = (pd.notna(support_val) and prev["close"] < support_val and last["close"] > support_val)
                if not (bullish_candle or hammer or engulfing or ma_reclaim):
                    continue
                reversal = "장악형" if engulfing else ("망치형" if hammer else ("이평재돌파" if ma_reclaim else "양봉"))
                # T0 거래량 서지
                t0_vol_vs_pb = last["volume"] / pb_avg_vol if pb_avg_vol > 0 else 0
                if t0_vol_vs_pb < 1.1:
                    continue
                # 종가 지지선 위 + 캔들 상단 마감
                if pd.notna(support_val) and last["close"] < support_val:
                    continue
                candle_mid = last["open"] + (last["high"] - last["low"]) * 0.5
                if last["close"] < candle_mid:
                    continue
                # 5일 내 중복 배제
                # (같은 check_signals 호출에서는 중복 없으므로 생략)
                # --- 스코어링 ---
                # 선행 추세 강도 (20)
                ma200_slope = 0
                if len(df) >= 26 and pd.notna(df["ma200"].iloc[-6]) and pd.notna(df["ma200"].iloc[-26]):
                    ma200_slope = (df["ma200"].iloc[-6] - df["ma200"].iloc[-26]) / df["ma200"].iloc[-26]
                s_trend = min(prior_up_pct / (min_up * 3), 1.0) * 10 + min(ma200_slope / 0.05, 1.0) * 5 + min(pb_dur / max_dur, 1.0) * 5
                # 조정 건강도 (15)
                s_health = (1.0 - down_ratio) * 8 + min(avg_close_pos / 0.7, 1.0) * 7
                # 지지 정확도 (15)
                touch_dist = abs(pb_low - support_val) / support_val if support_val > 0 else 1.0
                s_support = (1.0 - min(touch_dist / tol, 1.0)) * 15 if support_type == ma1_key.upper() else (1.0 - min(touch_dist / tol, 1.0)) * 10.5
                # 거래량 수축 (10)
                s_vol_dry = min((0.85 - vol_dry_ratio) / 0.35, 1.0) * 10 if vol_dry_ratio < 0.85 else 0
                # 반등 캔들 (10)
                s_rev_candle = 10 if engulfing else (7 if bullish_candle else (5 if hammer else 3))
                # 반등 거래량 (10)
                s_rev_vol = min(t0_vol_vs_pb / 2.5, 1.0) * 10
                # 피보나치 (10)
                s_fib = 10 if near_fib == "FIB_382" else (8 if near_fib == "FIB_500" else (6 if near_fib == "FIB_618" else 0))
                # MA200 정렬 (5)
                ma50_v = last.get("ma50")
                s_ma_align = 5 if (pd.notna(last.get("ma200")) and last["close"] > last["ma200"]
                                   and pd.notna(ma50_v) and ma50_v > last["ma200"]) else 0
                # 시장 추세 (5) — 기본 중립
                s_market_pb = 3
                pb_score = int(s_trend + s_health + s_support + s_vol_dry + s_rev_candle + s_rev_vol + s_fib + s_ma_align + s_market_pb)
                if pb_score >= min_sc and pb_score > best_pb_score:
                    best_pb_score = pb_score
                    fib_str = f", {near_fib.replace('_','')}" if near_fib else ""
                    dd_pct = drawdown * 100
                    best_pb = ("pullback", f"{pb_type} {support_type}({int(support_val)}) {reversal}, -{dd_pct:.1f}%→반등, RVOL {t0_vol_vs_pb:.1f}배{fib_str}, 강도 {pb_score}점")
            if best_pb:
                signals.append(best_pb)

    # 과매도 반등 (RSI_CLASSIC / RSI_SHORT / BB_BREAK 품질 필터)
    if len(df) >= 252 and pd.notna(last.get("rsi14")) and pd.notna(last.get("ma200")):
        # === 공통 상위 추세 필터 (최우선) ===
        os_ma200 = last.get("ma200")
        os_above_ma200 = pd.notna(os_ma200) and last["close"] > os_ma200
        os_ma200_flat_or_up = False
        if pd.notna(os_ma200) and len(df) >= 21:
            os_ma200_20ago = df["ma200"].iloc[-21]
            os_ma200_flat_or_up = pd.notna(os_ma200_20ago) and os_ma200 >= os_ma200_20ago
        # 1년 수익률
        os_yearly_ret = (last["close"] - df["close"].iloc[-252]) / df["close"].iloc[-252] if df["close"].iloc[-252] > 0 else -1
        # T0 공통 배제
        os_upper_limit = (last["close"] / prev["close"] - 1 > 0.295) if prev["close"] > 0 else False
        os_bearish_t0 = last["close"] < last["open"]
        os_close_pos = (last["close"] - last["low"]) / (last["high"] - last["low"]) if (last["high"] - last["low"]) > 0 else 0
        # 급락 형태 검증 (최근 10일)
        os_recent10 = df.iloc[-11:-1]  # T0 제외 직전 10일
        os_drop_10d = (last["close"] - os_recent10["close"].iloc[0]) / os_recent10["close"].iloc[0] if os_recent10["close"].iloc[0] > 0 else 0
        os_daily_rets = os_recent10["close"].pct_change().dropna()
        os_max_single_drop = os_daily_rets.min() if len(os_daily_rets) > 0 else 0
        # 연속 하락일
        os_consec_down = 0
        for k in range(len(df) - 2, max(0, len(df) - 12), -1):
            if df["close"].iloc[k] < df["close"].iloc[k - 1]:
                os_consec_down += 1
            else:
                break
        # 갭다운 -5% 배제
        os_gap_down = False
        for k in range(max(0, len(df) - 6), len(df)):
            if k > 0 and df["close"].iloc[k - 1] > 0:
                gap = (df["open"].iloc[k] - df["close"].iloc[k - 1]) / df["close"].iloc[k - 1]
                if gap < -0.05:
                    os_gap_down = True
                    break
        # 패닉 볼륨 배제 (최근 5일 거래량 > 50일평균 5배 + 음봉)
        os_avg_vol50 = last.get("vol_ma50", 0) or 0
        os_panic_vol = False
        for k in range(max(0, len(df) - 6), len(df) - 1):
            if os_avg_vol50 > 0 and df["volume"].iloc[k] > os_avg_vol50 * 5 and df["close"].iloc[k] < df["open"].iloc[k]:
                os_panic_vol = True
                break
        # 기본 배제 통과 확인
        os_basic_ok = (os_above_ma200 and os_yearly_ret >= -0.20
                       and not os_upper_limit and not os_bearish_t0 and os_close_pos >= 0.5
                       and os_max_single_drop > -0.10 and not os_gap_down and not os_panic_vol
                       and -0.25 <= os_drop_10d <= -0.05 and 2 <= os_consec_down <= 7)
        if os_basic_ok:
            best_os = None
            best_os_score = 0
            os_configs = [
                # (type, min_score, holding, target%)
                ("RSI_CLASSIC", 60, 7, 5.0),
                ("RSI_SHORT", 65, 3, 3.0),
                ("BB_BREAK", 60, 5, 4.0),
            ]
            for os_type, os_min_sc, os_hold, os_tgt in os_configs:
                triggered = False
                os_detail = ""
                if os_type == "RSI_CLASSIC":
                    # RSI(14) <= 30 최근 5일 내 + 현재 30~50
                    rsi14_min5 = df["rsi14"].iloc[-6:-1].min() if len(df) >= 6 else 99
                    rsi14_now = last["rsi14"]
                    if pd.notna(rsi14_min5) and rsi14_min5 <= 30 and 30 <= rsi14_now <= 50 and os_ma200_flat_or_up:
                        rsi_change = rsi14_now - prev["rsi14"] if pd.notna(prev.get("rsi14")) else 0
                        if rsi_change >= 2:
                            triggered = True
                            os_detail = f"RSI14 {rsi14_min5:.0f}→{rsi14_now:.0f}"
                elif os_type == "RSI_SHORT":
                    # RSI(2) <= 10 + 종가 < MA5
                    rsi2 = last.get("rsi2")
                    rsi2_prev = prev.get("rsi2") if prev is not None else None
                    ma5_v = last.get("ma5")
                    rsi2_check = pd.notna(rsi2_prev) and rsi2_prev <= 10
                    if not rsi2_check:
                        rsi2_check = pd.notna(rsi2) and rsi2 <= 10
                    if rsi2_check and pd.notna(ma5_v) and prev["close"] < ma5_v:
                        triggered = True
                        os_detail = f"RSI2 {rsi2:.0f}"
                elif os_type == "BB_BREAK":
                    # BB 하단 이탈 후 복귀
                    bb_lower_v = last.get("bb_lower")
                    bb_upper_v = last.get("bb_upper")
                    bb_mid_v = last.get("bb_mid")
                    if pd.notna(bb_lower_v) and pd.notna(bb_upper_v) and pd.notna(bb_mid_v):
                        bb_break_recent = False
                        for k in range(max(0, len(df) - 6), len(df) - 1):
                            if pd.notna(df.get("bb_lower", pd.Series()).iloc[k] if "bb_lower" in df else None):
                                bl = df["bb_lower"].iloc[k]
                                if pd.notna(bl) and df["close"].iloc[k] < bl:
                                    bb_break_recent = True
                                    break
                        bb_recovered = last["close"] > bb_lower_v
                        # BB 폭 확장
                        bb_width = (bb_upper_v - bb_lower_v) / bb_mid_v if bb_mid_v > 0 else 0
                        bb_width_avg = 0
                        if len(df) >= 21:
                            bbu = df["bb_upper"].iloc[-21:-1]
                            bbl = df["bb_lower"].iloc[-21:-1]
                            bbm = df["bb_mid"].iloc[-21:-1]
                            bw_series = (bbu - bbl) / bbm.replace(0, np.nan)
                            bb_width_avg = bw_series.mean() if bw_series.notna().any() else 0
                        bb_expanded = bb_width_avg > 0 and bb_width >= bb_width_avg * 1.2
                        # MA50 > MA200
                        os_ma50 = last.get("ma50")
                        ma_partial = pd.notna(os_ma50) and pd.notna(os_ma200) and os_ma50 > os_ma200
                        if bb_break_recent and bb_recovered and bb_expanded and ma_partial and os_ma200_flat_or_up:
                            triggered = True
                            os_detail = f"BB하단 이탈→복귀, 폭 {bb_width*100:.1f}%"
                if not triggered:
                    continue
                # === 반등 확증 ===
                body_pct_os = (last["close"] - last["open"]) / last["open"] if last["open"] > 0 else 0
                bullish_os = body_pct_os >= 0.015
                t0_range_os = last["high"] - last["low"]
                t0_body_os = abs(last["close"] - last["open"])
                lower_sh_os = min(last["close"], last["open"]) - last["low"]
                hammer_os = (t0_range_os > 0 and lower_sh_os / t0_range_os >= 0.65
                             and t0_body_os / t0_range_os >= 0.25 and last["close"] > last["open"])
                engulfing_os = (prev["close"] < prev["open"] and last["close"] > last["open"]
                                and last["close"] > prev["open"] and last["open"] < prev["close"])
                ma5_reclaim = pd.notna(last.get("ma5")) and prev["close"] < last["ma5"] and last["close"] > last["ma5"]
                if not (bullish_os or hammer_os or engulfing_os or ma5_reclaim):
                    continue
                rev_label = "장악형" if engulfing_os else ("망치형" if hammer_os else ("MA5재돌파" if ma5_reclaim else "양봉"))
                rev_strength = 1.0 if engulfing_os else (0.8 if hammer_os else (0.5 if bullish_os else 0.4))
                # 거래량 패턴: T0 볼륨 적절 범위
                os_t0_rvol = last["volume"] / os_avg_vol50 if os_avg_vol50 > 0 else 0
                if os_t0_rvol < 1.0 or os_t0_rvol > 3.0:
                    continue
                # RSI 상향 전환
                rsi14_now_v = last.get("rsi14", 0) or 0
                rsi14_prev_v = prev.get("rsi14", 0) or 0
                rsi_uptick = rsi14_now_v - rsi14_prev_v if (pd.notna(rsi14_now_v) and pd.notna(rsi14_prev_v)) else 0
                if rsi_uptick <= 0:
                    continue
                # === 스코어링 ===
                # 상위 추세 강도 (20)
                ma200_dist = (last["close"] - os_ma200) / os_ma200 if os_ma200 > 0 else 0
                os_ma200_slope = (os_ma200 - (df["ma200"].iloc[-21] if len(df) >= 21 else os_ma200)) / os_ma200 if os_ma200 > 0 else 0
                s_trend_os = min(ma200_dist / 0.15, 1.0) * 8 + min(os_ma200_slope / 0.03, 1.0) * 6 + min((os_yearly_ret + 0.1) / 0.3, 1.0) * 6
                s_trend_os = max(0, min(s_trend_os, 20))
                # 과매도 깊이 (15)
                if os_type == "RSI_CLASSIC":
                    rsi_min5 = df["rsi14"].iloc[-6:-1].min() if len(df) >= 6 else 30
                    s_depth = min((30 - max(rsi_min5, 15)) / 15, 1.0) * 15
                elif os_type == "RSI_SHORT":
                    rsi2_v = last.get("rsi2", 10) or 10
                    s_depth = min((10 - max(rsi2_v, 0)) / 10, 1.0) * 15
                else:
                    bb_break_depth = (bb_lower_v - pb_low if 'pb_low' in dir() else 0) / bb_lower_v if bb_lower_v > 0 else 0
                    s_depth = min(abs(os_drop_10d) / 0.20, 1.0) * 15
                # 반등 캔들 강도 (15)
                s_rev = rev_strength * 10 + min(body_pct_os / 0.04, 1.0) * 3 + (2 if os_close_pos >= 0.7 else 0)
                s_rev = min(s_rev, 15)
                # 거래량 패턴 (15)
                # 셀링 클라이맥스 확인 (급락 최저점 부근 거래량)
                low_idx = os_recent10["volume"].idxmax()
                climax_vol_ratio = os_recent10["volume"].max() / os_avg_vol50 if os_avg_vol50 > 0 else 0
                climax_ok = 1.5 <= climax_vol_ratio <= 5.0
                t0_vs_climax = last["volume"] / os_recent10["volume"].max() if os_recent10["volume"].max() > 0 else 0
                s_vol_os = (8 if climax_ok else 3) + min(os_t0_rvol / 2.0, 1.0) * 4 + (3 if t0_vs_climax < 0.8 else 0)
                s_vol_os = min(s_vol_os, 15)
                # 급락 건강도 (10)
                s_drop = (5 if os_max_single_drop > -0.07 else 2) + (5 if not os_gap_down else 0)
                s_drop = min(s_drop, 10)
                # RSI 상향 (10)
                s_rsi_up = min(rsi_uptick / 8, 1.0) * 10
                # 시장 추세 (10) — 기본 중립
                s_mkt_os = 5
                # 변동성 환경 (5) — 기본 정상
                s_vola = 3
                os_score = int(s_trend_os + s_depth + s_rev + s_vol_os + s_drop + s_rsi_up + s_mkt_os + s_vola)
                if os_score >= os_min_sc and os_score > best_os_score:
                    best_os_score = os_score
                    drop_pct_str = f"{os_drop_10d*100:.1f}%"
                    best_os = ("oversold", f"{os_type} {os_detail} {rev_label}, 10일 {drop_pct_str}, RVOL {os_t0_rvol:.1f}배, 강도 {os_score}점")
            if best_os:
                signals.append(best_os)

    # 거래량 폭발 (5유형 분류 + 중복 회피)
    if len(df) >= 62 and pd.notna(last.get("vol_ma50")) and last.get("vol_ma50", 0) > 0:
        # 중복 회피: 다른 시그널이 이미 잡은 종목은 양보
        existing_types = {s[0] for s in signals}
        vs_dedup = existing_types & {"new_high", "golden_cross", "breakout"}
        # 52주 신고가 98% 이상도 양보
        vs_high52 = last.get("high_52w")
        vs_near_ath = pd.notna(vs_high52) and vs_high52 > 0 and last["close"] > vs_high52 * 0.98
        if not vs_dedup and not vs_near_ath:
            # --- 거래량 이상치 탐지 (3가지 방법) ---
            vol_60 = df["volume"].iloc[-61:-1]
            vs_mean60 = vol_60.mean()
            vs_median60 = vol_60.median()
            vs_std60 = vol_60.std()
            vs_mad = (vol_60 - vs_median60).abs().median()
            vs_rvol = last["volume"] / vs_mean60 if vs_mean60 > 0 else 0
            vs_zscore = (last["volume"] - vs_mean60) / vs_std60 if vs_std60 > 0 else 0
            vs_robust = (last["volume"] - vs_median60) / (vs_mad * 1.4826) if vs_mad > 0 else 0
            vs_methods = (1 if vs_rvol >= 3.0 else 0) + (1 if vs_zscore >= 2.5 else 0) + (1 if vs_robust >= 4.0 else 0)
            vs_trading_val = last["close"] * last["volume"]
            vs_upper_limit = (last["close"] / prev["close"] - 1 > 0.295) if prev["close"] > 0 else False
            vs_is_60max = last["volume"] >= vol_60.max()
            if vs_methods >= 1 and vs_trading_val >= 5_000_000_000 and not vs_upper_limit:
                # 극단 거래량 경계 (RVOL > 20)
                vs_extreme = vs_rvol > 20
                # 캔들 분석
                vs_range = last["high"] - last["low"]
                vs_body = abs(last["close"] - last["open"])
                vs_close_pos = (last["close"] - last["low"]) / vs_range if vs_range > 0 else 0.5
                vs_body_ratio = vs_body / vs_range if vs_range > 0 else 0
                vs_upper_shadow = (last["high"] - max(last["close"], last["open"])) / vs_range if vs_range > 0 else 0
                vs_bullish = last["close"] > last["open"]
                vs_atr14 = last.get("atr14", 0) or 0
                vs_narrow_candle = vs_atr14 > 0 and vs_range < vs_atr14 * 0.8
                # 추세 컨텍스트
                vs_pct_20d = (last["close"] - df["close"].iloc[-21]) / df["close"].iloc[-21] if df["close"].iloc[-21] > 0 else 0
                vs_pct_60d = (last["close"] - df["close"].iloc[-61]) / df["close"].iloc[-61] if len(df) >= 62 and df["close"].iloc[-61] > 0 else 0
                vs_ma200 = last.get("ma200")
                vs_above_ma200 = pd.notna(vs_ma200) and last["close"] > vs_ma200
                vs_ma50 = last.get("ma50")
                vs_ma50_slope = 0
                if pd.notna(vs_ma50) and len(df) >= 21:
                    vs_ma50_20ago = df["ma50"].iloc[-21]
                    vs_ma50_slope = (vs_ma50 - vs_ma50_20ago) / vs_ma50_20ago if pd.notna(vs_ma50_20ago) and vs_ma50_20ago > 0 else 0
                # OBV 기울기 (최근 10일)
                vs_obv_slope = 0
                if pd.notna(last.get("obv")) and len(df) >= 11:
                    obv_now = last["obv"]
                    obv_10ago = df["obv"].iloc[-11]
                    vs_obv_slope = (obv_now - obv_10ago) / abs(obv_10ago) if pd.notna(obv_10ago) and obv_10ago != 0 else 0
                # === 유형 분류 (우선순위 순) ===
                vs_type = None
                vs_detail = ""
                daily_chg = (last["close"] - prev["close"]) / prev["close"] if prev["close"] > 0 else 0
                # 1. STOPPING_VOLUME: 하락 중 좁은 캔들 + 대량 + 종가 상단
                if vs_pct_20d < -0.05 and vs_narrow_candle and vs_close_pos >= 0.5:
                    vs_type = "STOPPING"
                    vs_detail = f"하락정지(좁은봉+대량)"
                # 2. SELLING_CLIMAX: 급락 중 최대 거래량 + 회복 캔들
                elif vs_pct_20d < -0.10 and vs_is_60max:
                    hammer_vs = vs_range > 0 and (min(last["close"], last["open"]) - last["low"]) / vs_range >= 0.65
                    intraday_recovery = last["low"] < prev["close"] * 0.97 and last["close"] > prev["close"]
                    doji_upper = vs_body_ratio < 0.3 and vs_close_pos >= 0.5
                    if hammer_vs or intraday_recovery or doji_upper:
                        vs_type = "CLIMAX_SELL"
                        vs_detail = f"셀링클라이맥스({'망치' if hammer_vs else '장중회복' if intraday_recovery else '도지'})"
                # 3. ACCUMULATION: 횡보 중 양봉 + 종가 상단 + 작은 변동
                elif abs(vs_pct_60d) < 0.10 and abs(vs_ma50_slope) < 0.01 and vs_bullish and vs_close_pos >= 0.6 and 0 <= daily_chg <= 0.05:
                    vs_type = "ACCUMULATION"
                    vs_detail = "매집의심(횡보+양봉)"
                # 4. BUYING_CLIMAX: 급등 중 최대 거래량 + 도지/유성형
                elif vs_pct_20d > 0.20 and vs_is_60max and (vs_upper_shadow >= 0.6 or (vs_body_ratio < 0.3 and vs_close_pos < 0.5)):
                    vs_type = "CLIMAX_BUY"
                    vs_detail = "바잉클라이맥스(급등후도지)"
                # 5. DISTRIBUTION: 상승 후 음봉/윗꼬리
                elif vs_pct_60d > 0.20 and (not vs_bullish or vs_upper_shadow >= 0.5):
                    vs_type = "DISTRIBUTION"
                    vs_detail = "분배의심(상승후음봉)"
                if vs_type is None:
                    pass  # UNCLASSIFIED → 미발동
                else:
                    # 카테고리 구분
                    is_buy_interest = vs_type in ("ACCUMULATION", "CLIMAX_SELL", "STOPPING")
                    # === 스코어링 ===
                    # 공통 (50점)
                    s_vol_intensity = min(vs_rvol / 10, 1.0) * 7 + min(vs_zscore / 5, 1.0) * 4 + min(vs_robust / 8, 1.0) * 4
                    s_tval = min(max(np.log10(vs_trading_val / 10_000_000_000), 0) / 1.5, 1.0) * 10 if vs_trading_val > 0 else 0
                    s_quality = 5  # 유니버스 필터 통과 시 기본점
                    s_flow = 5  # 외국인/기관 데이터 없을 때 중립
                    s_mkt_vs = 3  # 시장 추세 기본 중립
                    s_ma200_vs = 5 if vs_above_ma200 else 0
                    s_common = min(s_vol_intensity + s_tval + s_quality + s_flow + s_mkt_vs + s_ma200_vs, 50)
                    # 유형별 (50점)
                    s_type_score = 0
                    if vs_type == "ACCUMULATION":
                        # 횡보 기간(15), 종가 상단(15), OBV(10), 양봉 누적(10)
                        s_type_score = min(abs(vs_pct_60d) / 0.10, 1.0) * 15  # 횡보 좁을수록 가점 (역수)
                        s_type_score = (1.0 - min(abs(vs_pct_60d) / 0.05, 1.0)) * 15
                        s_type_score += min(vs_close_pos / 0.8, 1.0) * 15
                        s_type_score += (10 if vs_obv_slope > 0.05 else 5 if vs_obv_slope > 0 else 0)
                        # 최근 5일 양봉+거래량 동반 횟수
                        bull_vol_cnt = 0
                        for k in range(max(0, len(df) - 5), len(df)):
                            if df["close"].iloc[k] > df["open"].iloc[k] and df["volume"].iloc[k] > vs_mean60 * 1.5:
                                bull_vol_cnt += 1
                        s_type_score += min(bull_vol_cnt / 3, 1.0) * 10
                    elif vs_type == "CLIMAX_SELL":
                        s_type_score = 20 if vs_is_60max else 10
                        s_type_score += min(vs_close_pos / 0.7, 1.0) * 15
                        s_type_score += min(abs(vs_pct_20d) / 0.20, 1.0) * 10
                        s_type_score += 5 if vs_above_ma200 else 0
                    elif vs_type == "STOPPING":
                        candle_narrowness = (vs_atr14 - vs_range) / vs_atr14 if vs_atr14 > 0 else 0
                        s_type_score = min(candle_narrowness / 0.5, 1.0) * 20
                        s_type_score += min(vs_close_pos / 0.7, 1.0) * 15
                        s_type_score += 10 if (prev["close"] < prev["open"]) else 0  # 전일 음봉이면 가점
                        s_type_score += min(vs_rvol / 5, 1.0) * 5
                    elif vs_type == "CLIMAX_BUY":
                        s_type_score = 30  # 경고용, 최소 점수
                    elif vs_type == "DISTRIBUTION":
                        s_type_score = 30  # 경고용, 최소 점수
                    s_type_score = min(s_type_score, 50)
                    # 극단 거래량 감점
                    vs_penalty = 15 if vs_extreme else 0
                    vs_score = int(s_common + s_type_score - vs_penalty)
                    # 발동 조건
                    min_scores = {"ACCUMULATION": 60, "CLIMAX_SELL": 65, "STOPPING": 60, "CLIMAX_BUY": 0, "DISTRIBUTION": 0}
                    if is_buy_interest and vs_score >= min_scores.get(vs_type, 70):
                        extreme_flag = " [극단거래량]" if vs_extreme else ""
                        signals.append(("volume_spike", f"{vs_detail}, RVOL {vs_rvol:.1f}배(Z {vs_zscore:.1f}), 거래대금 {vs_trading_val/100_000_000:.0f}억, 강도 {vs_score}점{extreme_flag}"))

    # 평균회귀 (STATISTICAL + BOLLINGER_REVERSION, Hurst/ADF/Half-life 풀 검증)
    if len(df) >= 120:
        mr_closes = df["close"].dropna().values[-250:]
        # --- 풀 검증 ---
        hurst = calc_hurst_exponent(mr_closes)
        hurst_ok = hurst is not None and hurst < 0.45
        try:
            from statsmodels.tsa.stattools import adfuller
            adf_result = adfuller(mr_closes, maxlag=10, autolag="AIC")
            adf_pval = adf_result[1]
            adf_ok = adf_pval < 0.05
        except Exception:
            adf_pval = 1.0
            adf_ok = False
        if len(mr_closes) >= 30:
            y_ar = np.diff(mr_closes)
            x_ar = mr_closes[:-1]
            x_mean_ar = x_ar.mean()
            denom_ar = np.sum((x_ar - x_mean_ar) ** 2)
            beta_ar = np.sum((x_ar - x_mean_ar) * y_ar) / denom_ar if denom_ar > 0 else 0
            if beta_ar < 0 and (1 + beta_ar) > 0:
                half_life = -np.log(2) / np.log(1 + beta_ar)
            else:
                half_life = 999
        else:
            half_life = 999
        hl_ok = 3 <= half_life <= 30
        mr_pass_count = sum([hurst_ok, adf_ok, hl_ok])
        mr_eligible = mr_pass_count >= 1

        if mr_eligible:
            # --- 거래량 이상 배제 ---
            mr_avg_vol = last.get("vol_ma50") or last.get("vol_ma20") or 0
            mr_rvol = last["volume"] / mr_avg_vol if mr_avg_vol > 0 else 0
            mr_vol_explosion = mr_rvol >= 3.0

            # --- 추세 충돌 체크 ---
            mr_conflicts = 0
            mr_ma200 = last.get("ma200")
            if pd.notna(mr_ma200) and last["close"] < mr_ma200:
                mr_conflicts += 1
            mr_ma50 = last.get("ma50")
            if pd.notna(mr_ma50) and len(df) >= 21:
                mr_ma50_20ago = df["ma50"].iloc[-21]
                if pd.notna(mr_ma50_20ago) and mr_ma50_20ago > 0 and (mr_ma50 - mr_ma50_20ago) / mr_ma50_20ago < -0.05:
                    mr_conflicts += 1
            mr_low52 = last.get("low_52w")
            if pd.notna(mr_low52) and mr_low52 > 0 and (last["close"] - mr_low52) / mr_low52 < 0.05:
                mr_conflicts += 1
            mr_ma20v = last.get("ma20")
            mr_ma60v = last.get("ma60")
            if pd.notna(mr_ma20v) and pd.notna(mr_ma60v) and mr_ma20v < mr_ma60v:
                mr_conflicts += 1
            if len(df) >= 252:
                close_1y = df["close"].iloc[-252]
                if close_1y > 0 and (last["close"] - close_1y) / close_1y < -0.15:
                    mr_conflicts += 1
            mr_trend_ok = mr_conflicts <= 1

            if mr_trend_ok and not mr_vol_explosion:
                # --- 공통 변동성 지표 ---
                mr_atr10 = df["close"].diff().abs().iloc[-10:].mean() if len(df) >= 10 else None
                mr_atr60 = df["close"].diff().abs().iloc[-60:].mean() if len(df) >= 60 else None
                mr_vol_contract = (mr_atr10 is not None and mr_atr60 is not None and
                                   mr_atr60 > 0 and mr_atr10 < mr_atr60 * 0.8)

                best_mr_signal = None
                best_mr_score = 0

                # === STATISTICAL 타입 ===
                hl_n = max(5, min(30, int(round(half_life)))) if hl_ok else 20
                if len(df) >= hl_n + 1:
                    sma_n = df["close"].iloc[-hl_n:].mean()
                    std_n = df["close"].iloc[-hl_n:].std(ddof=1)
                    if std_n > 0:
                        z_score = (last["close"] - sma_n) / std_n
                        if z_score <= -2.0:
                            # 회귀 확인 시그널
                            mr_confirms = 0
                            if len(df) >= hl_n + 2:
                                z_prev = (df["close"].iloc[-2] - df["close"].iloc[-hl_n - 1:-1].mean()) / df["close"].iloc[-hl_n - 1:-1].std(ddof=1) if df["close"].iloc[-hl_n - 1:-1].std(ddof=1) > 0 else z_score
                                if z_score > z_prev:
                                    mr_confirms += 1
                            if last["close"] >= last["open"]:  # 양봉
                                mr_confirms += 1
                            body_pos = (last["close"] - last["low"]) / (last["high"] - last["low"]) if last["high"] > last["low"] else 0
                            if body_pos >= 0.3:  # 종가 하위 30% 이상
                                mr_confirms += 1
                            if len(df) >= 2 and last["close"] > df["close"].iloc[-2]:
                                mr_confirms += 1
                            rsi14v = last.get("rsi14")
                            if pd.notna(rsi14v) and rsi14v < 35:
                                mr_confirms += 1
                            if mr_confirms >= 2:
                                # 스코어링
                                s_hurst = min((0.5 - hurst) / 0.15, 1.0) * 15 if hurst is not None else 0
                                s_adf = 10 if adf_pval < 0.01 else (7 if adf_ok else 0)
                                hl_dist = abs(half_life - 15) / 15 if hl_ok else 1.0
                                s_hl = max(0, (1 - hl_dist)) * 10
                                s_z = min(abs(z_score + 2) / 2.0, 1.0) * 15
                                s_confirm = min(mr_confirms / 4, 1.0) * 15
                                s_trend = 15 if mr_conflicts == 0 else 7
                                s_vol_pat = min(max(0, 1 - abs(mr_rvol - 1.0) / 1.0), 1.0) * 10
                                s_vcontract = 10 if mr_vol_contract else 0
                                stat_score = int(s_hurst + s_adf + s_hl + s_z + s_confirm + s_trend + s_vol_pat + s_vcontract)
                                if stat_score >= 65 and stat_score > best_mr_score:
                                    hl_disp = half_life if hl_ok else 999
                                    confirm_label = f"회귀확인{mr_confirms}/5"
                                    best_mr_score = stat_score
                                    best_mr_signal = ("mean_reversion",
                                        f"STATISTICAL Z={z_score:.2f}(H={hurst:.2f}), HL={hl_disp:.0f}일, {confirm_label}, 강도 {stat_score}점")

                # === BOLLINGER_REVERSION 타입 ===
                bb_lower = last.get("bb_lower")
                bb_upper = last.get("bb_upper")
                bb_mid = last.get("bb_mid")
                rsi14_v = last.get("rsi14")
                if (pd.notna(bb_lower) and pd.notna(bb_upper) and pd.notna(bb_mid) and
                        pd.notna(rsi14_v)):
                    # BB 하단 이탈 이력 (최근 5일 내) + 현재 밴드 내 복귀
                    bb_broke = any(
                        df["close"].iloc[i] <= df["bb_lower"].iloc[i]
                        for i in range(-6, -1)
                        if pd.notna(df["bb_lower"].iloc[i])
                    ) if len(df) >= 6 else False
                    bb_inside = last["close"] > bb_lower
                    if bb_broke and bb_inside and 30 <= rsi14_v <= 45:
                        # BB Width 수축
                        bb_width_now = (bb_upper - bb_lower) / bb_mid if bb_mid > 0 else 0
                        if len(df) >= 20:
                            bb_width_avg = ((df["bb_upper"] - df["bb_lower"]) / df["bb_mid"].replace(0, np.nan)).iloc[-20:].mean()
                        else:
                            bb_width_avg = bb_width_now
                        bb_width_ok = pd.notna(bb_width_avg) and bb_width_now < bb_width_avg
                        # RSI 다이버전스 보너스
                        rsi_div = False
                        if len(df) >= 6:
                            prev_low_close = df["close"].iloc[-6:-1].min()
                            prev_low_rsi = df["rsi14"].iloc[-6:-1].min() if pd.notna(df["rsi14"].iloc[-6:-1].min()) else rsi14_v
                            if last["close"] <= prev_low_close and rsi14_v > prev_low_rsi:
                                rsi_div = True
                        # 회귀 확인 시그널
                        mr_bol_confirms = 0
                        if last["close"] >= last["open"]:
                            mr_bol_confirms += 1
                        body_pos_b = (last["close"] - last["low"]) / (last["high"] - last["low"]) if last["high"] > last["low"] else 0
                        if body_pos_b >= 0.3:
                            mr_bol_confirms += 1
                        if len(df) >= 2 and last["close"] > df["close"].iloc[-2]:
                            mr_bol_confirms += 1
                        if rsi_div:
                            mr_bol_confirms += 1
                        if last["close"] > bb_mid:
                            mr_bol_confirms += 1
                        if mr_bol_confirms >= 2:
                            # BB 이탈 정도 (이탈 당시 최저 이탈량)
                            max_breach = 0
                            for i in range(-6, -1):
                                if len(df) >= abs(i) and pd.notna(df["bb_lower"].iloc[i]):
                                    breach = (df["bb_lower"].iloc[i] - df["close"].iloc[i]) / df["bb_lower"].iloc[i]
                                    if breach > max_breach:
                                        max_breach = breach
                            # Z-Score (BB 기반)
                            bb_z = (bb_mid - last["close"]) / ((bb_upper - bb_lower) / 4) if (bb_upper - bb_lower) > 0 else 0
                            # 스코어링
                            s_hurst_b = min((0.5 - hurst) / 0.15, 1.0) * 15 if hurst is not None else 0
                            s_adf_b = 10 if adf_pval < 0.01 else (7 if adf_ok else 0)
                            hl_dist_b = abs(half_life - 15) / 15 if hl_ok else 1.0
                            s_hl_b = max(0, (1 - hl_dist_b)) * 10
                            s_breach = min(max_breach / 0.05, 1.0) * 15
                            s_confirm_b = min(mr_bol_confirms / 4, 1.0) * 15
                            s_trend_b = 15 if mr_conflicts == 0 else 7
                            s_vol_pat_b = min(max(0, 1 - abs(mr_rvol - 1.0) / 1.0), 1.0) * 10
                            s_vcontract_b = 10 if mr_vol_contract else 0
                            bol_score = int(s_hurst_b + s_adf_b + s_hl_b + s_breach + s_confirm_b + s_trend_b + s_vol_pat_b + s_vcontract_b)
                            if bol_score >= 65 and bol_score > best_mr_score:
                                hl_disp_b = half_life if hl_ok else 999
                                div_label = "+다이버전스" if rsi_div else ""
                                confirm_label_b = f"회귀확인{mr_bol_confirms}/5{div_label}"
                                best_mr_score = bol_score
                                best_mr_signal = ("mean_reversion",
                                    f"BOLLINGER_REVERSION RSI={rsi14_v:.0f}(H={hurst:.2f}), HL={hl_disp_b:.0f}일, {confirm_label_b}, 강도 {bol_score}점")

                if best_mr_signal is not None:
                    signals.append(best_mr_signal)

    # 듀얼모멘텀 (Quantocracy 64회)
    if pd.notna(last.get("mom_12m")) and pd.notna(last.get("mom_1m")):
        if last["mom_12m"] > 0.05 and last["mom_1m"] > 0 and score >= 40:
            signals.append(("dual_momentum", f"12M 수익률 {last['mom_12m']:.1%}, 1M {last['mom_1m']:.1%}, 스코어 {score}"))

    # === v3 시그널 ===

    # 채널 돌파 (변동성 채널: Donchian 20/55 + Keltner + BB Walking the Bands)
    if len(df) >= 60 and len(df) >= 2:
        # --- 공통 배제 ---
        _cb_upper_lmt = prev["close"] > 0 and (last["close"] / prev["close"] - 1) > 0.295
        _cb_gap = prev["close"] > 0 and (last["open"] / prev["close"] - 1) > 0.07
        _cb_bearish = last["close"] < last["open"]
        _cb_range = last["high"] - last["low"]
        _cb_body = abs(last["close"] - last["open"]) / _cb_range if _cb_range > 0 else 0
        _cb_doji = _cb_body < 0.3
        _cb_close_pos = (last["close"] - last["low"]) / _cb_range if _cb_range > 0 else 0.5
        _cb_tv = last["close"] * last["volume"] >= 5_000_000_000
        if not (_cb_upper_lmt or _cb_gap or _cb_bearish or _cb_doji or _cb_close_pos < 0.5 or not _cb_tv):
            _cb_avg_vol50 = last.get("vol_ma50") or 0
            _cb_rvol = last["volume"] / _cb_avg_vol50 if _cb_avg_vol50 > 0 else 0
            if _cb_rvol >= 1.5:
                _cb_channels = []  # (type, upper_level)

                # Donchian 20일 (어제 기준 채널 상단)
                _dc20_prev = df["donchian_upper"].iloc[-2]
                if pd.notna(_dc20_prev) and last["close"] > _dc20_prev * 1.005:
                    _cb_channels.append(("DONCHIAN20", float(_dc20_prev)))

                # Donchian 55일 (Turtle Trader 표준)
                _dc55_prev = df["donchian55_upper"].iloc[-2] if pd.notna(df["donchian55_upper"].iloc[-2]) else None
                if _dc55_prev and last["close"] > _dc55_prev * 1.005:
                    _cb_channels.append(("DONCHIAN55", float(_dc55_prev)))

                # Keltner Channel (EMA20 + 2×ATR14)
                _cb_ema20_p = df["ema20"].iloc[-2]
                _cb_atr14_p = df["atr14"].iloc[-2] if pd.notna(df.get("atr14", pd.Series()).iloc[-2] if "atr14" in df else float("nan")) else None
                if pd.notna(_cb_ema20_p) and pd.notna(last.get("atr14")):
                    _kelt_prev = float(_cb_ema20_p) + 2.0 * float(df["atr14"].iloc[-2])
                    if pd.notna(_kelt_prev) and last["close"] > _kelt_prev * 1.005:
                        _cb_channels.append(("KELTNER", _kelt_prev))

                # BB Walking the Bands (3일+ 연속 상단 위 종가 마감)
                _cb_walking = 0
                if pd.notna(last.get("bb_upper")) and len(df) >= 5:
                    for _wi in range(-1, -7, -1):
                        if (len(df) >= abs(_wi) and
                                pd.notna(df["bb_upper"].iloc[_wi]) and
                                df["close"].iloc[_wi] > df["bb_upper"].iloc[_wi]):
                            _cb_walking += 1
                        else:
                            break
                if _cb_walking >= 3:
                    _cb_channels.append(("BB_WALKING", float(last["bb_upper"])))

                if _cb_channels:
                    # 채널별 우선순위 (DONCHIAN55 > DONCHIAN20 > KELTNER > BB_WALKING)
                    _cb_types = [c[0] for c in _cb_channels]
                    _cb_dict = dict(_cb_channels)
                    if "DONCHIAN55" in _cb_types:
                        _cb_main, _cb_level = "DONCHIAN55", _cb_dict["DONCHIAN55"]
                    elif "DONCHIAN20" in _cb_types:
                        _cb_main, _cb_level = "DONCHIAN20", _cb_dict["DONCHIAN20"]
                    elif "KELTNER" in _cb_types:
                        _cb_main, _cb_level = "KELTNER", _cb_dict["KELTNER"]
                    else:
                        _cb_main, _cb_level = "BB_WALKING", _cb_dict["BB_WALKING"]

                    _cb_multi = len(_cb_channels)
                    _cb_breakout_pct = (last["close"] / _cb_level - 1) * 100 if _cb_level > 0 else 0

                    # 압축→폭발 패턴
                    _cb_vol_ch = df["volume"].iloc[-21:-1].mean()
                    _cb_vol10 = df["volume"].iloc[-11:-1].mean()
                    _cb_dry_ratio = _cb_vol10 / _cb_vol_ch if _cb_vol_ch > 0 else 1.0
                    _cb_exp_ratio = last["volume"] / _cb_vol_ch if _cb_vol_ch > 0 else 1.0
                    _cb_compress_ok = _cb_dry_ratio <= 0.85 and _cb_exp_ratio >= 2.0

                    # ATR 압축
                    _cb_atr_r = df["close"].diff().abs().iloc[-11:-1].mean()
                    _cb_atr_c = df["close"].diff().abs().iloc[-21:-1].mean()
                    _cb_atr_ratio = _cb_atr_r / _cb_atr_c if _cb_atr_c > 0 else 1.0
                    _cb_atr_ok = _cb_atr_ratio <= 0.8

                    # --- 스코어링 ---
                    # 공통 50점
                    _s_margin = min(_cb_breakout_pct / 3.0, 1.0) * 10
                    _s_rvol = min(_cb_rvol / 4.0, 1.0) * 15
                    _s_compress = 15 if _cb_compress_ok else (8 if _cb_exp_ratio >= 2.0 else 0)
                    _s_atr = 10 if _cb_atr_ok else (5 if _cb_atr_ratio <= 1.0 else 0)
                    # 유형별 50점
                    _s_base = {"DONCHIAN55": 20, "DONCHIAN20": 15, "KELTNER": 12, "BB_WALKING": 12}.get(_cb_main, 12)
                    _s_multi = min((_cb_multi - 1) * 10, 15)
                    _s_walk = min(_cb_walking / 5.0, 1.0) * 8 if "BB_WALKING" in _cb_types else 0
                    _cb_ma200 = last.get("ma200")
                    _s_ma200 = 5 if pd.notna(_cb_ma200) and last["close"] > _cb_ma200 else 0

                    _cb_score = int(_s_margin + _s_rvol + _s_compress + _s_atr +
                                    _s_base + _s_multi + _s_walk + _s_ma200)

                    if _cb_score >= 70:
                        _multi_lbl = f" [{_cb_multi}채널동시]" if _cb_multi >= 2 else ""
                        _comp_lbl = " [압축→폭발]" if _cb_compress_ok else ""
                        signals.append(("donchian_breakout",
                            f"{_cb_main}({int(_cb_level)}) RVOL {_cb_rvol:.1f}배{_multi_lbl}{_comp_lbl}, 강도 {_cb_score}점"))

    # 전고점돌파 (O'Neil CANSLIM + Minervini VCP 기반 업그레이드)
    if len(df) >= 62 and pd.notna(last.get("high_52w")) and pd.notna(last.get("vol_ma50")):
        _do_new_high = True
        # --- 배제 조건 먼저 ---
        # 상한가 배제
        if prev["close"] > 0 and (last["close"] / prev["close"] - 1) > 0.295:
            _do_new_high = False
        # 도지 배제 (캔들 몸통/전체 범위 < 0.3)
        _c_range = last["high"] - last["low"]
        _body = abs(last["close"] - last["open"])
        if _c_range > 0 and _body / _c_range < 0.3:
            _do_new_high = False
        # 갭 과도 배제 (+8% 초과)
        if prev["close"] > 0 and (last["open"] - prev["close"]) / prev["close"] > 0.08:
            _do_new_high = False
        # 장중 밀림 배제 (시가 대비 종가 -3% 초과)
        if last["open"] > 0 and (last["close"] - last["open"]) / last["open"] < -0.03:
            _do_new_high = False

        if _do_new_high:
            # --- 베이스 형성 ---
            _base_high = df["high"].iloc[-61:-1].max()
            _base_low = df["low"].iloc[-61:-1].min()
            _base_depth = (_base_high - _base_low) / _base_high if _base_high > 0 else 1.0
            _pivot = _base_high

            # --- 돌파 / 거래량 / 추세 조건 ---
            _avg_vol_50 = last.get("vol_ma50") or 0
            _rvol = last["volume"] / _avg_vol_50 if _avg_vol_50 > 0 else 0
            _ma50 = last.get("ma50")
            _ma150 = last.get("ma150")
            _ma200 = last.get("ma200")
            _ma200_prev = df["ma200"].iloc[-21] if len(df) >= 21 else None

            _breakout = last["close"] > _pivot * 1.01
            _near_ath = last["close"] >= last["high_52w"] * 0.85
            _bullish = last["close"] > last["open"]
            _volume_ok = _rvol >= 1.5
            _base_ok = _base_depth <= 0.30
            _trend_ok = (pd.notna(_ma50) and pd.notna(_ma150) and pd.notna(_ma200)
                         and last["close"] > _ma50 > _ma150 > _ma200)
            _ma200_rising = (pd.notna(_ma200) and pd.notna(_ma200_prev)
                             and _ma200 > _ma200_prev)

            if _breakout and _near_ath and _bullish and _volume_ok and _base_ok and _trend_ok and _ma200_rising:
                # --- 시그널 강도 점수 (0~100) ---
                _sc_rvol = min(_rvol / 5.0, 1.0) * 25
                _breakout_pct = (last["close"] / _pivot - 1.01)
                _sc_breakout = min(_breakout_pct / 0.05, 1.0) * 15
                _sc_base = 15.0  # 60일 베이스 고정 만점
                _vol5_avg = df["volume"].iloc[-6:-1].mean()
                _vol60_avg = df["volume"].iloc[-61:-1].mean()
                _sc_dryup = 10.0 if (_vol60_avg > 0 and _vol5_avg < _vol60_avg * 0.8) else 0.0
                _atr_recent = df["atr14"].iloc[-10:].mean() if pd.notna(last.get("atr14")) else None
                _atr_60 = df["atr14"].iloc[-60:].mean() if _atr_recent is not None else None
                _sc_atr = 10.0 if (_atr_60 and _atr_recent and _atr_recent < _atr_60 * 0.7) else 0.0
                _sc_ath = min(last["close"] / last["high_52w"], 1.0) * 10
                _ma200_slope = (_ma200 - _ma200_prev) / _ma200_prev if _ma200_prev > 0 else 0
                _sc_ma200 = min(max(_ma200_slope / 0.02, 0.0), 1.0) * 10
                _trading_value = last["close"] * last["volume"]
                _sc_value = min(max((_trading_value - 1e10) / 4e10, 0.0), 1.0) * 5
                _nh_score = (_sc_rvol + _sc_breakout + _sc_base + _sc_dryup
                             + _sc_atr + _sc_ath + _sc_ma200 + _sc_value)

                if _nh_score >= 55:
                    _pct = (last["close"] / _pivot - 1) * 100
                    signals.append(("new_high", f"전고점({int(_pivot)}원) 돌파 +{_pct:.1f}%, RVOL {_rvol:.1f}배, 강도 {int(_nh_score)}점"))

    # 연속 하락 후 반등 (Quantocracy 7건)
    consec = last.get("consecutive_days", 0)
    if isinstance(consec, (int, float)) and pd.notna(consec):
        if df.iloc[-2].get("consecutive_days", 0) <= -2 and last["close"] > last["open"]:
            signals.append(("bounce_after_drop", f"{abs(int(df.iloc[-2]['consecutive_days']))}일 연속 하락 후 반등 양봉"))

    return signals


def get_related_articles():
    """시그널별 Quantocracy 관련 글 매칭"""
    db_path = os.path.join(os.path.dirname(__file__), "quantocracy.db")
    if not os.path.exists(db_path):
        return {}

    import sqlite3
    conn = sqlite3.connect(db_path)

    # 시그널 타입 → 검색 키워드 매핑
    signal_keywords = {
        "golden_cross": ["golden cross", "moving average crossover", "MA cross"],
        "breakout": ["breakout", "range breakout", "resistance break", "new high"],
        "pullback": ["pullback", "dip buying", "buy the dip", "mean reversion support"],
        "oversold": ["oversold", "RSI", "bounce", "reversal"],
        "volume_spike": ["volume", "volume spike", "unusual volume", "volume breakout"],
        "mean_reversion": ["mean reversion", "bollinger", "Z-score", "reversion"],
        "dual_momentum": ["dual momentum", "absolute momentum", "relative momentum", "trend following momentum"],
        "donchian_breakout": ["donchian", "channel breakout", "turtle trading"],
        "new_high": ["new high", "52 week high", "all time high", "momentum"],
        "bounce_after_drop": ["consecutive", "reversal", "oversold bounce", "dead cat"],
    }

    result = {}
    for sig_type, keywords in signal_keywords.items():
        conditions = " OR ".join([f"LOWER(title||description) LIKE '%{kw.lower()}%'" for kw in keywords])
        rows = conn.execute(f"""
            SELECT title, source, url, description
            FROM articles WHERE {conditions}
            ORDER BY published_at DESC LIMIT 3
        """).fetchall()

        result[sig_type] = [
            {"title": t, "source": s, "url": u, "summary": d[:200]}
            for t, s, u, d in rows
        ]

    conn.close()
    return result


###############################################################################
# 듀얼 모멘텀 — 포트폴리오 단위 (6개월 룩백, 상위 10종목)
###############################################################################

DM_LOOKBACK = 126   # 6개월 거래일
DM_SKIP = 21        # 최근 1개월 제외 (12-1 → 6-1)
DM_N = 10           # 선정 종목 수
DM_MIN_PRICE = 5000  # 유니버스 최소 주가
DM_MIN_VOL_VAL = 5_000_000_000  # 50억 (일 평균 거래대금)
DM_MAX_VOL_PCT = 0.80  # 연환산 변동성 상한 80%
DM_MIN_ABS_RETURN = 0.0  # 절대 모멘텀: 6개월 수익률 > 0


def _dm_calc_entry(code: str, name: str, market: str, df) -> dict | None:
    """종목별 듀얼 모멘텀 지표 계산. 최소 데이터 미달 시 None 반환."""
    if len(df) < DM_LOOKBACK + DM_SKIP + 5:
        return None
    last = df.iloc[-1]
    close_arr = df["close"].values

    # 기본 필터
    if last["close"] < DM_MIN_PRICE:
        return None
    avg_vol_val = (df["close"] * df["volume"]).iloc[-60:].mean() if len(df) >= 60 else 0
    if avg_vol_val < DM_MIN_VOL_VAL:
        return None

    # 6-1개월 모멘텀 (최근 1개월 제외한 6개월 수익률)
    close_6m_ago = close_arr[-(DM_LOOKBACK + DM_SKIP)]
    close_1m_ago = close_arr[-DM_SKIP]
    if close_6m_ago <= 0 or close_1m_ago <= 0:
        return None
    return_6_1m = (close_1m_ago / close_6m_ago) - 1

    # 절대 모멘텀 통과 여부
    abs_pass = return_6_1m > DM_MIN_ABS_RETURN

    # 최근 1개월 수익률 (급등 필터용)
    return_1m = (last["close"] / close_arr[-DM_SKIP]) - 1 if close_arr[-DM_SKIP] > 0 else 0

    # Sharpe-like (126일 일별 수익률 기반)
    daily_ret = df["close"].pct_change().iloc[-DM_LOOKBACK:]
    mean_ret = daily_ret.mean()
    std_ret = daily_ret.std(ddof=1)
    sharpe = (mean_ret * DM_LOOKBACK) / (std_ret * (DM_LOOKBACK ** 0.5)) if std_ret > 0 else 0

    # 연환산 변동성
    ann_vol = std_ret * (252 ** 0.5) if std_ret > 0 else 0
    if ann_vol > DM_MAX_VOL_PCT:
        return None

    # R² 추세 강도 (log-가격의 OLS 선형 회귀)
    try:
        log_close = np.log(df["close"].iloc[-DM_LOOKBACK:].values)
        t = np.arange(len(log_close), dtype=float)
        t_mean = t.mean()
        lc_mean = log_close.mean()
        slope = np.sum((t - t_mean) * (log_close - lc_mean)) / np.sum((t - t_mean) ** 2)
        ss_res = np.sum((log_close - (lc_mean + slope * (t - t_mean))) ** 2)
        ss_tot = np.sum((log_close - lc_mean) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0
        r_squared = max(0.0, r_squared)
    except Exception:
        r_squared = 0.0

    return {
        "code": code,
        "name": name,
        "market": market,
        "close": int(last["close"]),
        "return_6_1m": round(return_6_1m, 4),
        "return_1m": round(return_1m, 4),
        "sharpe": round(sharpe, 3),
        "r_squared": round(r_squared, 3),
        "ann_vol": round(ann_vol, 3),
        "abs_pass": abs_pass,
    }


def run_dual_momentum(dm_candidates: list, today_str: str) -> None:
    """KOSPI 절대 모멘텀 + 상대 랭킹으로 상위 N종목 선정 후 JSON 저장."""
    # --- KOSPI 절대 모멘텀 ---
    market_regime = "UNKNOWN"
    kospi_return_6m = None
    kospi_above_ma200 = False
    market_abs_pass = False
    try:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=450)
        kospi_df = fdr.DataReader("KS11", start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d"))
        if kospi_df is not None and len(kospi_df) >= DM_LOOKBACK + DM_SKIP + 5:
            kospi_close = kospi_df["Close"].values
            k6 = kospi_close[-(DM_LOOKBACK + DM_SKIP)]
            k1 = kospi_close[-DM_SKIP]
            kospi_return_6m = round((k1 / k6 - 1), 4) if k6 > 0 else None
            if len(kospi_close) >= 200:
                ma200 = kospi_close[-200:].mean()
                kospi_above_ma200 = bool(kospi_close[-1] > ma200)
            kospi_pos_return = kospi_return_6m is not None and kospi_return_6m > 0
            market_abs_pass = kospi_pos_return and kospi_above_ma200
            if market_abs_pass:
                market_regime = "BULL"
            elif kospi_pos_return or kospi_above_ma200:
                market_regime = "NEUTRAL"
            else:
                market_regime = "BEAR"
    except Exception as e:
        print(f"KOSPI 데이터 조회 실패: {e}")

    # 유니버스: 절대 모멘텀 통과 + 최근 1개월 급등 +30% 배제
    universe = [
        c for c in dm_candidates
        if c["abs_pass"] and c["return_1m"] <= 0.30
    ]

    result = {
        "date": today_str,
        "market_regime": market_regime,
        "kospi_return_6m": kospi_return_6m,
        "kospi_above_ma200": kospi_above_ma200,
        "market_abs_pass": market_abs_pass,
        "universe_size": len(dm_candidates),
        "candidates_passed": len(universe),
        "portfolio": [],
        "warning": "" if market_abs_pass else "시장 절대 모멘텀 미통과 — 현금 보유 권장",
    }

    if not market_abs_pass or len(universe) < DM_N:
        os.makedirs(os.path.dirname(DM_OUTPUT_PATH), exist_ok=True)
        with open(DM_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"듀얼 모멘텀: {market_regime} — 포트폴리오 없음 ({len(universe)}개 후보)")
        return

    # --- 상대 모멘텀 점수 (percentile rank 기반) ---
    n = len(universe)

    def _prank(key: str) -> None:
        vals = [c[key] for c in universe]
        sorted_vals = sorted(vals)
        for c in universe:
            rank = sorted_vals.index(c[key])  # 낮은 값 = 낮은 순위
            c[f"_pr_{key}"] = rank / (n - 1) if n > 1 else 0.5

    _prank("return_6_1m")
    _prank("sharpe")
    _prank("r_squared")

    for c in universe:
        raw = (0.5 * c["_pr_return_6_1m"] +
               0.3 * c["_pr_sharpe"] +
               0.2 * c["_pr_r_squared"])
        c["final_score"] = round(raw * 100, 1)

    universe.sort(key=lambda x: x["final_score"], reverse=True)
    top10 = universe[:DM_N]

    weight = round(100.0 / DM_N, 1)
    portfolio = []
    for rank, c in enumerate(top10, 1):
        portfolio.append({
            "rank": rank,
            "code": c["code"],
            "name": c["name"],
            "market": c["market"],
            "close": c["close"],
            "return_6_1m": c["return_6_1m"],
            "return_1m": c["return_1m"],
            "sharpe": c["sharpe"],
            "r_squared": c["r_squared"],
            "ann_vol_pct": round(c["ann_vol"] * 100, 1),
            "final_score": c["final_score"],
            "weight": weight,
        })

    # 포트폴리오 신뢰도 점수 (0~100)
    avg_ret = sum(c["return_6_1m"] for c in top10) / DM_N
    avg_r2 = sum(c["r_squared"] for c in top10) / DM_N
    avg_vol = sum(c["ann_vol"] for c in top10) / DM_N
    s_market = min(max((kospi_return_6m or 0) / 0.20, 0), 1.0) * 20
    s_ret = min(avg_ret / 0.15, 1.0) * 20
    s_r2 = min(avg_r2 / 0.6, 1.0) * 15
    s_vol = (1 - min(avg_vol / 0.5, 1.0)) * 10  # 변동성 낮을수록 가점
    s_candidates = min(len(universe) / 100, 1.0) * 20  # 후보 많을수록 가점
    s_spread = min((top10[0]["final_score"] - top10[-1]["final_score"]) / 30, 1.0) * 15
    confidence = int(s_market + s_ret + s_r2 + s_vol + s_candidates + s_spread)

    result["portfolio"] = portfolio
    result["confidence_score"] = confidence
    result["confidence_warning"] = confidence < 60

    os.makedirs(os.path.dirname(DM_OUTPUT_PATH), exist_ok=True)
    with open(DM_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"듀얼 모멘텀: {market_regime} - {DM_N}종목 선정 (신뢰도 {confidence}점, 후보 {len(universe)}개/{len(dm_candidates)}개)")
    for p in portfolio:
        print(f"  #{p['rank']} {p['name']}({p['code']}) 6-1M {p['return_6_1m']:.1%} R²={p['r_squared']:.2f} 점수 {p['final_score']}")


def run_screener():
    # 전일 추천 종목 성과 추적 (스크리너 시작 전)
    try:
        from performance_tracker import track_performance
        print("=== 성과 추적 ===")
        track_performance()
        print()
    except Exception as e:
        print(f"성과 추적 실패 (계속 진행): {e}\n")

    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    today_str = end.strftime("%Y-%m-%d")

    # 매크로 레짐 판별
    macro = get_market_regime()
    score_threshold = macro["score_adjustment"]  # 시그널 기준 조정값

    # 관심도 히스토리 로드
    att_history = load_attention_history()

    tickers = get_all_tickers()

    signals = {
        "golden_cross": [],
        "breakout": [],
        "pullback": [],
        "oversold": [],
        "volume_spike": [],
        "mean_reversion": [],
        "dual_momentum": [],
        "donchian_breakout": [],
        "new_high": [],
        "bounce_after_drop": [],
    }

    day_trade_candidates = []  # 단타 모듈용 데이터 수집
    dm_candidates = []  # 듀얼 모멘텀용 데이터 수집

    for i, ticker in enumerate(tickers):
        code = ticker["code"]
        name = ticker["name"]

        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(tickers)} 처리 중... ({name})")

        df = get_ohlcv(code, start_str, end_str)
        if df is None or len(df) < MIN_DAYS:
            continue

        last = df.iloc[-1]
        if last["close"] < MIN_PRICE or last["volume"] < MIN_VOLUME:
            continue

        df = calc_indicators(df)
        if df is None:
            continue

        last = df.iloc[-1]  # 지표 계산 후 재할당

        # 듀얼 모멘텀 데이터 수집 (신호 유무 무관, 모든 종목)
        dm_entry = _dm_calc_entry(code, name, ticker["market"], df)
        if dm_entry is not None:
            dm_candidates.append(dm_entry)

        # 기본 스코어 (기술적)
        score, reasons = score_stock(df)
        stock_signals = check_signals(df, score, reasons)

        if not stock_signals:
            continue

        # 시그널 발생 종목만 펀더멘탈 + 관심도 + 공시 수집 (API 호출 절약)
        fund = get_fundamental_data(code)
        time.sleep(NAVER_API_DELAY)
        current_rate = get_naver_attention(code)
        time.sleep(NAVER_API_DELAY)

        # 네거티브 공시 체크
        neg_flag, neg_title = check_negative_disclosure(code)
        if neg_flag:
            fund["_negative_disclosure"] = True
            fund["_negative_title"] = neg_title
        time.sleep(NAVER_API_DELAY)

        # 히스토리 대비 급증 판단
        surge_ratio, avg_rate = calc_attention_surge(code, current_rate, att_history)
        score, reasons = score_stock(df, fund, (surge_ratio, avg_rate))

        # 히스토리에 오늘 데이터 기록
        if current_rate > 0:
            if code not in att_history:
                att_history[code] = []
            att_history[code].append({"date": today_str, "rate": current_rate})

        entry, stop_loss, target = calc_atr_targets(df)

        for sig_type, sig_detail in stock_signals:
            change_rate = 0
            if len(df) >= 2:
                change_rate = round((last["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"] * 100, 2)

            signals[sig_type].append({
                "code": code,
                "name": name,
                "market": ticker["market"],
                "price": int(last["close"]),
                "change_rate": change_rate,
                "score": score,
                "signal_detail": sig_detail,
                "reasons": ", ".join(reasons[:5]),
                "entry": entry,
                "stop_loss": stop_loss,
                "target": target,
                "per": fund.get("per"),
                "pbr": fund.get("pbr"),
                "foreign_ratio": fund.get("foreign_ratio"),
                "dividend_yield": fund.get("dividend_yield"),
                "eps": fund.get("eps"),
                "attention": current_rate,
                "attention_surge": surge_ratio,
                "attention_flag": surge_ratio >= 3,
                "negative_disclosure": neg_flag,
                "negative_title": neg_title if neg_flag else "",
                # pandas-ta 기술적 지표 (팩트 데이터)
                "rsi14": round(float(last.get("rsi14", 0) or 0), 1),
                "macd_line": round(float(last.get("macd_line", 0) or 0), 1),
                "macd_signal": round(float(last.get("macd_signal", 0) or 0), 1),
                "macd_hist": round(float(last.get("macd_hist", 0) or 0), 1),
                "bb_upper": int(last.get("bb_upper", 0) or 0),
                "bb_mid": int(last.get("bb_mid", 0) or 0),
                "bb_lower": int(last.get("bb_lower", 0) or 0),
                "atr14": round(float(last.get("atr14", 0) or 0), 1),
                "adx": round(float(last.get("adx", 0) or 0), 1),
                "stoch_k": round(float(last.get("stoch_k", 0) or 0), 1),
                "stoch_d": round(float(last.get("stoch_d", 0) or 0), 1),
                "obv": int(last.get("obv", 0) or 0),
                "ma5": int(last.get("ma5", 0) or 0),
                "ma20": int(last.get("ma20", 0) or 0),
                "ma60": int(last.get("ma60", 0) or 0),
                "ma120": int(last.get("ma120", 0) or 0),
                "ma200": int(last.get("ma200", 0) or 0),
                "donchian_upper": int(last.get("donchian_upper", 0) or 0),
                "donchian_lower": int(last.get("donchian_lower", 0) or 0),
                "mdd_60": round(float(last.get("mdd_60", 0) or 0) * 100, 1),
                "high_52w": int(last.get("high_52w", 0) or 0),
                "low_52w": int(last.get("low_52w", 0) or 0),
                "consecutive_days": int(last.get("consecutive", 0) or 0),
            })

        # 단타 모듈용 데이터 수집
        day_trade_candidates.append({
            "stock": {"code": code, "name": name, "attention_flag": surge_ratio >= 3, "swing_signals": [s[0] for s in stock_signals]},
            "df": df,
        })

    # 매크로 기준 적용: risk_off 시 최소 스코어 상향
    min_score = score_threshold  # VIX>30: 20점 이상만, VIX<15: -5(= 거의 모두 통과)
    for key in signals:
        signals[key] = [s for s in signals[key] if s["score"] >= min_score]
        signals[key] = sorted(signals[key], key=lambda x: x["score"], reverse=True)[:20]

    # 관심도 히스토리 저장
    save_attention_history(att_history)

    # 듀얼 모멘텀 포트폴리오 실행
    print("\n=== 듀얼 모멘텀 포트폴리오 ===")
    run_dual_momentum(dm_candidates, today_str)

    # 단타 모듈 실행
    day_trade = run_day_trade_module(day_trade_candidates, macro)

    # 킬스위치 상태 보존 (기존 signals.json에 auto_trade_enabled가 있으면 유지)
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            prev = json.load(f)
        prev_dt = prev.get("day_trade", {})
        if "auto_trade_enabled" in prev_dt:
            day_trade["auto_trade_enabled"] = prev_dt["auto_trade_enabled"]
        if "kill_switch_at" in prev_dt:
            day_trade["kill_switch_at"] = prev_dt["kill_switch_at"]
    except Exception:
        pass

    print(f"\n단타 시그널:")
    print(f"  장초반 공략: {len(day_trade['day_open_attack'])}개")
    print(f"  눌림 진입: {len(day_trade['day_pullback_entry'])}개")

    # Quantocracy 관련 글 매칭
    related_articles = get_related_articles()

    # 성과 데이터 로드
    perf_summary = {}
    try:
        with open(PERFORMANCE_PATH, "r", encoding="utf-8") as f:
            perf_summary = json.load(f).get("summary", {})
    except Exception:
        pass

    result = {
        "updated": datetime.now().isoformat(),
        "total_scanned": len(tickers),
        "market_regime": macro,
        "signals": signals,
        "day_trade": day_trade,
        "related_articles": related_articles,
        "summary": {k: len(v) for k, v in signals.items()},
        "performance": perf_summary,
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n완료! 결과: {OUTPUT_PATH}")
    for k, v in result["summary"].items():
        print(f"  {k}: {v}개")


###############################################################################
# 단타 모듈 — "내일의 단타"
###############################################################################

DAY_TRADE_EXCLUDED = ["스팩", "리츠", "SPAC"]


def day_trade_common_filter(stock, df):
    """공통 필터: 통과하면 True"""
    last = df.iloc[-1]
    close = last["close"]
    vol = last["volume"]

    # 거래대금 30억 이상
    trade_value = close * vol
    if trade_value < 3_000_000_000:
        return False

    # 연속 상승 5일 초과 제외
    consec = int(last.get("consecutive", 0) or 0)
    if consec > 5:
        return False

    # 상한가/하한가 제외 (변동 ±28% 이상)
    if len(df) >= 2:
        prev_close = df.iloc[-2]["close"]
        change_pct = abs(close - prev_close) / prev_close * 100 if prev_close > 0 else 0
        if change_pct > 28:
            return False

    # 종목명 필터
    name = stock.get("name", "")
    for kw in DAY_TRADE_EXCLUDED:
        if kw in name:
            return False

    return True


def day_trade_disqualifiers(df):
    """실격 사유 체크"""
    disq = []
    last = df.iloc[-1]

    if len(df) >= 2:
        prev = df.iloc[-2]
        prev_change = (prev["close"] - df.iloc[-3]["close"]) / df.iloc[-3]["close"] * 100 if len(df) >= 3 else 0
        # 급등 후 음봉
        if prev_change > 5 and last["close"] < last["open"]:
            disq.append("post_surge_reversal")
        # 거래량 급감 + 음봉
        if last["volume"] < prev["volume"] * 0.5 and last["close"] < last["open"]:
            disq.append("volume_dry_up")

    # 이상 급등 (±15%)
    if len(df) >= 2:
        change = abs(last["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"] * 100
        if change > 15:
            disq.append("extreme_move")

    return disq


def score_day_open_attack(df):
    """장초반 공략 스코어링 (100점 만점)"""
    last = df.iloc[-1]
    score = 0
    breakdown = {"momentum": 0, "volume": 0, "trend": 0, "volatility": 0}

    o, h, l, c = last["open"], last["high"], last["low"], last["close"]
    rng = h - l if h > l else 1

    # 모멘텀 (35)
    body_pct = (c - o) / o * 100 if o > 0 else 0
    if body_pct > 2:
        breakdown["momentum"] += 10
    if body_pct > 4:
        breakdown["momentum"] += 5
    if rng > 0 and (c - l) / rng > 0.75:  # 종가 상위 25%
        breakdown["momentum"] += 10
    macd_h = last.get("macd_hist", 0) or 0
    if len(df) >= 2:
        prev_macd_h = df.iloc[-2].get("macd_hist", 0) or 0
        if pd.notna(macd_h) and macd_h > 0 and macd_h > prev_macd_h:
            breakdown["momentum"] += 10

    # 거래량 (25)
    vol_ma20 = last.get("vol_ma20", 0) or 0
    if pd.notna(vol_ma20) and vol_ma20 > 0:
        vol_ratio = last["volume"] / vol_ma20
        if vol_ratio >= 2:
            breakdown["volume"] += 10
        if vol_ratio >= 3:
            breakdown["volume"] += 5
    trade_value = c * last["volume"]
    if trade_value >= 5_000_000_000:
        breakdown["volume"] += 5
    obv_rising = True
    if len(df) >= 5:
        for i in range(-5, -1):
            if (df.iloc[i].get("obv", 0) or 0) > (df.iloc[i + 1].get("obv", 0) or 0):
                obv_rising = False
                break
        if obv_rising:
            breakdown["volume"] += 5

    # 추세 (20)
    ma5 = last.get("ma5", 0) or 0
    ma20 = last.get("ma20", 0) or 0
    if pd.notna(ma5) and pd.notna(ma20) and c > ma5 > ma20:
        breakdown["trend"] += 10
    adx = last.get("adx", 0) or 0
    if pd.notna(adx):
        if adx > 25:
            breakdown["trend"] += 5
        if adx > 40:
            breakdown["trend"] += 5

    # 변동성 (20)
    atr = last.get("atr14", 0) or 0
    if pd.notna(atr) and atr > 0 and rng < atr * 0.8:
        breakdown["volatility"] += 10
    bb_upper = last.get("bb_upper", 0) or 0
    bb_lower = last.get("bb_lower", 0) or 0
    if pd.notna(bb_upper) and pd.notna(bb_lower) and bb_upper > bb_lower:
        bb_width = (bb_upper - bb_lower) / ((bb_upper + bb_lower) / 2) * 100
        if bb_width < 5:  # 스퀴즈
            breakdown["volatility"] += 5
    if len(df) >= 2:
        gap_pct = abs(o - df.iloc[-2]["close"]) / df.iloc[-2]["close"] * 100
        if gap_pct < 0.5:
            breakdown["volatility"] += 5

    score = sum(breakdown.values())
    return score, breakdown


def score_day_pullback_entry(df):
    """눌림 진입 스코어링 (100점 만점)"""
    last = df.iloc[-1]
    score = 0
    breakdown = {"trend": 0, "pullback": 0, "volume": 0, "volatility": 0}

    o, h, l, c = last["open"], last["high"], last["low"], last["close"]
    body = abs(c - o) if abs(c - o) > 0 else 1

    # 추세 건전성 (35)
    ma5 = last.get("ma5", 0) or 0
    ma20 = last.get("ma20", 0) or 0
    ma60 = last.get("ma60", 0) or 0
    if pd.notna(ma5) and pd.notna(ma20) and pd.notna(ma60) and ma5 > ma20 > ma60:
        breakdown["trend"] += 15
    rsi = last.get("rsi14", 0) or 0
    if pd.notna(rsi) and 45 <= rsi <= 65:
        breakdown["trend"] += 10
    consec = int(last.get("consecutive", 0) or 0)
    if 2 <= consec <= 4:
        breakdown["trend"] += 10

    # 눌림 시그널 (30)
    upper_shadow = h - max(o, c)
    if upper_shadow > body * 0.5:
        breakdown["pullback"] += 10
    if pd.notna(ma5) and ma5 > 0:
        dist_ma5 = (c - ma5) / ma5 * 100
        if -1 <= dist_ma5 <= 1:
            breakdown["pullback"] += 10
    stoch_k = last.get("stoch_k", 0) or 0
    if pd.notna(stoch_k) and 50 <= stoch_k <= 80:
        breakdown["pullback"] += 10

    # 거래량 (20)
    if len(df) >= 2:
        prev_vol = df.iloc[-2]["volume"]
        if prev_vol > 0 and last["volume"] < prev_vol * 0.7:
            breakdown["volume"] += 10
    trade_value = c * last["volume"]
    if trade_value >= 5_000_000_000:
        breakdown["volume"] += 5
    obv = last.get("obv", 0) or 0
    if len(df) >= 2:
        prev_obv = df.iloc[-2].get("obv", 0) or 0
        if pd.notna(obv) and pd.notna(prev_obv) and obv >= prev_obv:
            breakdown["volume"] += 5

    # 변동성 (15)
    atr = last.get("atr14", 0) or 0
    if pd.notna(atr) and c > 0 and atr / c > 0.015:
        breakdown["volatility"] += 10
    bb_mid = last.get("bb_mid", 0) or 0
    bb_upper = last.get("bb_upper", 0) or 0
    if pd.notna(bb_mid) and pd.notna(bb_upper) and c > bb_mid and c < bb_upper:
        breakdown["volatility"] += 5

    score = sum(breakdown.values())
    return score, breakdown


def run_day_trade_module(all_stock_data, macro):
    """단타 모듈: 기존 시그널 종목 풀에서 단타 적합 종목 선별"""
    # risk_off 시 단타 미생성
    if macro.get("regime") == "risk_off":
        return {"day_open_attack": [], "day_pullback_entry": [], "generated_at": datetime.now().isoformat(), "macro_regime": macro.get("regime", "")}

    open_attack = []
    pullback_entry = []

    for stock_info in all_stock_data:
        stock = stock_info["stock"]
        df = stock_info["df"]

        if not day_trade_common_filter(stock, df):
            continue

        disq = day_trade_disqualifiers(df)
        if disq:
            continue

        last = df.iloc[-1]
        atr = float(last.get("atr14", 0) or 0)
        close = int(last["close"])

        # v4b 필터: ADX 최소 추세
        adx_val = float(last.get("adx", 0) or 0)
        if pd.notna(adx_val) and adx_val < 15:
            continue

        # v4b 필터: 정배열 필수 (MA5>MA20>MA60)
        ma5 = last.get("ma5", 0) or 0
        ma20 = last.get("ma20", 0) or 0
        ma60 = last.get("ma60", 0) or 0
        if not (pd.notna(ma5) and pd.notna(ma20) and pd.notna(ma60) and ma5 > ma20 > ma60):
            continue

        # v4b 필터: 눌림 진입용 거래량 급감 체크
        vol_decline_ok = True
        if len(df) >= 10:
            recent_vols = df["volume"].iloc[-3:]
            peak_vol = df["volume"].iloc[-10:-3].max()
            if peak_vol > 0 and recent_vols.mean() > peak_vol * 0.5:
                vol_decline_ok = False

        # v4c 가산점 계산
        bonus = 0
        vol_ma20 = last.get("vol_ma20", 0) or 0
        if pd.notna(vol_ma20) and vol_ma20 > 0 and last["volume"] >= vol_ma20 * 2:
            prev_surges = 0
            for k in range(max(len(df) - 60, 0), len(df) - 1):
                row_k = df.iloc[k]
                v20 = row_k.get("vol_ma20", 0) or 0
                if pd.notna(v20) and v20 > 0 and row_k["volume"] >= v20 * 2:
                    if last["close"] > row_k["high"]:
                        prev_surges += 1
            if prev_surges >= 1:
                bonus += 10

        bullish_count = 0
        for k in range(max(len(df) - 120, 0), len(df)):
            row_k = df.iloc[k]
            trade_val = row_k["close"] * row_k["volume"]
            body_pct = (row_k["close"] - row_k["open"]) / row_k["open"] * 100 if row_k["open"] > 0 else 0
            if trade_val >= 100_000_000_000 and body_pct >= 10:
                bullish_count += 1
        if bullish_count >= 3:
            bonus += 10
        elif bullish_count >= 1:
            bonus += 5

        if len(df) >= 5:
            recent_trade_vals = df["close"].iloc[-5:] * df["volume"].iloc[-5:]
            if (recent_trade_vals >= 50_000_000_000).any():
                bonus += 5

        # 장초반 공략 (커트라인 70, v4c 가산 적용)
        oa_score, oa_breakdown = score_day_open_attack(df)
        oa_score += bonus
        if oa_score >= 70:
            sl = int(close - 0.9 * atr) if atr > 0 else int(close * 0.98)
            tp = int(close + 1.3 * atr) if atr > 0 else int(close * 1.025)
            rr = round(1.0 / 0.7, 2) if atr > 0 else 1.0
            open_attack.append({
                "code": stock["code"],
                "name": stock["name"],
                "close": close,
                "day_trade_score": oa_score,
                "score_breakdown": oa_breakdown,
                "entry_guide": {
                    "timing": "09:00~09:30",
                    "entry": close,
                    "stop_loss": sl,
                    "target": tp,
                    "risk_reward": f"1:{rr}",
                    "atr14": round(atr, 1),
                },
                "swing_signals": stock.get("swing_signals", []),
                "confidence": "강한" if oa_score >= 85 else "보통" if oa_score >= 75 else "약한",
                "attention_flag": stock.get("attention_flag", False),
            })

        # 눌림 진입 (커트라인 65, v4c 가산, 거래량 급감 필수)
        pe_score, pe_breakdown = score_day_pullback_entry(df)
        pe_score += bonus
        if pe_score >= 65 and vol_decline_ok:
            sl = int(close - 0.9 * atr) if atr > 0 else int(close * 0.98)
            tp = int(close + 1.3 * atr) if atr > 0 else int(close * 1.025)
            pullback_entry.append({
                "code": stock["code"],
                "name": stock["name"],
                "close": close,
                "day_trade_score": pe_score,
                "score_breakdown": pe_breakdown,
                "entry_guide": {
                    "timing": "09:30~10:00",
                    "entry": close,
                    "stop_loss": sl,
                    "target": tp,
                    "risk_reward": f"1:{round(1.0 / 0.7, 2)}" if atr > 0 else "1:1",
                    "atr14": round(atr, 1),
                },
                "swing_signals": stock.get("swing_signals", []),
                "confidence": "강한" if pe_score >= 85 else "보통" if pe_score >= 75 else "약한",
                "attention_flag": stock.get("attention_flag", False),
            })

    # 중복 제거: 양쪽 모두 3위 이내 → 스코어 높은 쪽 배정
    open_attack = sorted(open_attack, key=lambda x: x["day_trade_score"], reverse=True)
    pullback_entry = sorted(pullback_entry, key=lambda x: x["day_trade_score"], reverse=True)

    oa_codes = {s["code"] for s in open_attack[:3]}
    pe_codes = {s["code"] for s in pullback_entry[:3]}
    overlap = oa_codes & pe_codes
    for code in overlap:
        oa_item = next(s for s in open_attack if s["code"] == code)
        pe_item = next(s for s in pullback_entry if s["code"] == code)
        if oa_item["day_trade_score"] >= pe_item["day_trade_score"]:
            pullback_entry = [s for s in pullback_entry if s["code"] != code]
        else:
            open_attack = [s for s in open_attack if s["code"] != code]

    return {
        "generated_at": datetime.now().isoformat(),
        "macro_regime": macro.get("regime", ""),
        "day_open_attack": open_attack[:3],
        "day_pullback_entry": pullback_entry[:3],
    }


if __name__ == "__main__":
    run_screener()
