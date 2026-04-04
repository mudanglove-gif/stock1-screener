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

LOOKBACK_DAYS = 250
NAVER_API_DELAY = 0.1
FRED_API_KEY = "d41ee4f2e4718a0e25f8dfabaabe3ec4"


def get_market_regime():
    """FRED API로 시장 국면 판별 (VIX + 금리)"""
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)

        vix = fred.get_series("VIXCLS").dropna().iloc[-1]
        us10y = fred.get_series("DGS10").dropna().iloc[-1]

        # 레짐 판별
        if vix > 30:
            regime = "risk_off"
            desc = f"공포 (VIX:{vix:.1f}, 금리:{us10y:.2f}%)"
            score_adj = 20  # 시그널 기준 엄격화
        elif vix > 20:
            regime = "caution"
            desc = f"주의 (VIX:{vix:.1f}, 금리:{us10y:.2f}%)"
            score_adj = 10
        elif vix < 15:
            regime = "risk_on"
            desc = f"낙관 (VIX:{vix:.1f}, 금리:{us10y:.2f}%)"
            score_adj = -5  # 기준 완화
        else:
            regime = "neutral"
            desc = f"중립 (VIX:{vix:.1f}, 금리:{us10y:.2f}%)"
            score_adj = 0

        print(f"시장 국면: {regime} — {desc}")
        return {
            "regime": regime,
            "description": desc,
            "vix": round(float(vix), 2),
            "us10y": round(float(us10y), 2),
            "score_adjustment": score_adj,
        }
    except Exception as e:
        print(f"FRED API 조회 실패: {e}")
        return {"regime": "unknown", "description": "매크로 데이터 없음", "vix": 0, "us10y": 0, "score_adjustment": 0}


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
    df["ma60"] = ta.sma(df["close"], length=60)
    df["ma120"] = ta.sma(df["close"], length=120)
    df["ma200"] = ta.sma(df["close"], length=200)

    # RSI
    df["rsi14"] = ta.rsi(df["close"], length=14)

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

    # 모멘텀 (Quantocracy 검증: 12-1개월 모멘텀)
    if len(df) >= 252:
        df["mom_12m"] = df["close"].pct_change(252)
    if len(df) >= 21:
        df["mom_1m"] = df["close"].pct_change(21)

    # === v3 추가 지표 ===

    # Donchian Channel (Quantocracy 7건 — 터틀 트레이딩)
    df["donchian_upper"] = df["high"].rolling(window=20).max()
    df["donchian_lower"] = df["low"].rolling(window=20).min()

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


def score_stock(df, fundamental=None):
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

    return score, reasons


def calc_atr_targets(df):
    """ATR 기반 진입/손절/목표 (Quantocracy 94회 — 가장 검증된 리스크 관리)"""
    last = df.iloc[-1]
    atr = last.get("atr14", 0)
    close = last["close"]

    if not pd.notna(atr) or atr == 0:
        return int(close), int(close * 0.95), int(close * 1.10)

    entry = int(close)
    stop_loss = int(close - 2 * atr)    # 2 ATR 손절
    target = int(close + 3 * atr)       # 3 ATR 목표 (손익비 1.5:1)

    return entry, stop_loss, target


def check_signals(df, score, reasons):
    """시그널 카테고리 분류"""
    if df is None or len(df) < 25:
        return []
    last = df.iloc[-1]
    prev = df.iloc[-2]
    signals = []

    # 골든크로스
    if len(df) >= 6:
        for i in range(-5, 0):
            p, c = df.iloc[i - 1], df.iloc[i]
            if (pd.notna(p.get("ma5")) and pd.notna(p.get("ma20")) and
                    pd.notna(c.get("ma5")) and pd.notna(c.get("ma20"))):
                if p["ma5"] <= p["ma20"] and c["ma5"] > c["ma20"]:
                    vol_r = last["volume"] / last["vol_ma20"] * 100 if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0 else 0
                    if vol_r > 100:
                        signals.append(("golden_cross", f"MA5({int(last['ma5'])}) > MA20({int(last['ma20'])}), 거래량 {vol_r:.0f}%"))
                        break

    # 추세돌파
    if len(df) >= 21:
        prev_high = df["high"].iloc[-21:-1].max()
        vol_r = last["volume"] / last["vol_ma20"] * 100 if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0 else 0
        if last["close"] > prev_high and vol_r > 200 and last["close"] > last["open"]:
            signals.append(("breakout", f"20일 최고 {int(prev_high)} 돌파, 거래량 {vol_r:.0f}%"))

    # 눌림목
    if (pd.notna(last.get("ma5")) and pd.notna(last.get("ma20")) and pd.notna(last.get("ma60"))):
        if last["ma5"] > last["ma20"] > last["ma60"]:
            dist = abs(last["close"] - last["ma20"]) / last["ma20"] if last["ma20"] > 0 else 1
            if dist < 0.03 and last["close"] > last["open"]:
                signals.append(("pullback", f"정배열, MA20({int(last['ma20'])}) 지지 반등"))

    # 과매도 반등
    if pd.notna(last.get("rsi14")) and pd.notna(prev.get("rsi14")):
        if prev["rsi14"] < 35 and last["rsi14"] > prev["rsi14"] and last["close"] > last["open"]:
            macd_turn = (pd.notna(last.get("macd_hist")) and pd.notna(prev.get("macd_hist")) and
                         prev["macd_hist"] < 0 and last["macd_hist"] > prev["macd_hist"])
            if macd_turn:
                signals.append(("oversold", f"RSI {last['rsi14']:.1f} 반등, MACD 반전"))

    # 거래량 폭발
    if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0:
        vol_r = last["volume"] / last["vol_ma20"] * 100
        if vol_r > 300 and last["close"] > last["open"]:
            if pd.notna(last.get("ma20")) and last["close"] > last["ma20"]:
                signals.append(("volume_spike", f"거래량 {vol_r:.0f}%, 양봉, MA20 위"))

    # 평균회귀 (Quantocracy 97회 — 새 시그널)
    if pd.notna(last.get("bb_lower")) and pd.notna(last.get("rsi14")):
        if last["close"] <= last["bb_lower"] and last["rsi14"] < 30:
            signals.append(("mean_reversion", f"볼린저 하단 이탈 + RSI {last['rsi14']:.0f} 과매도"))

    # 듀얼모멘텀 (Quantocracy 64회)
    if pd.notna(last.get("mom_12m")) and pd.notna(last.get("mom_1m")):
        if last["mom_12m"] > 0.15 and last["mom_1m"] > 0 and score >= 50:
            signals.append(("dual_momentum", f"12M 수익률 {last['mom_12m']:.1%}, 1M {last['mom_1m']:.1%}, 스코어 {score}"))

    # === v3 시그널 ===

    # Donchian 채널 돌파 (Quantocracy 7건 — 터틀 트레이딩)
    if pd.notna(last.get("donchian_upper")):
        if last["close"] > last["donchian_upper"] and last["close"] > last["open"]:
            vol_r = last["volume"] / last["vol_ma20"] * 100 if pd.notna(last.get("vol_ma20")) and last["vol_ma20"] > 0 else 0
            if vol_r > 150:
                signals.append(("donchian_breakout", f"20일 Donchian 상단({int(last['donchian_upper'])}) 돌파, 거래량 {vol_r:.0f}%"))

    # 52주 신고가 (Quantocracy 29건)
    if pd.notna(last.get("high_52w")):
        dist = (last["high_52w"] - last["close"]) / last["high_52w"]
        if dist < 0.01 and last["close"] > last["open"]:
            signals.append(("new_high", f"52주 신고가 {int(last['high_52w'])} 근접/경신"))

    # 연속 하락 후 반등 (Quantocracy 7건)
    consec = last.get("consecutive_days", 0)
    if isinstance(consec, (int, float)) and pd.notna(consec):
        if df.iloc[-2].get("consecutive_days", 0) <= -3 and last["close"] > last["open"]:
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


def run_screener():
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    # 매크로 레짐 판별
    macro = get_market_regime()
    score_threshold = macro["score_adjustment"]  # 시그널 기준 조정값

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

        # 기본 스코어 (기술적)
        score, reasons = score_stock(df)
        stock_signals = check_signals(df, score, reasons)

        if not stock_signals:
            continue

        # 시그널 발생 종목만 펀더멘탈 데이터 수집 (API 호출 절약)
        fund = get_fundamental_data(code)
        if fund:
            score, reasons = score_stock(df, fund)
            time.sleep(NAVER_API_DELAY)

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
            })

    # 매크로 기준 적용: risk_off 시 최소 스코어 상향
    min_score = score_threshold  # VIX>30: 20점 이상만, VIX<15: -5(= 거의 모두 통과)
    for key in signals:
        signals[key] = [s for s in signals[key] if s["score"] >= min_score]
        signals[key] = sorted(signals[key], key=lambda x: x["score"], reverse=True)[:20]

    # Quantocracy 관련 글 매칭
    related_articles = get_related_articles()

    result = {
        "updated": datetime.now().isoformat(),
        "total_scanned": len(tickers),
        "market_regime": macro,
        "signals": signals,
        "related_articles": related_articles,
        "summary": {k: len(v) for k, v in signals.items()},
    }

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n완료! 결과: {OUTPUT_PATH}")
    for k, v in result["summary"].items():
        print(f"  {k}: {v}개")


if __name__ == "__main__":
    run_screener()
