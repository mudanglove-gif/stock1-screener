"""
Stock1 기술적 스크리너 v2 (Quantocracy 검증 전략 적용)
- FinanceDataReader로 KOSPI/KOSDAQ 전 종목 OHLCV 수집
- pandas-ta로 기술적 지표 계산
- 복합 스코어링 시스템으로 종목 순위화
- ATR 기반 손절/목표가 산출
"""

import json
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta
import FinanceDataReader as fdr

LOOKBACK_DAYS = 250
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
        df["mom_12m"] = df["close"].pct_change(252)  # 12개월 수익률
    if len(df) >= 21:
        df["mom_1m"] = df["close"].pct_change(21)   # 1개월 수익률

    return df


def score_stock(df):
    """복합 스코어링 (Quantocracy 기반 다중 팩터)"""
    if df is None or len(df) < MIN_DAYS:
        return 0, []

    last = df.iloc[-1]
    prev = df.iloc[-2]
    score = 0
    reasons = []

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

    # 듀얼모멘텀 (Quantocracy 64회 — 새 시그널)
    if pd.notna(last.get("mom_12m")) and pd.notna(last.get("mom_1m")):
        if last["mom_12m"] > 0.15 and last["mom_1m"] > 0 and score >= 50:
            signals.append(("dual_momentum", f"12M 수익률 {last['mom_12m']:.1%}, 1M {last['mom_1m']:.1%}, 스코어 {score}"))

    return signals


def run_screener():
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    tickers = get_all_tickers()

    signals = {
        "golden_cross": [],
        "breakout": [],
        "pullback": [],
        "oversold": [],
        "volume_spike": [],
        "mean_reversion": [],
        "dual_momentum": [],
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

        score, reasons = score_stock(df)
        stock_signals = check_signals(df, score, reasons)
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
                "reasons": ", ".join(reasons[:3]),
                "entry": entry,
                "stop_loss": stop_loss,
                "target": target,
            })

    # 스코어 기준 정렬 + 상위 20개
    for key in signals:
        signals[key] = sorted(signals[key], key=lambda x: x["score"], reverse=True)[:20]

    result = {
        "updated": datetime.now().isoformat(),
        "total_scanned": len(tickers),
        "signals": signals,
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
