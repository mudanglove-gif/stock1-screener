"""
Stock1 기술적 스크리너
- pykrx로 KOSPI/KOSDAQ 전 종목 OHLCV 수집
- pandas-ta로 기술적 지표 계산
- 시그널 발생 종목 필터링 → signals.json 출력
"""

import json
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta
import FinanceDataReader as fdr

# ── 설정 ──
LOOKBACK_DAYS = 150  # OHLCV 수집 기간 (120일 지표 + 여유)
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "signals.json")


def get_all_tickers():
    """KOSPI + KOSDAQ 전 종목 코드 수집 (FinanceDataReader)"""
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
    """종목별 OHLCV 수집 (FinanceDataReader)"""
    try:
        df = fdr.DataReader(code, start, end)
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df = df[["open", "high", "low", "close", "volume"]]
        df = df[df["volume"] > 0]
        return df
    except Exception:
        return None


def calc_indicators(df):
    """기술적 지표 계산 (pandas-ta)"""
    if df is None or len(df) < 60:
        return None

    df["ma5"] = ta.sma(df["close"], length=5)
    df["ma20"] = ta.sma(df["close"], length=20)
    df["ma60"] = ta.sma(df["close"], length=60)
    df["ma120"] = ta.sma(df["close"], length=120)
    df["rsi14"] = ta.rsi(df["close"], length=14)

    macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
    if macd_df is not None:
        df["macd_line"] = macd_df.iloc[:, 0]
        df["macd_signal"] = macd_df.iloc[:, 1]
        df["macd_hist"] = macd_df.iloc[:, 2]

    df["vol_ma20"] = ta.sma(df["volume"].astype(float), length=20)

    return df


def check_golden_cross(df):
    """골든크로스: 최근 5일 이내 MA5가 MA20 상향 돌파"""
    if df is None or len(df) < 25:
        return None
    recent = df.tail(6)
    if len(recent) < 6:
        return None

    for i in range(1, len(recent)):
        prev = recent.iloc[i - 1]
        curr = recent.iloc[i]
        if (pd.notna(prev["ma5"]) and pd.notna(prev["ma20"]) and
                pd.notna(curr["ma5"]) and pd.notna(curr["ma20"])):
            if prev["ma5"] <= prev["ma20"] and curr["ma5"] > curr["ma20"]:
                vol_ratio = curr["volume"] / curr["vol_ma20"] * 100 if curr["vol_ma20"] > 0 else 0
                if vol_ratio > 100:  # 평균 이상 거래량
                    last = df.iloc[-1]
                    return {
                        "signal_detail": f"MA5({int(last['ma5'])}) > MA20({int(last['ma20'])}), 거래량 {vol_ratio:.0f}%",
                        "entry": int(last["close"]),
                        "stop_loss": int(last["ma20"] * 0.97),
                        "target": int(last["close"] * 1.10),
                    }
    return None


def check_breakout(df):
    """추세 돌파: 20일 최고가 돌파 + 거래량 200%+"""
    if df is None or len(df) < 25:
        return None
    last = df.iloc[-1]
    prev_high = df["high"].iloc[-21:-1].max()
    vol_ratio = last["volume"] / last["vol_ma20"] * 100 if last["vol_ma20"] > 0 else 0

    if last["close"] > prev_high and vol_ratio > 200 and last["close"] > last["open"]:
        return {
            "signal_detail": f"20일 최고 {int(prev_high)} 돌파, 거래량 {vol_ratio:.0f}%",
            "entry": int(last["close"]),
            "stop_loss": int(prev_high * 0.97),
            "target": int(last["close"] * 1.15),
        }
    return None


def check_pullback(df):
    """눌림목: 정배열 + 20일선 부근 지지 반등"""
    if df is None or len(df) < 65:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]

    if not all(pd.notna([last["ma5"], last["ma20"], last["ma60"]])):
        return None

    # 정배열 확인
    if not (last["ma5"] > last["ma20"] > last["ma60"]):
        return None

    # 20일선 부근 (±3%) 에서 반등
    dist = abs(last["close"] - last["ma20"]) / last["ma20"]
    if dist < 0.03 and last["close"] > last["open"] and prev["close"] <= prev.get("ma20", 0):
        return {
            "signal_detail": f"정배열, MA20({int(last['ma20'])}) 지지 반등",
            "entry": int(last["close"]),
            "stop_loss": int(last["ma20"] * 0.95),
            "target": int(last["ma5"] * 1.05),
        }
    return None


def check_oversold(df):
    """과매도 반등: RSI < 35에서 반등"""
    if df is None or len(df) < 20:
        return None
    last = df.iloc[-1]
    prev = df.iloc[-2]

    if not (pd.notna(last["rsi14"]) and pd.notna(prev["rsi14"])):
        return None

    # RSI 30 이하에서 반등하여 현재 35~50 구간
    if prev["rsi14"] < 35 and 30 < last["rsi14"] < 50 and last["close"] > last["open"]:
        # MACD 히스토그램 반전 확인
        macd_signal = False
        if "macd_hist" in df.columns:
            h = df["macd_hist"]
            if len(h) >= 2 and pd.notna(h.iloc[-1]) and pd.notna(h.iloc[-2]):
                if h.iloc[-2] < 0 and h.iloc[-1] > h.iloc[-2]:
                    macd_signal = True

        if macd_signal:
            return {
                "signal_detail": f"RSI {last['rsi14']:.1f} (과매도 탈출), MACD 반전",
                "entry": int(last["close"]),
                "stop_loss": int(last["low"] * 0.95),
                "target": int(last["close"] * 1.12),
            }
    return None


def check_volume_spike(df):
    """거래량 폭발: 20일 평균 대비 300%+ + 양봉"""
    if df is None or len(df) < 25:
        return None
    last = df.iloc[-1]

    if not pd.notna(last["vol_ma20"]) or last["vol_ma20"] == 0:
        return None

    vol_ratio = last["volume"] / last["vol_ma20"] * 100

    if vol_ratio > 300 and last["close"] > last["open"]:
        # 주요 이평선 위 확인
        above_ma = pd.notna(last["ma20"]) and last["close"] > last["ma20"]
        if above_ma:
            return {
                "signal_detail": f"거래량 {vol_ratio:.0f}% (20일평균 대비), 양봉, MA20 위",
                "entry": int(last["close"]),
                "stop_loss": int(last["low"] * 0.95),
                "target": int(last["close"] * 1.15),
            }
    return None


def run_screener():
    """메인 스크리너 실행"""
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
    }

    checkers = {
        "golden_cross": check_golden_cross,
        "breakout": check_breakout,
        "pullback": check_pullback,
        "oversold": check_oversold,
        "volume_spike": check_volume_spike,
    }

    for i, ticker in enumerate(tickers):
        code = ticker["code"]
        name = ticker["name"]

        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{len(tickers)} 처리 중... ({name})")

        df = get_ohlcv(code, start_str, end_str)
        if df is None or len(df) < 60:
            continue

        # 시가총액 너무 작은 종목 제외 (종가 * 거래량 기준 간이 필터)
        last = df.iloc[-1]
        if last["close"] < 1000 or last["volume"] < 10000:
            continue

        df = calc_indicators(df)
        if df is None:
            continue

        for signal_name, checker in checkers.items():
            result = checker(df)
            if result:
                entry = {
                    "code": code,
                    "name": name,
                    "market": ticker["market"],
                    "price": int(last["close"]),
                    "change_rate": round(
                        (last["close"] - df.iloc[-2]["close"]) / df.iloc[-2]["close"] * 100, 2
                    ) if len(df) >= 2 else 0,
                    **result,
                }
                signals[signal_name].append(entry)

    # 각 시그널 카테고리별 상위 정렬 (거래량 비율 높은 순)
    for key in signals:
        signals[key] = signals[key][:20]  # 최대 20개

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
