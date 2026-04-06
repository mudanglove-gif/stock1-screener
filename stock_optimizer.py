"""
Stock1 6종목 개별 최적화 모듈
- 시그널 발생 종목 각각에 대해 최적 ATR 파라미터 그리드 서치
- Walk-Forward 190일/60일 + 4중 과적합 방지
- 결과를 signals.json optimized_params에 저장

실행: aftermarket_checker.py 완료 후 자동 트리거
사용: py -3.12 stock_optimizer.py
"""

import gc
import json
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta
import FinanceDataReader as fdr

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "docs")
SIGNALS_PATH = os.path.join(OUTPUT_DIR, "signals.json")

# 그리드 서치 범위
ATR_PERIODS = [7, 10, 14, 20]
SL_MULTIPLIERS = [round(x / 10, 1) for x in range(5, 16)]      # 0.5~1.5
TP_MULTIPLIERS = [round(x / 10, 1) for x in range(8, 21)]      # 0.8~2.0
MIN_RR = 1.0  # 손익비 최소 1:1

# Walk-Forward
TRAIN_DAYS = 190
TEST_DAYS = 60

# 과적합 방지
MIN_TRADE_COUNT = 10        # 최소 거래 횟수
MIN_IMPROVEMENT_PCT = 5.0   # 공통값 대비 5% 미만 개선이면 공통값 사용

# 공통 기본값
COMMON_PARAMS = {
    "atr_period": 14,
    "sl_multiplier": 0.9,
    "tp_multiplier": 1.3,  # 눌림 진입 기본
}


def prepare_stock_data(code, days=300):
    """종목별 OHLCV + 멀티 ATR 계산"""
    end = datetime.now()
    start = end - timedelta(days=days + 100)
    try:
        df = fdr.DataReader(code, start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
        if df is None or df.empty:
            return None
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        df = df[["open", "high", "low", "close", "volume"]]
        df = df[df["volume"] > 0]
        if len(df) < 100:
            return None

        # 멀티 ATR 계산
        for period in ATR_PERIODS:
            df[f"atr_{period}"] = ta.atr(df["high"], df["low"], df["close"], length=period)

        return df
    except Exception as e:
        print(f"  {code} 데이터 로드 실패: {e}")
        return None


def backtest_day_trade_single(df, atr_period, sl_mult, tp_mult, eval_days=None):
    """
    단일 종목 단일 파라미터 조합 백테스트
    Returns: dict with win_rate, pf, avg_pnl, trade_count, etc.
    """
    atr_col = f"atr_{atr_period}"
    if atr_col not in df.columns:
        return None

    eval_df = df if eval_days is None else df.iloc[-eval_days:]
    if len(eval_df) < 10:
        return None

    wins = []
    losses = []
    holds = []

    for i in range(len(eval_df) - 1):
        row = eval_df.iloc[i]
        next_row = eval_df.iloc[i + 1]

        atr = row.get(atr_col)
        if pd.isna(atr) or atr <= 0:
            continue

        close = row["close"]
        sl = close - sl_mult * atr
        tp = close + tp_mult * atr

        d1_open = next_row["open"]
        d1_high = next_row["high"]
        d1_low = next_row["low"]
        d1_close = next_row["close"]

        if d1_open <= 0:
            continue

        # 갭 필터
        gap = abs(d1_open - close) / atr if atr > 0 else 0
        if gap > 1.5:
            continue

        t_hit = d1_high >= tp
        s_hit = d1_low <= sl

        if t_hit and not s_hit:
            pnl = (tp - d1_open) / d1_open * 100
            wins.append(pnl)
        elif s_hit:
            pnl = (sl - d1_open) / d1_open * 100
            losses.append(pnl)
        else:
            pnl = (d1_close - d1_open) / d1_open * 100
            holds.append(pnl)

    decided = len(wins) + len(losses)
    if decided == 0:
        return None

    win_rate = len(wins) / decided
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0
    total_win = sum(w for w in wins if w > 0)
    total_loss = abs(sum(l for l in losses if l < 0))
    pf = total_win / total_loss if total_loss > 0 else 999

    all_pnls = wins + losses + holds
    avg_pnl = sum(all_pnls) / len(all_pnls) if all_pnls else 0

    return {
        "trade_count": decided,
        "wins": len(wins),
        "losses": len(losses),
        "holds": len(holds),
        "win_rate": round(win_rate, 3),
        "avg_pnl": round(avg_pnl, 3),
        "avg_win": round(avg_win, 3),
        "avg_loss": round(avg_loss, 3),
        "pf": round(pf, 2),
    }


def calculate_score(result):
    """복합 스코어 (승률40+수익률25+손익비15+안정성20)"""
    if result is None or result["trade_count"] < MIN_TRADE_COUNT:
        return -999

    score = 0
    score += result["win_rate"] * 40
    score += min(result["avg_pnl"] * 5, 25)
    score += min(result["pf"] * 5, 15)
    # 안정성: 거래 횟수가 많을수록 안정 (최대 20)
    stability = min(result["trade_count"] / 30, 1.0)
    score += stability * 20

    return round(score, 1)


def optimize_stock_grid_search(df):
    """그리드 서치"""
    results = []
    for atr_p in ATR_PERIODS:
        for sl in SL_MULTIPLIERS:
            for tp in TP_MULTIPLIERS:
                if tp / sl < MIN_RR:
                    continue
                result = backtest_day_trade_single(df, atr_p, sl, tp)
                if result is None:
                    continue
                score = calculate_score(result)
                results.append({
                    "atr_period": atr_p,
                    "sl_multiplier": sl,
                    "tp_multiplier": tp,
                    "result": result,
                    "score": score,
                })
    if not results:
        return None
    return sorted(results, key=lambda x: x["score"], reverse=True)


def validate_walk_forward(df, best_params):
    """Walk-Forward 검증: 190일 훈련 / 60일 검증"""
    if len(df) < TRAIN_DAYS + TEST_DAYS:
        return None

    test_df = df.iloc[-TEST_DAYS:]
    test_result = backtest_day_trade_single(
        test_df,
        best_params["atr_period"],
        best_params["sl_multiplier"],
        best_params["tp_multiplier"],
    )
    return test_result


def check_parameter_stability(grid_results, best):
    """파라미터 안정 구간 확인 — 최적값 ±1단계도 양호한지"""
    sl_step = 0.1
    tp_step = 0.1
    neighbors = []
    for r in grid_results:
        if r["atr_period"] != best["atr_period"]:
            continue
        sl_diff = abs(r["sl_multiplier"] - best["sl_multiplier"])
        tp_diff = abs(r["tp_multiplier"] - best["tp_multiplier"])
        if sl_diff <= sl_step + 0.001 and tp_diff <= tp_step + 0.001:
            neighbors.append(r["score"])
    if len(neighbors) < 3:
        return False
    avg_neighbor = sum(neighbors) / len(neighbors)
    return avg_neighbor >= best["score"] * 0.85  # 평균이 최적의 85% 이상이면 안정


def optimize_stock(code, name):
    """단일 종목 최적화"""
    print(f"\n[{code} {name}] 최적화 시작")
    df = prepare_stock_data(code, days=300)
    if df is None or len(df) < 100:
        print(f"  데이터 부족 → 공통값 사용")
        return {**COMMON_PARAMS, "method": "common", "reason": "데이터 부족"}

    # 1. 그리드 서치
    grid_results = optimize_stock_grid_search(df)
    if not grid_results:
        return {**COMMON_PARAMS, "method": "common", "reason": "유효 결과 없음"}

    best = grid_results[0]
    print(f"  최적 후보: ATR{best['atr_period']} SL{best['sl_multiplier']} TP{best['tp_multiplier']} 스코어{best['score']}")
    print(f"    거래 {best['result']['trade_count']}건, 승률 {best['result']['win_rate']*100:.1f}%, PF {best['result']['pf']}")

    # 2. 최소 거래 횟수
    if best["result"]["trade_count"] < MIN_TRADE_COUNT:
        print(f"  거래 {best['result']['trade_count']}건 < {MIN_TRADE_COUNT} → 공통값 사용")
        return {**COMMON_PARAMS, "method": "common", "reason": "거래 부족"}

    # 3. 파라미터 안정성
    if not check_parameter_stability(grid_results, best):
        print(f"  파라미터 안정성 부족 → 공통값 사용")
        return {**COMMON_PARAMS, "method": "common", "reason": "안정성 부족"}

    # 4. Walk-Forward 검증
    wf_result = validate_walk_forward(df, best)
    if wf_result is None or wf_result["trade_count"] < 3:
        print(f"  Walk-Forward 검증 데이터 부족 → 공통값 사용")
        return {**COMMON_PARAMS, "method": "common", "reason": "WF 검증 불가"}

    print(f"  Walk-Forward(60일): 승률 {wf_result['win_rate']*100:.1f}%, PF {wf_result['pf']}")

    # 5. 공통값 대비 개선폭 확인
    common_result = backtest_day_trade_single(
        df, COMMON_PARAMS["atr_period"], COMMON_PARAMS["sl_multiplier"], COMMON_PARAMS["tp_multiplier"]
    )
    if common_result:
        common_score = calculate_score(common_result)
        improvement = (best["score"] - common_score) / max(common_score, 1) * 100
        if improvement < MIN_IMPROVEMENT_PCT:
            print(f"  공통값 대비 개선 {improvement:.1f}% < {MIN_IMPROVEMENT_PCT}% → 공통값 사용")
            return {**COMMON_PARAMS, "method": "common", "reason": f"개선폭 {improvement:.1f}%"}

    # 채택
    print(f"  ✅ 개별 최적화 채택")
    return {
        "atr_period": best["atr_period"],
        "sl_multiplier": best["sl_multiplier"],
        "tp_multiplier": best["tp_multiplier"],
        "method": "individual",
        "reason": "검증 통과",
        "backtest_win_rate": best["result"]["win_rate"],
        "backtest_pf": best["result"]["pf"],
        "backtest_trade_count": best["result"]["trade_count"],
        "wf_win_rate": wf_result["win_rate"],
        "wf_pf": wf_result["pf"],
        "score": best["score"],
    }


def optimize_all_day_trade_stocks():
    """signals.json에서 단타 종목 추출 → 각각 최적화"""
    print(f"=== Stock1 6종목 개별 최적화 ===")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if not os.path.exists(SIGNALS_PATH):
        print(f"signals.json 없음: {SIGNALS_PATH}")
        return

    with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
        signals = json.load(f)

    day_trade = signals.get("day_trade", {})
    target_stocks = []
    for sig_type in ["day_open_attack", "day_pullback_entry"]:
        for stock in day_trade.get(sig_type, []):
            if stock.get("after_market", {}).get("disqualified"):
                continue
            target_stocks.append((sig_type, stock))

    if not target_stocks:
        print("최적화 대상 종목 없음")
        return

    print(f"대상: {len(target_stocks)}종목")

    for sig_type, stock in target_stocks:
        code = stock["code"]
        name = stock["name"]
        params = optimize_stock(code, name)
        stock["optimized_params"] = params

        # 진입가/손절가/목표가 재계산 (개별 최적화 적용)
        if params["method"] == "individual":
            close = stock["close"]
            # 마지막 ATR 값으로 가격 재산출
            df = prepare_stock_data(code, days=50)
            if df is not None:
                atr_col = f"atr_{params['atr_period']}"
                if atr_col in df.columns:
                    atr = float(df[atr_col].iloc[-1])
                    if atr > 0:
                        stock["entry_guide"]["stop_loss"] = int(close - params["sl_multiplier"] * atr)
                        stock["entry_guide"]["target"] = int(close + params["tp_multiplier"] * atr)
                        stock["entry_guide"]["atr14"] = round(atr, 1)
                        stock["entry_guide"]["risk_reward"] = f"1:{round(params['tp_multiplier']/params['sl_multiplier'], 2)}"

    # 저장
    with open(SIGNALS_PATH, "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)

    print(f"\n완료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"signals.json 업데이트: {SIGNALS_PATH}")


if __name__ == "__main__":
    optimize_all_day_trade_stocks()
