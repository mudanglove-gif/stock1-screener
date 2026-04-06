"""
Stock1 성과 추적 모듈
- 전일 추천 종목의 D+1 실제 결과를 수집
- performance.json에 누적 저장
- 매일 16:00 스크리너 실행 시 자동 호출
"""

import json
import os
from datetime import datetime, timedelta

import FinanceDataReader as fdr

SIGNALS_PATH = os.path.join(os.path.dirname(__file__), "docs", "signals.json")
PERFORMANCE_PATH = os.path.join(os.path.dirname(__file__), "docs", "performance.json")


def load_performance():
    try:
        with open(PERFORMANCE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"records": [], "summary": {}}


def save_performance(data):
    with open(PERFORMANCE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def load_signals():
    try:
        with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def get_today_ohlcv(code):
    """오늘(D+1) OHLCV 조회"""
    try:
        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        df = fdr.DataReader(code, yesterday, today)
        if df is None or df.empty:
            return None
        df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
        last = df.iloc[-1]
        return {
            "date": df.index[-1].strftime("%Y-%m-%d"),
            "open": int(last["open"]),
            "high": int(last["high"]),
            "low": int(last["low"]),
            "close": int(last["close"]),
            "volume": int(last["volume"]),
        }
    except Exception:
        return None


def evaluate_stock(stock, ohlcv, signal_type):
    """종목 성과 평가"""
    if not ohlcv:
        return None

    # 진입가 (스윙: entry, 단타: entry_guide.entry)
    if "entry_guide" in stock:
        entry = stock["entry_guide"].get("entry", stock.get("close", 0))
        stop_loss = stock["entry_guide"].get("stop_loss", 0)
        target = stock["entry_guide"].get("target", 0)
        category = "day_trade"
    else:
        entry = stock.get("entry", stock.get("price", 0))
        stop_loss = stock.get("stop_loss", 0)
        target = stock.get("target", 0)
        category = "swing"

    if entry == 0:
        return None

    # D+1 시가 진입 가정
    actual_entry = ohlcv["open"]
    d1_high = ohlcv["high"]
    d1_low = ohlcv["low"]
    d1_close = ohlcv["close"]

    # 목표/손절 도달 판정
    target_hit = d1_high >= target if target > 0 else False
    stop_hit = d1_low <= stop_loss if stop_loss > 0 else False

    # 결과 판정
    if target_hit and not stop_hit:
        result = "win"
    elif stop_hit and not target_hit:
        result = "loss"
    elif target_hit and stop_hit:
        # 양쪽 다 도달 → 보수적으로 loss 처리 (장중 손절 먼저 가정)
        result = "loss"
    else:
        result = "hold"  # 양쪽 미도달 → 종가 기준

    # 수익률 계산
    if actual_entry > 0:
        if result == "win":
            pnl_pct = round((target - actual_entry) / actual_entry * 100, 2)
        elif result == "loss":
            pnl_pct = round((stop_loss - actual_entry) / actual_entry * 100, 2)
        else:
            pnl_pct = round((d1_close - actual_entry) / actual_entry * 100, 2)
    else:
        pnl_pct = 0

    # 개별 최적화 + Go/No-Go 메타데이터 (있으면 기록)
    opt = stock.get("optimized_params", {})
    return {
        "date": ohlcv["date"],
        "code": stock.get("code", ""),
        "name": stock.get("name", ""),
        "category": category,
        "signal_type": signal_type,
        "score": stock.get("score") or stock.get("day_trade_score", 0),
        # 개별 최적화 파라미터
        "opt_method": opt.get("method", "common"),
        "opt_atr_period": opt.get("atr_period"),
        "opt_sl_mult": opt.get("sl_multiplier"),
        "opt_tp_mult": opt.get("tp_multiplier"),
        "entry_planned": entry,
        "entry_actual": actual_entry,
        "stop_loss": stop_loss,
        "target": target,
        "d1_open": ohlcv["open"],
        "d1_high": ohlcv["high"],
        "d1_low": ohlcv["low"],
        "d1_close": ohlcv["close"],
        "target_hit": target_hit,
        "stop_hit": stop_hit,
        "result": result,
        "pnl_pct": pnl_pct,
    }


def calc_summary(records):
    """전체 성과 요약"""
    if not records:
        return {}

    summary = {}
    for category in ["swing", "day_trade", "all"]:
        subset = records if category == "all" else [r for r in records if r["category"] == category]
        if not subset:
            continue

        wins = [r for r in subset if r["result"] == "win"]
        losses = [r for r in subset if r["result"] == "loss"]
        holds = [r for r in subset if r["result"] == "hold"]

        total = len(subset)
        decided = len(wins) + len(losses)
        win_rate = round(len(wins) / decided * 100, 1) if decided > 0 else 0
        avg_pnl = round(sum(r["pnl_pct"] for r in subset) / total, 2) if total > 0 else 0
        avg_win = round(sum(r["pnl_pct"] for r in wins) / len(wins), 2) if wins else 0
        avg_loss = round(sum(r["pnl_pct"] for r in losses) / len(losses), 2) if losses else 0

        summary[category] = {
            "total": total,
            "wins": len(wins),
            "losses": len(losses),
            "holds": len(holds),
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
        }

    # 시그널별 승률
    signal_types = set(r["signal_type"] for r in records)
    by_signal = {}
    for st in signal_types:
        subset = [r for r in records if r["signal_type"] == st]
        decided = [r for r in subset if r["result"] in ("win", "loss")]
        wins = [r for r in decided if r["result"] == "win"]
        by_signal[st] = {
            "total": len(subset),
            "win_rate": round(len(wins) / len(decided) * 100, 1) if decided else 0,
            "avg_pnl": round(sum(r["pnl_pct"] for r in subset) / len(subset), 2) if subset else 0,
        }
    summary["by_signal"] = by_signal

    return summary


def track_performance():
    """메인 실행: 전일 추천 종목의 D+1 결과 수집"""
    signals = load_signals()
    if not signals:
        print("성과 추적: signals.json 없음, 스킵")
        return

    perf = load_performance()
    existing_dates = set(r["date"] + r["code"] for r in perf["records"])

    # Go/No-Go 판정 정보 (전일 09:00 시점 판정)
    morning_check = signals.get("day_trade", {}).get("morning_check", {})
    mc_meta = {
        "mc_verdict": morning_check.get("verdict"),
        "mc_score": morning_check.get("total_score"),
        "mc_override": morning_check.get("override_triggered", False),
    }

    new_records = []

    # 스윙 시그널 추적
    for sig_type, stocks in signals.get("signals", {}).items():
        for stock in stocks[:5]:  # 각 시그널 상위 5개만
            code = stock.get("code", "")
            ohlcv = get_today_ohlcv(code)
            if not ohlcv:
                continue
            key = ohlcv["date"] + code
            if key in existing_dates:
                continue

            record = evaluate_stock(stock, ohlcv, sig_type)
            if record:
                new_records.append(record)
                print(f"  스윙 {sig_type}: {stock['name']}({code}) → {record['result']} ({record['pnl_pct']:+.1f}%)")

    # 단타 시그널 추적
    day_trade = signals.get("day_trade", {})
    for dt_type in ["day_open_attack", "day_pullback_entry"]:
        for stock in day_trade.get(dt_type, []):
            code = stock.get("code", "")
            ohlcv = get_today_ohlcv(code)
            if not ohlcv:
                continue
            key = ohlcv["date"] + code
            if key in existing_dates:
                continue

            record = evaluate_stock(stock, ohlcv, dt_type)
            if record:
                # 단타 기록에 Go/No-Go 메타데이터 추가
                record.update(mc_meta)
                new_records.append(record)
                print(f"  단타 {dt_type}: {stock['name']}({code}) → {record['result']} ({record['pnl_pct']:+.1f}%)")

    if new_records:
        perf["records"].extend(new_records)
        # 최근 90일만 유지
        cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        perf["records"] = [r for r in perf["records"] if r["date"] >= cutoff]
        perf["summary"] = calc_summary(perf["records"])
        perf["last_updated"] = datetime.now().isoformat()
        save_performance(perf)
        print(f"\n성과 추적: {len(new_records)}개 기록 추가 (총 {len(perf['records'])}개)")
        s = perf["summary"].get("all", {})
        if s:
            print(f"  전체 승률: {s.get('win_rate', 0)}% ({s.get('wins', 0)}승 {s.get('losses', 0)}패 {s.get('holds', 0)}보류)")
            print(f"  평균 수익률: {s.get('avg_pnl', 0):+.2f}%")
    else:
        print("성과 추적: 새로운 기록 없음")


if __name__ == "__main__":
    track_performance()
