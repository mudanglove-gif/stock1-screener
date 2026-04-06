"""
Stock1 단타 + 스윙 백테스트
- 과거 N거래일 동안 매일 시그널을 생성하고 D+1 결과를 평가
- 로컬 실행용
- 사용법: py -3.12 backtest.py --days 300
"""

import gc
import json
import os
import pickle
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pandas_ta as ta
import FinanceDataReader as fdr

# 기존 스크리너 함수 재사용
from screener import (
    calc_indicators, score_stock, check_signals, calc_atr_targets,
    score_day_open_attack, score_day_pullback_entry,
    day_trade_common_filter, day_trade_disqualifiers,
    MIN_PRICE, MIN_VOLUME, MIN_DAYS,
)

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "docs")
TRAILING_STOP = False  # v4a 결과: 일봉에서 부정확 → OFF. 실전 WebSocket에서만 사용


def evaluate_day_trade(d1_open, d1_high, d1_low, d1_close, entry, sl, tp, atr):
    """단타 D+1 결과 평가 (트레일링 스탑 포함)"""
    if d1_open <= 0:
        return "hold", 0

    t_hit = d1_high >= tp
    s_hit = d1_low <= sl

    if TRAILING_STOP and atr > 0:
        # 트레일링 Tier 1: 고가가 진입+0.5ATR 이상 도달 → 손절을 본전으로
        trail_tier1 = d1_high >= entry + 0.5 * atr
        # 트레일링 Tier 2: 고가가 진입+1.0ATR 이상 도달 → 손절을 진입+0.3ATR으로
        trail_tier2 = d1_high >= entry + 1.0 * atr

        if t_hit and not s_hit:
            result = "win"
        elif s_hit and not t_hit:
            # 손절 걸렸지만, 그 전에 트레일링이 발동했을 수 있음
            if trail_tier2 and d1_low > entry + 0.3 * atr:
                # Tier 2 발동 + 새 손절(진입+0.3ATR) 위에서 유지 → 수익
                result = "win"
            elif trail_tier1 and d1_low > entry:
                # Tier 1 발동 + 본전 위에서 유지 → 본전
                result = "breakeven"
            else:
                result = "loss"
        elif t_hit and s_hit:
            # 양쪽 다 도달 → 보수적으로 판단
            if trail_tier1 and d1_low > entry:
                result = "breakeven"
            else:
                result = "loss"
        else:
            # 양쪽 미도달
            if trail_tier1 and d1_close > entry:
                result = "breakeven"  # 본전 이상 종가
            else:
                result = "hold"
    else:
        # 트레일링 OFF (기존 로직)
        if t_hit and not s_hit:
            result = "win"
        elif s_hit:
            result = "loss"
        else:
            result = "hold"

    # PnL 계산
    if result == "win":
        pnl = round((tp - d1_open) / d1_open * 100, 2)
    elif result == "loss":
        pnl = round((sl - d1_open) / d1_open * 100, 2)
    elif result == "breakeven":
        pnl = 0.0
    else:
        pnl = round((d1_close - d1_open) / d1_open * 100, 2)

    return result, pnl


def get_all_tickers():
    tickers = []
    for market in ["KOSPI", "KOSDAQ"]:
        listing = fdr.StockListing(market)
        for _, row in listing.iterrows():
            code = row.get("Code", "")
            name = row.get("Name", "")
            if code and name and len(code) == 6:
                tickers.append({"code": code, "name": name, "market": market})
    return tickers


def run_backtest(backtest_days=60):
    print(f"=== Stock1 백테스트 ({backtest_days}거래일) ===")
    print(f"시작: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    tickers = get_all_tickers()
    print(f"종목 수: {len(tickers)}")

    # 데이터 로딩 기간: 지표 계산 250일 + 백테스트 기간 + 여유
    end = datetime.now()
    load_days = backtest_days + 400  # 지표 계산용 충분한 여유
    start = end - timedelta(days=load_days)
    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    # OHLCV 데이터 캐시
    cache_file = os.path.join(OUTPUT_DIR, f"backtest_cache_{backtest_days}d.pkl")
    use_cache = os.path.exists(cache_file) and "--refresh" not in sys.argv

    if use_cache:
        # 캐시 날짜 확인 (하루 지나면 자동 갱신)
        cache_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
        if (datetime.now() - cache_mtime).days >= 1:
            print("캐시가 하루 이상 지남 → 재다운로드")
            use_cache = False

    if use_cache:
        print(f"캐시 로드 중: {cache_file}")
        with open(cache_file, "rb") as f:
            all_data = pickle.load(f)
        print(f"캐시 로드 완료: {len(all_data)}종목")
    else:
        print(f"OHLCV 데이터 다운로드 중 (최근 {load_days}일)...")
        all_data = {}
        skipped = 0
        for i, ticker in enumerate(tickers):
            code = ticker["code"]
            if (i + 1) % 200 == 0:
                print(f"  {i + 1}/{len(tickers)} 로딩 중... ({ticker['name']}) [{len(all_data)}종목]")
            try:
                df = fdr.DataReader(code, start_str, end_str)
                if df is not None and not df.empty:
                    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
                    df = df[["open", "high", "low", "close", "volume"]]
                    df = df[df["volume"] > 0]
                    if len(df) >= MIN_DAYS + 10:
                        all_data[code] = {"df": df, "name": ticker["name"], "market": ticker["market"]}
                    else:
                        skipped += 1
                else:
                    skipped += 1
            except Exception:
                skipped += 1
            time.sleep(0.05)

        # 캐시 저장
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(cache_file, "wb") as f:
            pickle.dump(all_data, f)
        print(f"데이터 로드 완료: {len(all_data)}종목 (스킵: {skipped})")
        print(f"캐시 저장: {cache_file}")

    gc.collect()

    # 거래일 목록 추출
    sample_df = next(iter(all_data.values()))["df"]
    all_trading_days = sample_df.index.tolist()

    # 백테스트 가능한 거래일 (최소 MIN_DAYS 이후부터)
    available_days = len(all_trading_days)
    actual_backtest = min(backtest_days, available_days - MIN_DAYS - 1)
    if actual_backtest < 10:
        print(f"거래일 부족: {available_days}일 (최소 {MIN_DAYS + 11}일 필요)")
        return

    trading_days = all_trading_days[-(actual_backtest + 1):]  # +1 for D+1
    print(f"백테스트 기간: {trading_days[0].strftime('%Y-%m-%d')} ~ {trading_days[-1].strftime('%Y-%m-%d')} ({actual_backtest}거래일)")

    results = {
        "swing": [],
        "day_open_attack": [],
        "day_pullback_entry": [],
    }

    for day_idx in range(actual_backtest - 1):
        sim_date = trading_days[day_idx]
        next_date = trading_days[day_idx + 1]
        date_str = sim_date.strftime("%Y-%m-%d")

        if (day_idx + 1) % 20 == 0:
            print(f"\n[{day_idx + 1}/{actual_backtest - 1}] {date_str} 시뮬레이션 중...")

        for code, stock_info in all_data.items():
            df_full = stock_info["df"]
            name = stock_info["name"]

            # sim_date까지의 데이터로 자르기
            df_slice = df_full[df_full.index <= sim_date]
            if len(df_slice) < MIN_DAYS:
                continue

            last = df_slice.iloc[-1]
            if last["close"] < MIN_PRICE or last["volume"] < MIN_VOLUME:
                continue

            # 지표 계산
            df_ind = calc_indicators(df_slice.copy())
            if df_ind is None:
                continue

            last = df_ind.iloc[-1]

            # 스코어 + 시그널
            score, reasons = score_stock(df_ind)
            stock_signals = check_signals(df_ind, score, reasons)

            if not stock_signals:
                continue

            # D+1 데이터
            if next_date not in df_full.index:
                continue
            d1 = df_full.loc[next_date]

            # 스윙 평가
            entry, stop_loss, target = calc_atr_targets(df_ind)
            d1_open = int(d1["open"])
            d1_high = int(d1["high"])
            d1_low = int(d1["low"])
            d1_close = int(d1["close"])

            target_hit = d1_high >= target if target > 0 else False
            stop_hit = d1_low <= stop_loss if stop_loss > 0 else False

            if target_hit and not stop_hit:
                result = "win"
            elif stop_hit:
                result = "loss"
            else:
                result = "hold"

            if d1_open > 0:
                if result == "win":
                    pnl = round((target - d1_open) / d1_open * 100, 2)
                elif result == "loss":
                    pnl = round((stop_loss - d1_open) / d1_open * 100, 2)
                else:
                    pnl = round((d1_close - d1_open) / d1_open * 100, 2)
            else:
                pnl = 0

            for sig_type, _ in stock_signals[:1]:
                results["swing"].append({
                    "date": date_str,
                    "code": code,
                    "name": name,
                    "signal": sig_type,
                    "score": score,
                    "result": result,
                    "pnl": pnl,
                })

            # 단타 평가
            stock_dict = {"code": code, "name": name}
            adx_val = float(last.get("adx", 0) or 0)

            # v4b 필터: 정배열 필수 (MA5>MA20>MA60)
            ma5 = last.get("ma5", 0) or 0
            ma20 = last.get("ma20", 0) or 0
            ma60 = last.get("ma60", 0) or 0
            if not (pd.notna(ma5) and pd.notna(ma20) and pd.notna(ma60) and ma5 > ma20 > ma60):
                pass  # 정배열 아니면 단타 스킵
            elif day_trade_common_filter(stock_dict, df_ind) and (pd.notna(adx_val) and adx_val >= 15):
                disq = day_trade_disqualifiers(df_ind)

                # v4b 필터: 눌림 진입용 거래량 급감 체크
                vol_decline_ok = True
                if len(df_ind) >= 5:
                    recent_vols = df_ind["volume"].iloc[-3:]  # 최근 3일
                    peak_vol = df_ind["volume"].iloc[-10:-3].max() if len(df_ind) >= 10 else df_ind["volume"].iloc[:-3].max()
                    if peak_vol > 0 and recent_vols.mean() > peak_vol * 0.5:
                        vol_decline_ok = False  # 거래량 급감 아님

                if not disq:
                    # v4c 가산점 계산
                    bonus = 0

                    # 두 번째 대량 거래 (+10)
                    vol_ma20 = last.get("vol_ma20", 0) or 0
                    if pd.notna(vol_ma20) and vol_ma20 > 0 and last["volume"] >= vol_ma20 * 2:
                        # 현재 거래량 급증 — 60일 내 이전 급증 있었는지
                        prev_surges = 0
                        for k in range(max(len(df_ind) - 60, 0), len(df_ind) - 1):
                            row_k = df_ind.iloc[k]
                            v20 = row_k.get("vol_ma20", 0) or 0
                            if pd.notna(v20) and v20 > 0 and row_k["volume"] >= v20 * 2:
                                if last["close"] > row_k["high"]:  # 고가 돌파
                                    prev_surges += 1
                        if prev_surges >= 1:
                            bonus += 10  # 두 번째 대량 거래

                    # 강세 패턴 카운트 (+5~10)
                    bullish_count = 0
                    for k in range(max(len(df_ind) - 120, 0), len(df_ind)):
                        row_k = df_ind.iloc[k]
                        trade_val = row_k["close"] * row_k["volume"]
                        body_pct = (row_k["close"] - row_k["open"]) / row_k["open"] * 100 if row_k["open"] > 0 else 0
                        if trade_val >= 100_000_000_000 and body_pct >= 10:  # 거래대금 1000억+ & +10%
                            bullish_count += 1
                    if bullish_count >= 3:
                        bonus += 10
                    elif bullish_count >= 1:
                        bonus += 5

                    # 최근 5일 거래대금 500억+ (+5)
                    if len(df_ind) >= 5:
                        recent_trade_vals = (df_ind["close"].iloc[-5:] * df_ind["volume"].iloc[-5:])
                        if (recent_trade_vals >= 50_000_000_000).any():
                            bonus += 5

                    # 장초반 공략 (커트라인 70, v4c 가산 적용)
                    oa_score, _ = score_day_open_attack(df_ind)
                    oa_score += bonus
                    if oa_score >= 70:
                        atr = float(last.get("atr14", 0) or 0)
                        close = int(last["close"])

                        # v4b 갭 필터: 장초반은 갭다운 스킵
                        if atr > 0 and d1_open < close - 0.7 * atr:
                            pass  # 갭다운 과도 → 스킵
                        elif atr > 0 and d1_open > close + 1.5 * atr:
                            pass  # 갭업 과도 → 스킵
                        else:
                            sl = int(close - 0.9 * atr) if atr > 0 else int(close * 0.98)
                            tp = int(close + 1.3 * atr) if atr > 0 else int(close * 1.025)  # 최종: 1.3 (1.5/1.7 효과 미미)

                            dt_result, dt_pnl = evaluate_day_trade(d1_open, d1_high, d1_low, d1_close, close, sl, tp, atr)

                            results["day_open_attack"].append({
                            "date": date_str,
                            "code": code,
                            "name": name,
                            "score": oa_score,
                            "result": dt_result,
                            "pnl": dt_pnl,
                        })

                    # 눌림 진입 (커트라인 65, v4c 가산) + 거래량 급감 필수
                    pe_score, _ = score_day_pullback_entry(df_ind)
                    pe_score += bonus
                    if pe_score >= 65 and vol_decline_ok:
                        atr = float(last.get("atr14", 0) or 0)
                        close = int(last["close"])

                        # v4b 갭 필터: 눌림은 갭업 스킵, 소폭 갭다운 허용
                        if atr > 0 and d1_open > close + 0.7 * atr:
                            pass  # 갭업 과도 → 스킵
                        elif atr > 0 and d1_open < close - 1.5 * atr:
                            pass  # 갭다운 과도 → 스킵
                        else:
                            sl = int(close - 0.9 * atr) if atr > 0 else int(close * 0.98)
                            tp = int(close + 1.3 * atr) if atr > 0 else int(close * 1.025)

                            dt_result, dt_pnl = evaluate_day_trade(d1_open, d1_high, d1_low, d1_close, close, sl, tp, atr)

                        results["day_pullback_entry"].append({
                            "date": date_str,
                            "code": code,
                            "name": name,
                            "score": pe_score,
                            "result": dt_result,
                            "pnl": dt_pnl,
                        })

        # 주기적 메모리 정리
        if (day_idx + 1) % 50 == 0:
            gc.collect()

    # 결과 집계
    print("\n" + "=" * 50)
    print(f"백테스트 결과 ({backtest_days}거래일)")
    print("=" * 50)

    summary = {}
    for category, records in results.items():
        if not records:
            summary[category] = {"total": 0}
            print(f"\n{category}: 시그널 없음")
            continue

        wins = [r for r in records if r["result"] == "win"]
        losses = [r for r in records if r["result"] == "loss"]
        breakevens = [r for r in records if r["result"] == "breakeven"]
        holds = [r for r in records if r["result"] == "hold"]
        decided = len(wins) + len(losses) + len(breakevens)

        win_rate = round(len(wins) / decided * 100, 1) if decided > 0 else 0
        avg_pnl = round(sum(r["pnl"] for r in records) / len(records), 2)
        avg_win = round(sum(r["pnl"] for r in wins) / len(wins), 2) if wins else 0
        avg_loss = round(sum(r["pnl"] for r in losses) / len(losses), 2) if losses else 0
        total_win = sum(r["pnl"] for r in wins + breakevens if r["pnl"] > 0)
        total_loss = abs(sum(r["pnl"] for r in losses + holds if r["pnl"] < 0))
        pf_a = round(total_win / total_loss, 2) if total_loss > 0 else 999

        # PF-B: hold 제외, 시그널 품질 평가
        decided_only = [r for r in records if r["result"] in ("win", "loss")]
        total_win_b = sum(r["pnl"] for r in decided_only if r["pnl"] > 0)
        total_loss_b = abs(sum(r["pnl"] for r in decided_only if r["pnl"] < 0))
        pf_b = round(total_win_b / total_loss_b, 2) if total_loss_b > 0 else 999

        summary[category] = {
            "total": len(records),
            "wins": len(wins),
            "losses": len(losses),
            "breakevens": len(breakevens),
            "holds": len(holds),
            "win_rate": win_rate,
            "avg_pnl": avg_pnl,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "pf_a": pf_a,
            "pf_b": pf_b,
        }

        label = {"swing": "스윙", "day_open_attack": "장초반공략", "day_pullback_entry": "눌림진입"}.get(category, category)
        print(f"\n{label}:")
        print(f"  총 시그널: {len(records)}건")
        print(f"  승률: {win_rate}% ({len(wins)}승 {len(losses)}패 {len(breakevens)}본전 {len(holds)}보류)")
        print(f"  평균 수익률: {avg_pnl:+.2f}%")
        print(f"  평균 수익(승): {avg_win:+.2f}%")
        print(f"  평균 손실(패): {avg_loss:+.2f}%")
        print(f"  PF-A (hold포함): {pf_a}")
        print(f"  PF-B (시그널품질): {pf_b}")

        if decided > 0:
            print(f"  --- 스코어 구간별 ---")
            score_ranges = [(40, 50), (50, 60), (60, 70), (70, 80), (80, 90), (90, 100)]
            for lo, hi in score_ranges:
                subset = [r for r in records if lo <= r["score"] < hi and r["result"] in ("win", "loss")]
                sub_wins = [r for r in subset if r["result"] == "win"]
                if subset:
                    sr = round(len(sub_wins) / len(subset) * 100, 1)
                    print(f"    {lo}~{hi}점: 승률 {sr}% ({len(sub_wins)}/{len(subset)})")

    # 저장
    output_file = os.path.join(OUTPUT_DIR, f"backtest_result_{backtest_days}d.json")
    output = {
        "backtest_date": datetime.now().isoformat(),
        "period_days": backtest_days,
        "total_stocks": len(all_data),
        "summary": summary,
        "records": {k: v[:200] for k, v in results.items()},
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n결과 저장: {output_file}")
    print(f"완료: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    days = 60
    for arg in sys.argv[1:]:
        if arg.startswith("--days="):
            days = int(arg.split("=")[1])
        elif arg == "--days" and sys.argv.index(arg) + 1 < len(sys.argv):
            days = int(sys.argv[sys.argv.index(arg) + 1])
    run_backtest(days)
