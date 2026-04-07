"""
Stock1 자동매매 실행 모듈

진입 (entry): 09:00 KST
- signals.json에서 단타 종목 + morning_check 확인
- Go/No-Go 위험 시 스킵
- 종목별 비중 계산 후 시장가 매수
- trades.json에 매수 기록

청산 (exit): 15:20 KST
- 보유 종목 시장가 매도
- 손익 계산 후 trades.json 업데이트

사용:
  py -3.12 auto_trader.py entry
  py -3.12 auto_trader.py exit

환경변수:
  KIS_MOCK_APP_KEY/SECRET, KIS_MOCK_ACCOUNT_NO  (모의)
  AUTO_TRADE_MODE = mock | real (기본 mock)
  AUTO_TRADE_BUDGET_PCT = 종목당 예산 비율 (기본 15%)
  AUTO_TRADE_MAX_DAILY_LOSS = 일일 최대 손실 (원, 기본 -1000000)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import kis_order as ko

KST = timezone(timedelta(hours=9))
SCRIPT_DIR = Path(__file__).parent
SIGNALS_PATH = SCRIPT_DIR / "docs" / "signals.json"
TRADES_PATH = SCRIPT_DIR / "docs" / "trades.json"

# 설정
MODE = os.environ.get("AUTO_TRADE_MODE", "mock")
BUDGET_PCT = float(os.environ.get("AUTO_TRADE_BUDGET_PCT", "15")) / 100  # 15%
MAX_DAILY_LOSS = int(os.environ.get("AUTO_TRADE_MAX_DAILY_LOSS", "-1000000"))


def now_kst():
    return datetime.now(KST)


def now_iso():
    return now_kst().isoformat()


def today_str():
    return now_kst().strftime("%Y-%m-%d")


def load_signals():
    if not SIGNALS_PATH.exists():
        return None
    with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_trades():
    if not TRADES_PATH.exists():
        return {"records": [], "summary": {}}
    try:
        with open(TRADES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"records": [], "summary": {}}


def save_trades(data):
    TRADES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TRADES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def is_kill_switch_active(signals):
    """auto_trade_enabled = False면 킬스위치 발동"""
    flag = signals.get("day_trade", {}).get("auto_trade_enabled")
    return flag is False


def is_market_safe(signals):
    """morning_check 위험 판정 시 진입 금지"""
    mc = signals.get("day_trade", {}).get("morning_check", {})
    verdict = mc.get("verdict")
    if verdict == "danger":
        return False, mc.get("verdict_reason", "위험 판정")
    return True, mc.get("verdict_reason", "")


def get_target_stocks(signals):
    """진입 대상 종목 추출"""
    dt = signals.get("day_trade", {})
    targets = []
    seen = set()
    for sig_type in ["day_open_attack", "day_pullback_entry"]:
        for stock in dt.get(sig_type, []):
            code = stock.get("code")
            if not code or code in seen:
                continue
            # 시간외 실격 종목 제외
            am = stock.get("after_market", {})
            if am.get("disqualified"):
                continue
            seen.add(code)
            targets.append({
                "code": code,
                "name": stock.get("name"),
                "signal_type": sig_type,
                "score": stock.get("day_trade_score", 0),
                "entry": stock.get("entry_guide", {}).get("entry"),
                "stop_loss": stock.get("entry_guide", {}).get("stop_loss"),
                "target": stock.get("entry_guide", {}).get("target"),
                "atr14": stock.get("entry_guide", {}).get("atr14"),
                "optimized_params": stock.get("optimized_params", {}),
            })
    return targets


def calculate_position_size(available_cash: int, entry_price: int, num_stocks: int) -> int:
    """종목당 매수 수량 계산 (균등 분배 + 비중 제한)"""
    if entry_price <= 0 or num_stocks <= 0:
        return 0
    # 종목별 예산 = 가용 자금 × 비중 (균등 분배는 BUDGET_PCT × num_stocks ≤ 100% 가정)
    per_stock_budget = int(available_cash * BUDGET_PCT)
    qty = per_stock_budget // entry_price
    return max(0, qty)


def check_daily_loss(trades) -> int:
    """오늘의 누적 손익 확인 (킬스위치용)"""
    today = today_str()
    today_records = [r for r in trades["records"] if r.get("date") == today and r.get("status") == "closed"]
    return sum(r.get("pnl", 0) for r in today_records)


# ──────────────────────────────────────────────
# 진입 (09:00)
# ──────────────────────────────────────────────

def run_entry():
    print("=" * 60)
    print(f"Stock1 자동매매 진입 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    signals = load_signals()
    if not signals:
        print("❌ signals.json 없음")
        return

    # 1. 킬스위치 체크
    if is_kill_switch_active(signals):
        print("🚫 킬스위치 발동 — 자동매매 중단")
        return

    # 2. Go/No-Go 판정 체크
    safe, reason = is_market_safe(signals)
    if not safe:
        print(f"🔴 시장 위험 판정 — 진입 스킵 ({reason})")
        return
    print(f"✅ 시장 판정: {reason}")

    # 3. 일일 손실 체크 (전일 손실)
    trades = load_trades()
    today_pnl = check_daily_loss(trades)
    if today_pnl <= MAX_DAILY_LOSS:
        print(f"🚫 일일 최대 손실 초과 ({today_pnl:,}원) — 진입 중단")
        return

    # 4. 진입 대상 종목
    targets = get_target_stocks(signals)
    if not targets:
        print("진입 대상 없음")
        return
    print(f"\n진입 대상: {len(targets)}종목")

    # 5. KIS 모드 설정 + 잔고 확인
    ko.set_mode(MODE)
    bal = ko.get_balance()
    print(f"가용 자금: {bal['available_cash']:,}원 (총 평가 {bal['total_eval']:,}원)")

    if bal["available_cash"] < 100_000:
        print("❌ 가용 자금 부족 (10만원 미만)")
        return

    # 6. 종목별 매수 실행
    print("\n[매수 주문]")
    new_records = []
    for t in targets:
        entry_price = t["entry"]
        qty = calculate_position_size(bal["available_cash"], entry_price, len(targets))
        if qty <= 0:
            print(f"  {t['name']}({t['code']}): 수량 0 → 스킵")
            continue

        try:
            result = ko.buy(t["code"], qty)
            print(f"  ✅ {t['name']}({t['code']}) {qty}주 매수 (주문번호 {result['order_no']})")
            time.sleep(0.5)  # KIS 초당 거래 제한 회피
            new_records.append({
                "date": today_str(),
                "code": t["code"],
                "name": t["name"],
                "signal_type": t["signal_type"],
                "score": t["score"],
                "entry_planned": entry_price,
                "qty": qty,
                "stop_loss": t["stop_loss"],
                "target": t["target"],
                "atr14": t["atr14"],
                "optimized_params": t["optimized_params"],
                "buy_order_no": result["order_no"],
                "buy_time": now_iso(),
                "status": "open",
                "mode": MODE,
            })
        except Exception as e:
            print(f"  ❌ {t['name']} 매수 실패: {e}")

    if new_records:
        trades["records"].extend(new_records)
        save_trades(trades)
        print(f"\n✅ {len(new_records)}건 매수 완료, trades.json 업데이트")


# ──────────────────────────────────────────────
# 청산 (15:20)
# ──────────────────────────────────────────────

def run_exit():
    print("=" * 60)
    print(f"Stock1 자동매매 청산 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    # KIS 모드 설정 + 현재 잔고 조회 (잔고 기준으로 청산 — 더 안전)
    ko.set_mode(MODE)
    bal_before = ko.get_balance()
    holdings = bal_before["holdings"]

    if not holdings:
        print("청산 대상 없음 (보유 종목 0건)")
        return

    print(f"청산 대상: {len(holdings)}종목 (현재 잔고 기준)")

    # trades.json에서 오늘 진입 기록 매핑 (있으면 업데이트, 없으면 새로 기록)
    trades = load_trades()
    today = today_str()
    today_records = {r["code"]: r for r in trades["records"]
                     if r.get("date") == today and r.get("status") in ("open", "exit_failed")}

    print("\n[매도 주문]")
    for h in holdings:
        code = h["code"]
        qty = h["qty"]
        try:
            result = ko.sell(code, qty)
            sell_price = h["current_price"]
            buy_price = h["avg_price"]
            pnl = (sell_price - buy_price) * qty
            pnl_pct = round((sell_price - buy_price) / buy_price * 100, 2) if buy_price > 0 else 0
            print(f"  ✅ {h['name']}({code}) {qty}주 매도 → 손익 {pnl:+,}원 ({pnl_pct:+.2f}%)")
            time.sleep(0.5)

            # trades.json 업데이트 (오늘 기록이 있으면 갱신, 없으면 추가)
            r = today_records.get(code)
            if r is None:
                r = {
                    "date": today,
                    "code": code,
                    "name": h["name"],
                    "signal_type": "manual_exit",
                    "qty": qty,
                    "mode": MODE,
                }
                trades["records"].append(r)
            r["sell_order_no"] = result["order_no"]
            r["sell_time"] = now_iso()
            r["sell_price"] = sell_price
            r["actual_buy_price"] = buy_price
            r["actual_qty"] = qty
            r["pnl"] = pnl
            r["pnl_pct"] = pnl_pct
            r["status"] = "closed"
        except Exception as e:
            print(f"  ❌ {h['name']} 매도 실패: {e}")
            r = today_records.get(code)
            if r:
                r["status"] = "exit_failed"

    save_trades(trades)

    # 일일 요약
    closed = [r for r in trades["records"] if r.get("date") == today and r.get("status") == "closed"]
    if closed:
        total_pnl = sum(r["pnl"] for r in closed)
        wins = [r for r in closed if r["pnl"] > 0]
        losses = [r for r in closed if r["pnl"] < 0]
        print(f"\n[일일 요약 {today}]")
        print(f"  거래: {len(closed)}건 ({len(wins)}승 {len(losses)}패)")
        print(f"  총 손익: {total_pnl:+,}원")
        print(f"  평균 수익률: {sum(r['pnl_pct'] for r in closed) / len(closed):+.2f}%")


def main():
    if len(sys.argv) < 2:
        print("사용법: py auto_trader.py [entry|exit|status]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "entry":
        run_entry()
    elif cmd == "exit":
        run_exit()
    elif cmd == "status":
        ko.set_mode(MODE)
        bal = ko.get_balance()
        print(f"=== {MODE.upper()} 잔고 ===")
        print(f"예수금: {bal['cash']:,}원")
        print(f"평가금액: {bal['total_eval']:,}원")
        print(f"평가손익: {bal['total_pnl']:+,}원 ({bal['total_pnl_rate']:+.2f}%)")
        print(f"보유종목: {len(bal['holdings'])}건")
        for h in bal["holdings"]:
            print(f"  {h['name']}({h['code']}) {h['qty']}주 평단 {h['avg_price']:,} 손익 {h['pnl']:+,}원")
    else:
        print(f"알 수 없는 명령: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
