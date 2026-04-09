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
  AUTO_TRADE_MAX_DAILY_LOSS_PCT = 일일 최대 손실 비율 (%, 기본 1.5)
    - 계좌 평가금액 × 비율로 동적 산출
    - 시장 레짐에 따라 조정: risk_off×0.3 / caution×0.6 / neutral×1.0 / risk_on×1.3
  TELEGRAM_BOT_TOKEN = 텔레그램 봇 토큰 (선택)
  TELEGRAM_CHAT_ID   = 텔레그램 채팅 ID (선택)
"""

import base64
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta, time as dtime
from pathlib import Path

import requests

import kis_order as ko

KST = timezone(timedelta(hours=9))
SCRIPT_DIR = Path(__file__).parent
SIGNALS_PATH = SCRIPT_DIR / "docs" / "signals.json"
TRADES_PATH = SCRIPT_DIR / "docs" / "trades.json"  # 로컬 fallback 전용

# 비공개 거래 데이터: stock1-private repo에 GitHub Contents API로 저장
PRIVATE_REPO = "mudanglove-gif/stock1-private"
PRIVATE_TRADES_FILE = "trades.json"
PRIVATE_PAT = os.environ.get("STOCK1_PRIVATE_PAT", "")
_TRADES_SHA_CACHE = None  # PUT 시 필요한 sha 캐시

# 설정
MODE = os.environ.get("AUTO_TRADE_MODE", "mock")
BUDGET_PCT = float(os.environ.get("AUTO_TRADE_BUDGET_PCT", "15")) / 100  # 15%
MAX_DAILY_LOSS_PCT = float(os.environ.get("AUTO_TRADE_MAX_DAILY_LOSS_PCT", "1.5")) / 100  # 기본 1.5%

# 텔레그램
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def now_kst():
    return datetime.now(KST)


def now_iso():
    return now_kst().isoformat()


def today_str():
    return now_kst().strftime("%Y-%m-%d")


# ──────────────────────────────────────────────
# S7: 텔레그램 알림
# ──────────────────────────────────────────────

def send_telegram(msg: str):
    """텔레그램 메시지 전송 (TELEGRAM_BOT_TOKEN/CHAT_ID 없으면 무시)"""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        print(f"텔레그램 전송 실패: {e}")


# ──────────────────────────────────────────────
# S6: 동적 일일 손실 한도
# ──────────────────────────────────────────────

def calc_dynamic_max_loss(total_eval: int, signals) -> int:
    """
    계좌 잔고 × 손실 비율 × 레짐 배수 → 동적 최대 손실 한도 (음수)
    - 기본: -1.5% of total_eval
    - risk_off × 0.3 / caution × 0.6 / neutral × 1.0 / risk_on × 1.3
    - 최소 -100,000원 (너무 타이트해지지 않도록)
    """
    base_loss = -(total_eval * MAX_DAILY_LOSS_PCT)

    regime = "neutral"
    if signals:
        regime = signals.get("market_regime", {}).get("regime", "neutral")

    regime_mult = {"risk_off": 0.3, "caution": 0.6, "neutral": 1.0, "risk_on": 1.3}
    mult = regime_mult.get(regime, 1.0)

    dynamic_loss = int(base_loss * mult)
    result = max(dynamic_loss, -100_000)
    print(f"동적 손실 한도: {result:,}원 (레짐:{regime}, 배수:{mult}, 잔고:{total_eval:,})", flush=True)
    return result


def load_signals():
    if not SIGNALS_PATH.exists():
        return None
    with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _gh_headers():
    return {
        "Authorization": f"Bearer {PRIVATE_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def load_trades():
    """PAT 있으면 stock1-private에서 fetch, 없으면 로컬 파일 fallback"""
    global _TRADES_SHA_CACHE
    if PRIVATE_PAT:
        try:
            url = f"https://api.github.com/repos/{PRIVATE_REPO}/contents/{PRIVATE_TRADES_FILE}"
            r = requests.get(url, headers=_gh_headers(), timeout=15)
            if r.status_code == 404:
                _TRADES_SHA_CACHE = None
                return {"records": [], "summary": {}}
            r.raise_for_status()
            payload = r.json()
            _TRADES_SHA_CACHE = payload.get("sha")
            content = base64.b64decode(payload["content"]).decode("utf-8")
            return json.loads(content)
        except Exception as e:
            print(f"⚠️ stock1-private load 실패, 로컬 fallback: {e}")
    if not TRADES_PATH.exists():
        return {"records": [], "summary": {}}
    try:
        with open(TRADES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"records": [], "summary": {}}


def save_trades(data):
    """PAT 있으면 stock1-private에 commit, 없으면 로컬 파일에 저장"""
    global _TRADES_SHA_CACHE
    if PRIVATE_PAT:
        try:
            content_str = json.dumps(data, ensure_ascii=False, indent=2)
            body = {
                "message": f"trades update {now_iso()}",
                "content": base64.b64encode(content_str.encode("utf-8")).decode("ascii"),
            }
            if _TRADES_SHA_CACHE:
                body["sha"] = _TRADES_SHA_CACHE
            url = f"https://api.github.com/repos/{PRIVATE_REPO}/contents/{PRIVATE_TRADES_FILE}"
            r = requests.put(url, headers=_gh_headers(), json=body, timeout=15)
            r.raise_for_status()
            _TRADES_SHA_CACHE = r.json().get("content", {}).get("sha")
            return
        except Exception as e:
            print(f"⚠️ stock1-private save 실패, 로컬 저장으로 fallback: {e}")
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


def get_target_stocks(signals, signal_filter=None):
    """
    진입 대상 종목 추출
    signal_filter: "day_open_attack" | "day_pullback_entry" | None(전체)
    S5: optimized_params.method == "individual" 이면 최적화된 sl/tp_multiplier로 SL/TP 재산출
    """
    dt = signals.get("day_trade", {})
    targets = []
    seen = set()
    for sig_type in ["day_open_attack", "day_pullback_entry"]:
        if signal_filter and sig_type != signal_filter:
            continue
        for stock in dt.get(sig_type, []):
            code = stock.get("code")
            if not code or code in seen:
                continue
            # 시간외 실격 종목 제외
            am = stock.get("after_market", {})
            if am.get("disqualified"):
                continue
            seen.add(code)

            eg = stock.get("entry_guide", {})
            op = stock.get("optimized_params", {})
            entry = eg.get("entry", 0)
            atr14 = eg.get("atr14", 0)

            # S5: 개별 최적 파라미터가 있으면 SL/TP 재계산
            if op.get("method") == "individual" and entry > 0 and atr14 > 0:
                sl_mult = op.get("sl_multiplier", 0.9)
                tp_mult = op.get("tp_multiplier", 1.3)
                stop_loss = int(entry - sl_mult * atr14)
                target = int(entry + tp_mult * atr14)
                sl_source = f"optimized({sl_mult}/{tp_mult})"
            else:
                stop_loss = eg.get("stop_loss")
                target = eg.get("target")
                sl_source = "default(0.9/1.3)"

            targets.append({
                "code": code,
                "name": stock.get("name"),
                "signal_type": sig_type,
                "score": stock.get("day_trade_score", 0),
                "entry": entry,
                "stop_loss": stop_loss,
                "target": target,
                "atr14": atr14,
                "sl_source": sl_source,
                "optimized_params": op,
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

    # 0a. 시간 게이트 — 09:30 KST 초과 시 단타 진입 금지
    now = now_kst().time()
    if now > dtime(9, 30):
        print(f"⏰ 진입 시간 종료 ({now.strftime('%H:%M:%S')} > 09:30) — 스킵")
        return

    signals = load_signals()
    if not signals:
        print("❌ signals.json 없음")
        return

    # 0b. Idempotency — 오늘 이미 단타 진입 기록 있으면 스킵
    trades = load_trades()
    today = today_str()
    already = [r for r in trades["records"]
               if r.get("date") == today and r.get("signal_type", "").startswith("day_")]
    if already:
        print(f"✅ 오늘({today}) 이미 진입 완료 ({len(already)}건) — 스킵")
        return

    # 1. 킬스위치 체크
    if is_kill_switch_active(signals):
        msg = f"[Stock1] 킬스위치 발동 — 자동매매 중단 ({today})"
        print(f"🚫 {msg}")
        send_telegram(msg)
        return

    # 2. Go/No-Go 판정 체크
    safe, reason = is_market_safe(signals)
    if not safe:
        msg = f"[Stock1] 시장 위험 판정 — 진입 스킵\n{reason}"
        print(f"🔴 {msg}")
        send_telegram(msg)
        return
    print(f"✅ 시장 판정: {reason}")

    # 3. KIS 모드 설정 + 잔고 확인 (손실 한도 계산에 필요)
    ko.set_mode(MODE)
    bal = ko.get_balance()
    print(f"가용 자금: {bal['available_cash']:,}원 (총 평가 {bal['total_eval']:,}원)")

    if bal["available_cash"] < 100_000:
        print("❌ 가용 자금 부족 (10만원 미만)")
        return

    # 4. S6: 동적 일일 손실 한도 체크
    max_daily_loss = calc_dynamic_max_loss(bal["total_eval"], signals)
    today_pnl = check_daily_loss(trades)
    if today_pnl <= max_daily_loss:
        msg = f"[Stock1] 일일 최대 손실 초과 ({today_pnl:,}원 ≤ {max_daily_loss:,}원) — 진입 중단"
        print(f"🚫 {msg}")
        send_telegram(msg)
        return

    # 5. 진입 대상 종목 + 매수 (attack/pullback 전부)
    _run_buy(signals, "day_open_attack", trades, bal)
    _run_buy(signals, "day_pullback_entry", trades, bal)


# ──────────────────────────────────────────────
# 청산 (15:20, 비상/수동용)
# ──────────────────────────────────────────────

# ──────────────────────────────────────────────
# 공통 매도 헬퍼
# ──────────────────────────────────────────────

def _sell_positions(trades, signal_type_filter=None):
    """
    KIS 잔고에서 종목을 매도하고 trades.json을 갱신한다.
    signal_type_filter: None이면 전 종목, 지정하면 해당 signal_type의 open/exit_failed만 매도
    반환: 이번에 closed된 레코드 리스트
    """
    ko.set_mode(MODE)
    holdings = ko.get_balance()["holdings"]
    today = today_str()

    if signal_type_filter:
        # trades.json 기준으로 대상 코드 선별
        target_codes = {
            r["code"] for r in trades["records"]
            if r.get("status") in ("open", "exit_failed")
            and r.get("signal_type") == signal_type_filter
        }
        holdings = [h for h in holdings if h["code"] in target_codes]

    if not holdings:
        label = signal_type_filter or "전체"
        print(f"청산 대상 없음 ({label})")
        return []

    print(f"청산 대상: {len(holdings)}종목")

    def find_open_record(code):
        matches = [r for r in trades["records"]
                   if r.get("code") == code and r.get("status") in ("open", "exit_failed")]
        return min(matches, key=lambda r: r.get("date", "")) if matches else None

    closed_now = []
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
            print(f"  [OK] {h['name']}({code}) {qty}주 매도 -> 손익 {pnl:+,}원 ({pnl_pct:+.2f}%)")
            time.sleep(0.5)

            r = find_open_record(code)
            if r is None:
                r = {"date": today, "code": code, "name": h["name"],
                     "signal_type": signal_type_filter or "manual_exit", "qty": qty, "mode": MODE}
                trades["records"].append(r)
            r["sell_order_no"] = result["order_no"]
            r["sell_time"] = now_iso()
            r["sell_price"] = sell_price
            r["actual_buy_price"] = buy_price
            r["actual_qty"] = qty
            r["pnl"] = pnl
            r["pnl_pct"] = pnl_pct
            r["status"] = "closed"
            closed_now.append(r)
        except Exception as e:
            print(f"  [FAIL] {h['name']} 매도 실패: {e}")
            r = find_open_record(code)
            if r:
                r["status"] = "exit_failed"

    return closed_now


def _print_summary_and_notify(trades, today, label="청산 완료"):
    """오늘 청산된 전체 거래 요약 출력 + 텔레그램 전송"""
    closed = [r for r in trades["records"]
              if r.get("status") == "closed" and r.get("sell_time", "").startswith(today)]
    if not closed:
        return
    total_pnl = sum(r["pnl"] for r in closed)
    wins = [r for r in closed if r["pnl"] > 0]
    losses = [r for r in closed if r["pnl"] < 0]
    avg_pct = sum(r["pnl_pct"] for r in closed) / len(closed)
    print(f"\n[일일 요약 {today}]")
    print(f"  거래: {len(closed)}건 ({len(wins)}승 {len(losses)}패)")
    print(f"  총 손익: {total_pnl:+,}원  평균 수익률: {avg_pct:+.2f}%")
    lines = [
        f"[Stock1] {today} {label} ({MODE.upper()})",
        f"거래 {len(closed)}건 | {len(wins)}승 {len(losses)}패 | 총손익 {total_pnl:+,}원 ({avg_pct:+.2f}%)",
    ]
    for r in closed:
        lines.append(f"  {r['name']}({r['code']}) {r.get('pnl', 0):+,}원 ({r.get('pnl_pct', 0):+.2f}%)")
    send_telegram("\n".join(lines))


# ──────────────────────────────────────────────
# 공통 매수 헬퍼
# ──────────────────────────────────────────────

def _run_buy(signals, signal_filter, trades, bal):
    """
    signal_filter 타입 종목을 매수하고 trades 레코드에 추가한다.
    반환: 매수된 레코드 수
    """
    targets = get_target_stocks(signals, signal_filter)
    if not targets:
        print(f"진입 대상 없음 ({signal_filter})")
        return 0

    today = today_str()
    print(f"\n진입 대상: {len(targets)}종목 ({signal_filter})")
    print("\n[매수 주문]")
    new_records = []
    for t in targets:
        entry_price = t["entry"]
        qty = calculate_position_size(bal["available_cash"], entry_price, len(targets))
        if qty <= 0:
            print(f"  {t['name']}({t['code']}): 수량 0 -> 스킵")
            continue
        try:
            result = ko.buy(t["code"], qty)
            print(f"  [OK] {t['name']}({t['code']}) {qty}주 매수 (SL/TP:{t.get('sl_source','')})")
            time.sleep(0.5)
            new_records.append({
                "date": today,
                "code": t["code"],
                "name": t["name"],
                "signal_type": t["signal_type"],
                "score": t["score"],
                "entry_planned": entry_price,
                "qty": qty,
                "stop_loss": t["stop_loss"],
                "target": t["target"],
                "atr14": t["atr14"],
                "sl_source": t.get("sl_source", "default"),
                "optimized_params": t["optimized_params"],
                "buy_order_no": result["order_no"],
                "buy_time": now_iso(),
                "status": "open",
                "mode": MODE,
            })
        except Exception as e:
            print(f"  [FAIL] {t['name']} 매수 실패: {e}")

    if new_records:
        trades["records"].extend(new_records)
        save_trades(trades)
        msg_lines = [f"[Stock1] {today} 매수 완료 ({MODE.upper()}) [{signal_filter}]"]
        for rec in new_records:
            msg_lines.append(f"  {rec['name']}({rec['code']}) {rec['qty']}주")
        send_telegram("\n".join(msg_lines))
    return len(new_records)


# ──────────────────────────────────────────────
# 09:00 장초반 진입
# ──────────────────────────────────────────────

def run_attack():
    """09:00 — day_open_attack 종목 매수. 09:30 이후면 스킵."""
    print("=" * 60)
    print(f"Stock1 장초반 진입 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    now = now_kst().time()
    if now > dtime(9, 30):
        print(f"진입 시간 종료 ({now.strftime('%H:%M:%S')} > 09:30) - 스킵")
        return

    signals = load_signals()
    if not signals:
        print("signals.json 없음")
        return

    trades = load_trades()
    today = today_str()
    already = [r for r in trades["records"]
               if r.get("date") == today and r.get("signal_type") == "day_open_attack"]
    if already:
        print(f"오늘({today}) 장초반 진입 이미 완료 ({len(already)}건) - 스킵")
        return

    if is_kill_switch_active(signals):
        msg = f"[Stock1] 킬스위치 발동 - 자동매매 중단 ({today})"
        print(msg)
        send_telegram(msg)
        return

    safe, reason = is_market_safe(signals)
    if not safe:
        msg = f"[Stock1] 시장 위험 판정 - 진입 스킵\n{reason}"
        print(msg)
        send_telegram(msg)
        return
    print(f"시장 판정: {reason}")

    ko.set_mode(MODE)
    bal = ko.get_balance()
    print(f"가용 자금: {bal['available_cash']:,}원")
    if bal["available_cash"] < 100_000:
        print("가용 자금 부족 (10만원 미만)")
        return

    max_daily_loss = calc_dynamic_max_loss(bal["total_eval"], signals)
    if check_daily_loss(trades) <= max_daily_loss:
        msg = f"[Stock1] 일일 최대 손실 초과 - 진입 중단"
        print(msg)
        send_telegram(msg)
        return

    _run_buy(signals, "day_open_attack", trades, bal)


# ──────────────────────────────────────────────
# 09:30 교대: 장초반 강제탈출 + 눌림 진입
# ──────────────────────────────────────────────

def run_rotate():
    """09:30 — day_open_attack 미청산 강제탈출 후 day_pullback_entry 매수."""
    print("=" * 60)
    print(f"Stock1 포지션 교대 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    trades = load_trades()
    today = today_str()

    # 1단계: 장초반 미청산 강제탈출
    print("\n[1단계] 장초반 공략 강제탈출")
    closed_now = _sell_positions(trades, signal_type_filter="day_open_attack")
    save_trades(trades)

    if closed_now:
        total = sum(r["pnl"] for r in closed_now)
        wins = sum(1 for r in closed_now if r["pnl"] > 0)
        losses = sum(1 for r in closed_now if r["pnl"] < 0)
        send_telegram(
            f"[Stock1] {today} 장초반 청산 ({MODE.upper()})\n"
            f"{len(closed_now)}건 | {wins}승 {losses}패 | {total:+,}원"
        )

    # 2단계: 눌림 진입
    print("\n[2단계] 눌림 진입")
    now = now_kst().time()
    if now > dtime(10, 0):
        print(f"눌림 진입 시간 종료 ({now.strftime('%H:%M:%S')} > 10:00) - 스킵")
        return

    signals = load_signals()
    if not signals:
        print("signals.json 없음")
        return

    already = [r for r in trades["records"]
               if r.get("date") == today and r.get("signal_type") == "day_pullback_entry"]
    if already:
        print(f"오늘({today}) 눌림 진입 이미 완료 ({len(already)}건) - 스킵")
        return

    if is_kill_switch_active(signals):
        print("킬스위치 발동 - 눌림 진입 스킵")
        return

    ko.set_mode(MODE)
    bal = ko.get_balance()
    print(f"가용 자금: {bal['available_cash']:,}원")
    if bal["available_cash"] < 100_000:
        print("가용 자금 부족")
        return

    _run_buy(signals, "day_pullback_entry", trades, bal)


# ──────────────────────────────────────────────
# 10:00 눌림 강제탈출 + 일일 요약
# ──────────────────────────────────────────────

def run_cleanup():
    """10:00 — day_pullback_entry 미청산 강제탈출 + 일일 요약 텔레그램."""
    print("=" * 60)
    print(f"Stock1 눌림 청산 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    trades = load_trades()
    today = today_str()

    _sell_positions(trades, signal_type_filter="day_pullback_entry")
    save_trades(trades)
    _print_summary_and_notify(trades, today, label="일일 청산 완료")


# ──────────────────────────────────────────────
# 전체 청산 (비상/수동용)
# ──────────────────────────────────────────────

def run_exit():
    print("=" * 60)
    print(f"Stock1 자동매매 청산 ({MODE.upper()} 모드)")
    print(f"실행 시간: {now_iso()}")
    print("=" * 60)

    trades = load_trades()
    today = today_str()

    _sell_positions(trades)
    save_trades(trades)
    _print_summary_and_notify(trades, today)


def main():
    if len(sys.argv) < 2:
        print("사용법: py auto_trader.py [attack|rotate|cleanup|entry|exit|status]")
        print("  attack  - 09:00 장초반 공략 매수")
        print("  rotate  - 09:30 장초반 강제탈출 + 눌림 진입")
        print("  cleanup - 10:00 눌림 강제탈출 + 일일 요약")
        print("  entry   - (하위호환) 전체 진입")
        print("  exit    - (비상) 전체 청산")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "attack":
        run_attack()
    elif cmd == "rotate":
        run_rotate()
    elif cmd == "cleanup":
        run_cleanup()
    elif cmd == "entry":
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
