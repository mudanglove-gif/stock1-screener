"""
Stock1 시간외 보조 스크리너
- 16:00 단타 시그널 종목의 시간외 단일가 상태 확인
- 매일 20:10 KST 실행
"""

import json
import os
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

SIGNALS_URL = "https://mudanglove-gif.github.io/stock1-screener/signals.json"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "docs", "signals.json")
KST = timezone(timedelta(hours=9))


def now_kst_iso():
    return datetime.now(KST).isoformat()


def fetch_signals_json():
    """GitHub Pages에서 signals.json fetch"""
    try:
        resp = requests.get(SIGNALS_URL, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"signals.json fetch 실패: {e}")
        # fallback: 로컬 파일
        try:
            with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None


def fetch_after_market_data(code):
    """네이버 모바일 API에서 시간외 단일가 데이터 조회"""
    result = {
        "available": False,
        "price": None,
        "change_pct": None,
        "volume": None,
    }
    try:
        url = f"https://m.stock.naver.com/api/stock/{code}/basic"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if resp.status_code != 200:
            return result
        data = resp.json()

        # overMarketPriceInfo 추출
        omp = data.get("overMarketPriceInfo")
        if not omp:
            return result

        # 시간외 거래 활성 여부 확인 (장중이면 정규장이라 시간외 데이터 없음)
        status = omp.get("overMarketStatus")
        # OPEN, CLOSED, etc. — 가격이 있으면 사용
        over_price = omp.get("overPrice")
        if not over_price:
            return result

        try:
            price = int(over_price.replace(",", ""))
        except (ValueError, AttributeError):
            return result

        if price <= 0:
            return result

        result["price"] = price
        result["available"] = True

        # 등락률 (이미 API가 계산해줌)
        ratio = omp.get("fluctuationsRatio")
        if ratio:
            try:
                # 부호 처리: compareToPreviousPrice.code (1,2: 상승, 4,5: 하락)
                sign_code = omp.get("compareToPreviousPrice", {}).get("code", "3")
                pct = float(ratio)
                if sign_code in ("4", "5"):
                    pct = -pct
                result["change_pct"] = pct
            except (ValueError, TypeError):
                pass

    except Exception as e:
        print(f"  {code} 시간외 조회 실패: {e}")

    return result


def determine_status(results):
    """조회 상태 판정"""
    if not results:
        return "failed"
    checked = sum(1 for r in results if r.get("available"))
    if checked == len(results):
        return "checked"
    elif checked > 0:
        return "partial"
    else:
        return "failed"


def main():
    print("=== Stock1 시간외 보조 스크리너 ===")
    print(f"실행 시간: {now_kst_iso()}")

    # 1. signals.json fetch
    signals = fetch_signals_json()
    if signals is None:
        print("ERROR: signals.json을 가져올 수 없습니다.")
        return

    day_trade = signals.get("day_trade")
    if not day_trade:
        print("단타 시그널 데이터 없음. 종료.")
        return

    # 2. 단타 추천 종목 추출
    target_stocks = []
    seen_codes = set()
    for signal_type in ["day_open_attack", "day_pullback_entry"]:
        for stock in day_trade.get(signal_type, []):
            code = stock.get("code")
            if code and code not in seen_codes:
                target_stocks.append((signal_type, stock))
                seen_codes.add(code)

    if not target_stocks:
        print("단타 추천 종목 없음. 종료.")
        day_trade["after_market_checked_at"] = now_kst_iso()
        day_trade["after_market_status"] = "checked"
        save_and_exit(signals)
        return

    print(f"체크 대상: {len(target_stocks)}종목")

    # 3. 시간외 데이터 조회
    results = []
    for signal_type, stock in target_stocks:
        code = stock["code"]
        name = stock.get("name", code)
        print(f"  {name}({code}) 조회 중...")

        after_data = fetch_after_market_data(code)
        results.append(after_data)

        # 4. 판정
        badge = None
        disqualified = False
        disqualify_reason = None

        if after_data["available"] and after_data["change_pct"] is not None:
            change_pct = after_data["change_pct"]

            if change_pct > 1.0:
                badge = "after_hours_momentum"
                print(f"    -> ⚡ 시간외 모멘텀 +{change_pct:.1f}%")

            if change_pct <= -3.0:
                disqualified = True
                disqualify_reason = "after_hours_crash"
                print(f"    -> ⛔ 시간외 급락 {change_pct:.1f}% — 실격 처리")

            if not badge and not disqualified:
                print(f"    -> 시간외 {change_pct:+.1f}% (변동 없음)")
        else:
            print(f"    -> 시간외 체결 없음")

        # 5. signals.json 업데이트
        stock["after_market"] = {
            "checked_at": now_kst_iso(),
            "price": after_data.get("price"),
            "change_pct": after_data.get("change_pct"),
            "volume": after_data.get("volume"),
            "badge": badge,
            "disqualified": disqualified,
            "disqualify_reason": disqualify_reason,
        }

    # 6. 메타데이터 업데이트
    day_trade["after_market_checked_at"] = now_kst_iso()
    day_trade["after_market_status"] = determine_status(results)

    print(f"\n상태: {day_trade['after_market_status']}")
    save_and_exit(signals)


def save_and_exit(signals):
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)
    print(f"저장 완료: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
