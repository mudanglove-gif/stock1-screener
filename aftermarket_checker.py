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
    """네이버 금융에서 시간외 단일가 데이터 조회"""
    result = {
        "available": False,
        "price": None,
        "change_pct": None,
        "volume": None,
    }
    try:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")

        # 시간외 단일가 영역
        after_area = soup.select_one("div.after_hour")
        if not after_area:
            # 대안: 시간외 거래 테이블
            after_area = soup.select_one("table.after")

        if after_area:
            # 시간외 체결가
            price_el = after_area.select_one("span.tah.p11")
            if price_el:
                price_text = price_el.get_text(strip=True).replace(",", "")
                if price_text.isdigit():
                    result["price"] = int(price_text)
                    result["available"] = True

        # 시간외 가격을 못 찾으면 m.stock.naver.com 시도
        if not result["available"]:
            url2 = f"https://m.stock.naver.com/api/stock/{code}/integration"
            resp2 = requests.get(url2, headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
            if resp2.status_code == 200:
                data = resp2.json()
                # 시간외 단일가 정보
                for info in data.get("dealInfos", []):
                    if "시간외" in info.get("key", ""):
                        val = info.get("value", "").replace(",", "").strip()
                        if val.isdigit():
                            result["price"] = int(val)
                            result["available"] = True
                            break

        # 종가 대비 등락률 계산
        if result["available"] and result["price"]:
            # 정규장 종가 가져오기
            close_el = soup.select_one("p.no_today span.blind")
            if close_el:
                close_text = close_el.get_text(strip=True).replace(",", "")
                if close_text.isdigit():
                    close = int(close_text)
                    if close > 0:
                        result["change_pct"] = round((result["price"] - close) / close * 100, 2)

        # 시간외 거래량 (있으면)
        if after_area:
            vol_els = after_area.select("span.tah.p11")
            for el in vol_els:
                text = el.get_text(strip=True).replace(",", "")
                if text.isdigit() and int(text) > 100:  # 가격이 아닌 거래량
                    if result["price"] and int(text) != result["price"]:
                        result["volume"] = int(text)
                        break

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
