"""
Stock1 08:50 매크로 Go/No-Go 판정
- 미국 증시, VIX, 한국 선행지표, 환율, 이벤트 종합 평가
- 100점 만점 스코어링 → 낙관/보통/위험 3단계
- signals.json morning_check 섹션 업데이트

배점 (피드백 반영):
- 미국 증시 25 + VIX 15 + 한국 선행 35 + 환율 10 + 이벤트 15

실행: 매일 08:50 KST (월~금)
"""

import json
import os
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

KST = timezone(timedelta(hours=9))
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "docs")
SIGNALS_PATH = os.path.join(OUTPUT_DIR, "signals.json")

FRED_API_KEY = "d41ee4f2e4718a0e25f8dfabaabe3ec4"


def now_kst():
    return datetime.now(KST).isoformat()


# ──────────────────────────────────────────────
# 데이터 수집
# ──────────────────────────────────────────────

def fetch_us_market():
    """yfinance로 S&P500, 나스닥 전일대비 등락률"""
    try:
        import yfinance as yf
        data = yf.download(["^GSPC", "^IXIC"], period="3d", progress=False, auto_adjust=True)
        closes = data["Close"]
        sp = closes["^GSPC"].dropna()
        nq = closes["^IXIC"].dropna()
        sp_change = (sp.iloc[-1] / sp.iloc[-2] - 1) * 100
        nq_change = (nq.iloc[-1] / nq.iloc[-2] - 1) * 100
        return {
            "sp500_change_pct": round(float(sp_change), 2),
            "nasdaq_change_pct": round(float(nq_change), 2),
            "available": True,
        }
    except Exception as e:
        print(f"  US 증시 조회 실패: {e}")
        return {"available": False, "sp500_change_pct": 0, "nasdaq_change_pct": 0}


def fetch_vix():
    """FRED API로 VIX 종가 + 변동"""
    try:
        from fredapi import Fred
        fred = Fred(api_key=FRED_API_KEY)
        vix = fred.get_series("VIXCLS").dropna()
        current = float(vix.iloc[-1])
        previous = float(vix.iloc[-2])
        return {
            "vix_close": round(current, 2),
            "vix_change": round(current - previous, 2),
            "available": True,
        }
    except Exception as e:
        print(f"  VIX 조회 실패: {e}")
        return {"available": False, "vix_close": 20, "vix_change": 0}


def fetch_kpi200_futures():
    """네이버 모바일 API로 KOSPI200 지수 등락"""
    try:
        url = "https://m.stock.naver.com/api/index/KPI200/basic"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if resp.status_code != 200:
            return {"available": False, "kpi200_change_pct": 0}
        data = resp.json()
        ratio = float(data.get("fluctuationsRatio", 0))
        # 부호 처리
        sign_code = data.get("compareToPreviousPrice", {}).get("code", "3")
        if sign_code in ("4", "5"):
            ratio = -ratio
        return {
            "kpi200_change_pct": ratio,
            "available": True,
        }
    except Exception as e:
        print(f"  KPI200 조회 실패: {e}")
        return {"available": False, "kpi200_change_pct": 0}


def fetch_fx():
    """네이버 금융에서 원/달러 환율 + 변동"""
    try:
        url = "https://finance.naver.com/marketindex/exchangeDetail.naver?marketindexCd=FX_USDKRW"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(resp.text, "html.parser")
        rate_text = soup.select_one(".no_today").get_text(strip=True)
        rate = float(rate_text.replace("원", "").replace(",", "").strip())
        # 전일대비
        change_el = soup.select_one(".no_exday em")
        change = 0.0
        if change_el:
            try:
                change_text = change_el.get_text(strip=True).replace(",", "").replace("원", "")
                change = float(change_text)
            except Exception:
                pass
        return {
            "usd_krw": rate,
            "usd_krw_change": change,
            "available": True,
        }
    except Exception as e:
        print(f"  환율 조회 실패: {e}")
        return {"available": False, "usd_krw": 0, "usd_krw_change": 0}


def fetch_foreign_futures():
    """네이버 모바일 API로 KOSPI 외국인 매매동향 (현물 기준, 선물 데이터는 KIS API 필요)"""
    try:
        url = "https://m.stock.naver.com/api/index/KOSPI/integration"
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if resp.status_code != 200:
            return {"available": False, "net_buy": 0}
        data = resp.json()
        deal = data.get("dealTrendInfo", {})
        foreign = deal.get("foreignValue", "0")
        # "+844" / "-1,105" 형태 → 숫자
        try:
            net_buy = int(foreign.replace(",", "").replace("+", ""))
        except (ValueError, AttributeError):
            net_buy = 0
        return {
            "available": True,
            "net_buy": net_buy,  # 단위 추정: 억원
        }
    except Exception as e:
        print(f"  외국인 매매동향 조회 실패: {e}")
        return {"available": False, "net_buy": 0}


def fetch_events():
    """이벤트 캘린더 — 하드코딩 (분기별 업데이트 필요)"""
    today = datetime.now(KST).strftime("%Y-%m-%d")
    events = MAJOR_EVENTS.get(today, [])
    return {"today_events": events, "has_event": len(events) > 0}


MAJOR_EVENTS = {
    # 2026 Q2 주요 이벤트 (분기별로 갱신 필요)
    # 미국 고용지표 (NFP) - 매월 첫째 금요일
    "2026-04-03": ["미국 3월 고용지표 (NFP)"],
    "2026-05-01": ["미국 4월 고용지표 (NFP)"],
    "2026-06-05": ["미국 5월 고용지표 (NFP)"],
    # 미국 CPI - 매월 중순
    "2026-04-10": ["미국 3월 CPI"],
    "2026-05-12": ["미국 4월 CPI"],
    "2026-06-10": ["미국 5월 CPI"],
    # FOMC - 분기별 + 회의록
    "2026-04-29": ["FOMC 금리 결정 (4월)"],
    "2026-06-17": ["FOMC 금리 결정 + 점도표 (6월)"],
    # 한국은행 금통위 - 매월 중순
    "2026-04-10": ["한국은행 금통위"],
    "2026-05-22": ["한국은행 금통위"],
    "2026-07-10": ["한국은행 금통위"],
    # 미국 PCE 물가지수 - 월 마지막 영업일
    "2026-04-30": ["미국 3월 PCE"],
    "2026-05-30": ["미국 4월 PCE"],
    "2026-06-26": ["미국 5월 PCE"],
}


# ──────────────────────────────────────────────
# 스코어링
# ──────────────────────────────────────────────

def score_us_market(us):
    """미국 증시 25점"""
    if not us["available"]:
        return 12  # 중립값
    sp = us["sp500_change_pct"]
    nq = us["nasdaq_change_pct"]
    if sp >= 1.0 and nq >= 1.0:
        return 25
    elif sp >= 0.3 and nq >= 0.3:
        return 21
    elif abs(sp) < 0.3 and abs(nq) < 0.3:
        return 13
    elif sp <= -1 or nq <= -1:
        return 3
    elif sp <= -0.3 and nq <= -0.3:
        return 7
    return 13


def score_vix(vix):
    """VIX 15점"""
    if not vix["available"]:
        return 8
    v = vix["vix_close"]
    change = vix["vix_change"]
    score = 15 if v < 15 else 12 if v < 20 else 7 if v < 25 else 3 if v < 30 else 0
    if change >= 5:
        score = max(0, score - 5)
    return score


def score_korea_leading(kpi, foreign):
    """한국 선행 35점 (KPI200/선물 + 외국인)"""
    if not kpi["available"]:
        return 17  # 중립값
    change = kpi["kpi200_change_pct"]
    if change >= 0.5:
        score = 25
    elif change >= 0:
        score = 18
    elif change >= -0.5:
        score = 12
    elif change >= -1:
        score = 5
    else:
        score = 0
    # 외국인 보조 +5/-3 (구현 시)
    if foreign["available"]:
        if foreign["net_buy"] > 0:
            score += 5
        elif foreign["net_buy"] < 0:
            score -= 3
    return max(0, min(35, score))


def score_fx(fx):
    """환율 10점"""
    if not fx["available"]:
        return 5
    change = fx["usd_krw_change"]
    if change <= -10:
        return 10
    elif abs(change) <= 10:
        return 8
    elif change <= 20:
        return 4
    return 0


def score_events(events):
    """이벤트 15점"""
    if events["has_event"]:
        return 8  # 이벤트 있으면 보수적
    return 15


def check_override(us, vix, kpi, fx):
    """즉시 위험 오버라이드 — 총점과 무관하게 위험 판정"""
    reasons = []
    if us["available"] and (us["sp500_change_pct"] <= -3 or us["nasdaq_change_pct"] <= -3):
        reasons.append(f"미국 -3%↓ (S&P {us['sp500_change_pct']}%, Nasdaq {us['nasdaq_change_pct']}%)")
    if vix["available"] and vix["vix_close"] > 35:
        reasons.append(f"VIX {vix['vix_close']} > 35")
    if fx["available"] and fx["usd_krw_change"] >= 30:
        reasons.append(f"원/달러 +{fx['usd_krw_change']}원 급등")
    if kpi["available"] and kpi["kpi200_change_pct"] <= -2:
        reasons.append(f"KPI200 {kpi['kpi200_change_pct']}%")
    return reasons


def calculate_compound_penalty(scores):
    """복합 상호작용 페널티: 3개+ 카테고리 동시 하위 30% → -10점"""
    thresholds = {
        "us_market": 25 * 0.3,
        "vix": 15 * 0.3,
        "korea_leading": 35 * 0.3,
        "fx": 10 * 0.3,
        "events": 15 * 0.3,
    }
    low_count = sum(1 for k, v in scores.items() if v < thresholds.get(k, 0))
    return -10 if low_count >= 3 else 0


# ──────────────────────────────────────────────
# 메인
# ──────────────────────────────────────────────

def run_morning_check():
    print("=" * 60)
    print("Stock1 08:50 매크로 Go/No-Go 판정")
    print(f"실행 시간: {now_kst()}")
    print("=" * 60)

    # 데이터 수집
    print("\n[데이터 수집]")
    us = fetch_us_market()
    vix = fetch_vix()
    kpi = fetch_kpi200_futures()
    fx = fetch_fx()
    foreign = fetch_foreign_futures()
    events = fetch_events()

    print(f"  US: SP500 {us.get('sp500_change_pct', '?')}%, Nasdaq {us.get('nasdaq_change_pct', '?')}%")
    print(f"  VIX: {vix.get('vix_close', '?')} ({vix.get('vix_change', '?'):+})")
    print(f"  KPI200: {kpi.get('kpi200_change_pct', '?')}%")
    print(f"  USD/KRW: {fx.get('usd_krw', '?')}원 ({fx.get('usd_krw_change', '?'):+})")

    # 즉시 위험 오버라이드 체크
    override_reasons = check_override(us, vix, kpi, fx)

    # 스코어링
    scores = {
        "us_market": score_us_market(us),
        "vix": score_vix(vix),
        "korea_leading": score_korea_leading(kpi, foreign),
        "fx": score_fx(fx),
        "events": score_events(events),
    }
    compound_penalty = calculate_compound_penalty(scores)
    total_score = sum(scores.values()) + compound_penalty

    # 판정
    if override_reasons:
        verdict = "danger"
        verdict_emoji = "🔴"
        verdict_reason = "즉시 위험: " + " / ".join(override_reasons)
    elif total_score >= 70:
        verdict = "optimistic"
        verdict_emoji = "🟢"
        verdict_reason = "낙관"
    elif total_score >= 40:
        verdict = "normal"
        verdict_emoji = "🟡"
        verdict_reason = "보통 — 사용자 승인 필요"
    else:
        verdict = "danger"
        verdict_emoji = "🔴"
        verdict_reason = "위험 — 단타 금지"

    print(f"\n[스코어]")
    print(f"  미국 증시: {scores['us_market']}/25")
    print(f"  VIX: {scores['vix']}/15")
    print(f"  한국 선행: {scores['korea_leading']}/35")
    print(f"  환율: {scores['fx']}/10")
    print(f"  이벤트: {scores['events']}/15")
    if compound_penalty:
        print(f"  복합 페널티: {compound_penalty}")
    print(f"  총점: {total_score}/100")
    print(f"\n판정: {verdict_emoji} {verdict_reason}")

    # signals.json 업데이트
    if not os.path.exists(SIGNALS_PATH):
        print(f"\nsignals.json 없음, 종료")
        return

    with open(SIGNALS_PATH, "r", encoding="utf-8") as f:
        signals = json.load(f)

    if "day_trade" not in signals:
        signals["day_trade"] = {}

    signals["day_trade"]["morning_check"] = {
        "checked_at": now_kst(),
        "verdict": verdict,
        "total_score": total_score,
        "score_breakdown": scores,
        "compound_penalty": compound_penalty,
        "override_triggered": len(override_reasons) > 0,
        "override_reasons": override_reasons,
        "verdict_reason": verdict_reason,
        "details": {
            "sp500_change_pct": us.get("sp500_change_pct"),
            "nasdaq_change_pct": us.get("nasdaq_change_pct"),
            "vix_close": vix.get("vix_close"),
            "vix_change": vix.get("vix_change"),
            "kpi200_change_pct": kpi.get("kpi200_change_pct"),
            "usd_krw": fx.get("usd_krw"),
            "usd_krw_change": fx.get("usd_krw_change"),
            "events": events.get("today_events", []),
        },
    }

    with open(SIGNALS_PATH, "w", encoding="utf-8") as f:
        json.dump(signals, f, ensure_ascii=False, indent=2)

    print(f"\nsignals.json 업데이트 완료")
    return verdict


if __name__ == "__main__":
    run_morning_check()
