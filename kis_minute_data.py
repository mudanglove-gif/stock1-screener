"""
KIS OpenAPI 분봉 데이터 수집 모듈
- 환경변수 KIS_APP_KEY, KIS_APP_SECRET 필요
- 토큰 자동 발급 + 캐시
- 1/3/5/10/15/30/60분봉 지원
- 최근 30거래일 가능

사용:
  import kis_minute_data as km
  km.set_credentials(app_key, app_secret)
  df = km.fetch_minute_data("005930", interval="30", days=30)
"""

import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests

# 실전투자 URL (모의투자가 아님)
KIS_URL = "https://openapi.koreainvestment.com:9443"


def _load_env_file():
    """프로젝트 루트의 .env 파일을 환경변수로 로드 (python-dotenv 의존성 없이)"""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


# 모듈 import 시 자동 로드
_load_env_file()

_app_key: Optional[str] = None
_app_secret: Optional[str] = None
_access_token: Optional[str] = None
_token_expires_at: float = 0


def set_credentials(app_key: str, app_secret: str):
    """KIS API 키 설정"""
    global _app_key, _app_secret, _access_token, _token_expires_at
    _app_key = app_key
    _app_secret = app_secret
    _access_token = None
    _token_expires_at = 0


def _load_credentials_from_env():
    """환경변수에서 키 로드"""
    global _app_key, _app_secret
    if _app_key is None:
        _app_key = os.environ.get("KIS_APP_KEY")
    if _app_secret is None:
        _app_secret = os.environ.get("KIS_APP_SECRET")
    return _app_key is not None and _app_secret is not None


def _get_access_token() -> Optional[str]:
    """토큰 발급 또는 캐시 반환"""
    global _access_token, _token_expires_at
    if _access_token and time.time() < _token_expires_at:
        return _access_token

    if not _load_credentials_from_env():
        return None

    url = f"{KIS_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": _app_key,
        "appsecret": _app_secret,
    }
    try:
        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        _access_token = data["access_token"]
        # 토큰 유효시간: 86400초 (24시간), 안전하게 23시간으로
        _token_expires_at = time.time() + 23 * 3600
        return _access_token
    except Exception as e:
        print(f"KIS 토큰 발급 실패: {e}")
        return None


def _fetch_one_day_minute(stock_code: str, date_str: str, token: str) -> list:
    """
    특정 날짜의 1분봉 데이터 1회 호출 (최대 31건 = 09:00~14:30 일부)
    KIS는 시간 역순으로 응답 → start_time을 페이징으로 변경
    """
    url = f"{KIS_URL}/uapi/domestic-stock/v1/quotations/inquire-time-dailychartprice"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": _app_key,
        "appsecret": _app_secret,
        "tr_id": "FHKST03010230",  # 주식일분봉조회 (과거 분봉)
    }
    rows = []
    # 30분 간격으로 페이징 (09:30, 10:30, 11:30, 13:30, 14:30, 15:30)
    # 각 호출이 시간 역순 31건이므로 09:30 호출 = 약 09:00~09:30
    page_hours = ["153000", "143000", "133000", "123000", "113000", "103000", "093000"]
    for hour in page_hours:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": hour,
            "FID_INPUT_DATE_1": date_str,
            "FID_PW_DATA_INCU_YN": "N",
            "FID_FAKE_TICK_INCU_YN": "",
        }
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("rt_cd") != "0":
                continue
            output2 = data.get("output2", [])
            for row in output2:
                d_str = row.get("stck_bsop_date", "")
                t_str = row.get("stck_cntg_hour", "")
                if not d_str or not t_str:
                    continue
                try:
                    dt = datetime.strptime(f"{d_str}{t_str}", "%Y%m%d%H%M%S")
                    rows.append({
                        "datetime": dt,
                        "open": int(row.get("stck_oprc", 0)),
                        "high": int(row.get("stck_hgpr", 0)),
                        "low": int(row.get("stck_lwpr", 0)),
                        "close": int(row.get("stck_prpr", 0)),
                        "volume": int(row.get("cntg_vol", 0)),
                    })
                except (ValueError, TypeError):
                    continue
            time.sleep(0.1)
        except Exception:
            continue
    return rows


def fetch_minute_data(stock_code: str, interval: str = "30", days: int = 30) -> Optional[pd.DataFrame]:
    """
    과거 분봉 데이터 조회 (KIS FHKST03010230)
    Args:
        stock_code: 종목코드 (6자리)
        interval: 1/3/5/10/15/30/60분 (1분봉을 받아서 리샘플링)
        days: 최근 N거래일
    Returns:
        DataFrame [datetime, open, high, low, close, volume] or None
    """
    token = _get_access_token()
    if token is None:
        return None

    end_dt = datetime.now()
    all_rows = []
    fetched_days = 0
    day_offset = 0

    while fetched_days < days and day_offset < days * 2:
        date = end_dt - timedelta(days=day_offset)
        # 주말 스킵
        if date.weekday() >= 5:
            day_offset += 1
            continue
        date_str = date.strftime("%Y%m%d")
        rows = _fetch_one_day_minute(stock_code, date_str, token)
        if rows:
            all_rows.extend(rows)
            fetched_days += 1
        day_offset += 1

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows).drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)

    # interval에 맞게 리샘플링
    if interval != "1":
        df = df.set_index("datetime")
        rule = f"{interval}min"
        df = df.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }).dropna().reset_index()

    return df


def is_available() -> bool:
    """KIS API 사용 가능 여부 (키가 환경변수에 있는지)"""
    return _load_credentials_from_env()


if __name__ == "__main__":
    # 테스트
    if is_available():
        print("KIS 키 발견, 삼성전자 30분봉 5일치 조회...")
        df = fetch_minute_data("005930", interval="30", days=5)
        if df is not None:
            print(f"수신: {len(df)}건")
            print(df.tail())
        else:
            print("조회 실패")
    else:
        print("KIS_APP_KEY/KIS_APP_SECRET 환경변수 없음")
        print("export KIS_APP_KEY=...; export KIS_APP_SECRET=...")
