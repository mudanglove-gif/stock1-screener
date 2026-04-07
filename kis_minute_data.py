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
from typing import Optional

import pandas as pd
import requests

# 실전투자 URL (모의투자가 아님)
KIS_URL = "https://openapi.koreainvestment.com:9443"

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


def fetch_minute_data(stock_code: str, interval: str = "30", days: int = 30) -> Optional[pd.DataFrame]:
    """
    분봉 데이터 조회
    Args:
        stock_code: 종목코드 (6자리)
        interval: 1/3/5/10/15/30/60분
        days: 최근 N거래일
    Returns:
        DataFrame [datetime, open, high, low, close, volume] or None
    """
    token = _get_access_token()
    if token is None:
        return None

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": _app_key,
        "appsecret": _app_secret,
        "tr_id": "FHKST03010200",  # 주식분봉조회
    }

    end_dt = datetime.now()
    all_rows = []

    # 일자별로 페이징 (1회 100건 최대)
    for d in range(days):
        date = end_dt - timedelta(days=d)
        date_str = date.strftime("%Y%m%d")
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": stock_code,
            "FID_INPUT_HOUR_1": "153000",  # 장 마감 시각
            "FID_PW_DATA_INCU_YN": "Y",
        }
        url = f"{KIS_URL}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("rt_cd") != "0":
                continue
            output2 = data.get("output2", [])
            for row in output2:
                date_str = row.get("stck_bsop_date", "")
                time_str = row.get("stck_cntg_hour", "")
                if not date_str or not time_str:
                    continue
                dt_str = f"{date_str}{time_str}"
                try:
                    dt = datetime.strptime(dt_str, "%Y%m%d%H%M%S")
                    all_rows.append({
                        "datetime": dt,
                        "open": int(row.get("stck_oprc", 0)),
                        "high": int(row.get("stck_hgpr", 0)),
                        "low": int(row.get("stck_lwpr", 0)),
                        "close": int(row.get("stck_prpr", 0)),
                        "volume": int(row.get("cntg_vol", 0)),
                    })
                except (ValueError, TypeError):
                    continue
            time.sleep(0.1)  # API 호출 제한 회피
        except Exception as e:
            print(f"  {stock_code} 분봉 조회 실패: {e}")
            continue

    if not all_rows:
        return None

    df = pd.DataFrame(all_rows).drop_duplicates("datetime").sort_values("datetime")

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
