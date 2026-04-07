"""
KIS OpenAPI 주문 모듈
- 모의투자 / 실전투자 자동 분기
- 매수/매도, 잔고 조회, 미체결 주문 조회, 취소
- 환경변수:
  KIS_MOCK_APP_KEY, KIS_MOCK_APP_SECRET, KIS_MOCK_ACCOUNT_NO  (모의)
  KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NO                 (실전)

사용:
  import kis_order as ko
  ko.set_mode('mock')  # or 'real'
  ko.buy('005930', 1)
  ko.sell('005930', 1)
  ko.get_balance()
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests

# 환경 분기
URLS = {
    "mock": "https://openapivts.koreainvestment.com:29443",
    "real": "https://openapi.koreainvestment.com:9443",
}

# TR ID
TR_IDS = {
    "mock": {
        "balance": "VTTC8434R",
        "buy": "VTTC0802U",   # 주식주문(현금) 매수
        "sell": "VTTC0801U",  # 주식주문(현금) 매도
        "open_orders": "VTTC8036R",  # 정정취소가능주문조회
    },
    "real": {
        "balance": "TTTC8434R",
        "buy": "TTTC0802U",
        "sell": "TTTC0801U",
        "open_orders": "TTTC8036R",
    },
}

_mode: str = "mock"
_app_key: Optional[str] = None
_app_secret: Optional[str] = None
_account_no: Optional[str] = None
_access_token: Optional[str] = None
_token_expires_at: float = 0


def _load_env():
    """프로젝트 루트의 .env 자동 로드"""
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


_load_env()


def set_mode(mode: str):
    """모드 변경: 'mock' or 'real'"""
    global _mode, _app_key, _app_secret, _account_no, _access_token, _token_expires_at
    if mode not in ("mock", "real"):
        raise ValueError("mode must be 'mock' or 'real'")
    _mode = mode
    if mode == "mock":
        _app_key = os.environ.get("KIS_MOCK_APP_KEY")
        _app_secret = os.environ.get("KIS_MOCK_APP_SECRET")
        _account_no = os.environ.get("KIS_MOCK_ACCOUNT_NO")
    else:
        _app_key = os.environ.get("KIS_APP_KEY")
        _app_secret = os.environ.get("KIS_APP_SECRET")
        _account_no = os.environ.get("KIS_ACCOUNT_NO")
    _access_token = None
    _token_expires_at = 0


def _ensure_auth():
    if _app_key is None:
        set_mode(_mode)


def _get_token() -> str:
    global _access_token, _token_expires_at
    _ensure_auth()
    if _access_token and time.time() < _token_expires_at:
        return _access_token

    if not _app_key or not _app_secret:
        raise Exception(f"{_mode} 키가 환경변수에 없음")

    url = f"{URLS[_mode]}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": _app_key,
        "appsecret": _app_secret,
    }
    resp = requests.post(url, json=body, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    _access_token = data["access_token"]
    _token_expires_at = time.time() + 23 * 3600
    return _access_token


def _split_account() -> tuple:
    if not _account_no:
        raise Exception(f"{_mode} 계좌번호 없음")
    if "-" in _account_no:
        cano, prdt = _account_no.split("-")
    elif len(_account_no) >= 10:
        cano, prdt = _account_no[:8], _account_no[8:]
    else:
        cano, prdt = _account_no, "01"
    return cano, prdt


def get_balance() -> dict:
    """잔고 조회"""
    token = _get_token()
    cano, prdt = _split_account()
    url = f"{URLS[_mode]}/uapi/domestic-stock/v1/trading/inquire-balance"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": _app_key,
        "appsecret": _app_secret,
        "tr_id": TR_IDS[_mode]["balance"],
    }
    params = {
        "CANO": cano,
        "ACNT_PRDT_CD": prdt,
        "AFHR_FLPR_YN": "N",
        "OFL_YN": "",
        "INQR_DVSN": "02",
        "UNPR_DVSN": "01",
        "FUND_STTL_ICLD_YN": "N",
        "FNCG_AMT_AUTO_RDPT_YN": "N",
        "PRCS_DVSN": "01",
        "CTX_AREA_FK100": "",
        "CTX_AREA_NK100": "",
    }
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    data = resp.json()
    if data.get("rt_cd") != "0":
        raise Exception(f"잔고 조회 실패: {data.get('msg1')}")

    output1 = data.get("output1", [])
    output2 = data.get("output2", [{}])[0]

    holdings = []
    for h in output1:
        qty = int(h.get("hldg_qty", "0"))
        if qty <= 0:
            continue
        holdings.append({
            "code": h.get("pdno"),
            "name": h.get("prdt_name"),
            "qty": qty,
            "avg_price": int(float(h.get("pchs_avg_pric", "0"))),
            "current_price": int(h.get("prpr", "0")),
            "eval_amount": int(h.get("evlu_amt", "0")),
            "pnl": int(h.get("evlu_pfls_amt", "0")),
            "pnl_rate": float(h.get("evlu_pfls_rt", "0")),
        })

    return {
        "holdings": holdings,
        "cash": int(output2.get("dnca_tot_amt", "0")),
        "available_cash": int(output2.get("nxdy_excc_amt", "0")),
        "total_eval": int(output2.get("tot_evlu_amt", "0")),
        "total_purchase": int(output2.get("pchs_amt_smtl_amt", "0")),
        "total_pnl": int(output2.get("evlu_pfls_smtl_amt", "0")),
        "total_pnl_rate": float(output2.get("asst_icdc_erng_rt", "0")),
    }


def _place_order(side: str, code: str, qty: int, price: int = 0, order_type: str = "01") -> dict:
    """
    주문 실행
    Args:
        side: 'buy' or 'sell'
        code: 종목코드
        qty: 수량
        price: 가격 (시장가는 0)
        order_type: '00'=지정가, '01'=시장가
    """
    token = _get_token()
    cano, prdt = _split_account()
    url = f"{URLS[_mode]}/uapi/domestic-stock/v1/trading/order-cash"
    tr_id = TR_IDS[_mode]["buy"] if side == "buy" else TR_IDS[_mode]["sell"]

    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": _app_key,
        "appsecret": _app_secret,
        "tr_id": tr_id,
    }
    body = {
        "CANO": cano,
        "ACNT_PRDT_CD": prdt,
        "PDNO": code,
        "ORD_DVSN": order_type,
        "ORD_QTY": str(qty),
        "ORD_UNPR": str(price),
    }
    resp = requests.post(url, headers=headers, json=body, timeout=10)
    data = resp.json()
    if data.get("rt_cd") != "0":
        raise Exception(f"{side} 주문 실패: {data.get('msg1')} ({data.get('msg_cd')})")

    output = data.get("output", {})
    return {
        "ok": True,
        "order_no": output.get("ODNO"),       # 주문번호
        "order_time": output.get("ORD_TMD"),  # 주문시각
        "krx_fwdg_no": output.get("KRX_FWDG_ORD_ORGNO"),
        "msg": data.get("msg1"),
    }


def buy(code: str, qty: int, price: int = 0, order_type: str = "01") -> dict:
    """매수 주문 (기본: 시장가)"""
    return _place_order("buy", code, qty, price, order_type)


def sell(code: str, qty: int, price: int = 0, order_type: str = "01") -> dict:
    """매도 주문 (기본: 시장가)"""
    return _place_order("sell", code, qty, price, order_type)


def buy_limit(code: str, qty: int, price: int) -> dict:
    """지정가 매수"""
    return _place_order("buy", code, qty, price, "00")


def sell_limit(code: str, qty: int, price: int) -> dict:
    """지정가 매도"""
    return _place_order("sell", code, qty, price, "00")


if __name__ == "__main__":
    import sys
    set_mode("mock")
    print(f"=== KIS {_mode.upper()} 모드 테스트 ===")

    print("\n[1] 잔고 조회")
    bal = get_balance()
    print(f"  예수금: {bal['cash']:,}원")
    print(f"  주문가능: {bal['available_cash']:,}원")
    print(f"  평가금액: {bal['total_eval']:,}원")
    print(f"  보유종목: {len(bal['holdings'])}건")
    for h in bal["holdings"]:
        print(f"    {h['name']}({h['code']}) {h['qty']}주 평가 {h['eval_amount']:,}원 손익 {h['pnl']:+,}원 ({h['pnl_rate']:+.2f}%)")
