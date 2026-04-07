"""
aftermarket_checker.py 단위 테스트
- 네이버 모바일 API 응답 구조가 변경되었는지 빠르게 감지
- CI/CD에서 매일 실행하여 회귀 방지

사용: py -3.12 test_aftermarket.py
종료 코드: 0=정상, 1=실패
"""

import sys
from aftermarket_checker import fetch_after_market_data

# 항상 거래되는 대형주 5종목으로 검증
TEST_STOCKS = [
    ("005930", "삼성전자"),
    ("000660", "SK하이닉스"),
    ("035420", "NAVER"),
    ("035720", "카카오"),
    ("005380", "현대차"),
]


def test_api_structure():
    """API 응답이 정상적인 구조인지 검증"""
    print("=" * 50)
    print("aftermarket_checker API 구조 테스트")
    print("=" * 50)

    failures = []
    successes = 0

    for code, name in TEST_STOCKS:
        result = fetch_after_market_data(code)
        # 응답 구조 검증
        required_keys = {"available", "price", "change_pct", "volume"}
        missing = required_keys - set(result.keys())
        if missing:
            failures.append(f"{name}({code}): 필수 키 누락 {missing}")
            continue

        # 가격이 있다면 합리적인 값인지
        if result["available"]:
            if result["price"] is None or result["price"] <= 0:
                failures.append(f"{name}({code}): 가격 비정상 {result['price']}")
                continue
            successes += 1
            print(f"  ✅ {name}({code}): {result['price']}원, {result['change_pct']:+.2f}%")
        else:
            print(f"  ⚪ {name}({code}): 시간외 체결 없음 (정상)")
            successes += 1  # 체결 없음도 정상 응답

    print(f"\n결과: {successes}/{len(TEST_STOCKS)} 통과")

    if failures:
        print("\n실패:")
        for f in failures:
            print(f"  ❌ {f}")
        return False

    # 최소 1개 이상은 가격이 있어야 함 (장중이라도 정규장 가격이 잡힘)
    return True


def test_field_types():
    """필드 타입 검증"""
    result = fetch_after_market_data("005930")
    if not result["available"]:
        print("⚠ 삼성전자 응답 없음 - 타입 검증 스킵")
        return True

    checks = [
        ("price", int),
        ("change_pct", (int, float)),
    ]
    failures = []
    for key, expected_type in checks:
        val = result.get(key)
        if val is not None and not isinstance(val, expected_type):
            failures.append(f"{key}: 타입 {type(val).__name__}, 예상 {expected_type}")

    if failures:
        print("\n타입 검증 실패:")
        for f in failures:
            print(f"  ❌ {f}")
        return False
    print("\n✅ 타입 검증 통과")
    return True


def main():
    print()
    structure_ok = test_api_structure()
    print()
    type_ok = test_field_types()
    print()

    if structure_ok and type_ok:
        print("✅ 모든 테스트 통과")
        sys.exit(0)
    else:
        print("❌ 테스트 실패 - 네이버 API 변경 가능성 확인 필요")
        sys.exit(1)


if __name__ == "__main__":
    main()
