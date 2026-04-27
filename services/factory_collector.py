"""
Step 1 — 공장 데이터 수집 (하이브리드 방식)

방식:
  1단계: CSV 파일(전국등록공장현황 172K건)을 Supabase에 초기 로딩
  2단계: 팩토리온 API로 건축면적 + 전화번호 + 대표자 일일 보강
  3단계: 건축면적 660m² 이상 필터 → 태양광 후보 확정

※ 활용가이드_한국산업단지공단_공장등록필지정보_v2.0.docx 기반
"""
import pandas as pd
import requests
import json
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime
from config import FACTORY_API_KEY, FACTORY_API_URL, MIN_FACTORY_AREA

logger = logging.getLogger(__name__)

# 대형 산업단지 우선순위 (면적 큰 공장 집중 지역)
PRIORITY_COMPLEXES = [
    "남동국가산업단지", "반월국가산업단지", "시화국가산업단지",
    "구미국가산업단지", "창원국가산업단지", "명지녹산국가산업단지",
    "광주하남일반산업단지", "대구제3일반산업단지", "광주평동일반산업단지",
    "울산미포국가산업단지", "군산2국가산업단지", "아산1일반산업단지",
    "오창과학산업단지", "포항철강산업단지", "김해일반산업단지",
]

# CSV 초기 로딩 시 건물 입주 제외 패턴
TENANT_PATTERNS = [
    r'\d+호', r'\d+층',
    '지식산업센터', '벤처타운', '벤처센터', '벤처밸리',
    '테크노파크', '테크노밸리', '테크노타운',
    '아파트형', '비즈니스센터', '오피스',
    '창업보육', '창업센터', 'IT밸리', 'IT센터',
]

SIDO_MAP = {
    '서울특별시': '서울', '부산광역시': '부산', '대구광역시': '대구',
    '인천광역시': '인천', '광주광역시': '광주', '대전광역시': '대전',
    '울산광역시': '울산', '세종특별자치시': '세종',
    '경기도': '경기', '강원특별자치도': '강원', '강원도': '강원',
    '충청북도': '충북', '충청남도': '충남',
    '전북특별자치도': '전북', '전라북도': '전북', '전라남도': '전남',
    '경상북도': '경북', '경상남도': '경남', '제주특별자치도': '제주',
}


# ============================================================
# 1단계: CSV 초기 로딩
# ============================================================

def load_csv(csv_path: str) -> list[dict]:
    """
    전국등록공장현황 CSV → 건물입주 제외 → 독립 공장 리스트
    CSV 컬럼: 순번, 회사명, 단지명, 생산품, 공장주소
    """
    df = pd.read_csv(csv_path, encoding='cp949')
    logger.info(f"CSV 로드: {len(df):,}건")

    # 건물 입주 제외
    combined = '|'.join(TENANT_PATTERNS)
    tenant_mask = df['공장주소'].str.contains(combined, na=False, regex=True)
    standalone = df[~tenant_mask].copy()
    logger.info(f"건물 입주 제외: {tenant_mask.sum():,}건 → 독립 공장: {len(standalone):,}건")

    # 시도 추출
    def extract_sido(addr):
        if not isinstance(addr, str):
            return ''
        for full, short in SIDO_MAP.items():
            if addr.startswith(full):
                return short
        return ''

    factories = []
    for _, row in standalone.iterrows():
        factories.append({
            "company_name": str(row.get('회사명', '')).strip(),
            "industrial_complex": str(row.get('단지명', '')).strip(),
            "product": str(row.get('생산품', '')).strip(),
            "address": str(row.get('공장주소', '')).strip(),
            "region": extract_sido(row.get('공장주소', '')),
            # API 보강 대상 필드 (초기값 비어있음)
            "ceo_name": "",
            "phone": "",
            "building_area_m2": 0,
            "lot_area_m2": 0,
            "land_use": "",
            "factory_manage_no": "",
            # 상태
            "enriched": False,
            "solar_candidate": False,
            "email_sent": False,
            "collected_at": datetime.utcnow().isoformat(),
        })

    return factories


# ============================================================
# 2단계: API 보강 (건축면적 + 전화번호 + 대표자)
# ============================================================

def parse_xml_response(xml_text: str) -> list[dict]:
    """XML 응답을 파싱하여 item 리스트 반환"""
    try:
        root = ET.fromstring(xml_text)
        # resultCode 확인
        rc = root.findtext('.//resultCode')
        if rc and rc != '00':
            logger.error(f"API 에러: [{rc}] {root.findtext('.//resultMsg')}")
            return []
        
        items = []
        for item_el in root.findall('.//item'):
            item = {}
            for child in item_el:
                item[child.tag] = child.text or ""
            items.append(item)
        return items
    except ET.ParseError as e:
        logger.error(f"XML 파싱 실패: {e}")
        return []


def get_total_from_xml(xml_text: str) -> int:
    """XML 응답에서 totalCount 추출"""
    try:
        root = ET.fromstring(xml_text)
        tc = root.findtext('.//totalCount')
        return int(tc) if tc else 0
    except Exception:
        return 0


def call_factory_api(params: dict) -> tuple[list[dict], int]:
    """
    팩토리온 API 호출 → (item 리스트, totalCount) 반환
    JSON과 XML 응답 모두 처리
    """
    try:
        resp = requests.get(FACTORY_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()

        # JSON 시도
        if text.startswith('{'):
            data = json.loads(text)
            header = data.get("response", {}).get("header", {})
            if str(header.get("resultCode", "")) != "00":
                return [], 0
            body = data.get("response", {}).get("body", {})
            items = body.get("items", {})
            if isinstance(items, dict):
                item_list = items.get("item", [])
            elif isinstance(items, list):
                item_list = items
            else:
                return [], 0
            if isinstance(item_list, dict):
                item_list = [item_list]
            total = int(body.get("totalCount", 0))
            return item_list, total

        # XML 파싱
        items = parse_xml_response(text)
        total = get_total_from_xml(text)
        return items, total

    except Exception as e:
        logger.error(f"API 호출 실패: {e}")
        return [], 0


def enrich_factory(company_name: str) -> dict | None:
    """
    팩토리온 API로 회사명 조회 → 건축면적, 전화번호, 대표자 획득

    API 응답 필드:
      cmpnyNm         : 회사명
      rprsntvNm       : 대표자
      cmpnyTelno      : 회사전화번호
      fctryDongBuldAr : 건축면적 (m²)
      fctryLndpclAr   : 용지면적 (m²)
      rnAdres         : 도로명주소
      spfcSeCodeNm    : 용도지역
      irsttNm         : 산업단지명
      fctryManageNo   : 공장관리번호
    """
    if not FACTORY_API_KEY:
        return None

    params = {
        "serviceKey": FACTORY_API_KEY,
        "numOfRows": "5",
        "pageNo": "1",
        "type": "json",
        "cmpnyNm": company_name,
    }

    items, total = call_factory_api(params)
    if not items:
        return None

    item = items[0]

    def to_float(val):
        try:
            return float(str(val).replace(",", "")) if val else 0
        except ValueError:
            return 0

    return {
        "factory_manage_no": item.get("fctryManageNo", ""),
        "ceo_name": (item.get("rprsntvNm") or "").strip(),
        "phone": (item.get("cmpnyTelno") or "").strip(),
        "building_area_m2": to_float(item.get("fctryDongBuldAr")),
        "lot_area_m2": to_float(item.get("fctryLndpclAr")),
        "land_use": (item.get("spfcSeCodeNm") or "").strip(),
    }


def enrich_batch(factories: list[dict], max_calls: int = 900) -> dict:
    """
    미보강 공장에 대해 API로 일괄 보강

    우선순위:
      1. 대형 산업단지 소속 공장
      2. 나머지 공장 (주소순)

    일일 API 호출 한도(1,000건) 감안하여 max_calls 제한
    """
    # 미보강 건만 추출
    unenriched = [f for f in factories if not f.get("enriched")]
    if not unenriched:
        return {"enriched": 0, "candidates": 0, "message": "보강할 대상 없음"}

    # 우선순위 정렬: 대형 산업단지 먼저
    def priority_sort(f):
        complex_name = f.get("industrial_complex", "")
        if complex_name in PRIORITY_COMPLEXES:
            return (0, PRIORITY_COMPLEXES.index(complex_name))
        return (1, complex_name)

    unenriched.sort(key=priority_sort)

    enriched_count = 0
    candidate_count = 0
    calls = 0

    for factory in unenriched:
        if calls >= max_calls:
            break

        result = enrich_factory(factory["company_name"])
        calls += 1

        if result:
            factory.update(result)
            factory["enriched"] = True
            factory["solar_candidate"] = result["building_area_m2"] >= MIN_FACTORY_AREA
            enriched_count += 1
            if factory["solar_candidate"]:
                candidate_count += 1

            if enriched_count % 50 == 0:
                logger.info(f"보강 진행: {enriched_count}건 (후보: {candidate_count}건)")

        time.sleep(0.3)  # API 부하 방지

    logger.info(
        f"보강 완료: {enriched_count}건, "
        f"태양광 후보(≥{MIN_FACTORY_AREA}m²): {candidate_count}건, "
        f"API 호출: {calls}건"
    )

    return {
        "enriched": enriched_count,
        "candidates": candidate_count,
        "api_calls": calls,
        "remaining": len(unenriched) - enriched_count,
    }


# ============================================================
# 3단계: 태양광 후보 추출
# ============================================================

def get_solar_candidates(factories: list[dict]) -> list[dict]:
    """보강 완료 + 건축면적 660m² 이상인 공장 추출"""
    return [
        f for f in factories
        if f.get("enriched")
        and f.get("solar_candidate")
        and not f.get("email_sent")
    ]


# ============================================================
# 테스트
# ============================================================

def test_connection() -> dict:
    """API 연결 테스트"""
    if not FACTORY_API_KEY or FACTORY_API_KEY.startswith("여기"):
        return {"ok": False, "error": "FACTORY_API_KEY 미설정"}

    result = enrich_factory("삼성전자")
    if result:
        return {
            "ok": True,
            "sample": result,
            "message": f"삼성전자 조회 성공 — 건축면적: {result['building_area_m2']:,.0f}m²",
        }
    return {"ok": False, "error": "API 응답 없음"}


def get_stats(factories: list[dict]) -> dict:
    """현재 데이터 현황"""
    total = len(factories)
    enriched = sum(1 for f in factories if f.get("enriched"))
    candidates = sum(1 for f in factories if f.get("solar_candidate"))
    emailed = sum(1 for f in factories if f.get("email_sent"))
    with_phone = sum(1 for f in factories if f.get("phone"))

    return {
        "total_factories": total,
        "enriched": enriched,
        "unenriched": total - enriched,
        "solar_candidates": candidates,
        "with_phone": with_phone,
        "emails_sent": emailed,
    }
