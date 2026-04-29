"""
Step 2.5 — 이메일 수집 (DART API + 홈페이지 크롤링)

방식 A: DART 전자공시 API → 기업개황에서 이메일/홈페이지 조회
방식 B: 홈페이지 직접 크롤링 → 이메일 추출
두 방식 동시 실행, 모든 유효 이메일 수집

DART API: 완전 무료, 호출 제한 넉넉
"""
import re
import time
import logging
import requests
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from urllib.parse import urljoin
from config import DART_API_KEY

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

EXCLUDE_DOMAINS = {
    'example.com', 'test.com', 'gmail.com', 'naver.com',
    'hanmail.net', 'daum.net', 'yahoo.com', 'hotmail.com',
    'nate.com', 'kakao.com', 'outlook.com', 'jinhak.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com',
    'catch.co.kr', 'sentry.io', 'w3.org', 'schema.org',
}

CONTACT_PATHS = [
    '/company', '/about', '/contact', '/intro',
    '/sub/company', '/sub/about', '/sub/contact',
    '/kr/company', '/ko/company', '/ko/about',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
}

# DART 기업코드 캐시 (서버 시작 시 1회 로드)
_corp_code_cache = {}  # {"회사명": "고유번호"}


# ============================================================
# DART 기업코드 목록 로드
# ============================================================

def load_dart_corp_codes() -> dict:
    """
    DART에서 전체 기업 고유번호 목록을 다운로드합니다.
    ZIP 파일 안의 CORPCODE.xml을 파싱하여 {회사명: 고유번호} 딕셔너리 반환
    """
    global _corp_code_cache

    if _corp_code_cache:
        return _corp_code_cache

    if not DART_API_KEY:
        logger.warning("DART_API_KEY 미설정")
        return {}

    try:
        logger.info("DART 기업코드 목록 다운로드 중...")
        resp = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": DART_API_KEY},
            timeout=60,
        )

        if resp.status_code != 200:
            logger.error(f"DART 기업코드 다운로드 실패: {resp.status_code}")
            return {}

        # ZIP 압축 해제 → CORPCODE.xml 파싱
        z = zipfile.ZipFile(BytesIO(resp.content))
        xml_data = z.read('CORPCODE.xml').decode('utf-8')
        root = ET.fromstring(xml_data)

        for item in root.findall('.//list'):
            corp_code = item.findtext('corp_code', '')
            corp_name = item.findtext('corp_name', '')
            if corp_name and corp_code:
                # 정규화: (주), 주식회사 등 제거하여 매칭률 향상
                clean_name = normalize_company_name(corp_name)
                _corp_code_cache[clean_name] = corp_code
                # 원본 이름으로도 저장
                _corp_code_cache[corp_name] = corp_code

        logger.info(f"DART 기업코드 로드 완료: {len(_corp_code_cache)}건")
        return _corp_code_cache

    except Exception as e:
        logger.error(f"DART 기업코드 로드 실패: {e}")
        return {}


def normalize_company_name(name: str) -> str:
    """회사명 정규화 — 매칭률 향상을 위해"""
    name = name.strip()
    # (주), 주식회사, (유), (합) 등 제거
    for suffix in ['(주)', '주식회사', '(유)', '(합)', '(사)', '㈜']:
        name = name.replace(suffix, '')
    return name.strip()


def find_corp_code(company_name: str) -> str | None:
    """회사명으로 DART 고유번호 찾기"""
    codes = load_dart_corp_codes()
    if not codes:
        return None

    # 정확 매칭
    if company_name in codes:
        return codes[company_name]

    # 정규화 매칭
    clean = normalize_company_name(company_name)
    if clean in codes:
        return codes[clean]

    # 부분 매칭 (회사명이 포함된 경우)
    for name, code in codes.items():
        if clean in name or name in clean:
            return code

    return None


# ============================================================
# 방식 A: DART API — 기업개황 조회
# ============================================================

def dart_company_info(company_name: str) -> dict | None:
    """
    DART API로 기업개황 조회 → 이메일, 홈페이지, 대표자 등

    응답 필드:
      corp_name: 회사명
      ceo_nm: 대표자명
      hm_url: 홈페이지 URL
      email: 이메일
      phn_no: 전화번호
      adres: 주소
    """
    if not DART_API_KEY:
        return None

    corp_code = find_corp_code(company_name)
    if not corp_code:
        return None

    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={
                "crtfc_key": DART_API_KEY,
                "corp_code": corp_code,
            },
            timeout=10,
        )

        if resp.status_code != 200:
            return None

        data = resp.json()
        if data.get("status") != "000":  # 000 = 정상
            return None

        email = (data.get("email") or "").strip()
        hm_url = (data.get("hm_url") or "").strip()

        # 홈페이지 URL 정규화
        if hm_url and not hm_url.startswith("http"):
            hm_url = "http://" + hm_url

        return {
            "email": email,
            "homepage": hm_url,
            "ceo": (data.get("ceo_nm") or "").strip(),
            "phone": (data.get("phn_no") or "").strip(),
            "address": (data.get("adres") or "").strip(),
        }

    except Exception as e:
        logger.debug(f"DART API 오류 [{company_name}]: {e}")
        return None


# ============================================================
# 방식 B: 홈페이지 직접 크롤링
# ============================================================

def filter_emails(raw_emails: set) -> list[str]:
    """유효한 이메일만 필터링"""
    filtered = []
    for email in raw_emails:
        email = email.lower().strip()
        domain = email.split('@')[1] if '@' in email else ''
        if domain in EXCLUDE_DOMAINS:
            continue
        if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
            continue
        if len(email) > 100:
            continue
        if email not in filtered:
            filtered.append(email)
    return filtered


def crawl_website_emails(base_url: str) -> list[str]:
    """회사 홈페이지에서 이메일 추출"""
    all_emails = set()

    # 메인 페이지
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=10,
                            allow_redirects=True)
        if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
            resp.encoding = resp.apparent_encoding
        found = EMAIL_PATTERN.findall(resp.text)
        all_emails.update(e.lower() for e in found)
    except Exception:
        pass

    if all_emails:
        return filter_emails(all_emails)

    # 하위 페이지 탐색
    for path in CONTACT_PATHS:
        try:
            sub_url = urljoin(base_url, path)
            resp = requests.get(sub_url, headers=HEADERS, timeout=10,
                                allow_redirects=True)
            if resp.status_code == 200:
                if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                    resp.encoding = resp.apparent_encoding
                found = EMAIL_PATTERN.findall(resp.text)
                all_emails.update(e.lower() for e in found)
                if all_emails:
                    break
        except Exception:
            pass
        time.sleep(0.3)

    return filter_emails(all_emails)


# ============================================================
# 듀얼 수집 (DART + 크롤링)
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    """
    공장 1건에 대해 이메일 수집

    1. DART API로 기업개황 조회 (이메일 + 홈페이지)
    2. 홈페이지 크롤링으로 추가 이메일
    3. 모든 유효 이메일 반환
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    all_emails = set()
    website = ""

    # 방식 A: DART API
    dart_info = dart_company_info(company_name)
    if dart_info:
        if dart_info["email"]:
            all_emails.add(dart_info["email"].lower())
        if dart_info["homepage"]:
            website = dart_info["homepage"]

    # 방식 B: 홈페이지 크롤링
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    email_list = list(all_emails)
    # 대표 이메일 우선 (info@, admin@ 등)
    priority = ['info', 'admin', 'sales', 'contact', 'office', 'master']
    best = email_list[0]
    for email in email_list:
        prefix = email.split('@')[0]
        if prefix in priority:
            best = email
            break

    logger.info(f"이메일 발견 [{company_name}]: {email_list}")

    return {
        "email": best,
        "all_emails": email_list,
        "website": website,
    }


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    """
    태양광 후보 중 이메일 미확보 건 일괄 수집
    DART API 무료 + 크롤링 무료 = 비용 0원
    """
    # 첫 실행 시 DART 기업코드 미리 로드
    load_dart_corp_codes()

    targets = [
        f for f in factories
        if f.get("enriched")
        and f.get("solar_candidate")
        and not f.get("email")
    ][:max_count]

    if not targets:
        return {"harvested": 0, "message": "수집 대상 없음"}

    harvested = 0
    failed = 0
    dart_found = 0
    crawl_found = 0

    for factory in targets:
        result = harvest_email(factory)

        if result:
            factory["email"] = result["email"]
            factory["all_emails"] = result.get("all_emails", [])
            factory["website"] = result["website"]
            harvested += 1
        else:
            failed += 1

        if (harvested + failed) % 10 == 0:
            logger.info(f"이메일 수집 진행: "
                        f"{harvested}건 성공, {failed}건 실패 "
                        f"/ {len(targets)}건")

        time.sleep(0.5)  # API 부하 방지

    success_rate = (harvested / len(targets) * 100) if targets else 0
    logger.info(
        f"이메일 수집 완료: {harvested}/{len(targets)}건 "
        f"(성공률 {success_rate:.0f}%)"
    )

    return {
        "total_targets": len(targets),
        "harvested": harvested,
        "failed": failed,
        "success_rate": f"{success_rate:.0f}%",
    }
