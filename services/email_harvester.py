"""
Step 2.5 — 이메일 수집 (DART API + 홈페이지 크롤링)

메모리 최적화: DART 기업코드를 파일로 저장하고 디스크에서 조회
"""
import re
import os
import json
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
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
}

CORP_CODE_FILE = "data/dart_corp_codes.json"


# ============================================================
# DART 기업코드 — 파일 기반 조회 (메모리 절약)
# ============================================================

def download_dart_corp_codes() -> bool:
    """DART 기업코드 목록을 다운로드하여 JSON 파일로 저장 (1회)"""
    if not DART_API_KEY:
        return False

    try:
        logger.info("DART 기업코드 다운로드 중...")
        resp = requests.get(
            "https://opendart.fss.or.kr/api/corpCode.xml",
            params={"crtfc_key": DART_API_KEY},
            timeout=60,
        )
        if resp.status_code != 200:
            logger.error(f"DART 다운로드 실패: {resp.status_code}")
            return False

        z = zipfile.ZipFile(BytesIO(resp.content))
        xml_data = z.read('CORPCODE.xml').decode('utf-8')

        # 스트리밍 파싱으로 메모리 절약
        codes = {}
        for event, elem in ET.iterparse(BytesIO(xml_data.encode('utf-8'))):
            if elem.tag == 'list':
                code = elem.findtext('corp_code', '')
                name = elem.findtext('corp_name', '')
                if name and code:
                    clean = _normalize(name)
                    codes[clean] = code
                    if clean != name:
                        codes[name] = code
                elem.clear()  # 메모리 해제

        # JSON으로 저장
        os.makedirs(os.path.dirname(CORP_CODE_FILE), exist_ok=True)
        with open(CORP_CODE_FILE, 'w', encoding='utf-8') as f:
            json.dump(codes, f, ensure_ascii=False)

        logger.info(f"DART 기업코드 저장: {len(codes)}건 → {CORP_CODE_FILE}")
        return True

    except Exception as e:
        logger.error(f"DART 기업코드 다운로드 실패: {e}")
        return False


def _normalize(name: str) -> str:
    for s in ['(주)', '주식회사', '(유)', '(합)', '(사)', '㈜']:
        name = name.replace(s, '')
    return name.strip()


def find_corp_code(company_name: str) -> str | None:
    """회사명으로 DART 고유번호 찾기 (파일에서 조회)"""
    if not os.path.exists(CORP_CODE_FILE):
        if not download_dart_corp_codes():
            return None

    try:
        with open(CORP_CODE_FILE, 'r', encoding='utf-8') as f:
            codes = json.load(f)
    except Exception:
        return None

    # 정확 매칭
    if company_name in codes:
        return codes[company_name]

    # 정규화 매칭
    clean = _normalize(company_name)
    if clean in codes:
        return codes[clean]

    return None


# ============================================================
# DART API — 기업개황 조회
# ============================================================

def dart_company_info(company_name: str) -> dict | None:
    """DART API로 기업개황 조회 → 이메일, 홈페이지"""
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
        if data.get("status") != "000":
            return None

        email = (data.get("email") or "").strip()
        hm_url = (data.get("hm_url") or "").strip()
        if hm_url and not hm_url.startswith("http"):
            hm_url = "http://" + hm_url

        return {"email": email, "homepage": hm_url}

    except Exception as e:
        logger.debug(f"DART API 오류 [{company_name}]: {e}")
        return None


# ============================================================
# 홈페이지 크롤링
# ============================================================

def filter_emails(raw_emails: set) -> list[str]:
    filtered = []
    for email in raw_emails:
        email = email.lower().strip()
        domain = email.split('@')[1] if '@' in email else ''
        if domain in EXCLUDE_DOMAINS:
            continue
        if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
            continue
        if len(email) > 100 or email in filtered:
            continue
        filtered.append(email)
    return filtered


def crawl_website_emails(base_url: str) -> list[str]:
    all_emails = set()
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=10,
                            allow_redirects=True)
        if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
            resp.encoding = resp.apparent_encoding
        all_emails.update(e.lower() for e in EMAIL_PATTERN.findall(resp.text))
    except Exception:
        pass

    if all_emails:
        return filter_emails(all_emails)

    for path in CONTACT_PATHS:
        try:
            resp = requests.get(urljoin(base_url, path), headers=HEADERS,
                                timeout=10, allow_redirects=True)
            if resp.status_code == 200:
                if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                    resp.encoding = resp.apparent_encoding
                all_emails.update(
                    e.lower() for e in EMAIL_PATTERN.findall(resp.text))
                if all_emails:
                    break
        except Exception:
            pass
        time.sleep(0.3)

    return filter_emails(all_emails)


# ============================================================
# 듀얼 수집
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    all_emails = set()
    website = ""

    # DART API
    dart_info = dart_company_info(company_name)
    if dart_info:
        if dart_info["email"]:
            all_emails.add(dart_info["email"].lower())
        if dart_info["homepage"]:
            website = dart_info["homepage"]

    # 홈페이지 크롤링
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    email_list = list(all_emails)
    priority = ['info', 'admin', 'sales', 'contact', 'office', 'master']
    best = email_list[0]
    for email in email_list:
        if email.split('@')[0] in priority:
            best = email
            break

    logger.info(f"이메일 발견 [{company_name}]: {email_list}")
    return {"email": best, "all_emails": email_list, "website": website}


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    targets = [
        f for f in factories
        if f.get("enriched") and f.get("solar_candidate")
        and not f.get("email")
    ][:max_count]

    if not targets:
        return {"harvested": 0, "message": "수집 대상 없음"}

    # 첫 실행 시 DART 코드 다운로드
    if not os.path.exists(CORP_CODE_FILE):
        download_dart_corp_codes()

    harvested = 0
    failed = 0

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
        time.sleep(0.5)

    success_rate = (harvested / len(targets) * 100) if targets else 0
    logger.info(f"이메일 수집 완료: {harvested}/{len(targets)}건 "
                f"(성공률 {success_rate:.0f}%)")

    return {
        "total_targets": len(targets),
        "harvested": harvested,
        "failed": failed,
        "success_rate": f"{success_rate:.0f}%",
    }
