"""
Step 2.5 — 이메일 수집 (3중 방식)

방식 A: SerpAPI (Google 검색) → 검색 결과에서 이메일/홈페이지 추출
방식 B: DART 전자공시 API → 기업개황에서 이메일/홈페이지
방식 C: 홈페이지 직접 크롤링 → 이메일 추출

3가지 동시 실행, 모든 유효 이메일 수집, 비용: SerpAPI Free 월 250건
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
from config import DART_API_KEY, SERP_API_KEY

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
    'googleapis.com', 'google.com', 'gstatic.com',
}

SKIP_DOMAINS = [
    'google.', 'naver.com', 'daum.net', 'kakao.com',
    'youtube.com', 'facebook.com', 'instagram.com',
    'wikipedia.org', 'namu.wiki', 'tistory.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com',
]

CONTACT_PATHS = [
    '/company', '/about', '/contact', '/intro',
    '/sub/company', '/sub/about', '/sub/contact',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
}

CORP_CODE_FILE = "data/dart_corp_codes.json"


# ============================================================
# 공통 유틸
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


def pick_best_email(emails: list[str]) -> str:
    if not emails:
        return ""
    priority = ['info', 'admin', 'sales', 'contact', 'office', 'master']
    for email in emails:
        if email.split('@')[0] in priority:
            return email
    return emails[0]


# ============================================================
# 방식 A: SerpAPI (Google 검색)
# ============================================================

def serpapi_search(company_name: str) -> dict:
    """SerpAPI로 Google 검색 → 이메일 + 홈페이지 추출"""
    if not SERP_API_KEY:
        return {"emails": [], "website": None}

    emails = set()
    website = None

    try:
        resp = requests.get(
            "https://serpapi.com/search",
            params={
                "api_key": SERP_API_KEY,
                "engine": "google",
                "q": f"{company_name} 이메일 연락처",
                "hl": "ko",
                "gl": "kr",
                "num": 5,
            },
            timeout=15,
        )

        if resp.status_code != 200:
            logger.debug(f"SerpAPI 오류: {resp.status_code}")
            return {"emails": [], "website": None}

        data = resp.json()

        # 검색 결과에서 이메일 추출
        for result in data.get("organic_results", []):
            snippet = result.get("snippet", "")
            title = result.get("title", "")
            link = result.get("link", "")

            found = EMAIL_PATTERN.findall(snippet + " " + title)
            emails.update(e.lower() for e in found)

            # 홈페이지 URL 추출
            if not website and link:
                domain = requests.utils.urlparse(link).netloc.lower()
                if not any(s in domain for s in SKIP_DOMAINS):
                    parsed = requests.utils.urlparse(link)
                    website = f"{parsed.scheme}://{parsed.netloc}"

        # Knowledge graph에서도 추출
        knowledge = data.get("knowledge_graph", {})
        if knowledge:
            website = website or knowledge.get("website", "")
            # knowledge graph description에서 이메일 추출
            desc = knowledge.get("description", "")
            emails.update(e.lower() for e in EMAIL_PATTERN.findall(desc))

    except Exception as e:
        logger.debug(f"SerpAPI 실패 [{company_name}]: {e}")

    return {
        "emails": filter_emails(emails),
        "website": website,
    }


# ============================================================
# 방식 B: DART API
# ============================================================

def _normalize(name: str) -> str:
    for s in ['(주)', '주식회사', '(유)', '(합)', '(사)', '㈜']:
        name = name.replace(s, '')
    return name.strip()


def download_dart_corp_codes() -> bool:
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
            return False

        z = zipfile.ZipFile(BytesIO(resp.content))
        xml_data = z.read('CORPCODE.xml').decode('utf-8')

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
                elem.clear()

        os.makedirs(os.path.dirname(CORP_CODE_FILE), exist_ok=True)
        with open(CORP_CODE_FILE, 'w', encoding='utf-8') as f:
            json.dump(codes, f, ensure_ascii=False)

        logger.info(f"DART 기업코드 저장: {len(codes)}건")
        return True
    except Exception as e:
        logger.error(f"DART 기업코드 실패: {e}")
        return False


def find_corp_code(company_name: str) -> str | None:
    if not os.path.exists(CORP_CODE_FILE):
        if not download_dart_corp_codes():
            return None
    try:
        with open(CORP_CODE_FILE, 'r', encoding='utf-8') as f:
            codes = json.load(f)
    except Exception:
        return None

    if company_name in codes:
        return codes[company_name]
    clean = _normalize(company_name)
    if clean in codes:
        return codes[clean]
    return None


def dart_company_info(company_name: str) -> dict | None:
    if not DART_API_KEY:
        return None
    corp_code = find_corp_code(company_name)
    if not corp_code:
        return None
    try:
        resp = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": DART_API_KEY, "corp_code": corp_code},
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
    except Exception:
        return None


# ============================================================
# 방식 C: 홈페이지 크롤링
# ============================================================

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
# 3중 수집
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    """
    3가지 방식으로 이메일 수집:
    1. SerpAPI Google 검색
    2. DART 기업개황
    3. 홈페이지 크롤링
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    all_emails = set()
    website = ""

    # 방식 A: SerpAPI
    serp = serpapi_search(company_name)
    all_emails.update(serp["emails"])
    if serp["website"]:
        website = serp["website"]

    # 방식 B: DART API
    dart = dart_company_info(company_name)
    if dart:
        if dart["email"]:
            all_emails.add(dart["email"].lower())
        if dart["homepage"] and not website:
            website = dart["homepage"]

    # 방식 C: 홈페이지 크롤링
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    email_list = list(all_emails)
    best = pick_best_email(email_list)

    logger.info(f"이메일 발견 [{company_name}]: {email_list}")
    return {"email": best, "all_emails": email_list, "website": website}


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    # DART 코드 미리 다운로드
    if DART_API_KEY and not os.path.exists(CORP_CODE_FILE):
        download_dart_corp_codes()

    targets = [
        f for f in factories
        if f.get("enriched") and f.get("solar_candidate")
        and not f.get("email")
    ][:max_count]

    if not targets:
        return {"harvested": 0, "message": "수집 대상 없음"}

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
        time.sleep(1)  # API rate limit

    success_rate = (harvested / len(targets) * 100) if targets else 0
    logger.info(f"이메일 수집 완료: {harvested}/{len(targets)}건 "
                f"(성공률 {success_rate:.0f}%)")

    return {
        "total_targets": len(targets),
        "harvested": harvested,
        "failed": failed,
        "success_rate": f"{success_rate:.0f}%",
    }
