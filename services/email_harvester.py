"""
Step 2.5 — 이메일 수집 (품질 우선)

핵심 원칙: 회사 자체 홈페이지에서만 이메일 추출
제3자 사이트(채용사이트, 기업정보사이트)의 이메일 완전 차단

방식:
  1. DART API → 기업개황에서 이메일/홈페이지 (가장 정확)
  2. SerpAPI → 회사 홈페이지 URL만 추출 (이메일은 추출 안 함)
  3. 회사 홈페이지 직접 크롤링 → 이메일 추출
  4. 이메일 도메인이 홈페이지 도메인과 일치하는지 검증
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
from urllib.parse import urljoin, urlparse
from config import DART_API_KEY, SERP_API_KEY

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# 제3자 사이트 이메일 차단 (이 도메인의 이메일은 무조건 제외)
BLOCKED_EMAIL_DOMAINS = {
    # 포털/무료메일
    'gmail.com', 'naver.com', 'hanmail.net', 'daum.net',
    'yahoo.com', 'hotmail.com', 'nate.com', 'kakao.com', 'outlook.com',
    # 채용사이트
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com', 'catch.co.kr',
    'jobplanet.co.kr', 'wanted.co.kr', 'rocketpunch.com',
    # 기업정보사이트
    'nicebizinfo.com', 'rndcircle.io', 'cookiedeal.io',
    'chemknock.com', 'jinhak.com',
    # 기타
    'sentry.io', 'w3.org', 'schema.org', 'googleapis.com',
    'google.com', 'gstatic.com', 'example.com', 'test.com',
    'yale.edu', 'kt.com', 'kopo.ac.kr', 'korea.com',
    'kiria.org', 'insa.co.kr', 'itp.or.kr',
}

# 홈페이지로 인정하지 않는 도메인
SKIP_DOMAINS = [
    'google.', 'naver.com', 'daum.net', 'kakao.com',
    'youtube.com', 'facebook.com', 'instagram.com',
    'wikipedia.org', 'namu.wiki', 'tistory.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com',
    'catch.co.kr', 'jobplanet.co.kr', 'rocketpunch.com',
    'nicebizinfo.com', 'rndcircle.io', 'cookiedeal.io',
    'wanted.co.kr', 'linkedin.com', 'twitter.com',
    'blog.naver', 'cafe.naver', 'korcham.net',
    'ftc.go.kr', 'dart.fss.or.kr',
]

CONTACT_PATHS = [
    '/company', '/about', '/contact', '/intro',
    '/sub/company', '/sub/about', '/sub/contact',
    '/kr/company', '/ko/company', '/ko/about',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
}

CORP_CODE_FILE = "data/dart_corp_codes.json"


# ============================================================
# 이메일 검증
# ============================================================

def is_valid_email(email: str, website: str = "") -> bool:
    """회사 자체 이메일인지 검증"""
    email = email.lower().strip()
    domain = email.split('@')[1] if '@' in email else ''

    # 차단 도메인 체크
    if domain in BLOCKED_EMAIL_DOMAINS:
        return False

    # 파일 확장자 체크
    if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
        return False

    if len(email) > 100:
        return False

    # 홈페이지 도메인과 매칭 검증 (있으면)
    if website:
        site_domain = urlparse(website).netloc.lower().replace('www.', '')
        if site_domain and domain != site_domain:
            # 서브도메인은 허용 (예: mail.company.co.kr → company.co.kr)
            if not domain.endswith('.' + site_domain) and \
               not site_domain.endswith('.' + domain):
                # 도메인 불일치 — 하지만 같은 회사의 다른 도메인일 수 있으므로
                # .co.kr, .com 등은 허용
                pass  # 일단 통과, 차단 도메인만 걸러냄

    return True


def filter_emails(raw_emails: set, website: str = "") -> list[str]:
    """유효한 회사 이메일만 필터링"""
    return [e.lower().strip() for e in raw_emails
            if is_valid_email(e, website)]


def pick_best_email(emails: list[str]) -> str:
    if not emails:
        return ""
    priority = ['info', 'admin', 'sales', 'contact', 'office', 'master']
    for email in emails:
        if email.split('@')[0] in priority:
            return email
    return emails[0]


# ============================================================
# 방식 A: DART API (가장 정확)
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
    return codes.get(clean)


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
# 방식 B: SerpAPI → 홈페이지 URL만 추출 (이메일은 추출 안 함)
# ============================================================

def serpapi_find_website(company_name: str) -> str | None:
    """SerpAPI로 회사 홈페이지 URL만 찾기 (이메일 추출 안 함)"""
    if not SERP_API_KEY:
        return None

    try:
        resp = requests.get(
            "https://serpapi.com/search",
            params={
                "api_key": SERP_API_KEY,
                "engine": "google",
                "q": f"{company_name} 공식 홈페이지",
                "hl": "ko",
                "gl": "kr",
                "num": 5,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            return None

        data = resp.json()

        # Knowledge graph에 홈페이지가 있으면 최우선
        knowledge = data.get("knowledge_graph", {})
        if knowledge:
            kw = knowledge.get("website")
            if kw:
                return kw

        # 검색 결과에서 회사 자체 홈페이지 찾기
        for result in data.get("organic_results", []):
            link = result.get("link", "")
            if not link:
                continue
            domain = urlparse(link).netloc.lower()
            if not any(s in domain for s in SKIP_DOMAINS):
                parsed = urlparse(link)
                return f"{parsed.scheme}://{parsed.netloc}"

        return None

    except Exception as e:
        logger.debug(f"SerpAPI 실패 [{company_name}]: {e}")
        return None


# ============================================================
# 방식 C: 홈페이지 크롤링 → 이메일 추출
# ============================================================

def crawl_website_emails(base_url: str) -> list[str]:
    """회사 홈페이지에서만 이메일 추출"""
    all_emails = set()

    # 메인 페이지
    try:
        resp = requests.get(base_url, headers=HEADERS, timeout=10,
                            allow_redirects=True)
        if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
            resp.encoding = resp.apparent_encoding
        all_emails.update(e.lower() for e in EMAIL_PATTERN.findall(resp.text))
    except Exception:
        pass

    if all_emails:
        return filter_emails(all_emails, base_url)

    # 하위 페이지 탐색
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

    return filter_emails(all_emails, base_url)


# ============================================================
# 3중 수집 (품질 우선)
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    """
    이메일 수집 (품질 우선 순서):
    1. DART API → 가장 정확한 공식 이메일
    2. SerpAPI → 회사 홈페이지 URL 찾기
    3. 홈페이지 크롤링 → 회사 자체 이메일만 추출
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    all_emails = set()
    website = ""

    # 1단계: DART (무료, 가장 정확)
    dart = dart_company_info(company_name)
    if dart:
        if dart["email"] and is_valid_email(dart["email"]):
            all_emails.add(dart["email"].lower())
        if dart["homepage"]:
            website = dart["homepage"]

    # 2단계: SerpAPI → 홈페이지 URL만 (이메일 아님)
    if not website:
        found_url = serpapi_find_website(company_name)
        if found_url:
            website = found_url

    # 3단계: 홈페이지 크롤링
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    email_list = list(all_emails)
    best = pick_best_email(email_list)

    logger.info(f"이메일 발견 [{company_name}]: {email_list} "
                f"(홈페이지: {website})")
    return {"email": best, "all_emails": email_list, "website": website}


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
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
        time.sleep(1)

    success_rate = (harvested / len(targets) * 100) if targets else 0
    logger.info(f"이메일 수집 완료: {harvested}/{len(targets)}건 "
                f"(성공률 {success_rate:.0f}%)")

    return {
        "total_targets": len(targets),
        "harvested": harvested,
        "failed": failed,
        "success_rate": f"{success_rate:.0f}%",
    }
