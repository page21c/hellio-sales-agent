"""
Step 2.5 — 이메일 수집 (Claude 웹검색 + DART)

방식:
  1. DART API → 기업개황에서 이메일 (무료, 가장 정확)
  2. Claude API + 웹검색 → 지능적으로 회사 공식 이메일 찾기
  3. 홈페이지 크롤링 → 보조 수집

Claude 웹검색의 장점:
  - 제3자 이메일 자동 판별 (채용사이트, 기업정보사이트 이메일 제외)
  - 회사 공식 이메일만 선별
  - 검색 1건당 약 $0.01~0.02
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
from config import DART_API_KEY, ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

BLOCKED_EMAIL_DOMAINS = {
    'gmail.com', 'naver.com', 'hanmail.net', 'daum.net',
    'yahoo.com', 'hotmail.com', 'nate.com', 'kakao.com', 'outlook.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com', 'catch.co.kr',
    'jobplanet.co.kr', 'wanted.co.kr', 'rocketpunch.com',
    'nicebizinfo.com', 'rndcircle.io', 'cookiedeal.io',
    'gabia.com', 'example.com', 'test.com',
}

CORP_CODE_FILE = "data/dart_corp_codes.json"


# ============================================================
# DART API
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
        # DART 이메일 도메인 검증
        if email:
            domain = email.split('@')[1] if '@' in email else ''
            if domain in BLOCKED_EMAIL_DOMAINS:
                email = ""
        return {"email": email, "homepage": hm_url}
    except Exception:
        return None


# ============================================================
# Claude API + 웹검색
# ============================================================

def claude_find_email(company_name: str) -> dict | None:
    """
    Claude API에 웹검색을 시켜서 회사 공식 이메일을 찾습니다.
    Claude가 지능적으로 제3자 이메일을 걸러냅니다.
    """
    if not ANTHROPIC_API_KEY:
        return None

    prompt = f"""'{company_name}'의 공식 이메일 주소와 홈페이지를 찾아주세요.

규칙:
- 회사 자체 도메인 이메일만 (gmail, naver 등 무료메일 제외)
- 채용사이트(사람인, 잡코리아), 기업정보사이트의 이메일 제외
- 회사 홈페이지에 공개된 대표 이메일 우선

JSON으로만 응답:
{{"email": "이메일주소 또는 빈문자열", "website": "홈페이지URL 또는 빈문자열"}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type": "application/json",
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model": "claude-sonnet-4-6",
                "max_tokens": 300,
                "tools": [
                    {"type": "web_search_20250305", "name": "web_search"}
                ],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )

        if resp.status_code != 200:
            logger.debug(f"Claude API 오류: {resp.status_code}")
            return None

        data = resp.json()

        # 응답에서 텍스트 추출
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        if not text:
            return None

        # JSON 파싱
        clean = text.strip().strip("`").strip()
        if clean.startswith("json"):
            clean = clean[4:].strip()

        # JSON 부분만 추출
        json_match = re.search(r'\{[^}]+\}', clean)
        if json_match:
            result = json.loads(json_match.group())
        else:
            result = json.loads(clean)

        email = (result.get("email") or "").strip().lower()
        website = (result.get("website") or "").strip()

        # 이메일 검증
        if email:
            domain = email.split('@')[1] if '@' in email else ''
            if domain in BLOCKED_EMAIL_DOMAINS:
                email = ""

        if not email and not website:
            return None

        logger.info(f"Claude 검색 [{company_name}]: "
                    f"email={email}, website={website}")
        return {"email": email, "website": website}

    except json.JSONDecodeError:
        logger.debug(f"Claude 응답 JSON 파싱 실패: {text[:200]}")
        return None
    except Exception as e:
        logger.debug(f"Claude API 실패 [{company_name}]: {e}")
        return None


# ============================================================
# 홈페이지 크롤링
# ============================================================

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
}

CONTACT_PATHS = [
    '/company', '/about', '/contact', '/intro',
    '/sub/company', '/sub/about', '/sub/contact',
]


def filter_crawled_emails(raw_emails: set, website: str = "") -> list[str]:
    """크롤링된 이메일 필터링 — 홈페이지 도메인 매칭"""
    site_domain = ""
    if website:
        site_domain = urlparse(website).netloc.lower().replace('www.', '')

    filtered = []
    for email in raw_emails:
        email = email.lower().strip()
        domain = email.split('@')[1] if '@' in email else ''
        if domain in BLOCKED_EMAIL_DOMAINS:
            continue
        if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
            continue
        if len(email) > 100:
            continue
        # 홈페이지 도메인 매칭
        if site_domain and site_domain not in domain:
            continue
        if email not in filtered:
            filtered.append(email)
    return filtered


def crawl_website_emails(base_url: str) -> list[str]:
    """회사 홈페이지에서 이메일 추출"""
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
        return filter_crawled_emails(all_emails, base_url)

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

    return filter_crawled_emails(all_emails, base_url)


# ============================================================
# 수집
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    """
    1. DART (무료, 가장 정확)
    2. Claude 웹검색 (지능적 판별)
    3. 홈페이지 크롤링 (Claude가 URL만 찾은 경우)
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    email = ""
    website = ""

    # 1단계: DART
    dart = dart_company_info(company_name)
    if dart:
        email = dart.get("email", "")
        website = dart.get("homepage", "")

    # 2단계: Claude 웹검색
    if not email:
        claude = claude_find_email(company_name)
        if claude:
            email = claude.get("email", "")
            if not website:
                website = claude.get("website", "")

    # 3단계: 홈페이지 크롤링 (이메일 못 찾았지만 홈페이지는 있는 경우)
    if not email and website:
        crawled = crawl_website_emails(website)
        if crawled:
            email = crawled[0]

    if not email:
        return None

    logger.info(f"이메일 발견 [{company_name}]: {email}")
    return {"email": email, "all_emails": [email], "website": website}


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
