"""
Step 2.5 — 이메일 수집 (듀얼 방식)

방식 A: Google Custom Search API로 검색 → 이메일 추출
방식 B: 회사 홈페이지 직접 크롤링 → 이메일 추출
두 방식 동시 실행, 모든 유효 이메일 수집
"""
import re
import time
import logging
import requests
from urllib.parse import urljoin, urlparse
from config import GOOGLE_API_KEY, GOOGLE_CX

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

SKIP_DOMAINS = [
    'google.com', 'naver.com', 'daum.net', 'kakao.com',
    'youtube.com', 'facebook.com', 'instagram.com',
    'blog.naver', 'cafe.naver', 'wikipedia.org', 'namu.wiki',
    'tistory.com', 'saramin.co.kr', 'jobkorea.co.kr',
    'incruit.com', 'catch.co.kr', 'linkedin.com',
]


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


def pick_best_email(emails: list[str]) -> str:
    """가장 적합한 이메일 선택"""
    if not emails:
        return ""
    priority = ['info', 'admin', 'sales', 'contact',
                'office', 'master', 'webmaster', 'biz']
    for email in emails:
        prefix = email.split('@')[0]
        if prefix in priority:
            return email
    return emails[0]


# ============================================================
# 방식 A: Google Custom Search API
# ============================================================

def google_custom_search(company_name: str) -> dict:
    """
    Google Custom Search API로 회사 이메일 검색
    하루 100건 무료
    """
    if not GOOGLE_API_KEY or not GOOGLE_CX:
        return {"emails": [], "website": None, "snippets": []}

    emails = set()
    website = None
    snippets = []

    queries = [
        f"{company_name} 이메일 연락처",
        f"{company_name} email",
    ]

    for query in queries:
        try:
            resp = requests.get(
                "https://www.googleapis.com/customsearch/v1",
                params={
                    "key": GOOGLE_API_KEY,
                    "cx": GOOGLE_CX,
                    "q": query,
                    "num": 5,
                    "lr": "lang_ko",
                },
                timeout=10,
            )

            if resp.status_code == 429:
                logger.warning("Google API 일일 한도 초과")
                break

            if resp.status_code != 200:
                logger.debug(f"Google API 오류: {resp.status_code}")
                continue

            data = resp.json()
            items = data.get("items", [])

            for item in items:
                # 검색 결과 스니펫에서 이메일 추출
                snippet = item.get("snippet", "")
                title = item.get("title", "")
                link = item.get("link", "")
                snippets.append(snippet)

                found = EMAIL_PATTERN.findall(snippet + " " + title)
                emails.update(e.lower() for e in found)

                # 홈페이지 URL 추출
                if not website and link:
                    parsed = urlparse(link)
                    domain = parsed.netloc.lower()
                    if not any(s in domain for s in SKIP_DOMAINS):
                        website = f"{parsed.scheme}://{parsed.netloc}"

            if emails:
                break  # 이미 찾았으면 두 번째 쿼리 스킵

        except Exception as e:
            logger.debug(f"Google API 실패 [{company_name}]: {e}")

    return {
        "emails": filter_emails(emails),
        "website": website,
        "snippets": snippets,
    }


# ============================================================
# 방식 B: 홈페이지 직접 크롤링
# ============================================================

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
# 듀얼 수집 (A + B 동시)
# ============================================================

def harvest_email(factory: dict) -> dict | None:
    """
    공장 1건에 대해 이메일 수집 (듀얼 방식)

    1. Google Custom Search API로 이메일 + 홈페이지 URL
    2. 홈페이지 직접 크롤링으로 추가 이메일
    3. 모든 유효 이메일 반환
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    all_emails = set()
    website = None

    # 방식 A: Google Custom Search API
    search_result = google_custom_search(company_name)
    all_emails.update(search_result["emails"])
    website = search_result["website"]

    # 방식 B: 홈페이지 크롤링
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    email_list = list(all_emails)
    best = pick_best_email(email_list)

    logger.info(f"이메일 발견 [{company_name}]: {email_list}")

    return {
        "email": best,
        "all_emails": email_list,
        "website": website or "",
    }


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    """
    태양광 후보 중 이메일 미확보 건 일괄 수집
    Google API 하루 100건 무료 감안
    """
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

    for factory in targets:
        result = harvest_email(factory)

        if result:
            factory["email"] = result["email"]
            factory["all_emails"] = result["all_emails"]
            factory["website"] = result["website"]
            harvested += 1
        else:
            failed += 1

        if (harvested + failed) % 10 == 0:
            logger.info(f"이메일 수집 진행: "
                        f"{harvested}건 성공, {failed}건 실패 "
                        f"/ {len(targets)}건")

        time.sleep(1)  # API rate limit 방지

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
