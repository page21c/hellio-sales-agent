"""
Step 2.5 — Google 검색 기반 이메일 수집

방식:
  1. Google 검색 "회사명 이메일 연락처" → 검색 결과에서 이메일 추출
  2. 검색 결과에서 회사 홈페이지 URL 확보
  3. 홈페이지 + 하위 페이지(회사소개, 연락처) 크롤링
  4. 이메일 필터링 및 저장

기존 Naver 방식 → 해외 IP 차단으로 성공률 0%
Google 방식 → 검색 결과 자체에서 이메일 확보 가능
"""
import re
import time
import logging
import requests
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# 제외할 이메일 도메인
EXCLUDE_DOMAINS = {
    'example.com', 'test.com', 'gmail.com', 'naver.com',
    'hanmail.net', 'daum.net', 'yahoo.com', 'hotmail.com',
    'nate.com', 'kakao.com', 'outlook.com', 'jinhak.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com',
    'catch.co.kr', 'sentry.io', 'w3.org', 'schema.org',
    'googleapis.com', 'google.com', 'gstatic.com',
}

# 연락처 페이지 키워드
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

# 포털/SNS 등 제외할 도메인
SKIP_DOMAINS = [
    'google.com', 'naver.com', 'daum.net', 'kakao.com',
    'youtube.com', 'facebook.com', 'instagram.com',
    'blog.naver', 'cafe.naver', 'search.naver',
    'wikipedia.org', 'namu.wiki', 'tistory.com',
    'saramin.co.kr', 'jobkorea.co.kr', 'incruit.com',
    'catch.co.kr', 'linkedin.com', 'twitter.com',
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

        filtered.append(email)
    return filtered


def pick_best_email(emails: list[str]) -> str:
    """가장 적합한 이메일 선택"""
    if not emails:
        return ""
    priority_prefixes = ['info', 'admin', 'sales', 'contact',
                         'office', 'master', 'webmaster', 'biz']
    for email in emails:
        prefix = email.split('@')[0]
        if prefix in priority_prefixes:
            return email
    return emails[0]


def google_search_emails(company_name: str) -> dict:
    """
    Google 검색으로 회사 이메일과 홈페이지를 찾습니다.

    검색어: "회사명 이메일 연락처"
    검색 결과 HTML에서 이메일 패턴과 홈페이지 URL을 추출합니다.
    """
    emails = set()
    website = None

    queries = [
        f"{company_name} 이메일 연락처",
        f"{company_name} email contact",
    ]

    for query in queries:
        try:
            resp = requests.get(
                "https://www.google.com/search",
                params={"q": query, "hl": "ko", "num": 10},
                headers=HEADERS,
                timeout=10,
            )
            if resp.status_code != 200:
                continue

            html = resp.text

            # 검색 결과에서 이메일 추출
            found = EMAIL_PATTERN.findall(html)
            for email in found:
                emails.add(email.lower())

            # 홈페이지 URL 추출
            if not website:
                urls = re.findall(
                    r'href="(https?://(?:www\.)?'
                    r'[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^"]*)"',
                    html
                )
                for url in urls:
                    parsed = urlparse(url)
                    domain = parsed.netloc.lower()
                    if not any(s in domain for s in SKIP_DOMAINS):
                        website = f"{parsed.scheme}://{parsed.netloc}"
                        break

            if emails:
                break  # 이미 이메일을 찾았으면 두 번째 쿼리 스킵

            time.sleep(1)

        except Exception as e:
            logger.debug(f"Google 검색 실패 [{company_name}]: {e}")

    return {
        "emails": filter_emails(emails),
        "website": website,
    }


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

    # 이미 이메일을 찾았으면 리턴
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


def harvest_email(factory: dict) -> dict | None:
    """
    공장 1건에 대해 이메일 수집

    1단계: Google 검색에서 이메일 + 홈페이지 URL 확보
    2단계: 홈페이지 크롤링으로 추가 이메일 수집
    3단계: 가장 적합한 이메일 선택
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    # 1단계: Google 검색
    search_result = google_search_emails(company_name)
    all_emails = set(search_result["emails"])
    website = search_result["website"]

    # 2단계: 홈페이지 크롤링 (Google에서 못 찾았거나 추가 수집)
    if website:
        site_emails = crawl_website_emails(website)
        all_emails.update(site_emails)

    if not all_emails:
        return None

    # 3단계: 최적 이메일 선택
    best = pick_best_email(list(all_emails))

    logger.info(f"이메일 발견 [{company_name}]: {best} "
                f"(총 {len(all_emails)}개)")

    return {
        "email": best,
        "all_emails": list(all_emails),
        "website": website or "",
    }


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    """
    태양광 후보 중 이메일 미확보 건에 대해 일괄 수집

    Google 검색 rate limit 감안하여 요청 간 2초 대기
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
            factory["website"] = result["website"]
            harvested += 1
        else:
            failed += 1

        if (harvested + failed) % 10 == 0:
            logger.info(f"이메일 수집 진행: "
                        f"{harvested}건 성공, {failed}건 실패 "
                        f"/ {len(targets)}건")

        time.sleep(2)  # Google rate limit 방지

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
