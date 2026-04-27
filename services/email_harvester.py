"""
Step 2.5 — 회사 홈페이지 크롤링으로 이메일 수집

흐름:
  1. 회사명으로 Google/Naver 검색 → 홈페이지 URL 추출
  2. 홈페이지 + 하위 페이지(회사소개, 연락처) 크롤링
  3. 이메일 주소 패턴(xxx@xxx.xxx) 추출
  4. 유효성 검증 후 저장

예상 성공률: 20~30% (홈페이지 없거나 이메일 비노출 업체 제외)
"""
import re
import time
import logging
import requests
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# 이메일 추출 정규식
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# 제외할 이메일 도메인 (일반 서비스 이메일)
EXCLUDE_DOMAINS = {
    'example.com', 'test.com', 'gmail.com', 'naver.com',
    'hanmail.net', 'daum.net', 'yahoo.com', 'hotmail.com',
    'nate.com', 'kakao.com', 'outlook.com',
}

# 회사 연락처가 있을 확률 높은 페이지 키워드
CONTACT_PATHS = [
    '/company', '/about', '/contact', '/intro',
    '/sub/company', '/sub/about', '/sub/contact',
    '/kr/company', '/ko/company', '/ko/about',
    '/회사소개', '/연락처', '/오시는길',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'ko-KR,ko;q=0.9,en;q=0.8',
}


def search_company_website(company_name: str) -> str | None:
    """
    Naver 검색으로 회사 홈페이지 URL 찾기
    """
    query = f"{company_name} 홈페이지"
    search_url = "https://search.naver.com/search.naver"

    try:
        resp = requests.get(
            search_url,
            params={"query": query},
            headers=HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        html = resp.text

        # 검색 결과에서 URL 추출 (홈페이지 링크)
        # Naver 검색 결과의 사이트 링크 패턴
        url_patterns = re.findall(
            r'href="(https?://(?:www\.)?[a-zA-Z0-9\-]+\.[a-zA-Z]{2,}[^"]*)"',
            html
        )

        for url in url_patterns:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            # 검색엔진/포털/SNS 제외
            skip = ['naver.com', 'google.com', 'daum.net', 'kakao.com',
                    'youtube.com', 'facebook.com', 'instagram.com',
                    'blog.naver', 'cafe.naver', 'search.naver',
                    'wikipedia.org', 'saramin.co.kr', 'jobkorea.co.kr',
                    'namu.wiki', 'tistory.com']
            if any(s in domain for s in skip):
                continue
            # 회사 고유 홈페이지로 추정되는 URL
            return f"{parsed.scheme}://{parsed.netloc}"

        return None

    except Exception as e:
        logger.debug(f"검색 실패 [{company_name}]: {e}")
        return None


def extract_emails_from_url(url: str) -> set[str]:
    """
    URL에서 이메일 주소 추출
    """
    emails = set()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10,
                            allow_redirects=True)
        resp.raise_for_status()

        # 인코딩 처리
        if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
            resp.encoding = resp.apparent_encoding

        html = resp.text
        found = EMAIL_PATTERN.findall(html)

        for email in found:
            email = email.lower().strip()
            domain = email.split('@')[1] if '@' in email else ''

            # 제외 도메인 필터
            if domain in EXCLUDE_DOMAINS:
                continue
            # 이미지 파일 확장자 제외
            if domain.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
                continue

            emails.add(email)

    except Exception as e:
        logger.debug(f"크롤링 실패 [{url}]: {e}")

    return emails


def crawl_company_emails(company_name: str, base_url: str) -> list[str]:
    """
    회사 홈페이지 + 하위 페이지에서 이메일 수집

    1. 메인 페이지 크롤링
    2. 회사소개/연락처 페이지 크롤링
    3. 메인 페이지 내 링크에서 추가 탐색
    """
    all_emails = set()

    # 1. 메인 페이지
    main_emails = extract_emails_from_url(base_url)
    all_emails.update(main_emails)

    # 2. 주요 하위 페이지 (회사소개, 연락처 등)
    for path in CONTACT_PATHS:
        if all_emails:  # 이미 찾았으면 추가 탐색 중단
            break
        sub_url = urljoin(base_url, path)
        sub_emails = extract_emails_from_url(sub_url)
        all_emails.update(sub_emails)
        time.sleep(0.3)

    # 3. 메인 페이지에서 "회사소개", "연락처" 링크 탐색
    if not all_emails:
        try:
            resp = requests.get(base_url, headers=HEADERS, timeout=10)
            if resp.encoding and resp.encoding.lower() == 'iso-8859-1':
                resp.encoding = resp.apparent_encoding
            html = resp.text

            # 연락처/회사소개 관련 링크 찾기
            contact_links = re.findall(
                r'href="([^"]*(?:contact|about|company|intro|회사|연락|소개)[^"]*)"',
                html, re.IGNORECASE
            )
            for link in contact_links[:3]:
                full_url = urljoin(base_url, link)
                sub_emails = extract_emails_from_url(full_url)
                all_emails.update(sub_emails)
                if all_emails:
                    break
                time.sleep(0.3)
        except Exception:
            pass

    result = list(all_emails)
    if result:
        logger.info(f"이메일 발견 [{company_name}]: {result}")
    return result


def harvest_email(factory: dict) -> dict | None:
    """
    공장 1건에 대해 이메일 수집 시도

    Returns:
        {"email": "xxx@company.com", "website": "https://..."} 또는 None
    """
    company_name = factory.get("company_name", "")
    if not company_name:
        return None

    # 1. 홈페이지 검색
    website = search_company_website(company_name)
    if not website:
        return None

    # 2. 이메일 크롤링
    emails = crawl_company_emails(company_name, website)
    if not emails:
        return None

    # 가장 적합한 이메일 선택 (info@, admin@, sales@ 우선)
    priority_prefixes = ['info', 'admin', 'sales', 'contact', 'office', 'master']
    best_email = emails[0]
    for email in emails:
        prefix = email.split('@')[0]
        if prefix in priority_prefixes:
            best_email = email
            break

    return {
        "email": best_email,
        "all_emails": emails,
        "website": website,
    }


def harvest_batch(factories: list[dict], max_count: int = 100) -> dict:
    """
    미수집 후보에 대해 일괄 이메일 수집

    대상: 보강 완료 + 660m²↑ + 이메일 미확보
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

        if harvested % 10 == 0 and harvested > 0:
            logger.info(f"이메일 수집 진행: {harvested}건 성공, {failed}건 실패")

        time.sleep(1)  # 검색엔진 부하 방지

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
