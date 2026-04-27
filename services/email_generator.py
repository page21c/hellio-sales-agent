"""
Step 3 — Claude API로 맞춤형 콜드메일 생성
공장의 업종, 면적, 지역 정보를 기반으로 개인화된 이메일을 작성합니다.
"""
import requests
import json
import logging
from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """당신은 HELLIO BRIDGE(헬리오브릿지)의 태양광 지붕 임대 전문 컨설턴트입니다.
공장 건물주에게 보내는 콜드메일을 작성합니다.

핵심 가치 제안:
- 공장 지붕을 활용한 태양광 발전소 임대사업
- 건물주는 초기 투자 없이 월 임대수익 확보
- 전기요금 절감 효과 추가
- 20년 장기 안정 수익

작성 규칙:
1. 제목은 15자 이내로 간결하게
2. 본문은 200자 이내로 핵심만
3. 수신자의 업종과 공장 규모에 맞는 구체적 수치 제시
4. 부담 없는 "무료 상담" 제안으로 마무리
5. 격식체 사용, 과도한 마케팅 표현 자제
"""


def generate_cold_email(factory: dict) -> dict | None:
    """
    공장 정보 기반 맞춤 콜드메일 생성

    Returns:
        {"subject": "제목", "body": "본문"} 또는 None
    """
    if not ANTHROPIC_API_KEY or ANTHROPIC_API_KEY.startswith("여기"):
        logger.error("ANTHROPIC_API_KEY가 설정되지 않았습니다")
        return None

    # 예상 임대수익 간이 계산 (건축면적 기반)
    area = factory.get("building_area_m2", 0)
    # 대략 100m²당 10kW 설치, kW당 월 임대료 약 3,000원
    estimated_kw = (area / 100) * 10
    estimated_monthly = int(estimated_kw * 3000)
    estimated_monthly_man = round(estimated_monthly / 10000, 1)

    user_prompt = f"""다음 공장에 보낼 콜드메일을 작성해주세요.

회사명: {factory.get('company_name', '(미상)')}
대표자: {factory.get('ceo_name', '대표님')}
도로명주소: {factory.get('address', '')}
산업단지: {factory.get('industrial_complex', '')}
건축면적: {area:,.0f}m²
예상 설치용량: {estimated_kw:,.0f}kW
예상 월 임대수익: 약 {estimated_monthly_man}만원

JSON 형식으로 응답해주세요:
{{"subject": "이메일 제목", "body": "이메일 본문"}}
"""

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
                "max_tokens": 500,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # 응답에서 텍스트 추출
        text = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                text += block.get("text", "")

        # JSON 파싱 (```json 감싸기 제거)
        clean = text.strip().strip("`").strip()
        if clean.startswith("json"):
            clean = clean[4:].strip()

        result = json.loads(clean)
        logger.info(f"메일 생성 완료: {factory.get('factory_name')}")
        return {
            "subject": result.get("subject", ""),
            "body": result.get("body", ""),
        }

    except json.JSONDecodeError:
        logger.error(f"JSON 파싱 실패: {text[:200]}")
        return None
    except Exception as e:
        logger.error(f"Claude API 오류: {e}")
        return None


def generate_batch(factories: list[dict],
                   max_count: int = 30) -> list[dict]:
    """여러 공장에 대한 콜드메일 일괄 생성"""
    results = []
    for factory in factories[:max_count]:
        email = generate_cold_email(factory)
        if email:
            results.append({
                **factory,
                "email_subject": email["subject"],
                "email_body": email["body"],
                "email_generated": True,
            })
    logger.info(f"메일 생성: {len(results)}/{len(factories[:max_count])}건")
    return results
