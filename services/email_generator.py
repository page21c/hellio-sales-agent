"""
Step 3 — 고정 템플릿 콜드메일 생성
Claude API 호출 없이, TK가 확정한 템플릿으로 발송합니다.
"""
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# TK 확정 콜드메일 템플릿
# ============================================================

EMAIL_SUBJECT = "공장 지붕 임대수익 관련 안내"

EMAIL_BODY = """안녕하세요. 대표님.
헬리오브릿지의 지붕형 태양광발전소 임대사업 담당자입니다.

유휴 지붕을 활용하여 초기 비용 부담 없이 안정적인 임대수익을 확보하실 수 있는 방안을 안내드리고자 연락드렸습니다.

별도 투자 없이 지붕만 제공해 주시면, 태양광 발전소 설치부터 운영까지 사업자가 전담하며, 매년 4만원/1kW의 임대료를 받으실 수 있으십니다.
*ex. 1MW (2500평~3000평) : 연간 임대료 4천만원

① 비용 부담 0원  —  설비 투자, 시공, 유지보수 전액 사업자 부담
② 안정적 임대수익  —  20년 장기 계약 기반 연 고정 임대료 보장
③ 탄소중립 실현  —  연간 수천 톤 규모의 온실가스 저감 기여
④ 시설 보호 효과  —  지붕 방수·단열 기능 개선으로 유지보수비 절감
⑤ 프로모션 계약 성사금 지급

관심있으시면, 홈페이지를 통해 접수 또는 편하신 시간에 회신 부탁드립니다.

helliobridge.com"""


def generate_cold_email(factory: dict) -> dict | None:
    """
    고정 템플릿 기반 콜드메일 생성
    Claude API 호출 없이 즉시 반환
    """
    return {
        "subject": EMAIL_SUBJECT,
        "body": EMAIL_BODY,
    }


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
    logger.info(f"메일 생성: {len(results)}건 (고정 템플릿)")
    return results


def get_template() -> dict:
    """현재 템플릿 반환"""
    return {
        "subject": EMAIL_SUBJECT,
        "body": EMAIL_BODY,
    }
