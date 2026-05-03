"""
Step 3-B — 이메일 발송 서비스
Gmail SMTP를 통해 콜드메일을 발송하고 결과를 Supabase에 기록합니다.
"""
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from config import SMTP_EMAIL, SMTP_PASSWORD, SMTP_FROM_NAME
from services.database import save_email_log

logger = logging.getLogger(__name__)


def send_email(to_email: str, subject: str, body: str,
               company_name: str = "") -> dict:
    """
    Gmail SMTP로 이메일 1통 발송 + Supabase 로그 저장
    """
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        return {"ok": False, "error": "SMTP 설정 미완료"}

    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{SMTP_FROM_NAME} <{SMTP_EMAIL}>"
        msg["To"] = to_email
        msg["Subject"] = subject

        # 본문 (plain text + HTML)
        html_body = body.replace("\n", "<br>")
        msg.attach(MIMEText(body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())

        logger.info(f"발송 완료: {to_email}")

        # Supabase에 발송 로그 저장
        save_email_log({
            "company_name": company_name,
            "to_email": to_email,
            "subject": subject,
            "status": "sent",
            "sent_at": datetime.utcnow().isoformat(),
        })

        return {
            "ok": True,
            "to": to_email,
            "subject": subject,
            "sent_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"발송 실패 [{to_email}]: {e}")

        # 실패도 기록
        save_email_log({
            "company_name": company_name,
            "to_email": to_email,
            "subject": subject,
            "status": f"failed: {str(e)[:200]}",
            "sent_at": datetime.utcnow().isoformat(),
        })

        return {"ok": False, "to": to_email, "error": str(e)}


def send_batch(email_list: list[dict], max_per_day: int = 30) -> dict:
    """일괄 발송"""
    sent = 0
    failed = 0
    results = []

    for item in email_list[:max_per_day]:
        to = item.get("to_email", "")
        if not to or "@" not in to:
            logger.warning(f"이메일 주소 없음: {item.get('company_name')}")
            failed += 1
            continue

        result = send_email(
            to, item["subject"], item["body"],
            company_name=item.get("company_name", ""),
        )
        results.append(result)

        if result["ok"]:
            sent += 1
        else:
            failed += 1

    summary = {
        "total": len(email_list[:max_per_day]),
        "sent": sent,
        "failed": failed,
        "results": results,
        "batch_at": datetime.utcnow().isoformat(),
    }
    logger.info(f"일괄 발송 완료: 성공 {sent}, 실패 {failed}")
    return summary
