"""
HELLIO BRIDGE — 영업 자동화 에이전트 (하이브리드 방식)
=====================================================
1단계: CSV 172K건 초기 로딩 (1회)
2단계: 팩토리온 API로 건축면적+전화번호 일일 보강 (매일 06시, ~900건/일)
3단계: 660m² 이상 후보에 콜드메일 생성·발송 (매일 09시)

실행: uvicorn main:app --host 0.0.0.0 --port 8000
"""
import os
import json
import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, BackgroundTasks, UploadFile, File
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import config
from services.factory_collector import (
    load_csv, enrich_batch, get_solar_candidates,
    test_connection, get_stats, PRIORITY_COMPLEXES,
)
from services.email_generator import generate_cold_email, generate_batch
from services.email_sender import send_email, send_batch
from services.email_harvester import harvest_batch, harvest_email
from services.database import (
    save_factories, save_email_log, get_candidates,
    get_dashboard_stats, load_enriched_factories, merge_with_csv,
    is_connected,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("hellio-agent")

scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

# 메모리 저장소 (서버 재시작 시 CSV 다시 로딩 필요)
store = {
    "factories": [],         # 전체 172K 공장 리스트
    "loaded": False,         # CSV 로딩 완료 여부
    "last_enrich": None,     # 마지막 보강 결과
    "last_harvest": None,    # 마지막 이메일 수집 결과
    "last_email": None,      # 마지막 발송 결과
    # 실시간 작업 상태
    "job_status": {
        "enrich": {"running": False, "message": "대기", "progress": ""},
        "harvest": {"running": False, "message": "대기", "progress": ""},
        "send": {"running": False, "message": "대기", "progress": ""},
    },
}

CSV_PATH = os.getenv("CSV_PATH", "data/factories.csv")


# ============================================================
# 자동화 작업
# ============================================================

async def job_enrich():
    """매일 06시: API로 건축면적+전화번호 보강 (~900건)"""
    if not store["loaded"]:
        logger.warning("CSV 미로딩 — /init/load-csv 먼저 실행 필요")
        return

    store["job_status"]["enrich"] = {
        "running": True, "message": "보강 진행 중",
        "progress": "API 호출 시작...",
        "started_at": datetime.now().isoformat(),
    }

    logger.info("=== 일일 보강 시작 ===")
    # 블로킹 작업을 별도 스레드에서 실행 (이벤트 루프 차단 방지)
    result = await asyncio.to_thread(
        enrich_batch, store["factories"], 900
    )
    store["last_enrich"] = {**result, "at": datetime.now().isoformat()}

    # DB에도 보강된 데이터 저장
    enriched = [f for f in store["factories"] if f.get("enriched")]
    await asyncio.to_thread(save_factories, enriched)

    store["job_status"]["enrich"] = {
        "running": False, "message": "보강 완료",
        "progress": f"보강 {result.get('enriched', 0)}건, 후보 {result.get('candidates', 0)}건",
        "completed_at": datetime.now().isoformat(),
    }
    logger.info(f"보강 완료: {result}")


async def job_harvest_emails():
    """매일 08시: Google 검색 기반 이메일 수집 (~100건)"""
    if not store["loaded"]:
        return

    store["job_status"]["harvest"] = {
        "running": True, "message": "이메일 수집 중",
        "progress": "Google 검색 시작...",
        "started_at": datetime.now().isoformat(),
    }

    logger.info("=== 이메일 수집 시작 (Google 검색) ===")
    result = await asyncio.to_thread(
        harvest_batch, store["factories"], 100
    )
    store["last_harvest"] = {**result, "at": datetime.now().isoformat()}

    # 이메일 확보된 건 DB에 저장
    with_email = [f for f in store["factories"]
                  if f.get("email") and f.get("enriched")]
    if with_email:
        await asyncio.to_thread(save_factories, with_email)

    store["job_status"]["harvest"] = {
        "running": False, "message": "이메일 수집 완료",
        "progress": f"성공 {result.get('harvested', 0)}건, 실패 {result.get('failed', 0)}건 ({result.get('success_rate', '0%')})",
        "completed_at": datetime.now().isoformat(),
    }
    logger.info(f"이메일 수집 완료: {result}")


async def job_send_emails():
    """매일 09시: 이메일 확보된 후보에 콜드메일 생성·발송"""
    if not store["loaded"]:
        return

    store["job_status"]["send"] = {
        "running": True, "message": "발송 진행 중",
        "progress": "대상 확인 중...",
        "started_at": datetime.now().isoformat(),
    }

    logger.info("=== 콜드메일 발송 시작 ===")

    # 이메일이 있는 후보만 추출
    candidates = [
        f for f in store["factories"]
        if f.get("enriched")
        and f.get("solar_candidate")
        and f.get("email")
        and not f.get("email_sent")
    ]

    if not candidates:
        logger.info("발송 대상 없음 (이메일 확보 + 660m²↑ + 미발송 건 없음)")
        store["last_email"] = {"sent": 0, "at": datetime.now().isoformat()}
        store["job_status"]["send"] = {
            "running": False, "message": "발송 대상 없음",
            "progress": "이메일 확보된 미발송 건이 없습니다",
            "completed_at": datetime.now().isoformat(),
        }
        return

    target = candidates[:config.MAX_EMAILS_PER_DAY]
    logger.info(f"발송 대상: {len(target)}건")

    # 메일 생성
    emails = generate_batch(target, max_count=config.MAX_EMAILS_PER_DAY)

    # 발송
    to_send = []
    for e in emails:
        e["to_email"] = e.get("email", "")
        e["subject"] = e.get("email_subject", "")
        e["body"] = e.get("email_body", "")
        if e["to_email"]:
            to_send.append(e)

    if to_send:
        result = send_batch(to_send, max_per_day=config.MAX_EMAILS_PER_DAY)
        # 발송 성공한 건 마킹
        for item in to_send:
            for f in store["factories"]:
                if f.get("company_name") == item.get("company_name"):
                    f["email_sent"] = True
                    break
        store["last_email"] = {**result, "at": datetime.now().isoformat()}
        store["job_status"]["send"] = {
            "running": False, "message": "발송 완료",
            "progress": f"성공 {result.get('sent', 0)}건, 실패 {result.get('failed', 0)}건",
            "completed_at": datetime.now().isoformat(),
        }
        logger.info(f"발송 완료: {result.get('sent', 0)}건")
    else:
        store["last_email"] = {
            "generated": len(emails),
            "no_valid_email": True,
            "at": datetime.now().isoformat(),
        }
        store["job_status"]["send"] = {
            "running": False, "message": "발송 대상 없음",
            "progress": "유효한 이메일이 없습니다",
            "completed_at": datetime.now().isoformat(),
        }


# ============================================================
# FastAPI
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # CSV 로딩을 백그라운드에서 실행 (health check 타임아웃 방지)
    import asyncio

    async def _load_data():
        if os.path.exists(CSV_PATH):
            logger.info(f"CSV 자동 로딩: {CSV_PATH}")
            store["factories"] = load_csv(CSV_PATH)
            store["loaded"] = True
            logger.info(f"CSV 로딩 완료: {len(store['factories']):,}건")

            # Supabase에서 이전 보강 데이터 복원
            if is_connected():
                logger.info("Supabase에서 보강 데이터 복원 중...")
                db_data = load_enriched_factories()
                if db_data:
                    store["factories"] = merge_with_csv(
                        store["factories"], db_data)
                    enriched = sum(1 for f in store["factories"]
                                   if f.get("enriched"))
                    logger.info(f"복원 완료: 보강 {enriched}건")
                else:
                    logger.info("Supabase에 보강 데이터 없음 (첫 실행)")
            else:
                logger.warning("Supabase 미연결 — 메모리 전용 모드")

    asyncio.create_task(_load_data())

    scheduler.add_job(job_enrich, "cron",
                      hour=config.DAILY_COLLECT_HOUR, minute=0,
                      id="enrich", replace_existing=True)
    scheduler.add_job(job_harvest_emails, "cron",
                      hour=8, minute=0,
                      id="harvest", replace_existing=True)
    scheduler.add_job(job_send_emails, "cron",
                      hour=config.DAILY_EMAIL_HOUR, minute=0,
                      id="email", replace_existing=True)
    scheduler.start()
    logger.info(f"스케줄러 시작 — 보강: {config.DAILY_COLLECT_HOUR}시, "
                f"이메일수집: 8시, 발송: {config.DAILY_EMAIL_HOUR}시")
    yield
    scheduler.shutdown()


app = FastAPI(
    title="HELLIO BRIDGE 영업 자동화 에이전트",
    description="공장 지붕 태양광 임대 — CSV 기반 + API 보강 하이브리드",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── 관리자 대시보드 ───────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard():
    """관리자 대시보드 UI"""
    html_path = Path(__file__).parent / "templates" / "admin.html"
    if html_path.exists():
        return HTMLResponse(html_path.read_text(encoding="utf-8"))
    return HTMLResponse("<h1>admin.html 파일이 없습니다</h1>")


# ── 상태 ───────────────────────────────────────────────────

@app.get("/")
async def root():
    stats = get_stats(store["factories"]) if store["loaded"] else {}
    return {
        "service": "HELLIO BRIDGE Sales Agent v2",
        "csv_loaded": store["loaded"],
        "stats": stats,
        "schedule": {
            "enrich": f"매일 {config.DAILY_COLLECT_HOUR}:00 (API 보강 ~900건/일)",
            "email": f"매일 {config.DAILY_EMAIL_HOUR}:00 (콜드메일 ~{config.MAX_EMAILS_PER_DAY}건/일)",
        },
        "last_enrich": store.get("last_enrich"),
        "last_email": store.get("last_email"),
    }


@app.get("/health")
async def health():
    return {"ok": True, "loaded": store["loaded"],
            "factories": len(store["factories"]),
            "time": datetime.now().isoformat()}


@app.get("/healthz")
async def healthz():
    """Render health check 전용 — 항상 즉시 응답"""
    return {"ok": True}


@app.get("/status")
async def job_status():
    """각 작업의 실시간 상태 반환"""
    return store["job_status"]


# ── 초기 세팅 ─────────────────────────────────────────────

@app.post("/init/load-csv")
async def init_load_csv(background_tasks: BackgroundTasks,
                        path: str = CSV_PATH):
    """CSV 파일 로딩 (최초 1회)"""
    if not os.path.exists(path):
        return {"ok": False, "error": f"파일 없음: {path}",
                "hint": "data/factories.csv 위치에 CSV를 넣거나 path 파라미터로 경로 지정"}

    def _load():
        store["factories"] = load_csv(path)
        store["loaded"] = True
        logger.info(f"CSV 로딩 완료: {len(store['factories']):,}건")

    background_tasks.add_task(_load)
    return {"ok": True, "message": f"로딩 시작: {path}"}


@app.get("/setup/sql")
async def setup_sql():
    """Supabase 테이블 생성 SQL"""
    return {"sql": "이미 테이블이 생성되었습니다.", "instruction": "Supabase에서 확인하세요"}


# ── 테스트 ─────────────────────────────────────────────────

@app.get("/test/factory-api")
async def test_factory_api():
    """팩토리온 API 연결 테스트"""
    return test_connection()


@app.get("/test/email")
async def test_email(to: str = ""):
    """이메일 발송 테스트"""
    if not to:
        return {"error": "?to=이메일주소 파라미터 필요"}
    return send_email(to, "[테스트] HELLIO BRIDGE",
                      "영업 자동화 에이전트 이메일 테스트입니다.")


@app.get("/test/generate-email")
async def test_generate():
    """콜드메일 템플릿 확인"""
    from services.email_generator import get_template
    template = get_template()
    return {"template": template, "mode": "고정 템플릿 (Claude API 미사용)"}


# ── 수동 실행 ──────────────────────────────────────────────

@app.post("/run/enrich")
async def run_enrich(max_calls: int = 900):
    """API 보강 수동 실행"""
    if not store["loaded"]:
        return {"error": "CSV 미로딩 — /init/load-csv 먼저 실행"}

    stats = get_stats(store["factories"])
    asyncio.create_task(job_enrich())
    return {
        "message": f"보강 시작 (미보강: {stats['unenriched']:,}건, 최대 {max_calls}건)",
        "status": "running",
    }


@app.post("/run/send-emails")
async def run_send_emails():
    """콜드메일 발송 수동 실행"""
    if not store["loaded"]:
        return {"error": "CSV 미로딩"}
    asyncio.create_task(job_send_emails())
    return {"message": "발송 작업 시작", "status": "running"}


@app.post("/run/harvest")
async def run_harvest(max_count: int = 100):
    """이메일 수집 수동 실행"""
    if not store["loaded"]:
        return {"error": "CSV 미로딩"}

    candidates_no_email = sum(
        1 for f in store["factories"]
        if f.get("solar_candidate") and f.get("enriched") and not f.get("email")
    )
    asyncio.create_task(job_harvest_emails())
    return {
        "message": f"이메일 수집 시작 (대상: {candidates_no_email}건, 최대 {max_count}건)",
        "status": "running",
    }


@app.get("/test/harvest-one")
async def test_harvest_one(company: str = ""):
    """이메일 수집 단건 테스트"""
    if not company:
        return {"error": "?company=회사명 파라미터 필요"}
    result = harvest_email({"company_name": company})
    if result:
        return {"ok": True, "company": company, **result}
    return {"ok": False, "company": company, "message": "이메일 찾기 실패"}


# ── 대시보드 ───────────────────────────────────────────────

@app.get("/dashboard")
async def dashboard():
    """영업 현황 대시보드"""
    stats = get_stats(store["factories"]) if store["loaded"] else {}
    days_to_complete = 0
    if stats.get("unenriched", 0) > 0:
        days_to_complete = stats["unenriched"] // 900 + 1

    # 이메일 확보 현황
    with_email = sum(1 for f in store["factories"]
                     if f.get("email") and f.get("solar_candidate"))
    candidates_total = stats.get("solar_candidates", 0)

    return {
        "stats": {
            **stats,
            "with_email": with_email,
            "email_ready_to_send": sum(
                1 for f in store["factories"]
                if f.get("email") and f.get("solar_candidate")
                and not f.get("email_sent")
            ),
        },
        "enrichment_progress": {
            "done": stats.get("enriched", 0),
            "remaining": stats.get("unenriched", 0),
            "days_to_complete": days_to_complete,
            "rate": "~900건/일 (API 일일 한도)",
        },
        "schedule": {
            "enrich": f"매일 {config.DAILY_COLLECT_HOUR}:00 (API 보강)",
            "harvest": "매일 08:00 (이메일 수집)",
            "email": f"매일 {config.DAILY_EMAIL_HOUR}:00 (콜드메일 발송)",
        },
        "last_enrich": store.get("last_enrich"),
        "last_harvest": store.get("last_harvest"),
        "last_email": store.get("last_email"),
        "priority_complexes": PRIORITY_COMPLEXES,
    }


@app.get("/dashboard/candidates")
async def dashboard_candidates(limit: int = 50):
    """태양광 후보 목록 (보강 완료 + 660m²↑)"""
    candidates = get_solar_candidates(store["factories"])
    # 건축면적 내림차순
    candidates.sort(key=lambda x: x.get("building_area_m2", 0), reverse=True)

    return {
        "total_candidates": len(candidates),
        "showing": min(limit, len(candidates)),
        "candidates": [
            {
                "company_name": c.get("company_name"),
                "ceo_name": c.get("ceo_name"),
                "phone": c.get("phone"),
                "address": c.get("address"),
                "industrial_complex": c.get("industrial_complex"),
                "building_area_m2": c.get("building_area_m2"),
                "product": c.get("product"),
                "email_sent": c.get("email_sent"),
            }
            for c in candidates[:limit]
        ],
    }


@app.get("/dashboard/phone-list")
async def dashboard_phone_list(limit: int = 100):
    """전화번호 보유 후보 목록 (콜드콜용)"""
    candidates = get_solar_candidates(store["factories"])
    with_phone = [c for c in candidates if c.get("phone")]
    with_phone.sort(key=lambda x: x.get("building_area_m2", 0), reverse=True)

    return {
        "total_with_phone": len(with_phone),
        "list": [
            {
                "company_name": c.get("company_name"),
                "ceo_name": c.get("ceo_name"),
                "phone": c.get("phone"),
                "building_area_m2": c.get("building_area_m2"),
                "address": c.get("address"),
            }
            for c in with_phone[:limit]
        ],
    }


@app.get("/dashboard/email-list")
async def dashboard_email_list(limit: int = 100):
    """이메일 확보된 후보 목록 (콜드메일 발송 대상)"""
    with_email = [
        f for f in store["factories"]
        if f.get("email") and f.get("solar_candidate") and f.get("enriched")
    ]
    with_email.sort(key=lambda x: x.get("building_area_m2", 0), reverse=True)

    return {
        "total_with_email": len(with_email),
        "sent": sum(1 for f in with_email if f.get("email_sent")),
        "unsent": sum(1 for f in with_email if not f.get("email_sent")),
        "list": [
            {
                "company_name": c.get("company_name"),
                "ceo_name": c.get("ceo_name"),
                "email": c.get("email"),
                "phone": c.get("phone"),
                "building_area_m2": c.get("building_area_m2"),
                "address": c.get("address"),
                "website": c.get("website", ""),
                "email_sent": c.get("email_sent", False),
            }
            for c in with_email[:limit]
        ],
    }
