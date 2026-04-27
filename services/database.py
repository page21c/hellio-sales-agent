"""
데이터베이스 서비스 — Supabase에 공장/이메일 데이터 저장
"""
import requests
import logging
from datetime import datetime
from config import SUPABASE_URL, SUPABASE_KEY

logger = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal",
}


def _url(table: str) -> str:
    return f"{SUPABASE_URL}/rest/v1/{table}"


def save_factories(factories: list[dict]) -> int:
    """공장 데이터 Supabase에 저장 (upsert)"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.warning("Supabase 미설정 — 로컬 저장만 진행")
        return 0

    rows = []
    for f in factories:
        rows.append({
            "factory_manage_no": f.get("factory_manage_no", ""),
            "company_name": f.get("company_name", ""),
            "ceo_name": f.get("ceo_name", ""),
            "address": f.get("address", ""),
            "phone": f.get("phone", ""),
            "admin_org": f.get("admin_org", ""),
            "industrial_complex": f.get("industrial_complex", ""),
            "building_area_m2": f.get("building_area_m2", 0),
            "lot_area_m2": f.get("lot_area_m2", 0),
            "land_use": f.get("land_use", ""),
            "region": f.get("region", ""),
            "solar_candidate": f.get("solar_candidate", False),
            "collected_at": f.get("collected_at", datetime.utcnow().isoformat()),
        })

    try:
        # upsert (address 기준 중복 방지)
        resp = requests.post(
            _url("factories"),
            headers={**HEADERS, "Prefer": "resolution=merge-duplicates,return=minimal"},
            json=rows,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            logger.info(f"DB 저장: {len(rows)}건")
            return len(rows)
        else:
            logger.error(f"DB 저장 실패: {resp.status_code} {resp.text[:200]}")
            return 0
    except Exception as e:
        logger.error(f"DB 연결 실패: {e}")
        return 0


def save_email_log(log: dict) -> bool:
    """이메일 발송 기록 저장"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False

    try:
        resp = requests.post(
            _url("email_logs"),
            headers=HEADERS,
            json=log,
            timeout=15,
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False


def get_candidates(limit: int = 50, offset: int = 0) -> list[dict]:
    """태양광 후보 공장 목록 조회 (이메일 미발송 건)"""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return []

    try:
        resp = requests.get(
            _url("factories"),
            headers=HEADERS,
            params={
                "solar_candidate": "eq.true",
                "email_sent": "eq.false",
                "select": "*",
                "limit": limit,
                "offset": offset,
                "order": "factory_area_m2.desc",
            },
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception:
        return []


def get_dashboard_stats() -> dict:
    """대시보드 통계"""
    stats = {
        "total_factories": 0,
        "solar_candidates": 0,
        "emails_sent": 0,
        "emails_opened": 0,
    }

    if not SUPABASE_URL or not SUPABASE_KEY:
        return stats

    try:
        # 전체 공장 수
        r = requests.get(
            _url("factories"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["total_factories"] = int(
                r.headers["content-range"].split("/")[-1]
            )

        # 태양광 후보 수
        r = requests.get(
            _url("factories"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "solar_candidate": "eq.true", "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["solar_candidates"] = int(
                r.headers["content-range"].split("/")[-1]
            )

        # 발송 이메일 수
        r = requests.get(
            _url("email_logs"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["emails_sent"] = int(
                r.headers["content-range"].split("/")[-1]
            )

    except Exception as e:
        logger.error(f"통계 조회 실패: {e}")

    return stats


# Supabase 테이블 생성 SQL (1회만 실행)
SETUP_SQL = """
-- 공장 데이터 테이블
CREATE TABLE IF NOT EXISTS factories (
    id BIGSERIAL PRIMARY KEY,
    factory_manage_no TEXT UNIQUE,
    company_name TEXT NOT NULL,
    ceo_name TEXT DEFAULT '',
    address TEXT NOT NULL,
    phone TEXT DEFAULT '',
    admin_org TEXT DEFAULT '',
    industrial_complex TEXT DEFAULT '',
    building_area_m2 REAL DEFAULT 0,
    lot_area_m2 REAL DEFAULT 0,
    land_use TEXT DEFAULT '',
    region TEXT DEFAULT '',
    solar_candidate BOOLEAN DEFAULT FALSE,
    email_sent BOOLEAN DEFAULT FALSE,
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 이메일 발송 로그
CREATE TABLE IF NOT EXISTS email_logs (
    id BIGSERIAL PRIMARY KEY,
    factory_id BIGINT REFERENCES factories(id),
    company_name TEXT,
    to_email TEXT,
    subject TEXT,
    status TEXT DEFAULT 'sent',
    sent_at TIMESTAMPTZ DEFAULT NOW(),
    opened_at TIMESTAMPTZ,
    replied_at TIMESTAMPTZ
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_factories_solar ON factories(solar_candidate);
CREATE INDEX IF NOT EXISTS idx_factories_region ON factories(region);
CREATE INDEX IF NOT EXISTS idx_factories_email_sent ON factories(email_sent);
CREATE INDEX IF NOT EXISTS idx_factories_area ON factories(building_area_m2);
"""
