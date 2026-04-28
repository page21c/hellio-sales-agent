"""
데이터베이스 서비스 — Supabase에 공장/이메일 데이터 저장 및 로드
서버 재시작 시에도 보강·이메일·발송 이력이 유지됩니다.
"""
import requests
import json
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


def is_connected() -> bool:
    """Supabase 연결 여부 확인"""
    return bool(SUPABASE_URL and SUPABASE_KEY
                and not SUPABASE_URL.startswith("여기")
                and not SUPABASE_KEY.startswith("여기"))


# ============================================================
# 저장
# ============================================================

def save_factories(factories: list[dict]) -> int:
    """보강된 공장 데이터를 Supabase에 저장 (upsert)"""
    if not is_connected():
        return 0

    # 50건씩 배치 저장
    saved = 0
    batch_size = 50

    for i in range(0, len(factories), batch_size):
        batch = factories[i:i + batch_size]
        rows = []
        for f in batch:
            row = {
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
                "enriched": f.get("enriched", False),
                "email": f.get("email", ""),
                "website": f.get("website", ""),
                "email_sent": f.get("email_sent", False),
                "collected_at": f.get("collected_at",
                                      datetime.utcnow().isoformat()),
            }
            # factory_manage_no가 있으면 추가 (upsert 키)
            fmn = f.get("factory_manage_no", "")
            if fmn:
                row["factory_manage_no"] = fmn
            rows.append(row)

        try:
            resp = requests.post(
                _url("factories"),
                headers={
                    **HEADERS,
                    "Prefer": "resolution=merge-duplicates,return=minimal",
                },
                json=rows,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                saved += len(rows)
            else:
                logger.error(f"DB 저장 실패: {resp.status_code} "
                             f"{resp.text[:300]}")
        except Exception as e:
            logger.error(f"DB 연결 실패: {e}")

    if saved > 0:
        logger.info(f"DB 저장: {saved}건")
    return saved


def save_email_log(log_entry: dict) -> bool:
    """이메일 발송 기록 저장"""
    if not is_connected():
        return False

    try:
        resp = requests.post(
            _url("email_logs"),
            headers=HEADERS,
            json=log_entry,
            timeout=15,
        )
        return resp.status_code in (200, 201)
    except Exception:
        return False


# ============================================================
# 로드 (서버 재시작 시 데이터 복원)
# ============================================================

def load_enriched_factories() -> list[dict]:
    """
    Supabase에서 보강 완료된 공장 데이터를 모두 로드합니다.
    서버 재시작 시 CSV + 이 데이터를 합쳐서 이전 상태를 복원합니다.
    """
    if not is_connected():
        return []

    all_rows = []
    limit = 1000
    offset = 0

    try:
        while True:
            resp = requests.get(
                _url("factories"),
                headers=HEADERS,
                params={
                    "enriched": "eq.true",
                    "select": "*",
                    "limit": limit,
                    "offset": offset,
                    "order": "id.asc",
                },
                timeout=30,
            )
            if resp.status_code != 200:
                logger.error(f"DB 로드 실패: {resp.status_code} "
                             f"{resp.text[:200]}")
                break

            rows = resp.json()
            if not rows:
                break

            all_rows.extend(rows)
            offset += limit

            if len(rows) < limit:
                break

        logger.info(f"DB에서 보강 데이터 로드: {len(all_rows)}건")
        return all_rows

    except Exception as e:
        logger.error(f"DB 로드 실패: {e}")
        return []


def merge_with_csv(csv_factories: list[dict],
                   db_factories: list[dict]) -> list[dict]:
    """
    CSV 데이터와 DB 보강 데이터를 합칩니다.

    DB에 있는 공장은 보강 정보(면적, 전화번호, 이메일 등)를 덮어쓰고,
    DB에 없는 공장은 CSV 원본 그대로 유지합니다.
    """
    # DB 데이터를 주소 기준으로 인덱싱
    db_map = {}
    for f in db_factories:
        addr = f.get("address", "")
        if addr:
            db_map[addr] = f

    merged = 0
    for factory in csv_factories:
        addr = factory.get("address", "")
        if addr in db_map:
            db_row = db_map[addr]
            # DB 보강 정보로 업데이트
            factory["factory_manage_no"] = db_row.get(
                "factory_manage_no", "")
            factory["ceo_name"] = db_row.get("ceo_name", "")
            factory["phone"] = db_row.get("phone", "")
            factory["building_area_m2"] = db_row.get(
                "building_area_m2", 0)
            factory["lot_area_m2"] = db_row.get("lot_area_m2", 0)
            factory["land_use"] = db_row.get("land_use", "")
            factory["admin_org"] = db_row.get("admin_org", "")
            factory["enriched"] = True
            factory["solar_candidate"] = db_row.get(
                "solar_candidate", False)
            factory["email"] = db_row.get("email", "")
            factory["website"] = db_row.get("website", "")
            factory["email_sent"] = db_row.get("email_sent", False)
            merged += 1

    logger.info(f"CSV + DB 병합: {merged}건 복원됨")
    return csv_factories


# ============================================================
# 조회
# ============================================================

def get_candidates(limit: int = 50, offset: int = 0) -> list[dict]:
    """태양광 후보 공장 목록 조회 (이메일 미발송 건)"""
    if not is_connected():
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
                "order": "building_area_m2.desc",
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
        "enriched": 0,
        "with_email": 0,
        "with_phone": 0,
    }

    if not is_connected():
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
                r.headers["content-range"].split("/")[-1])

        # 보강 완료 수
        r = requests.get(
            _url("factories"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "enriched": "eq.true", "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["enriched"] = int(
                r.headers["content-range"].split("/")[-1])

        # 태양광 후보 수
        r = requests.get(
            _url("factories"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "solar_candidate": "eq.true",
                    "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["solar_candidates"] = int(
                r.headers["content-range"].split("/")[-1])

        # 발송 이메일 수
        r = requests.get(
            _url("email_logs"),
            headers={**HEADERS, "Prefer": "count=exact"},
            params={"select": "id", "limit": 0},
            timeout=10,
        )
        if "content-range" in r.headers:
            stats["emails_sent"] = int(
                r.headers["content-range"].split("/")[-1])

    except Exception as e:
        logger.error(f"통계 조회 실패: {e}")

    return stats
