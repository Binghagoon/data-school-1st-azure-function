import os
import time
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import execute_values
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST"),
    "port":     os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "sslmode":  "require",
}

HOT_API_KEY  = os.getenv("HOT_SHELTER_API")
COLD_API_KEY = os.getenv("COLD_SHELTER_API")
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")
AIR_API_KEY  = os.getenv("AIR_API_KEY")

HOT_API_URL  = f"http://openapi.seoul.go.kr:8088/{HOT_API_KEY}/json/TbGtnHwcwP"
COLD_API_URL = f"http://openapi.seoul.go.kr:8088/{COLD_API_KEY}/json/TbGtnCwP"
AIR_API_URL  = (
    f"http://openapi.seoul.go.kr:8088/{AIR_API_KEY}/json/"
    "ListAirQualityByDistrictService/1/25/"
)
WEATHER_URL  = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

PAGE_SIZE = 1000
BRONZE_RETENTION_DAYS = 90

SEOUL_DISTRICTS = [
    {"name": "강남구",   "nx": 61, "ny": 126},
    {"name": "강동구",   "nx": 62, "ny": 126},
    {"name": "강북구",   "nx": 61, "ny": 128},
    {"name": "강서구",   "nx": 58, "ny": 126},
    {"name": "관악구",   "nx": 59, "ny": 125},
    {"name": "광진구",   "nx": 62, "ny": 126},
    {"name": "구로구",   "nx": 58, "ny": 125},
    {"name": "금천구",   "nx": 59, "ny": 124},
    {"name": "노원구",   "nx": 61, "ny": 129},
    {"name": "도봉구",   "nx": 61, "ny": 129},
    {"name": "동대문구", "nx": 61, "ny": 127},
    {"name": "동작구",   "nx": 59, "ny": 125},
    {"name": "마포구",   "nx": 59, "ny": 127},
    {"name": "서대문구", "nx": 59, "ny": 127},
    {"name": "서초구",   "nx": 61, "ny": 125},
    {"name": "성동구",   "nx": 61, "ny": 127},
    {"name": "성북구",   "nx": 61, "ny": 127},
    {"name": "송파구",   "nx": 62, "ny": 126},
    {"name": "양천구",   "nx": 58, "ny": 126},
    {"name": "영등포구", "nx": 58, "ny": 126},
    {"name": "용산구",   "nx": 60, "ny": 126},
    {"name": "은평구",   "nx": 59, "ny": 127},
    {"name": "종로구",   "nx": 60, "ny": 127},
    {"name": "중구",     "nx": 60, "ny": 127},
    {"name": "중랑구",   "nx": 62, "ny": 128},
]


# ───────────────────────────────────────────
# 공통 유틸
# ───────────────────────────────────────────

def clean_str(val, max_len: int = None):
    if val is None:
        return None
    val = str(val).strip()
    if val in ("", "null", "NULL", "None"):
        return None
    if max_len and len(val) > max_len:
        logger.warning(f"문자열 초과 자름: '{val[:20]}...' ({len(val)} -> {max_len})")
        val = val[:max_len]
    return val


def clean_bpchar(val, length: int = 10):
    v = clean_str(val)
    if v is None:
        return None
    return v.ljust(length)[:length]


def clean_int(val):
    try:
        return int(float(str(val).strip())) if val not in (None, "", "null") else None
    except (ValueError, TypeError):
        return None


def clean_float(val):
    try:
        v = str(val).strip().replace(",", "")
        return float(v) if v not in ("", "null", "NULL") else None
    except (ValueError, TypeError):
        return None


def clean_numeric(val):
    try:
        v = str(val).strip().replace(",", "")
        if v in ("", "null", "NULL"):
            return None
        return Decimal(v)
    except (InvalidOperation, TypeError):
        return None


def parse_air_value(val):
    """점검중/비정상값 → None, 정상수치 → float"""
    if val is None:
        return None
    v = str(val).strip()
    if v in ("", "점검중", "null", "NULL", "-", "N/A"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def fetch_api(api_url: str, result_key: str, label: str, max_retries: int = 3) -> list:
    """페이지네이션 + 실패 시 재시도 포함 API 전체 수신"""
    all_rows, start = [], 1
    while True:
        end = start + PAGE_SIZE - 1
        url = f"{api_url}/{start}/{end}/"
        for attempt in range(max_retries):
            try:
                res    = requests.get(url, timeout=10)
                res.raise_for_status()
                data   = res.json()
                result = data.get(result_key, {})
                rows   = result.get("row", [])
                if not rows:
                    logger.info(f"[{label}] 총 {len(all_rows)}건 수신 완료")
                    return all_rows
                all_rows.extend(rows)
                total = int(result.get("list_total_count", 0))
                logger.info(f"[{label}] {start}~{end} / 전체 {total}건")
                if end >= total:
                    logger.info(f"[{label}] 총 {len(all_rows)}건 수신 완료")
                    return all_rows
                start += PAGE_SIZE
                break
            except Exception as exc:
                if attempt < max_retries - 1:
                    logger.warning(f"[{label}] 재시도 {attempt+1}/{max_retries} ({start}~{end}): {exc}")
                    time.sleep(2)
                else:
                    logger.error(f"[{label}] API 호출 최종 실패 ({start}~{end}): {exc}")
                    return all_rows
    return all_rows


def log_pipeline(conn, layer, category, status, count, duration, error_msg=None):
    """bronze.pipeline_log에 파이프라인 실행 이력 기록"""
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO bronze.pipeline_log
                    (layer, category, status, collected_count, duration_sec, error_msg)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (layer, category, status, count, round(duration, 2), error_msg),
            )
        conn.commit()
        logger.info(f"[LOG] {layer} | {category} | {status} | {count}건 | {round(duration,2)}초")
    except Exception as exc:
        logger.error(f"[LOG] 기록 실패: {exc}")
        conn.rollback()


def purge_old_bronze(conn):
    """오래된 bronze 원본 데이터 정리 (BRONZE_RETENTION_DAYS 기준)"""
    cutoff = f"NOW() - INTERVAL '{BRONZE_RETENTION_DAYS} days'"
    tables = [
        "bronze.environment_raw",
        "bronze.heat_shelter_raw",
        "bronze.cold_shelter_raw",
    ]
    total_deleted = 0
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f"DELETE FROM {table} WHERE loaded_at < {cutoff}")
            deleted = cur.rowcount
            if deleted > 0:
                logger.info(f"[정리] {table} 오래된 데이터 {deleted}건 삭제 ({BRONZE_RETENTION_DAYS}일 초과)")
            total_deleted += deleted
    conn.commit()
    if total_deleted > 0:
        logger.info(f"[정리] bronze 오래된 데이터 총 {total_deleted}건 정리 완료")


def now_ts() -> datetime:
    return datetime.now()