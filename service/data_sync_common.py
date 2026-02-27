import os
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "sslmode": "require",
}

HOT_API_KEY = os.getenv("HOT_SHELTER_API")
COLD_API_KEY = os.getenv("COLD_SHELTER_API")
WEATHER_KEY = os.getenv("WEATHER_API_KEY")
AIR_API_KEY = os.getenv("AIR_API_KEY")

HOT_API_URL = f"http://openapi.seoul.go.kr:8088/{HOT_API_KEY}/json/TbGtnHwcwP"
COLD_API_URL = f"http://openapi.seoul.go.kr:8088/{COLD_API_KEY}/json/TbGtnCwP"
AIR_API_URL = (
    f"http://openapi.seoul.go.kr:8088/{AIR_API_KEY}/json/"
    "ListAirQualityByDistrictService/1/25/"
)
WEATHER_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

PAGE_SIZE = 1000

SEOUL_DISTRICTS = [
    {"name": "강남구", "nx": 61, "ny": 126},
    {"name": "강동구", "nx": 62, "ny": 126},
    {"name": "강북구", "nx": 61, "ny": 128},
    {"name": "강서구", "nx": 58, "ny": 126},
    {"name": "관악구", "nx": 59, "ny": 125},
    {"name": "광진구", "nx": 62, "ny": 126},
    {"name": "구로구", "nx": 58, "ny": 125},
    {"name": "금천구", "nx": 59, "ny": 124},
    {"name": "노원구", "nx": 61, "ny": 129},
    {"name": "도봉구", "nx": 61, "ny": 129},
    {"name": "동대문구", "nx": 61, "ny": 127},
    {"name": "동작구", "nx": 59, "ny": 125},
    {"name": "마포구", "nx": 59, "ny": 127},
    {"name": "서대문구", "nx": 59, "ny": 127},
    {"name": "서초구", "nx": 61, "ny": 125},
    {"name": "성동구", "nx": 61, "ny": 127},
    {"name": "성북구", "nx": 61, "ny": 127},
    {"name": "송파구", "nx": 62, "ny": 126},
    {"name": "양천구", "nx": 58, "ny": 126},
    {"name": "영등포구", "nx": 58, "ny": 126},
    {"name": "용산구", "nx": 60, "ny": 126},
    {"name": "은평구", "nx": 59, "ny": 127},
    {"name": "종로구", "nx": 60, "ny": 127},
    {"name": "중구", "nx": 60, "ny": 127},
    {"name": "중랑구", "nx": 62, "ny": 128},
]


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


def fetch_api(api_url: str, result_key: str, label: str) -> list[dict]:
    all_rows = []
    start = 1
    while True:
        end = start + PAGE_SIZE - 1
        url = f"{api_url}/{start}/{end}/"
        try:
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            data = res.json()
            result = data.get(result_key, {})
            rows = result.get("row", [])
            if not rows:
                break
            all_rows.extend(rows)
            total = int(result.get("list_total_count", 0))
            logger.info(f"[{label}] {start}~{end} 수신: {len(rows)}건 (전체 {total}건)")
            if end >= total:
                break
            start += PAGE_SIZE
        except Exception as exc:
            logger.error(f"[{label}] API 호출 실패 ({start}~{end}): {exc}")
            break
    logger.info(f"[{label}] 총 {len(all_rows)}건 수신 완료")
    return all_rows


def get_db_conn():
    return psycopg.connect(**DB_CONFIG)


def now_ts() -> datetime:
    return datetime.now()
