import azure.functions as func
from endpoints.crawl import bp as crawl_bp
from endpoints.db_health import bp as db_health_bp
from endpoints.disasters import bp as disasters_bp
from endpoints.main import bp as main_bp

app = func.FunctionApp()

app.register_blueprint(main_bp)
app.register_blueprint(crawl_bp)
app.register_blueprint(db_health_bp)
app.register_blueprint(disasters_bp)

# ------------------------------------------------------

import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from decimal import Decimal, InvalidOperation
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

try:
    import azure.functions as func
    AZURE_FUNCTIONS_AVAILABLE = True
except ImportError:
    AZURE_FUNCTIONS_AVAILABLE = False

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


# ───────────────────────────────────────────
# DB / API 설정
# ───────────────────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST"),
    "port":     os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "sslmode":  "require"
}

HOT_API_KEY  = os.getenv("HOT_SHELTER_API")
COLD_API_KEY = os.getenv("COLD_SHELTER_API")
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")
AIR_API_KEY  = os.getenv("AIR_API_KEY")

HOT_API_URL  = f"http://openapi.seoul.go.kr:8088/{HOT_API_KEY}/json/TbGtnHwcwP"
COLD_API_URL = f"http://openapi.seoul.go.kr:8088/{COLD_API_KEY}/json/TbGtnCwP"
AIR_API_URL  = f"http://openapi.seoul.go.kr:8088/{AIR_API_KEY}/json/ListAirQualityByDistrictService/1/25/"
WEATHER_URL  = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"

PAGE_SIZE = 1000

# 서울시 25개 구 좌표
SEOUL_DISTRICTS = [
    {'name': '강남구',   'nx': 61, 'ny': 126}, {'name': '강동구',   'nx': 62, 'ny': 126},
    {'name': '강북구',   'nx': 61, 'ny': 128}, {'name': '강서구',   'nx': 58, 'ny': 126},
    {'name': '관악구',   'nx': 59, 'ny': 125}, {'name': '광진구',   'nx': 62, 'ny': 126},
    {'name': '구로구',   'nx': 58, 'ny': 125}, {'name': '금천구',   'nx': 59, 'ny': 124},
    {'name': '노원구',   'nx': 61, 'ny': 129}, {'name': '도봉구',   'nx': 61, 'ny': 129},
    {'name': '동대문구', 'nx': 61, 'ny': 127}, {'name': '동작구',   'nx': 59, 'ny': 125},
    {'name': '마포구',   'nx': 59, 'ny': 127}, {'name': '서대문구', 'nx': 59, 'ny': 127},
    {'name': '서초구',   'nx': 61, 'ny': 125}, {'name': '성동구',   'nx': 61, 'ny': 127},
    {'name': '성북구',   'nx': 61, 'ny': 127}, {'name': '송파구',   'nx': 62, 'ny': 126},
    {'name': '양천구',   'nx': 58, 'ny': 126}, {'name': '영등포구', 'nx': 58, 'ny': 126},
    {'name': '용산구',   'nx': 60, 'ny': 126}, {'name': '은평구',   'nx': 59, 'ny': 127},
    {'name': '종로구',   'nx': 60, 'ny': 127}, {'name': '중구',     'nx': 60, 'ny': 127},
    {'name': '중랑구',   'nx': 62, 'ny': 128},
]


# ═══════════════════════════════════════════
# 공통 유틸 함수
# ═══════════════════════════════════════════

def clean_str(val, max_len: int = None):
    if val is None:
        return None
    val = str(val).strip()
    if val in ("", "null", "NULL", "None"):
        return None
    if max_len and len(val) > max_len:
        logger.warning(f"문자열 초과 자름: '{val[:20]}...' ({len(val)} → {max_len})")
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
    """페이지네이션 포함 API 전체 수신 (쉼터용 공통)"""
    all_rows = []
    start    = 1
    while True:
        end = start + PAGE_SIZE - 1
        url = f"{api_url}/{start}/{end}/"
        try:
            res    = requests.get(url, timeout=10)
            res.raise_for_status()
            data   = res.json()
            result = data.get(result_key, {})
            rows   = result.get("row", [])
            if not rows:
                break
            all_rows.extend(rows)
            total = int(result.get("list_total_count", 0))
            logger.info(f"[{label}] {start}~{end} 수신: {len(rows)}건 (전체 {total}건)")
            if end >= total:
                break
            start += PAGE_SIZE
        except Exception as e:
            logger.error(f"[{label}] API 호출 실패 ({start}~{end}): {e}")
            break
    logger.info(f"[{label}] 총 {len(all_rows)}건 수신 완료")
    return all_rows

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


# ═══════════════════════════════════════════
# ① 무더위 쉼터 (heat_shelter)
# ═══════════════════════════════════════════

def parse_heat_row(r: dict) -> tuple:
    return (
        clean_int(r.get("YEAR")),
        clean_bpchar(r.get("AREA_CD"), 10),
        clean_str(r.get("FACILITY_TYPE1"), 50),
        clean_str(r.get("FACILITY_TYPE2"), 50),
        clean_str(r.get("R_AREA_NM"), 100),
        clean_str(r.get("R_DETL_ADD"), 200),
        clean_str(r.get("LOTNO_ADDR"), 200),
        clean_float(r.get("R_AREA_SQR")),
        clean_float(r.get("USE_PRNB")),
        clean_str(r.get("RMRK"), 500),
        clean_float(r.get("LON")),
        clean_float(r.get("LAT")),
        clean_numeric(r.get("MAP_COORD_X")),
        clean_numeric(r.get("MAP_COORD_Y")),
        datetime.now(),
    )

def setup_heat_shelter(conn):
    sqls = [
        "ALTER TABLE public.heat_shelter ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE public.heat_shelter ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT false",
        """DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'heat_shelter_unique'
            ) THEN
                ALTER TABLE public.heat_shelter
                ADD CONSTRAINT heat_shelter_unique UNIQUE (area_cd, shelter_name);
            END IF;
        END $$;"""
    ]
    with conn.cursor() as cur:
        for sql in sqls:
            cur.execute(sql)
    conn.commit()
    logger.info("[무더위] 초기 컬럼 및 제약 조건 설정 완료")

def upsert_heat_shelters(conn, rows: list[dict]):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.heat_shelter WHERE is_deleted = false
        """)
        before = {(r[0].strip(), r[1]): r[2:] for r in cur.fetchall()}

    values  = []
    skipped = 0
    for r in rows:
        try:
            use_prnb = clean_float(r.get("USE_PRNB"))
            if not use_prnb or use_prnb <= 0:
                skipped += 1
                continue
            values.append(parse_heat_row(r))
        except Exception as e:
            logger.warning(f"[무더위] 행 전처리 실패: {r.get('R_AREA_NM')} / {e}")

    logger.info(f"[무더위] 이용가능인원 0/NULL 제외: {skipped}건")
    if not values:
        logger.warning("[무더위] 적재할 데이터 없음")
        return

    sql = """
        INSERT INTO public.heat_shelter (
            facility_year, area_cd, facility_type1, facility_type2, shelter_name,
            road_addr, lot_addr, facility_area, capacity, remark,
            lon, lat, coord_x, coord_y, updated_at
        ) VALUES %s
        ON CONFLICT (area_cd, shelter_name)
        DO UPDATE SET
            facility_type1 = EXCLUDED.facility_type1,
            facility_type2 = EXCLUDED.facility_type2,
            road_addr      = EXCLUDED.road_addr,
            facility_area  = EXCLUDED.facility_area,
            capacity       = EXCLUDED.capacity,
            remark         = EXCLUDED.remark,
            lon            = EXCLUDED.lon,
            lat            = EXCLUDED.lat,
            coord_x        = EXCLUDED.coord_x,
            coord_y        = EXCLUDED.coord_y,
            updated_at     = EXCLUDED.updated_at,
            is_deleted     = false
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.heat_shelter WHERE is_deleted = false
        """)
        after = {(r[0].strip(), r[1]): r[2:] for r in cur.fetchall()}

    new_count = changed_count = 0
    for key, after_val in after.items():
        if key not in before:
            logger.info(f"[무더위][NEW] {key[1]} ({key[0].strip()})")
            new_count += 1
        elif before[key] != after_val:
            logger.info(f"[무더위][UPDATE] {key[1]}")
            for i, field in enumerate(["road_addr", "facility_area", "capacity", "remark"]):
                if before[key][i] != after_val[i]:
                    logger.info(f"  - {field}: '{before[key][i]}' → '{after_val[i]}'")
            changed_count += 1

    logger.info(f"[무더위] 신규: {new_count}건 | 변경: {changed_count}건 | 전체 UPSERT: {len(values)}건")

def soft_delete_heat(conn, current_keys: list[tuple]):
    with conn.cursor() as cur:
        cur.execute("SELECT area_cd, shelter_name FROM public.heat_shelter WHERE is_deleted = false")
        db_keys = set(tuple(r) for r in cur.fetchall())

    api_keys     = set((clean_bpchar(k[0], 10), k[1]) for k in current_keys)
    deleted_keys = db_keys - api_keys
    if not deleted_keys:
        logger.info("[무더위] 삭제된 쉼터 없음")
        return
    with conn.cursor() as cur:
        for key in deleted_keys:
            logger.info(f"[무더위][DELETE] {key[1]} ({key[0].strip()})")
            cur.execute(
                "UPDATE public.heat_shelter SET is_deleted = true "
                "WHERE area_cd = %s AND shelter_name = %s", key
            )
    conn.commit()
    logger.info(f"[무더위] 총 {len(deleted_keys)}건 소프트 삭제 완료")

def run_heat_shelter(conn):
    logger.info("───────────────────────────────────────────")
    logger.info("  [무더위 쉼터] 갱신 시작")
    logger.info("───────────────────────────────────────────")
    setup_heat_shelter(conn)
    rows = fetch_api(HOT_API_URL, "TbGtnHwcwP", "무더위")
    if not rows:
        logger.warning("[무더위] 수신된 데이터 없음, 건너뜀")
        return
    upsert_heat_shelters(conn, rows)
    soft_delete_heat(conn, [(r.get("AREA_CD"), r.get("R_AREA_NM")) for r in rows])
    logger.info("[무더위 쉼터] 갱신 완료")


# ═══════════════════════════════════════════
# ② 한파 쉼터 (cold_shelter)
# ═══════════════════════════════════════════

def parse_cold_row(r: dict) -> tuple:
    return (
        clean_int(r.get("NO")),
        clean_str(r.get("FACILITY_TYPE1"), 50),
        clean_str(r.get("FACILITY_TYPE2"), 50),
        clean_str(r.get("RESTAREA_NM"), 100),
        clean_str(r.get("ROAD_NM_ADDR"), 200),
        clean_str(r.get("LOTNO_ADDR"), 200),
        clean_float(r.get("FCAR")),
        clean_int(r.get("UTZTN_PSBLTY_NOPE")),
        clean_str(r.get("RMRK"), 500),
        clean_float(r.get("LOT")),
        clean_float(r.get("LAT")),
        clean_numeric(r.get("XCRD")),
        clean_numeric(r.get("YCRD")),
        clean_str(r.get("USE_YN"), 10),
        clean_str(r.get("USE_TYPE"), 50),
        datetime.now(),
    )

def setup_cold_shelter(conn):
    sqls = [
        "ALTER TABLE public.cold_shelter ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()",
        "ALTER TABLE public.cold_shelter ADD COLUMN IF NOT EXISTS is_deleted BOOLEAN DEFAULT false",
        """DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint WHERE conname = 'cold_shelter_unique'
            ) THEN
                ALTER TABLE public.cold_shelter
                ADD CONSTRAINT cold_shelter_unique UNIQUE (no, shelter_name);
            END IF;
        END $$;"""
    ]
    with conn.cursor() as cur:
        for sql in sqls:
            cur.execute(sql)
    conn.commit()
    logger.info("[한파] 초기 컬럼 및 제약 조건 설정 완료")

def upsert_cold_shelters(conn, rows: list[dict]):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT no, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.cold_shelter WHERE is_deleted = false
        """)
        before = {(r[0], r[1]): r[2:] for r in cur.fetchall()}

    values = []
    skipped = filtered = 0
    for r in rows:
        try:
            if clean_str(r.get("USE_YN")) == "N":
                filtered += 1
                continue
            use_prnb = clean_int(r.get("UTZTN_PSBLTY_NOPE"))
            if not use_prnb or use_prnb <= 0:
                skipped += 1
                continue
            values.append(parse_cold_row(r))
        except Exception as e:
            logger.warning(f"[한파] 행 전처리 실패: {r.get('RESTAREA_NM')} / {e}")

    logger.info(f"[한파] 사용여부 N 제외: {filtered}건 | 이용가능인원 0/NULL 제외: {skipped}건")
    if not values:
        logger.warning("[한파] 적재할 데이터 없음")
        return

    sql = """
        INSERT INTO public.cold_shelter (
            no, facility_type1, facility_type2, shelter_name,
            road_addr, lot_addr, facility_area, capacity, remark,
            lon, lat, coord_x, coord_y, use_yn, use_type, updated_at
        ) VALUES %s
        ON CONFLICT (no, shelter_name)
        DO UPDATE SET
            facility_type1 = EXCLUDED.facility_type1,
            facility_type2 = EXCLUDED.facility_type2,
            road_addr      = EXCLUDED.road_addr,
            facility_area  = EXCLUDED.facility_area,
            capacity       = EXCLUDED.capacity,
            remark         = EXCLUDED.remark,
            lon            = EXCLUDED.lon,
            lat            = EXCLUDED.lat,
            coord_x        = EXCLUDED.coord_x,
            coord_y        = EXCLUDED.coord_y,
            use_yn         = EXCLUDED.use_yn,
            use_type       = EXCLUDED.use_type,
            updated_at     = EXCLUDED.updated_at,
            is_deleted     = false
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, values)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("""
            SELECT no, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.cold_shelter WHERE is_deleted = false
        """)
        after = {(r[0], r[1]): r[2:] for r in cur.fetchall()}

    new_count = changed_count = 0
    for key, after_val in after.items():
        if key not in before:
            logger.info(f"[한파][NEW] {key[1]}")
            new_count += 1
        elif before[key] != after_val:
            logger.info(f"[한파][UPDATE] {key[1]}")
            for i, field in enumerate(["road_addr", "facility_area", "capacity", "remark"]):
                if before[key][i] != after_val[i]:
                    logger.info(f"  - {field}: '{before[key][i]}' → '{after_val[i]}'")
            changed_count += 1

    logger.info(f"[한파] 신규: {new_count}건 | 변경: {changed_count}건 | 전체 UPSERT: {len(values)}건")

def soft_delete_cold(conn, current_keys: list[tuple]):
    with conn.cursor() as cur:
        cur.execute("SELECT no, shelter_name FROM public.cold_shelter WHERE is_deleted = false")
        db_keys = set(tuple(r) for r in cur.fetchall())

    deleted_keys = db_keys - set(current_keys)
    if not deleted_keys:
        logger.info("[한파] 삭제된 쉼터 없음")
        return
    with conn.cursor() as cur:
        for key in deleted_keys:
            logger.info(f"[한파][DELETE] {key[1]}")
            cur.execute(
                "UPDATE public.cold_shelter SET is_deleted = true "
                "WHERE no = %s AND shelter_name = %s", key
            )
    conn.commit()
    logger.info(f"[한파] 총 {len(deleted_keys)}건 소프트 삭제 완료")

def run_cold_shelter(conn):
    logger.info("───────────────────────────────────────────")
    logger.info("  [한파 쉼터] 갱신 시작")
    logger.info("───────────────────────────────────────────")
    setup_cold_shelter(conn)
    rows = fetch_api(COLD_API_URL, "TbGtnCwP", "한파")
    if not rows:
        logger.warning("[한파] 수신된 데이터 없음, 건너뜀")
        return
    upsert_cold_shelters(conn, rows)
    current_keys = [
        (clean_int(r.get("NO")), clean_str(r.get("RESTAREA_NM"), 100))
        for r in rows
        if clean_str(r.get("USE_YN")) != "N"
        and clean_int(r.get("UTZTN_PSBLTY_NOPE")) and clean_int(r.get("UTZTN_PSBLTY_NOPE")) > 0
    ]
    soft_delete_cold(conn, current_keys)
    logger.info("[한파 쉼터] 갱신 완료")


# ═══════════════════════════════════════════
# ③ 기후 + 미세먼지 (seoul_environment)
# ═══════════════════════════════════════════

def get_base_time_and_date():
    """기상청 API 호출 기준 시간 계산 (분 < 40이면 1시간 전 사용)"""
    now    = datetime.now()
    target = now - timedelta(hours=1) if now.minute < 40 else now
    return target.strftime("%Y%m%d"), target.strftime("%H00")

def fetch_air_quality() -> dict:
    """서울시 미세먼지 → {구이름: {pm10, pm25, grade}} 딕셔너리"""
    air_map = {}
    try:
        res  = requests.get(AIR_API_URL, timeout=10)
        rows = res.json().get("ListAirQualityByDistrictService", {}).get("row", [])
        for row in rows:
            name  = row.get("MSRSTN_NM", "")
            pm10  = float(row["PM"])  if str(row.get("PM",  "")).replace(".", "", 1).isdigit() else 0.0
            pm25  = float(row["FPM"]) if str(row.get("FPM", "")).replace(".", "", 1).isdigit() else 0.0
            grade = row.get("CAI_GRD") or "정보없음"
            air_map[name] = {"pm10": pm10, "pm25": pm25, "grade": grade}
        logger.info(f"[환경] 미세먼지 {len(air_map)}개 구 수신 완료")
    except Exception as e:
        logger.error(f"[환경] 미세먼지 수집 실패: {e}")
    return air_map

def fetch_weather_and_air() -> list[tuple]:
    """25개 구 기상 + 미세먼지 통합 수집"""
    base_date, base_time = get_base_time_and_date()
    air_map = fetch_air_quality()
    results = []

    logger.info(f"[환경] 기상 수집 시작 (기준: {base_date} {base_time})")

    for dist in SEOUL_DISTRICTS:
        params = {
            "serviceKey": WEATHER_KEY,
            "dataType":   "JSON",
            "base_date":  base_date,
            "base_time":  base_time,
            "nx":         dist["nx"],
            "ny":         dist["ny"],
        }
        try:
            res    = requests.get(WEATHER_URL, params=params, timeout=10)
            items  = res.json()["response"]["body"]["items"]["item"]
            w_data = {i["category"]: i["obsrValue"] for i in items}

            temp = float(w_data.get("T1H", 0))
            humi = float(w_data.get("REH", 0))
            wind = float(w_data.get("WSD", 0))
            rain = float(w_data.get("RN1", 0))
            air  = air_map.get(dist["name"], {"pm10": 0.0, "pm25": 0.0, "grade": "데이터없음"})

            results.append((
                dist["name"], temp, humi, wind, rain,
                air["pm10"], air["pm25"], air["grade"]
            ))
            logger.info(
                f"[환경] {dist['name']} | 기온:{temp} 습도:{humi} "
                f"풍속:{wind} 강수:{rain} PM10:{air['pm10']} PM2.5:{air['pm25']}"
            )
            time.sleep(0.05)  # API 부하 방지

        except Exception as e:
            logger.error(f"[환경] {dist['name']} 수집 실패: {e}")

    return results

def save_environment(conn, data_list: list[tuple]):
    """seoul_environment 테이블에 INSERT (매시간 누적 저장)"""
    if not data_list:
        logger.warning("[환경] 저장할 데이터 없음")
        return
    sql = """
        INSERT INTO public.seoul_environment
            (dist_name, temp, humi, wind, rain, pm10, pm25, air_grade)
        VALUES %s
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, data_list)
    conn.commit()
    logger.info(f"[환경] seoul_environment 저장 완료: {len(data_list)}건")

def run_environment(conn):
    logger.info("───────────────────────────────────────────")
    logger.info("  [기후 + 미세먼지] 수집 시작")
    logger.info("───────────────────────────────────────────")
    data = fetch_weather_and_air()
    save_environment(conn, data)
    logger.info("[기후 + 미세먼지] 수집 완료")


# ═══════════════════════════════════════════
# 메인 함수 (타이머별 진입점)
# ═══════════════════════════════════════════

def main_shelter():
    """무더위 + 한파 쉼터 갱신 (매일 1회)"""
    logger.info("=" * 50)
    logger.info("  [쉼터] 데이터 갱신 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    required = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
                "HOT_SHELTER_API", "COLD_SHELTER_API"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"필수 환경변수 누락: {missing}")
        return

    try:
        conn = get_db_conn()
        logger.info("[DB] 연결 성공")
    except Exception as e:
        logger.error(f"[DB] 연결 실패: {e}")
        return

    try:
        try:
            run_heat_shelter(conn)
        except Exception as e:
            logger.error(f"[무더위] 오류: {e}")
            conn.rollback()

        try:
            run_cold_shelter(conn)
        except Exception as e:
            logger.error(f"[한파] 오류: {e}")
            conn.rollback()
    finally:
        conn.close()
        logger.info("=" * 50)
        logger.info("  [쉼터] 데이터 갱신 완료")
        logger.info("=" * 50)


def main_environment():
    """기후 + 미세먼지 수집 (매시간)"""
    logger.info("=" * 50)
    logger.info("  [환경] 데이터 수집 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    required = ["POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD",
                "WEATHER_API_KEY", "AIR_API_KEY"]
    missing  = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"필수 환경변수 누락: {missing}")
        return

    try:
        conn = get_db_conn()
        logger.info("[DB] 연결 성공")
    except Exception as e:
        logger.error(f"[DB] 연결 실패: {e}")
        return

    try:
        run_environment(conn)
    except Exception as e:
        logger.error(f"[환경] 오류: {e}")
        conn.rollback()
    finally:
        conn.close()
        logger.info("=" * 50)
        logger.info("  [환경] 데이터 수집 완료")
        logger.info("=" * 50)


# ═══════════════════════════════════════════
# Azure Functions Timer Triggers
#
# shelter_timer : 매일 KST 06:00 (UTC 21:00)  → 쉼터 (하루 1회)
# env_timer     : 매시간 정각 (UTC 0분)        → 기후 + 미세먼지 (1시간마다)
# ═══════════════════════════════════════════

if AZURE_FUNCTIONS_AVAILABLE:
    app = func.FunctionApp()

    @app.timer_trigger(
        schedule="0 0 21 * * *",   # UTC 21:00 = KST 06:00, 하루 1회
        arg_name="shelter_timer",
        run_on_startup=False
    )
    def shelter_timer(shelter_timer: func.TimerRequest) -> None:
        main_shelter()

    @app.timer_trigger(
        schedule="0 0 * * * *",    # 매시간 0분 0초, 1시간마다
        arg_name="env_timer",
        run_on_startup=False
    )
    def env_timer(env_timer: func.TimerRequest) -> None:
        main_environment()


# ═══════════════════════════════════════════
# 로컬 즉시 실행
# python function_app.py          → 전체 실행
# python function_app.py shelter  → 쉼터만
# python function_app.py env      → 기후/미세먼지만
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    if target in ("shelter", "all"):
        main_shelter()
    if target in ("env", "all"):
        main_environment()