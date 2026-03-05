import azure.functions as func
import logging
from dotenv import load_dotenv

from blueprint.db_health import bp as db_health_bp
from blueprint.disasters import bp as disasters_bp
from blueprint.main import bp as main_bp
from blueprint.timestamps import bp as timestamps_bp
from service.environment_sync import main_environment
from service.hello_service import print_hello
from service.shelter_sync import main_shelter

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_blueprint(main_bp)
app.register_blueprint(db_health_bp)
app.register_blueprint(disasters_bp)
app.register_blueprint(timestamps_bp)

# ================================================================
# 메달리온 아키텍처 ETL 파이프라인
# Bronze(원본 보존) → Silver(정제/검증) → Gold(집계/서비스)
#
# [쉼터] 매일 KST 06:00 실행
#   - Bronze : API 응답 JSONB 원본 저장
#   - Silver : 이용불가(USE_YN=N, 인원0) 제외 / 중복제거 / UPSERT / 소프트삭제
#   - Gold   : shelter_summary 갱신 (area_cd 없음)
#
# [환경] 매시간 15분 실행
#   - Bronze : 기상 + 미세먼지 원본 저장
#   - Silver : 이상값 검증(범위체크) / 음수강수→0 보정 / measured_at 변환
#   - Gold   : 시간별/일별 집계 UPSERT
# ================================================================
load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)


@app.timer_trigger(
    schedule="0 0 21 * * *",  # UTC 21:00 = KST 06:00
    arg_name="shelter_timer",
    run_on_startup=False,
)
def shelter_timer(shelter_timer: func.TimerRequest) -> None:
    main_shelter()


@app.timer_trigger(
    schedule="0 0 * * * *",  # hourly
    arg_name="env_timer",
    run_on_startup=False,
)
def env_timer(env_timer: func.TimerRequest) -> None:
    main_environment()


@app.timer_trigger(
    schedule="*/1 * * * * *",  # every second
    arg_name="hello_timer",
    run_on_startup=False,
)
def hello_timer(hello_timer: func.TimerRequest) -> None:
    print_hello()


PAGE_SIZE = 1000

# 오래된 bronze 원본 보존 기간 (일)
BRONZE_RETENTION_DAYS = 90

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
    {"name": "서대문구", "nx": 59, "ny": 128},
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


# ═══════════════════════════════════════════
# 공통 유틸
# ═══════════════════════════════════════════


def clean_str(val, max_len=None):
    if val is None:
        return None
    val = str(val).strip()
    if val in ("", "null", "NULL", "None"):
        return None
    if max_len and len(val) > max_len:
        logger.warning(f"문자열 초과 자름: '{val[:20]}...' ({len(val)} → {max_len})")
        val = val[:max_len]
    return val


def clean_bpchar(val, length=10):
    v = clean_str(val)
    return v.ljust(length)[:length] if v else None


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
        return Decimal(v) if v not in ("", "null", "NULL") else None
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


def fetch_api(api_url, result_key, label, max_retries=3):
    """페이지네이션 + 실패 시 재시도 포함 API 전체 수신"""
    all_rows, start = [], 1
    while True:
        end = start + PAGE_SIZE - 1
        url = f"{api_url}/{start}/{end}/"
        for attempt in range(max_retries):
            try:
                res = requests.get(url, timeout=10)
                res.raise_for_status()
                data = res.json()
                result = data.get(result_key, {})
                rows = result.get("row", [])
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
                break  # 페이지 성공 → 다음 페이지로
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"[{label}] 재시도 {attempt+1}/{max_retries} ({start}~{end}): {e}"
                    )
                    time.sleep(2)
                else:
                    logger.error(f"[{label}] API 호출 최종 실패 ({start}~{end}): {e}")
                    return all_rows  # 수집된 것까지만 반환
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
        logger.info(
            f"[LOG] {layer} | {category} | {status} | {count}건 | {round(duration,2)}초"
        )
    except Exception as e:
        logger.error(f"[LOG] 기록 실패: {e}")
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
                logger.info(
                    f"[정리] {table} 오래된 데이터 {deleted}건 삭제 ({BRONZE_RETENTION_DAYS}일 초과)"
                )
            total_deleted += deleted
    conn.commit()
    if total_deleted > 0:
        logger.info(f"[정리] bronze 오래된 데이터 총 {total_deleted}건 정리 완료")


# ═══════════════════════════════════════════
# ① 무더위 쉼터 파이프라인
# ═══════════════════════════════════════════


def parse_heat_row(r):
    return (
        clean_int(r.get("YEAR")),
        clean_bpchar(r.get("AREA_CD"), 10),
        clean_str(r.get("FACILITY_TYPE1"), 50),
        clean_str(r.get("FACILITY_TYPE2"), 50),
        clean_str(r.get("R_AREA_NM"), 100),  # shelter_name
        clean_str(r.get("R_DETL_ADD"), 200),  # road_addr
        clean_str(r.get("LOTNO_ADDR"), 200),
        clean_float(r.get("R_AREA_SQR")),  # facility_area FLOAT8
        clean_float(r.get("USE_PRNB")),  # capacity      FLOAT8
        clean_str(r.get("RMRK"), 500),
        clean_float(r.get("LON")),
        clean_float(r.get("LAT")),
        clean_numeric(r.get("MAP_COORD_X")),  # coord_x NUMERIC(15,7)
        clean_numeric(r.get("MAP_COORD_Y")),
        datetime.now(),
    )


def run_heat_shelter(conn):
    logger.info("═" * 55)
    logger.info("  [무더위] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    # ── Bronze: 원본 JSONB 저장 ──────────────────────────────────
    rows = fetch_api(HOT_API_URL, "TbGtnHwcwP", "무더위")
    if not rows:
        log_pipeline(
            conn,
            "bronze",
            "heat_shelter",
            "FAIL",
            0,
            time.time() - start,
            "API 수신 데이터 없음",
        )
        logger.warning("[무더위] 수신 데이터 없음 → 파이프라인 중단")
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO bronze.heat_shelter_raw (raw_data) VALUES %s",
            [(json.dumps(r, ensure_ascii=False),) for r in rows],
        )
    conn.commit()
    log_pipeline(
        conn, "bronze", "heat_shelter", "SUCCESS", len(rows), time.time() - start
    )
    logger.info(f"[무더위-브론즈] {len(rows)}건 원본 저장 완료")

    # ── Silver: 검증 + 중복제거 + UPSERT + 소프트삭제 ──────────
    t = time.time()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM silver.heat_shelter_cleaned WHERE is_deleted = false
        """
        )
        before = {(r[0].strip() if r[0] else "", r[1]): r[2:] for r in cur.fetchall()}

    values, skipped = [], 0
    for r in rows:
        try:
            use_prnb = clean_float(r.get("USE_PRNB"))
            if not use_prnb or use_prnb <= 0:
                skipped += 1
                continue
            values.append(parse_heat_row(r))
        except Exception as e:
            logger.warning(f"[무더위-실버] 행 전처리 실패: {r.get('R_AREA_NM')} / {e}")
    logger.info(f"[무더위-실버] 이용가능인원 0/NULL 제외: {skipped}건")

    # 중복 제거: (area_cd, shelter_name) 기준
    seen, deduped = set(), []
    for v in values:
        key = (v[1], v[4])
        if key not in seen:
            seen.add(key)
            deduped.append(v)
    dup_count = len(values) - len(deduped)
    values = deduped
    if dup_count > 0:
        logger.info(f"[무더위-실버] 중복 제거: {dup_count}건 → 최종 {len(values)}건")

    if not values:
        logger.warning("[무더위-실버] 적재할 데이터 없음")
        log_pipeline(
            conn,
            "silver",
            "heat_shelter",
            "FAIL",
            0,
            time.time() - t,
            "적재 데이터 없음",
        )
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO silver.heat_shelter_cleaned (
                facility_year, area_cd, facility_type1, facility_type2, shelter_name,
                road_addr, lot_addr, facility_area, capacity, remark,
                lon, lat, coord_x, coord_y, updated_at
            ) VALUES %s
            ON CONFLICT (area_cd, shelter_name) DO UPDATE SET
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
        """,
            values,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM silver.heat_shelter_cleaned WHERE is_deleted = false
        """
        )
        after = {(r[0].strip() if r[0] else "", r[1]): r[2:] for r in cur.fetchall()}

    new_count = changed_count = 0
    for key, after_val in after.items():
        if key not in before:
            logger.info(f"[무더위-실버][NEW] {key[1]} ({key[0].strip()})")
            new_count += 1
        elif before[key] != after_val:
            logger.info(f"[무더위-실버][UPDATE] {key[1]}")
            for i, field in enumerate(
                ["road_addr", "facility_area", "capacity", "remark"]
            ):
                if before[key][i] != after_val[i]:
                    logger.info(f"  - {field}: '{before[key][i]}' → '{after_val[i]}'")
            changed_count += 1
    logger.info(
        f"[무더위-실버] 신규: {new_count}건 | 변경: {changed_count}건 | 전체 UPSERT: {len(values)}건"
    )

    api_keys = {
        (clean_bpchar(r.get("AREA_CD"), 10), clean_str(r.get("R_AREA_NM"), 100))
        for r in rows
    }
    with conn.cursor() as cur:
        cur.execute(
            "SELECT area_cd, shelter_name FROM silver.heat_shelter_cleaned WHERE is_deleted = false"
        )
        deleted = set(tuple(r) for r in cur.fetchall()) - api_keys
        for key in deleted:
            logger.info(f"[무더위-실버][DELETE] {key[1]}")
            cur.execute(
                "UPDATE silver.heat_shelter_cleaned SET is_deleted = true "
                "WHERE area_cd=%s AND shelter_name=%s",
                key,
            )
    conn.commit()
    if deleted:
        logger.info(f"[무더위-실버] {len(deleted)}건 소프트 삭제 완료")
    log_pipeline(
        conn, "silver", "heat_shelter", "SUCCESS", len(values), time.time() - t
    )

    # ── Gold: area_cd 제외하고 shelter_summary 반영 ──────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.shelter_summary
                (shelter_type, shelter_name, road_addr, capacity, lon, lat, updated_at)
            SELECT DISTINCT ON (lon, lat) 'heat', shelter_name, road_addr, capacity, lon, lat, updated_at
            FROM silver.heat_shelter_cleaned
            WHERE is_deleted = false AND lon IS NOT NULL AND lat IS NOT NULL
            ORDER BY lon, lat, updated_at DESC
            ON CONFLICT (shelter_type, lon, lat) DO UPDATE SET
                road_addr  = EXCLUDED.road_addr,
                capacity   = EXCLUDED.capacity,
                lon        = EXCLUDED.lon,
                lat        = EXCLUDED.lat,
                updated_at = EXCLUDED.updated_at
        """
        )
        gold_count = cur.rowcount
    conn.commit()
    log_pipeline(conn, "gold", "heat_shelter", "SUCCESS", gold_count, time.time() - t)
    logger.info(f"[무더위-골드] shelter_summary {gold_count}건 반영 완료")


# ═══════════════════════════════════════════
# ② 한파 쉼터 파이프라인
# ═══════════════════════════════════════════


def parse_cold_row(r):
    return (
        clean_str(r.get("FACILITY_TYPE1"), 50),
        clean_str(r.get("FACILITY_TYPE2"), 50),
        clean_str(r.get("RESTAREA_NM"), 100),  # shelter_name
        clean_str(r.get("ROAD_NM_ADDR"), 200),
        clean_str(r.get("LOTNO_ADDR"), 200),
        clean_float(r.get("FCAR")),  # facility_area FLOAT8
        clean_int(r.get("UTZTN_PSBLTY_NOPE")),  # capacity      INT4
        clean_str(r.get("RMRK"), 500),
        clean_float(r.get("LOT")),  # lon
        clean_float(r.get("LAT")),
        clean_float(r.get("XCRD")),  # coord_x FLOAT8
        clean_float(r.get("YCRD")),
        clean_str(r.get("USE_YN"), 10),
        clean_str(r.get("USE_TYPE"), 50),
        datetime.now(),
    )


def run_cold_shelter(conn):
    logger.info("═" * 55)
    logger.info("  [한파] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    # ── Bronze: 원본 JSONB 저장 ──────────────────────────────────
    rows = fetch_api(COLD_API_URL, "TbGtnCwP", "한파")
    if not rows:
        log_pipeline(
            conn,
            "bronze",
            "cold_shelter",
            "FAIL",
            0,
            time.time() - start,
            "API 수신 데이터 없음",
        )
        logger.warning("[한파] 수신 데이터 없음 → 파이프라인 중단")
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO bronze.cold_shelter_raw (raw_data) VALUES %s",
            [(json.dumps(r, ensure_ascii=False),) for r in rows],
        )
    conn.commit()
    log_pipeline(
        conn, "bronze", "cold_shelter", "SUCCESS", len(rows), time.time() - start
    )
    logger.info(f"[한파-브론즈] {len(rows)}건 원본 저장 완료")

    # ── Silver: 검증 + 중복제거 + UPSERT + 소프트삭제 ──────────
    t = time.time()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shelter_name, road_addr, facility_area, capacity, remark
            FROM silver.cold_shelter_cleaned WHERE is_deleted = false
        """
        )
        before = {r[0]: r[1:] for r in cur.fetchall()}

    values, skipped, filtered = [], 0, 0
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
            logger.warning(f"[한파-실버] 행 전처리 실패: {r.get('RESTAREA_NM')} / {e}")
    logger.info(
        f"[한파-실버] USE_YN=N 제외: {filtered}건 | 이용가능인원 0/NULL 제외: {skipped}건"
    )

    # 중복 제거: shelter_name 기준
    seen, deduped = set(), []
    for v in values:
        key = v[2]
        if key not in seen:
            seen.add(key)
            deduped.append(v)
    dup_count = len(values) - len(deduped)
    values = deduped
    if dup_count > 0:
        logger.info(f"[한파-실버] 중복 제거: {dup_count}건 → 최종 {len(values)}건")

    if not values:
        logger.warning("[한파-실버] 적재할 데이터 없음")
        log_pipeline(
            conn,
            "silver",
            "cold_shelter",
            "FAIL",
            0,
            time.time() - t,
            "적재 데이터 없음",
        )
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO silver.cold_shelter_cleaned (
                facility_type1, facility_type2, shelter_name,
                road_addr, lot_addr, facility_area, capacity, remark,
                lon, lat, coord_x, coord_y, use_yn, use_type, updated_at
            ) VALUES %s
            ON CONFLICT (shelter_name) DO UPDATE SET
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
        """,
            values,
        )
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT shelter_name, road_addr, facility_area, capacity, remark
            FROM silver.cold_shelter_cleaned WHERE is_deleted = false
        """
        )
        after = {r[0]: r[1:] for r in cur.fetchall()}

    new_count = changed_count = 0
    for name, after_val in after.items():
        if name not in before:
            logger.info(f"[한파-실버][NEW] {name}")
            new_count += 1
        elif before[name] != after_val:
            logger.info(f"[한파-실버][UPDATE] {name}")
            for i, field in enumerate(
                ["road_addr", "facility_area", "capacity", "remark"]
            ):
                if before[name][i] != after_val[i]:
                    logger.info(f"  - {field}: '{before[name][i]}' → '{after_val[i]}'")
            changed_count += 1
    logger.info(
        f"[한파-실버] 신규: {new_count}건 | 변경: {changed_count}건 | 전체 UPSERT: {len(values)}건"
    )

    api_names = {
        clean_str(r.get("RESTAREA_NM"), 100)
        for r in rows
        if clean_str(r.get("USE_YN")) != "N"
        and clean_int(r.get("UTZTN_PSBLTY_NOPE"))
        and clean_int(r.get("UTZTN_PSBLTY_NOPE")) > 0
    }
    with conn.cursor() as cur:
        cur.execute(
            "SELECT shelter_name FROM silver.cold_shelter_cleaned WHERE is_deleted = false"
        )
        deleted = {r[0] for r in cur.fetchall()} - api_names
        for name in deleted:
            logger.info(f"[한파-실버][DELETE] {name}")
            cur.execute(
                "UPDATE silver.cold_shelter_cleaned SET is_deleted = true WHERE shelter_name=%s",
                (name,),
            )
    conn.commit()
    if deleted:
        logger.info(f"[한파-실버] {len(deleted)}건 소프트 삭제 완료")
    log_pipeline(
        conn, "silver", "cold_shelter", "SUCCESS", len(values), time.time() - t
    )

    # ── Gold: shelter_summary 반영 ───────────────────────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.shelter_summary
                (shelter_type, shelter_name, road_addr, capacity, lon, lat, updated_at)
            SELECT DISTINCT ON (lon, lat) 'cold', shelter_name, road_addr, capacity::FLOAT8, lon, lat, updated_at
            FROM silver.cold_shelter_cleaned
            WHERE is_deleted = false AND lon IS NOT NULL AND lat IS NOT NULL
            ORDER BY lon, lat, updated_at DESC
            ON CONFLICT (shelter_type, lon, lat) DO UPDATE SET
                shelter_name = EXCLUDED.shelter_name,
                road_addr    = EXCLUDED.road_addr,
                capacity     = EXCLUDED.capacity,
                updated_at   = EXCLUDED.updated_at
        """
        )
        gold_count = cur.rowcount
    conn.commit()
    log_pipeline(conn, "gold", "cold_shelter", "SUCCESS", gold_count, time.time() - t)
    logger.info(f"[한파-골드] shelter_summary {gold_count}건 반영 완료")


# ═══════════════════════════════════════════
# ③ 기후 + 미세먼지 파이프라인
# ═══════════════════════════════════════════


def get_base_time_and_date():
    """기상청 기준 시간 계산 (분 < 15이면 1시간 전 사용)"""
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H00")


def fetch_air_quality():
    """
    서울시 미세먼지 수집 → {구이름: {pm10, pm25, grade}}

    air_grade 판단 기준:
    - PM 값이 "점검중" 문자열  → grade = "점검중", pm10/pm25 = None
    - pm10, pm25 둘 다 None    → grade = "점검중"  (수치 없으면 점검중으로 간주)
    - 그 외                     → CAI_GRD 값 사용, 없으면 "정보없음"
    """
    air_map = {}
    try:
        res = requests.get(AIR_API_URL, timeout=10)
        rows = res.json().get("ListAirQualityByDistrictService", {}).get("row", [])
        for row in rows:
            name = row.get("MSRSTN_NM", "")
            pm10 = parse_air_value(row.get("PM"))
            pm25 = parse_air_value(row.get("FPM"))
            raw_pm = str(row.get("PM", "")).strip()

            # ★ 수정: pm10/pm25 값이 하나라도 있으면 점검중 아님
            if raw_pm == "점검중" or (pm10 is None and pm25 is None):
                grade = "점검중"
                logger.warning(f"[환경] {name} 측정소 점검중 → pm10/pm25 NULL 저장")
            else:
                grade = row.get("CAI_GRD") or "정보없음"

            air_map[name] = {"pm10": pm10, "pm25": pm25, "grade": grade}
        logger.info(f"[환경-브론즈] 미세먼지 {len(air_map)}개 구 수신")
    except Exception as e:
        logger.error(f"[환경-브론즈] 미세먼지 수집 실패: {e}")
    return air_map


async def fetch_single_district(session, dist, base_date, base_time, air_map):
    """
    개별 구 기상 데이터 비동기 수집 (실패 시 재시도 3회)
    - 이상값(기온/습도/풍속 범위 초과) 감지 시 스킵
    """
    params = {
        "serviceKey": WEATHER_KEY,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": dist["nx"],
        "ny": dist["ny"],
    }
    for attempt in range(3):
        try:
            async with session.get(
                WEATHER_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as res:
                if res.status != 200:
                    raise ValueError(f"HTTP {res.status}")
                data = await res.json(content_type=None)
                items = (
                    data.get("response", {})
                    .get("body", {})
                    .get("items", {})
                    .get("item", [])
                )
                if not items:
                    raise ValueError("빈 응답")

                w_data = {i["category"]: i["obsrValue"] for i in items}
                temp = float(w_data.get("T1H", 0))
                humi = float(w_data.get("REH", 0))
                wind = float(w_data.get("WSD", 0))
                rain = float(w_data.get("RN1", 0))
                # PTY: 0=없음 1=비 2=비/눈 3=눈 4=소나기 5=빗방울 6=빗방울/눈날림 7=눈날림
                # SNO는 초단기실황 API 미제공 → 단기예보(weather_forecast)에서만 수집
                pty = int(float(w_data.get("PTY", 0)))
                air = air_map.get(
                    dist["name"], {"pm10": None, "pm25": None, "grade": "데이터없음"}
                )

                # 이상값 검증
                if not (-30 <= temp <= 50):
                    logger.warning(f"[검증] {dist['name']} 기온 이상값: {temp}℃ → 스킵")
                    return None
                if not (0 <= humi <= 100):
                    logger.warning(f"[검증] {dist['name']} 습도 이상값: {humi}% → 스킵")
                    return None
                if not (0 <= wind <= 50):
                    logger.warning(
                        f"[검증] {dist['name']} 풍속 이상값: {wind}m/s → 스킵"
                    )
                    return None

                pty_label = {
                    0: "없음",
                    1: "비",
                    2: "비/눈",
                    3: "눈",
                    4: "소나기",
                    5: "빗방울",
                    6: "빗방울/눈날림",
                    7: "눈날림",
                }.get(pty, "없음")
                logger.info(
                    f"[환경] {dist['name']} | 기온:{temp} 습도:{humi} "
                    f"풍속:{wind} 강수:{rain} 강수형태:{pty_label} "
                    f"PM10:{air['pm10']} PM2.5:{air['pm25']} 등급:{air['grade']}"
                )
                return {
                    "dist_name": dist["name"],
                    "temp": temp,
                    "humi": humi,
                    "wind": wind,
                    "rain": rain,
                    "pty": pty,
                    "pm10": air["pm10"],
                    "pm25": air["pm25"],
                    "grade": air["grade"],
                    "base_date": base_date,
                    "base_time": base_time,
                }
        except Exception as e:
            if attempt < 2:
                logger.warning(f"[환경] {dist['name']} 재시도 {attempt+1}/3: {e}")
                await asyncio.sleep(1)
            else:
                logger.error(f"[환경] {dist['name']} 최종 실패 (3회 초과): {e}")
    return None


async def fetch_all_districts_async():
    """25개 구 전체를 비동기 병렬 수집"""
    base_date, base_time = get_base_time_and_date()
    air_map = fetch_air_quality()
    logger.info(f"[환경-브론즈] 기상 수집 시작 (기준: {base_date} {base_time})")
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[
                fetch_single_district(session, d, base_date, base_time, air_map)
                for d in SEOUL_DISTRICTS
            ]
        )
    return [r for r in results if r is not None]


def run_environment(conn):
    logger.info("═" * 55)
    logger.info("  [환경] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    # ── Bronze: 원본 저장 ────────────────────────────────────────
    raw_list = asyncio.run(fetch_all_districts_async())
    if not raw_list:
        log_pipeline(
            conn,
            "bronze",
            "environment",
            "FAIL",
            0,
            time.time() - start,
            "수집 데이터 없음",
        )
        logger.warning("[환경] 수집 데이터 없음 → 파이프라인 중단")
        return

    bronze_values = [
        (
            r["dist_name"],
            r["temp"],
            r["humi"],
            r["wind"],
            r["rain"],
            r["pm10"],
            r["pm25"],
            r["grade"],
            r["base_date"],
            r["base_time"],
            r["pty"],
        )
        for r in raw_list
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bronze.environment_raw
                (dist_name, temp, humi, wind, rain, pm10, pm25, air_grade, base_date, base_time,
                 pty)
            VALUES %s
        """,
            bronze_values,
        )
    conn.commit()

    status = "PARTIAL" if len(raw_list) < len(SEOUL_DISTRICTS) else "SUCCESS"
    if status == "PARTIAL":
        missing = set(d["name"] for d in SEOUL_DISTRICTS) - set(
            r["dist_name"] for r in raw_list
        )
        logger.warning(f"[환경-브론즈] 누락 구: {missing}")
    log_pipeline(
        conn, "bronze", "environment", status, len(raw_list), time.time() - start
    )
    logger.info(f"[환경-브론즈] {len(raw_list)}건 원본 저장 완료")

    # ── Silver: 이상값 보정 + measured_at 변환 ──────────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO silver.environment_cleaned
                (dist_name, temp, humi, wind, rain, pm10, pm25, air_grade, measured_at, pty)
            SELECT
                dist_name,
                CASE WHEN temp BETWEEN -30 AND 50  THEN temp ELSE NULL END,
                CASE WHEN humi BETWEEN 0   AND 100 THEN humi ELSE NULL END,
                CASE WHEN wind BETWEEN 0   AND 50  THEN wind ELSE NULL END,
                CASE WHEN rain < 0 THEN 0 ELSE rain END,
                pm10, pm25, air_grade,
                TO_TIMESTAMP(base_date || base_time, 'YYYYMMDDHH24MI'),
                pty
            FROM bronze.environment_raw
            WHERE loaded_at > COALESCE(
                (SELECT MAX(created_at) FROM silver.environment_cleaned), '1970-01-01'
            )
            ON CONFLICT (dist_name, measured_at) DO NOTHING
        """
        )
        silver_count = cur.rowcount
    conn.commit()
    log_pipeline(
        conn, "silver", "environment", "SUCCESS", silver_count, time.time() - t
    )
    logger.info(f"[환경-실버] {silver_count}건 정제 완료")

    # ── Gold: 시간별/일별 집계 ───────────────────────────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.environment_hourly
                (dist_name, measured_at, avg_temp, max_temp, min_temp,
                 avg_humi, avg_wind, total_rain, avg_pm10, avg_pm25, air_grade, pty)
            SELECT
                dist_name,
                DATE_TRUNC('hour', measured_at),
                ROUND(AVG(temp)::NUMERIC, 1), ROUND(MAX(temp)::NUMERIC, 1), ROUND(MIN(temp)::NUMERIC, 1),
                ROUND(AVG(humi)::NUMERIC, 1),
                ROUND(AVG(wind)::NUMERIC, 1),
                ROUND(SUM(rain)::NUMERIC, 1),
                ROUND(AVG(pm10)::NUMERIC, 1),
                ROUND(AVG(pm25)::NUMERIC, 1),
                MODE() WITHIN GROUP (ORDER BY air_grade),
                MODE() WITHIN GROUP (ORDER BY pty)
            FROM silver.environment_cleaned
            WHERE measured_at >= NOW() - INTERVAL '25 hours'
            GROUP BY dist_name, DATE_TRUNC('hour', measured_at)
            ON CONFLICT (dist_name, measured_at) DO UPDATE SET
                avg_temp   = EXCLUDED.avg_temp,  max_temp   = EXCLUDED.max_temp,
                min_temp   = EXCLUDED.min_temp,  avg_humi   = EXCLUDED.avg_humi,
                avg_wind   = EXCLUDED.avg_wind,  total_rain = EXCLUDED.total_rain,
                avg_pm10   = EXCLUDED.avg_pm10,  avg_pm25   = EXCLUDED.avg_pm25,
                air_grade  = EXCLUDED.air_grade, pty        = EXCLUDED.pty
        """
        )
        hourly = cur.rowcount

        cur.execute(
            """
            INSERT INTO gold.environment_daily
                (dist_name, measured_date, avg_temp, max_temp, min_temp,
                 avg_humi, total_rain, avg_pm10, avg_pm25)
            SELECT
                dist_name, measured_at::DATE,
                ROUND(AVG(temp)::NUMERIC, 1), ROUND(MAX(temp)::NUMERIC, 1), ROUND(MIN(temp)::NUMERIC, 1),
                ROUND(AVG(humi)::NUMERIC, 1),
                ROUND(SUM(rain)::NUMERIC, 1),
                ROUND(AVG(pm10)::NUMERIC, 1),
                ROUND(AVG(pm25)::NUMERIC, 1)
            FROM silver.environment_cleaned
            WHERE measured_at::DATE >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY dist_name, measured_at::DATE
            ON CONFLICT (dist_name, measured_date) DO UPDATE SET
                avg_temp   = EXCLUDED.avg_temp,  max_temp   = EXCLUDED.max_temp,
                min_temp   = EXCLUDED.min_temp,  avg_humi   = EXCLUDED.avg_humi,
                total_rain = EXCLUDED.total_rain,
                avg_pm10   = EXCLUDED.avg_pm10,  avg_pm25   = EXCLUDED.avg_pm25
        """
        )
        daily = cur.rowcount
    conn.commit()
    log_pipeline(conn, "gold", "environment", "SUCCESS", silver_count, time.time() - t)
    logger.info(f"[환경-골드] 시간별 {hourly}건 / 일별 {daily}건 집계 완료")
    logger.info(f"[환경] 파이프라인 완료 (총 {round(time.time() - start, 2)}초)")


# ═══════════════════════════════════════════
# 메인 진입점
# ═══════════════════════════════════════════


def main_shelter():
    logger.info("=" * 55)
    logger.info("  [쉼터] 파이프라인 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    missing = [
        k
        for k in [
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "HOT_SHELTER_API",
            "COLD_SHELTER_API",
        ]
        if not os.getenv(k)
    ]
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
        run_heat_shelter(conn)
    except Exception as e:
        logger.error(f"[무더위] 오류: {e}")
        conn.rollback()

    try:
        run_cold_shelter(conn)
    except Exception as e:
        logger.error(f"[한파] 오류: {e}")
        conn.rollback()

    try:
        purge_old_bronze(conn)
    except Exception as e:
        logger.warning(f"[정리] bronze 정리 중 오류 (무시): {e}")

    conn.close()
    logger.info("=" * 55)
    logger.info("  [쉼터] 파이프라인 완료")
    logger.info("=" * 55)


def main_environment():
    logger.info("=" * 55)
    logger.info("  [환경] 파이프라인 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    missing = [
        k
        for k in [
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "WEATHER_API_KEY",
            "AIR_API_KEY",
        ]
        if not os.getenv(k)
    ]
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

    logger.info("=" * 55)
    logger.info("  [환경] 파이프라인 완료")
    logger.info("=" * 55)


# ═══════════════════════════════════════════
# ④ 내일 날씨 단기예보 파이프라인
# 매일 KST 07:00 실행 / 기준시간 05:00
# Bronze → Silver → Gold
# ═══════════════════════════════════════════


def run_weather_forecast(conn):
    logger.info("═" * 55)
    logger.info("  [예보] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    now = datetime.now()
    base_date = now.strftime("%Y%m%d")
    base_time = "0500"
    tomorrow_date = (now + timedelta(days=1)).strftime("%Y%m%d")
    logger.info(
        f"[예보] 기준: {base_date} {base_time} → 내일({tomorrow_date}) 24시간 예보 수집"
    )

    FORECAST_URL = (
        "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    )
    all_raw, failed = [], []

    for dist in SEOUL_DISTRICTS:
        params = {
            "serviceKey": WEATHER_KEY,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": dist["nx"],
            "ny": dist["ny"],
            "numOfRows": 1000,
        }
        for attempt in range(3):
            try:
                res = requests.get(FORECAST_URL, params=params, timeout=10)
                res.raise_for_status()
                items = res.json()["response"]["body"]["items"]["item"]
                tomorrow_items = [i for i in items if i["fcstDate"] == tomorrow_date]
                all_raw.append({"dist_name": dist["name"], "items": tomorrow_items})
                logger.info(f"[예보] {dist['name']}: {len(tomorrow_items)}건 수신")
                time.sleep(0.05)
                break
            except Exception as e:
                if attempt < 2:
                    logger.warning(f"[예보] {dist['name']} 재시도 {attempt+1}/3: {e}")
                    time.sleep(2)
                else:
                    logger.error(f"[예보] {dist['name']} 최종 실패: {e}")
                    failed.append(dist["name"])

    if not all_raw:
        log_pipeline(
            conn,
            "bronze",
            "weather_forecast",
            "FAIL",
            0,
            time.time() - start,
            "수집 데이터 없음",
        )
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO bronze.weather_forecast_raw (raw_data) VALUES %s",
            [(json.dumps(r, ensure_ascii=False),) for r in all_raw],
        )
    conn.commit()
    total_items = sum(len(r["items"]) for r in all_raw)
    status = "PARTIAL" if failed else "SUCCESS"
    if failed:
        logger.warning(f"[예보-브론즈] 누락 구: {failed}")
    log_pipeline(
        conn, "bronze", "weather_forecast", status, total_items, time.time() - start
    )
    logger.info(f"[예보-브론즈] {len(all_raw)}개 구 / {total_items}건 원본 저장 완료")

    # ── Silver ───────────────────────────────────────────────────
    t = time.time()
    silver_values = []
    for r in all_raw:
        dist_name = r["dist_name"]
        hourly = {}
        for item in r["items"]:
            ft = item["fcstTime"]
            if ft not in hourly:
                hourly[ft] = {}
            hourly[ft][item["category"]] = item["fcstValue"]
        for ft in sorted(hourly.keys()):
            info = hourly[ft]
            # PTY: 0=없음 1=비 2=비/눈 3=눈 4=소나기
            pty_val = clean_int(info.get("PTY"))
            # SNO: "적설없음" 또는 수치(cm)
            sno_raw = info.get("SNO", "적설없음")
            snow_val = (
                None
                if str(sno_raw).strip() in ("적설없음", "", "null")
                else clean_str(sno_raw, 20)
            )
            silver_values.append(
                (
                    dist_name,
                    tomorrow_date,
                    ft,
                    clean_float(info.get("TMP")),
                    clean_float(info.get("REH")),
                    clean_float(info.get("POP")),
                    clean_str(info.get("PCP"), 20),
                    snow_val,
                    pty_val,
                )
            )

    if not silver_values:
        log_pipeline(
            conn,
            "silver",
            "weather_forecast",
            "FAIL",
            0,
            time.time() - t,
            "정제 데이터 없음",
        )
        return

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO silver.weather_forecast_cleaned
                (dist_name, fcst_date, fcst_time, temp, humi, pop, rain, snow, pty)
            VALUES %s
            ON CONFLICT (dist_name, fcst_date, fcst_time) DO UPDATE SET
                temp = EXCLUDED.temp,
                humi = EXCLUDED.humi,
                pop  = EXCLUDED.pop,
                rain = EXCLUDED.rain,
                snow = EXCLUDED.snow,
                pty  = EXCLUDED.pty
        """,
            silver_values,
        )
    conn.commit()
    log_pipeline(
        conn,
        "silver",
        "weather_forecast",
        "SUCCESS",
        len(silver_values),
        time.time() - t,
    )
    logger.info(f"[예보-실버] {len(silver_values)}건 정제 완료")

    # ── Gold ─────────────────────────────────────────────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.weather_forecast_daily
                (dist_name, fcst_date, fcst_time, temp, humi, pop, rain, snow, pty, updated_at)
            SELECT dist_name, fcst_date, fcst_time, temp, humi, pop, rain, snow, pty, NOW()
            FROM silver.weather_forecast_cleaned
            WHERE fcst_date = %s
            ON CONFLICT (dist_name, fcst_date, fcst_time) DO UPDATE SET
                temp       = EXCLUDED.temp,
                humi       = EXCLUDED.humi,
                pop        = EXCLUDED.pop,
                rain       = EXCLUDED.rain,
                snow       = EXCLUDED.snow,
                pty        = EXCLUDED.pty,
                updated_at = EXCLUDED.updated_at
        """,
            (tomorrow_date,),
        )
        gold_count = cur.rowcount
    conn.commit()
    log_pipeline(
        conn, "gold", "weather_forecast", "SUCCESS", gold_count, time.time() - t
    )
    logger.info(f"[예보-골드] {gold_count}건 반영 완료")
    logger.info(f"[예보] 파이프라인 완료 (총 {round(time.time() - start, 2)}초)")


def main_forecast():
    logger.info("=" * 55)
    logger.info("  [예보] 파이프라인 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    missing = [
        k
        for k in [
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "WEATHER_API_KEY",
        ]
        if not os.getenv(k)
    ]
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
        run_weather_forecast(conn)
    except Exception as e:
        logger.error(f"[예보] 오류: {e}")
        conn.rollback()
        log_pipeline(conn, "pipeline", "weather_forecast", "FAIL", 0, 0, str(e))
    finally:
        conn.close()

    logger.info("=" * 55)
    logger.info("  [예보] 파이프라인 완료")
    logger.info("=" * 55)


# ═══════════════════════════════════════════
# Azure Functions Timer Triggers
# shelter_timer  : 매일 KST 06:00 (UTC 21:00)
# env_timer      : 매시간 15분
# forecast_timer : 매일 KST 07:00 (UTC 22:00)
# ═══════════════════════════════════════════


@app.timer_trigger(
    schedule="0 0 21 * * *", arg_name="shelter_timer", run_on_startup=False
)
def shelter_timer(shelter_timer: func.TimerRequest) -> None:
    main_shelter()


@app.timer_trigger(schedule="0 15 * * * *", arg_name="env_timer", run_on_startup=False)
def env_timer(env_timer: func.TimerRequest) -> None:
    main_environment()


@app.timer_trigger(
    schedule="0 0 22 * * *", arg_name="forecast_timer", run_on_startup=False
)
def forecast_timer(forecast_timer: func.TimerRequest) -> None:
    main_forecast()


# ═══════════════════════════════════════════
# 로컬 실행
# python function_app.py           → 전체
# python function_app.py shelter   → 쉼터만
# python function_app.py env       → 환경만
# python function_app.py forecast  → 예보만
# ═══════════════════════════════════════════

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"
    if target in ("shelter", "all"):
        main_shelter()
    if target in ("env", "all"):
        main_environment()
    if target in ("forecast", "all"):
        main_forecast()
