import os
from datetime import datetime

from service.data_sync_common import (
    COLD_API_URL,
    HOT_API_URL,
    clean_bpchar,
    clean_float,
    clean_int,
    clean_numeric,
    clean_str,
    fetch_api,
    get_db_conn,
    logger,
)


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
        END $$;""",
    ]
    with conn.cursor() as cur:
        for sql in sqls:
            cur.execute(sql)
    conn.commit()
    logger.info("[무더위] 초기 컬럼 및 제약 조건 설정 완료")


def upsert_heat_shelters(conn, rows: list[dict]):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.heat_shelter WHERE is_deleted = false
        """
        )
        before = {(r[0].strip(), r[1]): r[2:] for r in cur.fetchall()}

    values = []
    skipped = 0
    for r in rows:
        try:
            use_prnb = clean_float(r.get("USE_PRNB"))
            if not use_prnb or use_prnb <= 0:
                skipped += 1
                continue
            values.append(parse_heat_row(r))
        except Exception as exc:
            logger.warning(f"[무더위] 행 전처리 실패: {r.get('R_AREA_NM')} / {exc}")

    logger.info(f"[무더위] 이용가능인원 0/NULL 제외: {skipped}건")
    if not values:
        logger.warning("[무더위] 적재할 데이터 없음")
        return

    sql = """
        INSERT INTO public.heat_shelter (
            facility_year, area_cd, facility_type1, facility_type2, shelter_name,
            road_addr, lot_addr, facility_area, capacity, remark,
            lon, lat, coord_x, coord_y, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
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
        cur.executemany(sql, values)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT area_cd, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.heat_shelter WHERE is_deleted = false
        """
        )
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
                    logger.info(f"  - {field}: '{before[key][i]}' -> '{after_val[i]}'")
            changed_count += 1

    logger.info(f"[무더위] 신규: {new_count}건 | 변경: {changed_count}건 | 전체 UPSERT: {len(values)}건")


def soft_delete_heat(conn, current_keys: list[tuple]):
    with conn.cursor() as cur:
        cur.execute("SELECT area_cd, shelter_name FROM public.heat_shelter WHERE is_deleted = false")
        db_keys = set(tuple(r) for r in cur.fetchall())

    api_keys = set((clean_bpchar(k[0], 10), k[1]) for k in current_keys)
    deleted_keys = db_keys - api_keys
    if not deleted_keys:
        logger.info("[무더위] 삭제된 쉼터 없음")
        return
    with conn.cursor() as cur:
        for key in deleted_keys:
            logger.info(f"[무더위][DELETE] {key[1]} ({key[0].strip()})")
            cur.execute(
                "UPDATE public.heat_shelter SET is_deleted = true "
                "WHERE area_cd = %s AND shelter_name = %s",
                key,
            )
    conn.commit()
    logger.info(f"[무더위] 총 {len(deleted_keys)}건 소프트 삭제 완료")


def run_heat_shelter(conn):
    logger.info("-------------------------------------------")
    logger.info("  [무더위 쉼터] 갱신 시작")
    logger.info("-------------------------------------------")
    setup_heat_shelter(conn)
    rows = fetch_api(HOT_API_URL, "TbGtnHwcwP", "무더위")
    if not rows:
        logger.warning("[무더위] 수신된 데이터 없음, 건너뜀")
        return
    upsert_heat_shelters(conn, rows)
    soft_delete_heat(conn, [(r.get("AREA_CD"), r.get("R_AREA_NM")) for r in rows])
    logger.info("[무더위 쉼터] 갱신 완료")


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
        END $$;""",
    ]
    with conn.cursor() as cur:
        for sql in sqls:
            cur.execute(sql)
    conn.commit()
    logger.info("[한파] 초기 컬럼 및 제약 조건 설정 완료")


def upsert_cold_shelters(conn, rows: list[dict]):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT no, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.cold_shelter WHERE is_deleted = false
        """
        )
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
        except Exception as exc:
            logger.warning(f"[한파] 행 전처리 실패: {r.get('RESTAREA_NM')} / {exc}")

    logger.info(f"[한파] 사용여부 N 제외: {filtered}건 | 이용가능인원 0/NULL 제외: {skipped}건")
    if not values:
        logger.warning("[한파] 적재할 데이터 없음")
        return

    sql = """
        INSERT INTO public.cold_shelter (
            no, facility_type1, facility_type2, shelter_name,
            road_addr, lot_addr, facility_area, capacity, remark,
            lon, lat, coord_x, coord_y, use_yn, use_type, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
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
        cur.executemany(sql, values)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT no, shelter_name, road_addr, facility_area, capacity, remark
            FROM public.cold_shelter WHERE is_deleted = false
        """
        )
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
                    logger.info(f"  - {field}: '{before[key][i]}' -> '{after_val[i]}'")
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
                "WHERE no = %s AND shelter_name = %s",
                key,
            )
    conn.commit()
    logger.info(f"[한파] 총 {len(deleted_keys)}건 소프트 삭제 완료")


def run_cold_shelter(conn):
    logger.info("-------------------------------------------")
    logger.info("  [한파 쉼터] 갱신 시작")
    logger.info("-------------------------------------------")
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
        and clean_int(r.get("UTZTN_PSBLTY_NOPE"))
        and clean_int(r.get("UTZTN_PSBLTY_NOPE")) > 0
    ]
    soft_delete_cold(conn, current_keys)
    logger.info("[한파 쉼터] 갱신 완료")


def main_shelter():
    logger.info("=" * 50)
    logger.info("  [쉼터] 데이터 갱신 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    required = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "HOT_SHELTER_API",
        "COLD_SHELTER_API",
    ]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"필수 환경변수 누락: {missing}")
        return

    try:
        conn = get_db_conn()
        logger.info("[DB] 연결 성공")
    except Exception as exc:
        logger.error(f"[DB] 연결 실패: {exc}")
        return

    try:
        try:
            run_heat_shelter(conn)
        except Exception as exc:
            logger.error(f"[무더위] 오류: {exc}")
            conn.rollback()

        try:
            run_cold_shelter(conn)
        except Exception as exc:
            logger.error(f"[한파] 오류: {exc}")
            conn.rollback()
    finally:
        conn.close()
        logger.info("=" * 50)
        logger.info("  [쉼터] 데이터 갱신 완료")
        logger.info("=" * 50)
