import json
import os
import time
from datetime import datetime
from typing import TypedDict

from psycopg2.extras import execute_values

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
    log_pipeline,
    logger,
    purge_old_bronze,
)


class ShelterSchema(TypedDict):
    type: str
    name: str
    addr: str | None
    capacity: float | int | None
    lon: float
    lat: float


# ═══════════════════════════════════════════
# ① 무더위 쉼터 파이프라인
# ═══════════════════════════════════════════


def parse_heat_row(r: dict) -> tuple:
    return (
        clean_int(r.get("YEAR")),
        clean_bpchar(r.get("AREA_CD"), 10),
        clean_str(r.get("FACILITY_TYPE1"), 50),
        clean_str(r.get("FACILITY_TYPE2"), 50),
        clean_str(r.get("R_AREA_NM"), 100),  # name
        clean_str(r.get("R_DETL_ADD"), 200),  # addr
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
        except Exception as exc:
            logger.warning(
                f"[무더위-실버] 행 전처리 실패: {r.get('R_AREA_NM')} / {exc}"
            )
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

    # ── Gold: shelter_summary 반영 ───────────────────────────────
    t = time.time()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gold.shelter_summary
                (shelter_type, shelter_name, road_addr, capacity, lon, lat, updated_at)
            SELECT DISTINCT ON (lon, lat)
                'heat', shelter_name, road_addr, capacity, lon, lat, updated_at
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


def parse_cold_row(r: dict) -> tuple:
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
        except Exception as exc:
            logger.warning(
                f"[한파-실버] 행 전처리 실패: {r.get('RESTAREA_NM')} / {exc}"
            )
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
            SELECT DISTINCT ON (lon, lat)
                'cold', shelter_name, road_addr, capacity::FLOAT8, lon, lat, updated_at
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
    except Exception as exc:
        logger.error(f"[DB] 연결 실패: {exc}")
        return

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

    try:
        purge_old_bronze(conn)
    except Exception as exc:
        logger.warning(f"[정리] bronze 정리 중 오류 (무시): {exc}")

    conn.close()
    logger.info("=" * 55)
    logger.info("  [쉼터] 파이프라인 완료")
    logger.info("=" * 55)


def get_shelters(limit: int | None = None) -> list[ShelterSchema]:
    """Return shelter summaries for map/API responses."""
    conn = get_db_conn()
    with conn.cursor() as cur:
        if limit is None:
            cur.execute(
                """
                SELECT shelter_type, shelter_name, road_addr, capacity, lon, lat
                FROM gold.shelter_summary
                WHERE lon IS NOT NULL AND lat IS NOT NULL
                ORDER BY updated_at DESC
                """
            )
        else:
            cur.execute(
                """
                SELECT shelter_type, shelter_name, road_addr, capacity, lon, lat
                FROM gold.shelter_summary
                WHERE lon IS NOT NULL AND lat IS NOT NULL
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
        shelters = [
            {
                "type": r[0],
                "name": r[1],
                "addr": r[2],
                "capacity": r[3],
                "lon": float(r[4]),
                "lat": float(r[5]),
            }
            for r in cur.fetchall()
        ]
    conn.close()
    return shelters
