import asyncio
import json
import os
import time
from datetime import datetime

import aiohttp
import requests

from service.data_sync_common import (
    AIR_API_URL,
    SEOUL_DISTRICTS,
    WEATHER_KEY,
    WEATHER_URL,
    get_db_conn,
    log_pipeline,
    logger,
    parse_air_value,
)
from psycopg2.extras import execute_values


# ───────────────────────────────────────────
# 기준 시간 계산 (분 < 15이면 1시간 전 사용)
# ───────────────────────────────────────────

def get_base_time_and_date():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H00")


# ───────────────────────────────────────────
# 미세먼지 수집
# ───────────────────────────────────────────

def fetch_air_quality() -> dict:
    """
    서울시 미세먼지 수집 → {구이름: {pm10, pm25, grade}}

    air_grade 판단 기준:
    - PM 값이 "점검중" 문자열  → grade = "점검중", pm10/pm25 = None
    - pm10, pm25 둘 다 None    → grade = "점검중"
    - 그 외                     → CAI_GRD 값 사용, 없으면 "정보없음"
    """
    air_map = {}
    try:
        res  = requests.get(AIR_API_URL, timeout=10)
        rows = res.json().get("ListAirQualityByDistrictService", {}).get("row", [])
        for row in rows:
            name   = row.get("MSRSTN_NM", "")
            pm10   = parse_air_value(row.get("PM"))
            pm25   = parse_air_value(row.get("FPM"))
            raw_pm = str(row.get("PM", "")).strip()

            if raw_pm == "점검중" or (pm10 is None and pm25 is None):
                grade = "점검중"
                logger.warning(f"[환경] {name} 측정소 점검중 → pm10/pm25 NULL 저장")
            else:
                grade = row.get("CAI_GRD") or "정보없음"

            air_map[name] = {"pm10": pm10, "pm25": pm25, "grade": grade}
        logger.info(f"[환경-브론즈] 미세먼지 {len(air_map)}개 구 수신")
    except Exception as exc:
        logger.error(f"[환경-브론즈] 미세먼지 수집 실패: {exc}")
    return air_map


# ───────────────────────────────────────────
# 기상 비동기 수집
# ───────────────────────────────────────────

async def fetch_single_district(session, dist, base_date, base_time, air_map):
    """
    개별 구 기상 데이터 비동기 수집 (실패 시 재시도 3회)
    - 이상값(기온/습도/풍속 범위 초과) 감지 시 스킵
    """
    params = {
        "serviceKey": WEATHER_KEY,
        "dataType":   "JSON",
        "base_date":  base_date,
        "base_time":  base_time,
        "nx":         dist["nx"],
        "ny":         dist["ny"],
    }
    for attempt in range(3):
        try:
            async with session.get(
                WEATHER_URL, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as res:
                if res.status != 200:
                    raise ValueError(f"HTTP {res.status}")
                data  = await res.json(content_type=None)
                items = (
                    data.get("response", {})
                    .get("body", {})
                    .get("items", {})
                    .get("item", [])
                )
                if not items:
                    raise ValueError("빈 응답")

                w_data = {i["category"]: i["obsrValue"] for i in items}
                temp   = float(w_data.get("T1H", 0))
                humi   = float(w_data.get("REH", 0))
                wind   = float(w_data.get("WSD", 0))
                rain   = float(w_data.get("RN1", 0))
                pty    = int(float(w_data.get("PTY", 0)))
                air    = air_map.get(
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
                    logger.warning(f"[검증] {dist['name']} 풍속 이상값: {wind}m/s → 스킵")
                    return None

                pty_label = {
                    0: "없음", 1: "비", 2: "비/눈", 3: "눈",
                    4: "소나기", 5: "빗방울", 6: "빗방울/눈날림", 7: "눈날림"
                }.get(pty, "없음")
                logger.info(
                    f"[환경] {dist['name']} | 기온:{temp} 습도:{humi} "
                    f"풍속:{wind} 강수:{rain} 강수형태:{pty_label} "
                    f"PM10:{air['pm10']} PM2.5:{air['pm25']} 등급:{air['grade']}"
                )
                return {
                    "dist_name": dist["name"],
                    "temp": temp, "humi": humi, "wind": wind, "rain": rain,
                    "pty": pty,
                    "pm10": air["pm10"], "pm25": air["pm25"], "grade": air["grade"],
                    "base_date": base_date, "base_time": base_time,
                }
        except Exception as exc:
            if attempt < 2:
                logger.warning(f"[환경] {dist['name']} 재시도 {attempt+1}/3: {exc}")
                await asyncio.sleep(1)
            else:
                logger.error(f"[환경] {dist['name']} 최종 실패 (3회 초과): {exc}")
    return None


async def fetch_all_districts_async():
    """25개 구 전체를 비동기 병렬 수집"""
    base_date, base_time = get_base_time_and_date()
    air_map = fetch_air_quality()
    logger.info(f"[환경-브론즈] 기상 수집 시작 (기준: {base_date} {base_time})")
    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(*[
            fetch_single_district(session, d, base_date, base_time, air_map)
            for d in SEOUL_DISTRICTS
        ])
    return [r for r in results if r is not None]


# ───────────────────────────────────────────
# Bronze → Silver → Gold 파이프라인
# ───────────────────────────────────────────

def run_environment(conn):
    logger.info("═" * 55)
    logger.info("  [환경] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    # ── Bronze: 원본 저장 ────────────────────────────────────────
    raw_list = asyncio.run(fetch_all_districts_async())
    if not raw_list:
        log_pipeline(conn, "bronze", "environment", "FAIL", 0, time.time() - start, "수집 데이터 없음")
        logger.warning("[환경] 수집 데이터 없음 → 파이프라인 중단")
        return

    bronze_values = [
        (
            r["dist_name"], r["temp"], r["humi"], r["wind"], r["rain"],
            r["pm10"], r["pm25"], r["grade"], r["base_date"], r["base_time"],
            r["pty"],
        )
        for r in raw_list
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bronze.environment_raw
                (dist_name, temp, humi, wind, rain, pm10, pm25, air_grade,
                 base_date, base_time, pty)
            VALUES %s
            """,
            bronze_values,
        )
    conn.commit()

    status = "PARTIAL" if len(raw_list) < len(SEOUL_DISTRICTS) else "SUCCESS"
    if status == "PARTIAL":
        missing = set(d["name"] for d in SEOUL_DISTRICTS) - set(r["dist_name"] for r in raw_list)
        logger.warning(f"[환경-브론즈] 누락 구: {missing}")
    log_pipeline(conn, "bronze", "environment", status, len(raw_list), time.time() - start)
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
    log_pipeline(conn, "silver", "environment", "SUCCESS", silver_count, time.time() - t)
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


def main_environment():
    logger.info("=" * 55)
    logger.info("  [환경] 파이프라인 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    missing = [
        k for k in [
            "POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER",
            "POSTGRES_PASSWORD", "WEATHER_API_KEY", "AIR_API_KEY",
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
        run_environment(conn)
    except Exception as exc:
        logger.error(f"[환경] 오류: {exc}")
        conn.rollback()
    finally:
        conn.close()

    logger.info("=" * 55)
    logger.info("  [환경] 파이프라인 완료")
    logger.info("=" * 55)