import json
import os
import time
from datetime import datetime, timedelta

import requests
from psycopg2.extras import execute_values

from service.data_sync_common import (
    SEOUL_DISTRICTS,
    WEATHER_KEY,
    clean_float,
    clean_int,
    clean_str,
    get_db_conn,
    log_pipeline,
    logger,
)


def run_weather_forecast(conn):
    logger.info("═" * 55)
    logger.info("  [예보] Bronze → Silver → Gold 파이프라인 시작")
    logger.info("═" * 55)
    start = time.time()

    now           = datetime.now()
    base_date     = now.strftime("%Y%m%d")
    base_time     = "0500"
    tomorrow_date = (now + timedelta(days=1)).strftime("%Y%m%d")
    logger.info(f"[예보] 기준: {base_date} {base_time} → 내일({tomorrow_date}) 24시간 예보 수집")

    forecast_url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst"
    all_raw, failed = [], []

    for dist in SEOUL_DISTRICTS:
        params = {
            "serviceKey": WEATHER_KEY,
            "dataType":   "JSON",
            "base_date":  base_date,
            "base_time":  base_time,
            "nx":         dist["nx"],
            "ny":         dist["ny"],
            "numOfRows":  1000,
        }
        for attempt in range(3):
            try:
                res = requests.get(forecast_url, params=params, timeout=10)
                res.raise_for_status()
                items = res.json()["response"]["body"]["items"]["item"]
                tomorrow_items = [i for i in items if i["fcstDate"] == tomorrow_date]
                all_raw.append({"dist_name": dist["name"], "items": tomorrow_items})
                logger.info(f"[예보] {dist['name']}: {len(tomorrow_items)}건 수신")
                time.sleep(0.05)
                break
            except Exception as exc:
                if attempt < 2:
                    logger.warning(f"[예보] {dist['name']} 재시도 {attempt+1}/3: {exc}")
                    time.sleep(2)
                else:
                    logger.error(f"[예보] {dist['name']} 최종 실패: {exc}")
                    failed.append(dist["name"])

    if not all_raw:
        log_pipeline(conn, "bronze", "weather_forecast", "FAIL", 0, time.time() - start, "수집 데이터 없음")
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
    log_pipeline(conn, "bronze", "weather_forecast", status, total_items, time.time() - start)
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
            pty_val = clean_int(info.get("PTY"))
            sno_raw = info.get("SNO", "적설없음")
            snow_val = (
                None
                if str(sno_raw).strip() in ("적설없음", "", "null")
                else clean_str(sno_raw, 20)
            )
            silver_values.append((
                dist_name, tomorrow_date, ft,
                clean_float(info.get("TMP")),
                clean_float(info.get("REH")),
                clean_float(info.get("POP")),
                clean_str(info.get("PCP"), 20),
                snow_val,
                pty_val,
            ))

    if not silver_values:
        log_pipeline(conn, "silver", "weather_forecast", "FAIL", 0, time.time() - t, "정제 데이터 없음")
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
    log_pipeline(conn, "silver", "weather_forecast", "SUCCESS", len(silver_values), time.time() - t)
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
    log_pipeline(conn, "gold", "weather_forecast", "SUCCESS", gold_count, time.time() - t)
    logger.info(f"[예보-골드] {gold_count}건 반영 완료")
    logger.info(f"[예보] 파이프라인 완료 (총 {round(time.time() - start, 2)}초)")


def main_forecast():
    logger.info("=" * 55)
    logger.info("  [예보] 파이프라인 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 55)

    missing = [
        k for k in [
            "POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER",
            "POSTGRES_PASSWORD", "WEATHER_API_KEY",
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
        run_weather_forecast(conn)
    except Exception as exc:
        logger.error(f"[예보] 오류: {exc}")
        conn.rollback()
        log_pipeline(conn, "pipeline", "weather_forecast", "FAIL", 0, 0, str(exc))
    finally:
        conn.close()

    logger.info("=" * 55)
    logger.info("  [예보] 파이프라인 완료")
    logger.info("=" * 55)