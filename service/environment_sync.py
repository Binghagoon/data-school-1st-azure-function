import os
import time
from datetime import datetime, timedelta

import requests

from service.data_sync_common import (
    AIR_API_URL,
    SEOUL_DISTRICTS,
    WEATHER_KEY,
    WEATHER_URL,
    get_db_conn,
    logger,
)


def get_base_time_and_date():
    now = datetime.now()
    target = now - timedelta(hours=1) if now.minute < 40 else now
    return target.strftime("%Y%m%d"), target.strftime("%H00")


def fetch_air_quality() -> dict:
    air_map = {}
    try:
        res = requests.get(AIR_API_URL, timeout=10)
        rows = res.json().get("ListAirQualityByDistrictService", {}).get("row", [])
        for row in rows:
            name = row.get("MSRSTN_NM", "")
            pm10 = float(row["PM"]) if str(row.get("PM", "")).replace(".", "", 1).isdigit() else 0.0
            pm25 = float(row["FPM"]) if str(row.get("FPM", "")).replace(".", "", 1).isdigit() else 0.0
            grade = row.get("CAI_GRD") or "정보없음"
            air_map[name] = {"pm10": pm10, "pm25": pm25, "grade": grade}
        logger.info(f"[환경] 미세먼지 {len(air_map)}개 구 수신 완료")
    except Exception as exc:
        logger.error(f"[환경] 미세먼지 수집 실패: {exc}")
    return air_map


def fetch_weather_and_air() -> list[tuple]:
    base_date, base_time = get_base_time_and_date()
    air_map = fetch_air_quality()
    results = []

    logger.info(f"[환경] 기상 수집 시작 (기준: {base_date} {base_time})")

    for dist in SEOUL_DISTRICTS:
        params = {
            "serviceKey": WEATHER_KEY,
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": dist["nx"],
            "ny": dist["ny"],
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                res = requests.get(WEATHER_URL, params=params, timeout=10)
                items = res.json()["response"]["body"]["items"]["item"]
                w_data = {i["category"]: i["obsrValue"] for i in items}

                temp = float(w_data.get("T1H", 0))
                humi = float(w_data.get("REH", 0))
                wind = float(w_data.get("WSD", 0))
                rain = float(w_data.get("RN1", 0))
                air = air_map.get(dist["name"], {"pm10": 0.0, "pm25": 0.0, "grade": "데이터없음"})

                results.append(
                    (
                        dist["name"],
                        temp,
                        humi,
                        wind,
                        rain,
                        air["pm10"],
                        air["pm25"],
                        air["grade"],
                        datetime.now(),
                    )
                )
                logger.info(
                    f"[환경] {dist['name']} | 기온:{temp} 습도:{humi} "
                    f"풍속:{wind} 강수:{rain} PM10:{air['pm10']} PM2.5:{air['pm25']}"
                )
                time.sleep(0.05)
                break
            except Exception as exc:
                if attempt < max_retries - 1:
                    logger.warning(f"[환경] {dist['name']} 재시도 {attempt + 1}/{max_retries}: {exc}")
                    time.sleep(1)
                else:
                    logger.error(f"[환경] {dist['name']} 수집 실패 (최대 재시도 초과): {exc}")

    return results


def save_environment(conn, data_list: list[tuple]):
    if not data_list:
        logger.warning("[환경] 저장할 데이터 없음")
        return
    sql = """
        INSERT INTO public.seoul_environment
            (dist_name, temp, humi, wind, rain, pm10, pm25, air_grade, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql, data_list)
    conn.commit()
    logger.info(f"[환경] seoul_environment 저장 완료: {len(data_list)}건")


def run_environment(conn):
    logger.info("-------------------------------------------")
    logger.info("  [기후 + 미세먼지] 수집 시작")
    logger.info("-------------------------------------------")
    data = fetch_weather_and_air()
    save_environment(conn, data)
    logger.info("[기후 + 미세먼지] 수집 완료")


def main_environment():
    logger.info("=" * 50)
    logger.info("  [환경] 데이터 수집 시작")
    logger.info(f"  실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 50)

    required = [
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "WEATHER_API_KEY",
        "AIR_API_KEY",
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
        run_environment(conn)
    except Exception as exc:
        logger.error(f"[환경] 오류: {exc}")
        conn.rollback()
    finally:
        conn.close()
        logger.info("=" * 50)
        logger.info("  [환경] 데이터 수집 완료")
        logger.info("=" * 50)
