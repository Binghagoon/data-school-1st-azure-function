import azure.functions as func
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

from blueprint.db_health import bp as db_health_bp
from blueprint.disasters import bp as disasters_bp
from blueprint.main import bp as main_bp
from blueprint.timestamps import bp as timestamps_bp
from service.data_sync_common import (
    SEOUL_DISTRICTS,
    WEATHER_KEY,
    clean_float,
    clean_int,
    clean_str,
    get_db_conn,
    logger,
)
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
    schedule="*/1 * * * * *",  # every second
    arg_name="hello_timer",
    run_on_startup=False,
)
def hello_timer(hello_timer: func.TimerRequest) -> None:
    print_hello()


def parse_air_value(val):
    if val is None:
        return None
    value = str(val).strip()
    if value in ("", "점검중", "null", "NULL", "-", "N/A"):
        return None
    try:
        return float(value)
    except ValueError:
        return None


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
        cur.executemany(
            "INSERT INTO bronze.weather_forecast_raw (raw_data) VALUES (%s)",
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
        cur.executemany(
            """
            INSERT INTO silver.weather_forecast_cleaned
                (dist_name, fcst_date, fcst_time, temp, humi, pop, rain, snow, pty)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
