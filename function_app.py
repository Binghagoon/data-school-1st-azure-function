import azure.functions as func
import logging
import sys
from dotenv import load_dotenv

from blueprint.bronze import bp as bronze_bp
from blueprint.db_health import bp as db_health_bp
from blueprint.disasters import bp as disasters_bp
from blueprint.main import bp as main_bp
from blueprint.timestamps import bp as timestamps_bp
from service.environment_sync import main_environment
from service.forecast_sync import main_forecast
from service.hello_service import print_hello
from service.shelter_sync import main_shelter

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

app.register_blueprint(main_bp)
app.register_blueprint(db_health_bp)
app.register_blueprint(disasters_bp)
app.register_blueprint(timestamps_bp)
app.register_blueprint(bronze_bp)

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


# ═══════════════════════════════════════════
# Azure Functions Timer Triggers
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
#
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
