import sys

import azure.functions as func

from blueprint.db_health import bp as db_health_bp
from blueprint.disasters import bp as disasters_bp
from blueprint.main import bp as main_bp
from service.environment_sync import main_environment
from service.shelter_sync import main_shelter

app = func.FunctionApp()

app.register_blueprint(main_bp)
app.register_blueprint(db_health_bp)
app.register_blueprint(disasters_bp)


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


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    if target in ("shelter", "all"):
        main_shelter()
    if target in ("env", "all"):
        main_environment()
