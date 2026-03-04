from datetime import datetime
import logging
import azure.functions as func

app = func.FunctionApp()


@app.function_name(name="minute_timer")
@app.schedule(
    schedule="*/1 * * * * *",  # Every 1 second
    arg_name="mytimer",
    run_on_startup=False,
    use_monitor=True,
)
def minute_timer(mytimer: func.TimerRequest) -> None:

    if mytimer.past_due:
        logging.warning("The timer is past due!")
    utc_timestamp = datetime.now(datetime.timezone.utc).isoformat()
    kst_timestamp = datetime.now(
        datetime.timezone(datetime.timedelta(hours=9))
    ).isoformat()

    logging.info(
        f"Timer trigger function ran at {utc_timestamp} (KST: {kst_timestamp})"
    )
