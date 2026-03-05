import logging
from datetime import datetime, timezone
from service.timestamp_service import append_timestamp


def print_hello() -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    logging.info("hello (%s)", now_utc)
    append_timestamp()
