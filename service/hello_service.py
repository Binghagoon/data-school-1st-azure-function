import logging
from datetime import datetime, timezone


def print_hello() -> None:
    now_utc = datetime.now(timezone.utc).isoformat()
    logging.info("hello (%s)", now_utc)
