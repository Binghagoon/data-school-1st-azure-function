from datetime import datetime, timezone

_timestamps: list[str] = []


def append_timestamp() -> str:
    timestamp = datetime.now(timezone.utc).isoformat()
    _timestamps.append(timestamp)
    return timestamp


def list_timestamps() -> list[str]:
    return list(_timestamps)
