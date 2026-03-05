from datetime import datetime, timezone

_timestamps: list[str] = []
start_time = datetime.now(timezone.utc).isoformat()


def append_timestamp(data: dict[str, str] | None = None) -> str:
    timestamp = datetime.now(timezone.utc).isoformat()
    if data and data.get("timestamp"):
        timestamp = data["timestamp"]
    _timestamps.append(timestamp)
    return timestamp


def list_timestamps() -> dict[str, str | list[str]]:
    return {
        "start_time": start_time,
        "timestamps": list(_timestamps),
    }
