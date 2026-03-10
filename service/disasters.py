from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

import requests
import json

from db.postgres_connector import get_connection
from db.sql_debug import render_sql_for_log
from service.webhook import send_webhook


URL = "https://www.safekorea.go.kr/idsiSFK/sfk/cs/sua/web/DisasterSmsList.do"
HEADERS = {"Content-Type": "application/json"}
KST = timezone(timedelta(hours=9))


def _validate_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("date must be in YYYY-MM-DD format") from exc
    return value


def _resolve_date_range(date: str | dict[str, str]) -> tuple[str, str]:
    if isinstance(date, str):
        day = _validate_date(date)
        return day, day

    if not isinstance(date, dict):
        raise TypeError("date must be a string or dict with start/end")

    start = date.get("start")
    end = date.get("end")

    if not start and not end:
        raise ValueError("date dict must include at least one of start/end")

    if not start:
        start = end
    if not end:
        end = start

    return _validate_date(start), _validate_date(end)


def _build_payload(
    start_date: str, end_date: str, area_id: str, page_size: int
) -> dict[str, Any]:
    return {
        "searchInfo": {
            "pageIndex": "1",
            "pageUnit": "10",
            "pageSize": page_size,
            "firstIndex": "1",
            "lastIndex": "1",
            "recordCountPerPage": "10",
            "searchBgnDe": start_date,
            "searchEndDe": end_date,
            "searchGb": "1",
            "searchWrd": "",
            "rcv_Area_Id": "",
            "dstr_se_Id": "",
            "c_ocrc_type": "",
            "sbLawArea1": area_id,
            "sbLawArea2": "",
            "sbLawArea3": "",
        }
    }


def get_disaster(
    date: str | dict[str, str],
    *,
    area_id: str = "1100000000",
    page_size: int = 10,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    start_date, end_date = _resolve_date_range(date)

    payload = _build_payload(start_date, end_date, area_id, page_size)
    response = requests.post(URL, json=payload, headers=HEADERS, timeout=timeout)
    response.raise_for_status()

    data = response.json()
    return data.get("disasterSmsList", [])


def get_disasters(date: str | dict[str, str]) -> list[dict[str, Any]]:
    """Backward-compatible wrapper."""
    return get_disaster(date)


def save_disasters(
    disasters: list[dict[str, Any]],
    override: bool = False,
) -> tuple[tuple["not saved", int], dict[str, Any]]:
    """Placeholder function to save disasters to a database or file."""
    with get_connection() as connection:
        with connection.cursor() as cursor:
            ids_query = cursor.execute(
                "SELECT md101_sn FROM disaster_messages ORDER BY md101_sn;"
            )
            ids = set(id for (id,) in ids_query.fetchall())
            not_saved = []
            saved_disasters = []
            for disaster in disasters:
                id = disaster.get("MD101_SN")
                if id in ids:
                    if not override:
                        print(f"Skipping existing disaster with MD101_SN={id}")
                        not_saved.append(id)
                        continue
                    else:
                        print(f"Overriding existing disaster with MD101_SN={id}")
                        query = "DELETE FROM disaster_messages WHERE md101_sn = %s"
                        cursor.execute(query, (id,))
                query = """
                    INSERT INTO disaster_messages (md101_sn, dsstr_se_id, dsstr_se_nm, msg_se_cd, msg_cn, rcv_area_id, rcv_area_nm, emrgncy_step_id, emrgncy_step_nm, delete_at, register_id, updusr_id, rnum, creat_dt, regist_dt, modf_dt)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                params = (
                    disaster.get("MD101_SN"),
                    disaster.get("DSSTR_SE_ID"),
                    disaster.get("DSSTR_SE_NM"),
                    disaster.get("MSG_SE_CD"),
                    disaster.get("MSG_CN"),
                    disaster.get("RCV_AREA_ID"),
                    disaster.get("RCV_AREA_NM"),
                    disaster.get("EMRGNCY_STEP_ID"),
                    disaster.get("EMRGNCY_STEP_NM"),
                    disaster.get("DELETE_AT", "N"),
                    disaster.get("REGISTER_ID"),
                    disaster.get("UPDUSR_ID"),
                    disaster.get("RNUM"),
                    disaster.get("CREAT_DT"),
                    disaster.get("REGIST_DT"),
                    disaster.get("MODF_DT"),
                )
                print(render_sql_for_log(query, params))
                cursor.execute(query, params)
                saved_disasters.append(disaster)
        connection.commit()
    # Implement actual saving logic here (e.g., write to a database or file)
    print(f"Saving {len(saved_disasters)} disasters...")
    return ("not saved", len(not_saved)), saved_disasters


def process_disaster_events(
    date: str | dict[str, str] | None = None,
    override: bool = False,
    not_alert_to_teams: bool = False,
) -> None:
    if date is None:
        date = datetime.now(KST).strftime("%Y-%m-%d")
    elif isinstance(date, str):
        _validate_date(date)
    elif isinstance(date, dict):
        _validate_date(date.get("start"))
        _validate_date(date.get("end", datetime.now(KST).strftime("%Y-%m-%d")))

    disasters = get_disaster(date)
    (_, not_saved_count), saved_disasters = save_disasters(disasters, override=override)
    print(f"Not saved disasters: {not_saved_count} out of {len(disasters)}")
    print(f"Saved disasters: {len(saved_disasters)}")
    if not_alert_to_teams:
        print("Skipping Teams alert as not_alert_to_teams=True")
        return
    for disaster in saved_disasters:
        title = disaster.get("DSSTR_SE_NM", "재난 발생")
        facts = {
            "재난 유형": disaster.get("DSSTR_SE_NM", ""),
            "메시지 구분": disaster.get("MSG_SE_CD", ""),
            "수신 지역": disaster.get("RCV_AREA_NM", ""),
            "긴급 단계": disaster.get("EMRGNCY_STEP_NM", ""),
            "발생 시각": disaster.get("CREAT_DT", ""),
        }
        message = disaster.get("MSG_CN", "")
        send_webhook(
            type=disaster.get("MSG_SE_CD", ""),
            title=title,
            facts=facts,
            message=message,
        )


# Test code
if __name__ == "__main__":
    # text read from cli
    date_string = input("Enter a date (YYYY-MM-DD) or date range (start/end): ")
    _validate_date(date_string)  # Validate the input date format
    disasters = get_disaster(date_string)

    print(json.dumps(disasters, indent=2, ensure_ascii=False))
