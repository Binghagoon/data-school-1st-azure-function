import json
from datetime import datetime, timedelta, timezone

import azure.functions as func

from service.shelter_sync import get_shelters

bp = func.Blueprint()
_count = 0


@bp.function_name(name="ApiRoot")
@bp.route(route="api", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_root(req: func.HttpRequest) -> func.HttpResponse:
    body = {"message": "API is running"}
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiNowTime")
@bp.route(route="api/now-time", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_now_time(req: func.HttpRequest) -> func.HttpResponse:
    now_utc = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    now_kst = now_utc.astimezone(kst)
    body = {
        "now_time_utc": now_utc.isoformat(),
        "now_time_kst": now_kst.isoformat(),
        "utc": {
            "year": now_utc.year,
            "month": now_utc.month,
            "day": now_utc.day,
            "hour": now_utc.hour,
            "minute": now_utc.minute,
            "second": now_utc.second,
            "microsecond": now_utc.microsecond,
            "offset": "+00:00",
        },
        "kst": {
            "year": now_kst.year,
            "month": now_kst.month,
            "day": now_kst.day,
            "hour": now_kst.hour,
            "minute": now_kst.minute,
            "second": now_kst.second,
            "microsecond": now_kst.microsecond,
            "offset": "+09:00",
        },
    }
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiCount")
@bp.route(route="api/count", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_count(req: func.HttpRequest) -> func.HttpResponse:
    global _count
    _count += 1
    body = {"count": _count}
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiShelters")
@bp.route(route="api/shelters", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_shelters(req: func.HttpRequest) -> func.HttpResponse:
    raw_limit = req.params.get("limit")
    limit: int | None = None
    if raw_limit is not None:
        try:
            limit = int(raw_limit)
        except ValueError:
            return func.HttpResponse("limit must be an integer", status_code=400)
        if limit <= 0:
            return func.HttpResponse("limit must be greater than 0", status_code=400)

    try:
        shelters = get_shelters(limit=limit)
        return func.HttpResponse(
            json.dumps(shelters, ensure_ascii=False),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as exc:
        return func.HttpResponse(f"Failed to fetch shelters: {exc}", status_code=500)
