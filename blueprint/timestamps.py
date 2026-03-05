import json

import azure.functions as func

from service.timestamp_service import append_timestamp, list_timestamps

bp = func.Blueprint()


@bp.function_name(name="TimestampList")
@bp.route(route="timestamps", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def timestamp_list(req: func.HttpRequest) -> func.HttpResponse:
    body = json.dumps(list_timestamps())
    return func.HttpResponse(body, status_code=200, mimetype="application/json")


@bp.function_name(name="TimestampAppend")
@bp.route(route="timestamps/append", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def timestamp_append(req: func.HttpRequest) -> func.HttpResponse:
    timestamp = append_timestamp()
    body = json.dumps({"timestamp": timestamp, "timestamps": list_timestamps()})
    return func.HttpResponse(body, status_code=201, mimetype="application/json")
