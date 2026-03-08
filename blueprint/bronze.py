import json

import azure.functions as func

from service.bronze_api import (
    get_bronze_table_columns,
    get_bronze_table_rows,
    list_bronze_tables,
)

bp = func.Blueprint()


def _parse_positive_int(
    req: func.HttpRequest, key: str, *, default: int | None = None
) -> int | None:
    raw = req.params.get(key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    return value


@bp.function_name(name="BronzeTableList")
@bp.route(route="bronze/tables", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bronze_table_list(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = {"schema": "bronze", "tables": list_bronze_tables()}
        return func.HttpResponse(
            json.dumps(body, default=str), status_code=200, mimetype="application/json"
        )
    except ValueError as exc:
        return func.HttpResponse(str(exc), status_code=500)
    except Exception as exc:
        return func.HttpResponse(f"Failed to list bronze tables: {exc}", status_code=500)


@bp.function_name(name="BronzeTableColumns")
@bp.route(
    route="bronze/{table}/columns",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS,
)
def bronze_table_columns(req: func.HttpRequest) -> func.HttpResponse:
    table_name = req.route_params.get("table")
    if not table_name:
        return func.HttpResponse("table is required", status_code=400)

    try:
        columns = get_bronze_table_columns(table_name)
        if not columns:
            return func.HttpResponse(
                f"table 'bronze.{table_name}' not found", status_code=404
            )
        body = {"schema": "bronze", "table": table_name, "columns": columns}
        return func.HttpResponse(
            json.dumps(body, default=str), status_code=200, mimetype="application/json"
        )
    except Exception as exc:
        return func.HttpResponse(
            f"Failed to fetch columns for bronze.{table_name}: {exc}", status_code=500
        )


@bp.function_name(name="BronzeTableRows")
@bp.route(route="bronze/{table}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def bronze_table_rows(req: func.HttpRequest) -> func.HttpResponse:
    table_name = req.route_params.get("table")
    if not table_name:
        return func.HttpResponse("table is required", status_code=400)

    try:
        limit = _parse_positive_int(req, "limit")
        offset = _parse_positive_int(req, "offset", default=0)
        order_by = req.params.get("orderBy")
        order_dir = (req.params.get("orderDir") or "desc").lower()
        if order_dir not in {"asc", "desc"}:
            return func.HttpResponse("orderDir must be 'asc' or 'desc'", status_code=400)

        payload = get_bronze_table_rows(
            table_name,
            limit=limit,
            offset=offset,
            order_by=order_by,
            order_dir=order_dir,
        )
        return func.HttpResponse(
            json.dumps(payload, default=str), status_code=200, mimetype="application/json"
        )
    except ValueError as exc:
        message = str(exc)
        status = 404 if "does not exist" in message else 400
        return func.HttpResponse(message, status_code=status)
    except Exception as exc:
        return func.HttpResponse(
            f"Failed to fetch rows for bronze.{table_name}: {exc}", status_code=500
        )
