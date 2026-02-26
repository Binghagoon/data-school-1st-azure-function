import logging

import azure.functions as func

from db.postgres_connector import check_connection

bp = func.Blueprint()


@bp.function_name(name="DbHealth")
@bp.route(route="db-health", methods=["GET"])
def db_health(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger function that checks PostgreSQL connectivity."""
    try:
        is_connected = check_connection()
    except ValueError as exc:
        logging.error("PostgreSQL configuration error: %s", exc)
        return func.HttpResponse(str(exc), status_code=500)
    except Exception as exc:
        logging.error("PostgreSQL connection failed: %s", exc)
        return func.HttpResponse(f"PostgreSQL connection failed: {exc}", status_code=502)

    if not is_connected:
        return func.HttpResponse("PostgreSQL connection check failed.", status_code=502)

    return func.HttpResponse("PostgreSQL connection is healthy.", status_code=200)
