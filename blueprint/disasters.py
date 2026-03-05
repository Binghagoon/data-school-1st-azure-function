import json
import logging
import traceback

import azure.functions as func
import requests

from service.disasters import get_disaster, save_disasters

bp = func.Blueprint()


@bp.function_name(name="Disasters")
@bp.route(route="disasters", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def disasters(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger that returns disaster messages by date or date range."""
    date = req.params.get("date")
    start = req.params.get("start")
    end = req.params.get("end")
    # save = req.params.get("save", "false").lower() == "true"
    no_save = req.params.get("noSave", "false").lower() == "true"
    save = not no_save
    area_id = req.params.get("areaId", "1100000000")
    page_size_raw = req.params.get("pageSize", "10")

    if date and (start or end):
        return func.HttpResponse(
            "Use either 'date' or 'start/end' parameters, not both.",
            status_code=400,
        )
    if not date and not start and not end:
        return func.HttpResponse(
            "Provide 'date=YYYY-MM-DD' or a range with 'start' and/or 'end'.",
            status_code=400,
        )

    try:
        page_size = int(page_size_raw)
        if page_size <= 0:
            raise ValueError
    except ValueError:
        return func.HttpResponse(
            "'pageSize' must be a positive integer.", status_code=400
        )

    date_input: str | dict[str, str]
    if date:
        date_input = date
    else:
        date_input = {"start": start or "", "end": end or ""}

    try:
        items = get_disaster(date_input, area_id=area_id, page_size=page_size)
        body = json.dumps(items, ensure_ascii=False)
        if save:
            logging.info("Saving disasters to database...")
            save_disasters(items)
        return func.HttpResponse(body, status_code=200, mimetype="application/json")
    except ValueError as exc:
        traceback.print_exc()
        return func.HttpResponse(str(exc), status_code=400)
    except requests.RequestException as exc:
        logging.error("Failed to fetch disaster data: %s", exc)
        return func.HttpResponse(
            f"Failed to fetch disaster data: {exc}", status_code=502
        )
    except Exception as exc:
        logging.error("Unexpected error while fetching disasters: %s", exc)
        return func.HttpResponse(f"Unexpected server error: {exc}", status_code=500)
