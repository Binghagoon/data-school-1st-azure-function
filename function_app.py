import logging
import os

import azure.functions as func
import requests

from db.postgres_connector import check_connection

app = func.FunctionApp()


@app.function_name(name="Main")
@app.route(route="main", methods=["GET"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        "Welcome to the Azure Function App! Use the /crawl endpoint to crawl a target API.",
        status_code=200,
    )


@app.function_name(name="CrawlApi")
@app.route(route="crawl", methods=["GET"])
def crawl_api(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP trigger function that crawls a target API and returns the result."""
    logging.info("CrawlApi function triggered.")

    target_url = req.params.get("url") or os.environ.get("TARGET_API_URL", "")

    if not target_url:
        return func.HttpResponse(
            "Please provide a target URL via the 'url' query parameter or set the "
            "TARGET_API_URL environment variable.",
            status_code=400,
        )

    try:
        response = requests.get(target_url, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logging.error("Request to %s timed out.", target_url)
        return func.HttpResponse(
            f"Request to {target_url} timed out.",
            status_code=504,
        )
    except requests.exceptions.RequestException as exc:
        logging.error("Failed to crawl %s: %s", target_url, exc)
        return func.HttpResponse(
            f"Failed to crawl {target_url}: {exc}",
            status_code=502,
        )

    content_type = response.headers.get("Content-Type", "application/octet-stream")
    return func.HttpResponse(
        response.text,
        status_code=200,
        mimetype=content_type.split(";")[0].strip(),
    )


@app.function_name(name="DbHealth")
@app.route(route="db-health", methods=["GET"])
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
