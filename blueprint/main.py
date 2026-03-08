import azure.functions as func

bp = func.Blueprint()


@bp.function_name(name="Main")
@bp.route(route="main", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        "Welcome to the Azure Function App! Available endpoints include /disasters, /db-health, /timestamps, /bronze/tables, and /bronze/{table}.",
        status_code=200,
    )
