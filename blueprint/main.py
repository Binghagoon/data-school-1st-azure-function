import azure.functions as func

bp = func.Blueprint()


@bp.function_name(name="Main")
@bp.route(route="main", methods=["GET"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    return func.HttpResponse(
        "Welcome to the Azure Function App! Available endpoints include /disasters, /db-health, and /timestamps.",
        status_code=200,
    )
