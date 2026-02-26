import azure.functions as func
from endpoints.crawl import bp as crawl_bp
from endpoints.db_health import bp as db_health_bp
from endpoints.main import bp as main_bp

app = func.FunctionApp()

app.register_blueprint(main_bp)
app.register_blueprint(crawl_bp)
app.register_blueprint(db_health_bp)
