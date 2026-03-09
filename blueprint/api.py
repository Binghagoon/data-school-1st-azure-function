import json
import decimal
from datetime import datetime, timedelta, timezone
import azure.functions as func
from service.shelter_sync import get_shelters
import pg8000.dbapi
from db.postgres_connector import get_connection
from service.shelter_sync import get_shelters


# Decimal 처리를 위한 커스텀 JSON 인코더
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
        
bp = func.Blueprint()
_count = 0

@bp.function_name(name="ApiRoot")
@bp.route(route="api", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_root(req: func.HttpRequest) -> func.HttpResponse:
    body = {"message": "API is running"}
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiShelters")
@bp.route(route="shelters", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
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


# ═══════════════════════════════════════════
# 🌟 심 팀장님 오리지널 코드 이식 (로그인 / 회원가입)
# ═══════════════════════════════════════════



@bp.function_name(name="ApiSignup")
@bp.route(route="signup", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def api_signup(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        conn = get_connection()
        cursor = conn.cursor()
        

        # 스크린샷의 users 테이블 컬럼명과 정확히 일치시켰습니다.
        query = """
            INSERT INTO users (userid, password, name, address, birthyear)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(
            query,
            (
                data["userid"],
                data["password"],
                data["name"],
                data["address"],
                int(data["birthyear"]),
            ),
        )

        conn.commit()
        cursor.close()
        conn.close()

        return func.HttpResponse(
            body=json.dumps({"message": "가입을 환영합니다!"}),
            mimetype="application/json",
            status_code=200,
        )
    except Exception as e:
        return func.HttpResponse(
            body=json.dumps({"detail": f"가입 실패: {str(e)}"}),
            mimetype="application/json",
            status_code=500,
        )

@bp.function_name(name="ApiLogin")
@bp.route(route="login", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def api_login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = req.get_json()
        conn = get_connection()
        cursor = conn.cursor()
        query = "SELECT name FROM users WHERE userid = %s AND password = %s"
        cursor.execute(query, (data["userid"], data["password"]))
        result = cursor.fetchone()
        cursor.close()
        conn.close()

        if result:
            return func.HttpResponse(
                body=json.dumps({"username": result[0]}), mimetype="application/json"
            )
        else:
            return func.HttpResponse(
                body=json.dumps({"detail": "정보 불일치"}),
                mimetype="application/json",
                status_code=401,
            )
    except Exception as e:
        return func.HttpResponse(body=json.dumps({"detail": str(e)}), mimetype="application/json", status_code=500)

@bp.function_name(name="ApiEnvCurrent")
@bp.route(route="env/current", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_env_current(req: func.HttpRequest) -> func.HttpResponse:
    district = req.params.get("district")
    if not district:
        return func.HttpResponse(json.dumps({"detail": "district 파라미터가 필요합니다."}, ensure_ascii=False), status_code=400, mimetype="application/json")

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, dist_name, "temp", humi, wind, rain, pm10, pm25, air_grade, measured_at, created_at, pty
            FROM silver.environment_cleaned
            WHERE dist_name = %s
            ORDER BY measured_at DESC
            LIMIT 1
        """, (district,))
        
        row = cursor.fetchone()
        if not row:
            return func.HttpResponse(json.dumps({"detail": f"'{district}' 데이터를 찾을 수 없습니다."}, ensure_ascii=False), status_code=404, mimetype="application/json")

        result = {
            "id": row[0], "dist_name": row[1], "temp": row[2], "humi": row[3],
            "wind": row[4], "rain": row[5], "pm10": row[6], "pm25": row[7],
            "air_grade": row[8], "measured_at": str(row[9]), "created_at": str(row[10]), "pty": row[11]
        }

        return func.HttpResponse(
            json.dumps(result, ensure_ascii=False, cls=DecimalEncoder), 
            status_code=200, 
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(json.dumps({"detail": f"DB 조회 에러: {str(e)}"}, ensure_ascii=False), status_code=500, mimetype="application/json")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@bp.function_name(name="ApiWeatherTomorrow")
@bp.route(route="weather/tomorrow", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_weather_tomorrow(req: func.HttpRequest) -> func.HttpResponse:
    district = req.params.get("district")
    if not district:
        return func.HttpResponse(json.dumps({"detail": "district 파라미터가 필요합니다."}, ensure_ascii=False), status_code=400, mimetype="application/json")

    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        kst = timezone(timedelta(hours=9))
        tomorrow_kst = datetime.now(kst) + timedelta(days=1)
        target_date = tomorrow_kst.strftime("%Y%m%d")

        cursor.execute("""
            SELECT id, dist_name, fcst_date, fcst_time, "temp", humi, pop, rain, created_at, snow, pty
            FROM silver.weather_forecast_cleaned
            WHERE dist_name = %s AND fcst_date = %s
            ORDER BY fcst_time ASC
        """, (district, target_date))
        
        rows = cursor.fetchall()
        forecast_list = []
        for row in rows:
            forecast_list.append({
                "id": row[0], "dist_name": row[1], "fcst_date": row[2], "fcst_time": row[3],
                "temp": row[4], "humi": row[5], "pop": row[6], "rain": row[7],
                "created_at": str(row[8]), "snow": row[9], "pty": row[10]
            })

        result = {
            "district": district,
            "target_date": target_date,
            "count": len(forecast_list),
            "forecasts": forecast_list
        }

        # 🌟 여기서 스페이스 한 칸이 모자랐던 부분을 고쳤습니다!
        return func.HttpResponse(
            json.dumps(result, ensure_ascii=False, cls=DecimalEncoder), 
            status_code=200, 
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(json.dumps({"detail": f"DB 조회 에러: {str(e)}"}, ensure_ascii=False), status_code=500, mimetype="application/json")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
        return func.HttpResponse(
            body=json.dumps({"detail": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
