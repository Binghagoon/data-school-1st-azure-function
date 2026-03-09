import json
from datetime import datetime, timedelta, timezone

import azure.functions as func

from db.postgres_connector import get_connection
from service.shelter_sync import get_shelters

bp = func.Blueprint()
_count = 0



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



# [기능 1] 회원가입: 데이터를 DB에 저장!
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


# [기능 2] 로그인 (기존 코드 유지)
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
        return func.HttpResponse(
            body=json.dumps({"detail": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
