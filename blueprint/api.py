import json
from datetime import datetime, timedelta, timezone

import azure.functions as func

from service.shelter_sync import get_shelters

import pg8000.native
import requests
import os

bp = func.Blueprint()
_count = 0


@bp.function_name(name="ApiRoot")
@bp.route(route="api", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_root(req: func.HttpRequest) -> func.HttpResponse:
    body = {"message": "API is running"}
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiNowTime")
@bp.route(route="api/now-time", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_now_time(req: func.HttpRequest) -> func.HttpResponse:
    now_utc = datetime.now(timezone.utc)
    kst = timezone(timedelta(hours=9))
    now_kst = now_utc.astimezone(kst)
    body = {
        "now_time_utc": now_utc.isoformat(),
        "now_time_kst": now_kst.isoformat(),
        "utc": {
            "year": now_utc.year,
            "month": now_utc.month,
            "day": now_utc.day,
            "hour": now_utc.hour,
            "minute": now_utc.minute,
            "second": now_utc.second,
            "microsecond": now_utc.microsecond,
            "offset": "+00:00",
        },
        "kst": {
            "year": now_kst.year,
            "month": now_kst.month,
            "day": now_kst.day,
            "hour": now_kst.hour,
            "minute": now_kst.minute,
            "second": now_kst.second,
            "microsecond": now_kst.microsecond,
            "offset": "+09:00",
        },
    }
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiCount")
@bp.route(route="api/count", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_count(req: func.HttpRequest) -> func.HttpResponse:
    global _count
    _count += 1
    body = {"count": _count}
    return func.HttpResponse(
        json.dumps(body), status_code=200, mimetype="application/json"
    )


@bp.function_name(name="ApiShelters")
@bp.route(route="api/shelters", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
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

@bp.function_name(name="ApiLogin")
@bp.route(route="api/login", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def api_login(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        user_id = req_body.get('id')
        user_pw = req_body.get('password')

        # DB 연결 (환경변수 사용 추천)
        conn = pg8000.native.Connection(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME", "postgres"),
            port=5432
        )

        # 사용자 조회 쿼리
        user = conn.run("SELECT name FROM users WHERE id = :id AND password = :pw", id=user_id, pw=user_pw)

        if user:
            return func.HttpResponse(json.dumps({"status": "success", "name": user[0][0]}), mimetype="application/json")
        else:
            return func.HttpResponse(json.dumps({"status": "fail", "message": "ID 또는 비밀번호가 틀립니다."}), status_code=401)

    except Exception as e:
        return func.HttpResponse(f"Login Error: {e}", status_code=500)

# [기능 2] 회원가입 API
@bp.function_name(name="ApiSignup")
@bp.route(route="api/signup", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
def api_signup(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        # ... 회원가입 정보 추출 및 DB INSERT 로직 ...
        return func.HttpResponse(json.dumps({"message": "회원가입 성공"}), status_code=201)
    except Exception as e:
        return func.HttpResponse(f"Signup Error: {e}", status_code=500)

# [기능 3] 실시간 날씨 API
@bp.function_name(name="ApiWeather")
@bp.route(route="api/weather", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def api_weather(req: func.HttpRequest) -> func.HttpResponse:
    # 서울 날씨 오픈 API 호출
    url = "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&current_weather=true"
    try:
        response = requests.get(url)
        data = response.json()
        current = data['current_weather']
        
        body = {
            "temp": round(current['temperature']),
            "status": "맑음" if current['weathercode'] <= 3 else "흐림",
            "msg": "실시간 기상 데이터 연동 성공!"
        }
        return func.HttpResponse(json.dumps(body), mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Weather Error: {e}", status_code=500)