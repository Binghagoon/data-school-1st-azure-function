 
from db.postgres_connector import get_connection


def get_shelter_data_from_db():
    try:
        conn = get_connection()
        cur = conn.cursor()
        # DB에서 데이터 가져오기 (컬럼명 확인 필수!)
        query = """
            SELECT shelter_name, road_addr, lat, lon FROM heat_shelter
            UNION ALL
            SELECT shelter_name, road_addr, lat, lon FROM cold_shelter
        """
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
       
        # 리액트가 이해하기 쉬운 JSON 형태로 가공
        result = []
        for r in rows:
            result.append({
                "name": r[0],
                "addr": r[1],
                "lat": float(r[2]),
                "lng": float(r[3])
            })
        return result
    except Exception as e:
        print(f"❌ DB 에러: {e}")
        return []
 
# 2. 리액트에서 호출할 API 경로 생성
def get_shelters():
    data = get_shelter_data_from_db()
    return jsonify(data)  