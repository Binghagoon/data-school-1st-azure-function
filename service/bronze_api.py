from __future__ import annotations

from typing import Any

from db.postgres_connector import get_connection

BRONZE_SCHEMA = "bronze"
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000


def _normalize_paging(limit: int | None, offset: int | None) -> tuple[int, int]:
    normalized_limit = DEFAULT_LIMIT if limit is None else limit
    normalized_offset = 0 if offset is None else offset

    if normalized_limit <= 0:
        raise ValueError("limit must be a positive integer")
    if normalized_limit > MAX_LIMIT:
        raise ValueError(f"limit must be <= {MAX_LIMIT}")
    if normalized_offset < 0:
        raise ValueError("offset must be >= 0")

    return normalized_limit, normalized_offset


def list_bronze_tables() -> list[str]:
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (BRONZE_SCHEMA,))
            return [row[0] for row in cursor.fetchall()]


def get_bronze_table_columns(table_name: str) -> list[dict[str, Any]]:
    query = """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position;
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (BRONZE_SCHEMA, table_name))
            rows = cursor.fetchall()
            return [
                {
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_nullable": row[2],
                    "column_default": row[3],
                    "ordinal_position": row[4],
                }
                for row in rows
            ]


def get_bronze_table_rows(
    table_name: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    order_by: str | None = None,
    order_dir: str = "desc",
) -> dict[str, Any]:
    normalized_limit, normalized_offset = _normalize_paging(limit, offset)

    columns = get_bronze_table_columns(table_name)
    if not columns:
        raise ValueError(f"table '{BRONZE_SCHEMA}.{table_name}' does not exist")

    allowed_columns = {col["column_name"] for col in columns}
    order_column = order_by if order_by in allowed_columns else None
    direction = "ASC" if order_dir.lower() == "asc" else "DESC"

    with get_connection() as connection:
        with connection.cursor() as cursor:
            count_query = f'SELECT COUNT(*) FROM "{BRONZE_SCHEMA}"."{table_name}";'
            cursor.execute(count_query)
            total_count = cursor.fetchone()[0]

            if order_column:
                data_query = (
                    f'SELECT * FROM "{BRONZE_SCHEMA}"."{table_name}" '
                    f'ORDER BY "{order_column}" {direction} '
                    "LIMIT %s OFFSET %s;"
                )
            else:
                data_query = (
                    f'SELECT * FROM "{BRONZE_SCHEMA}"."{table_name}" '
                    "LIMIT %s OFFSET %s;"
                )

            cursor.execute(data_query, (normalized_limit, normalized_offset))
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

    items = [dict(zip(column_names, row)) for row in rows]
    return {
        "schema": BRONZE_SCHEMA,
        "table": table_name,
        "limit": normalized_limit,
        "offset": normalized_offset,
        "returned_count": len(items),
        "total_count": total_count,
        "items": items,
    }
