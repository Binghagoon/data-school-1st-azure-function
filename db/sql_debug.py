from __future__ import annotations

from datetime import date, datetime, time
from typing import Any, Sequence


def _to_sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return "'" + value.isoformat(sep=" ") + "'"
    return "'" + str(value).replace("'", "''") + "'"


def render_sql_for_log(query: str, params: Sequence[Any] | None = None) -> str:
    """Render a SQL string with %s params replaced for debug logging."""
    if not params:
        return query

    rendered = query
    for param in params:
        rendered = rendered.replace("%s", _to_sql_literal(param), 1)
    return rendered

