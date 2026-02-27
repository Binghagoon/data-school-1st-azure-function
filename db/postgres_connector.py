import os

import psycopg


def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_connection() -> psycopg.Connection:
    host = _get_required_env("POSTGRES_HOST")
    dbname = _get_required_env("POSTGRES_DB")
    user = _get_required_env("POSTGRES_USER")
    password = _get_required_env("POSTGRES_PASSWORD")

    port = os.environ.get("POSTGRES_PORT", "5432")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "require")

    return psycopg.connect(
        host=host,
        dbname=dbname,
        user=user,
        password=password,
        port=port,
        sslmode=sslmode,
    )


def check_connection() -> bool:
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1;")
            row = cursor.fetchone()
            return bool(row and row[0] == 1)


def get_connection_settings() -> dict[str, str]:
    """Returns the PostgreSQL connection settings as a dictionary."""
    return {
        "host": _get_required_env("POSTGRES_HOST"),
        "dbname": _get_required_env("POSTGRES_DB"),
        "user": _get_required_env("POSTGRES_USER"),
        "password": _get_required_env("POSTGRES_PASSWORD"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "sslmode": os.environ.get("POSTGRES_SSLMODE", "require"),
    }
