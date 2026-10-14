from __future__ import annotations

import pymysql
from pymysql.connections import Connection

from src.config import MySQLSettings


def connect(settings: MySQLSettings, database: str | None = None) -> Connection:
    """Crea y retorna una conexión a MySQL usando la configuración recibida."""
    return pymysql.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        password=settings.password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
    )
