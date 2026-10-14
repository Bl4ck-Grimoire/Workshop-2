from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
# Raíz del proyecto (directorio superior a src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    # Carga automática del archivo .env si existe en la raíz
    load_dotenv(PROJECT_ROOT / ".env", override=False)
except ImportError:
    pass


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name} (check .env)")
    return value  # type: ignore[return-value]


def _env_path(name: str, default_relative: str) -> Path:
    val = os.getenv(name)
    if val:
        path = Path(val)
        return path.resolve() if path.is_absolute() else (PROJECT_ROOT / path).resolve()
    return (PROJECT_ROOT / default_relative).resolve()


@dataclass(frozen=True)
class MySQLSettings:
    host: str
    port: int
    user: str
    password: str
    source_db: str
    dw_db: str


def mysql_settings() -> MySQLSettings:
    return MySQLSettings(
        host=_env("MYSQL_HOST", "localhost"),
        port=int(_env("MYSQL_PORT", "3306")),
        user=_env("MYSQL_ETL_USER", required=True),
        password=_env("MYSQL_ETL_PASSWORD", required=True),
        source_db=_env("MYSQL_SOURCE_DB", "grammy_source"),
        dw_db=_env("MYSQL_DW_DB", "music_dw"),
    )


def spotify_csv_path() -> Path:
    return _env_path("SPOTIFY_CSV_PATH", "data/raw/spotify_dataset.csv")


def grammy_csv_path() -> Path:
    return _env_path("GRAMMY_CSV_PATH", "data/raw/the_grammy_awards.csv")


def sql_dir() -> Path:
    return _env_path("SQL_DIR", "sql")


def evidence_dir() -> Path:
    return _env_path("EVIDENCE_DIR", "docs/evidence")


def staging_dir() -> Path:
    return _env_path("STAGING_DIR", "data/staging")


def batch_dir(batch_id):
    """Folder with the files of one batch (raw extracts and prepared tables). Tasks pass paths, never the data itself."""
    return staging_dir() / str(batch_id)
