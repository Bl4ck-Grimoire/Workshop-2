import json
from datetime import datetime, timezone

import pandas as pd

from src import config
from src.db import connect

GRAMMY_COLUMNS = ["year", "title", "published_at", "updated_at", "category", "nominee", "artist", "workers", "img", "winner"]


def save_raw(df, name, batch_id, source):
    """Saves the extracted data and a small metadata file, so the evaluated batch can always be investigated."""
    folder = config.batch_dir(batch_id)
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.parquet"
    df.to_parquet(path, index=False)
    info = {"batch_id": batch_id, "dataset": name, "path": str(path), "source": source,
            "rows": int(len(df)), "columns": int(len(df.columns)),
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    (folder / f"{name}_metadata.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    print(f"[extract] batch={batch_id} dataset={name} rows={info['rows']} columns={info['columns']} source={source}")
    return info


def extract_spotify(batch_id):
    path = config.spotify_csv_path()
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False)
    except (UnicodeDecodeError, pd.errors.ParserError):
        df = pd.read_csv(path, sep=";", encoding="latin1", low_memory=False)
    cols = [c for c in df.columns if not str(c).startswith("Unnamed:")]
    df = df[cols]
    return save_raw(df, "spotify_raw", batch_id, str(path))


def read_grammy_table():
    """All the rows of the MySQL source table, in the original order. Every field is text (see sql/source_setup.sql)."""
    settings = config.mysql_settings()
    conn = connect(settings, settings.source_db)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT source_row_id, {', '.join(GRAMMY_COLUMNS)} FROM grammy_awards ORDER BY source_row_id")
            rows = cursor.fetchall()
            names = [column[0] for column in cursor.description]
    finally:
        conn.close()
    return pd.DataFrame(rows, columns=names)


def extract_grammys(batch_id):
    settings = config.mysql_settings()
    df = read_grammy_table()
    source = f"mysql://{settings.host}:{settings.port}/{settings.source_db}.grammy_awards"
    return save_raw(df, "grammy_raw", batch_id, source)
