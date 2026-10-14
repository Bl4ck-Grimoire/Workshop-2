import json
from datetime import datetime, timezone

import pandas as pd

from src import config
from src.db import connect
from src.source_prep import split_sql_statements
from src.transform import PREPARED_COLUMNS, read_prepared

LOAD_ORDER = ["dim_artist", "dim_track", "dim_genre", "dim_year", "dim_category",
              "fact_track_credit", "fact_grammy_nomination"]
BATCH_SIZE = 5000


def to_rows(df):
    """DataFrame -> list of tuples with Python values (NaN and NA become None, that is NULL)."""
    return [tuple(None if pd.isna(value) else value for value in row) for row in df.itertuples(index=False, name=None)]


def insert_table(cursor, table, df):
    columns = PREPARED_COLUMNS[table]
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    rows = to_rows(df[columns])
    for start in range(0, len(rows), BATCH_SIZE):
        cursor.executemany(sql, rows[start:start + BATCH_SIZE])


def snapshot(cursor):
    """Row counts and two measures: used to compare the DW before and after a load (and after a rerun)."""
    rows = {}
    for table in LOAD_ORDER:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        rows[table] = int(cursor.fetchone()[0])
    cursor.execute("SELECT COALESCE(SUM(win_count), 0) FROM fact_grammy_nomination")
    total_wins = int(cursor.fetchone()[0])
    cursor.execute("SELECT COALESCE(ROUND(AVG(popularity), 4), 0) FROM fact_track_credit")
    avg_popularity = float(cursor.fetchone()[0])
    return {"rows": rows, "total_wins": total_wins, "avg_popularity": avg_popularity}


def load_dw(prepared_dir, validation, batch_id=""):
    """`validation` is the result of validate_prepared: the load is refused if it did not pass."""
    if not validation["passed"]:
        raise RuntimeError("load_dw refused: validate_prepared has critical failures")

    tables = read_prepared(prepared_dir)
    expected = {name: int(len(tables[name])) for name in LOAD_ORDER}
    settings = config.mysql_settings()
    conn = connect(settings, settings.dw_db)
    try:
        with conn.cursor() as cursor:
            for statement in split_sql_statements((config.sql_dir() / "dw_schema.sql").read_text(encoding="utf-8")):
                cursor.execute(statement)
            conn.commit()
            before = snapshot(cursor)
            try:
                for table in reversed(LOAD_ORDER):
                    cursor.execute(f"DELETE FROM {table}")
                for table in LOAD_ORDER:
                    insert_table(cursor, table, tables[table])
                after = snapshot(cursor)
                if after["rows"] != expected:
                    raise RuntimeError(f"Row counts do not match the prepared tables: {after['rows']} vs {expected}")
                conn.commit()
            except Exception:
                conn.rollback()
                print(f"[load_dw] batch={batch_id} FAILED: transaction rolled back, the DW keeps its previous content")
                raise
    finally:
        conn.close()

    summary = {"batch_id": batch_id, "loaded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
               "strategy": "replace in one transaction", "prepared_dir": str(prepared_dir),
               "before": before, "after": after}
    out_dir = config.evidence_dir() / "load"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"load_{batch_id}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}.json"
    out_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"[load_dw] batch={batch_id} rows before={before['rows']} -> after={after['rows']} | "
          f"wins={after['total_wins']} avg_popularity={after['avg_popularity']} | summary={out_file}")
    return {"batch_id": batch_id, "rows": after["rows"], "summary_file": str(out_file)}
