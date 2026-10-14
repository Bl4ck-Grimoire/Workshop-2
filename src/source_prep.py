import re
import pandas as pd

from src import config
from src.db import connect


def split_sql_statements(sql_text: str) -> list[str]:
    """Divide un script SQL con múltiples sentencias delimitadas por ';' en una lista de sentencias ejecutables individuales,

    limpiando comentarios y respetando puntos y comas que estén dentro de cadenas de texto entre comillas simples.
    """
    cleaned = re.sub(r"/\*.*?\*/", "", sql_text, flags=re.DOTALL)
    lines = []
    for line in cleaned.splitlines():
        line_stripped = line.strip()
        if line_stripped.startswith("--") or line_stripped.startswith("#"):
            continue
        lines.append(line)
    cleaned = "\n".join(lines)

    statements = []
    parts = re.split(r";(?=(?:[^']*'[^']*')*[^']*$)", cleaned)
    for part in parts:
        stmt = part.strip()
        if stmt:
            statements.append(stmt)
    return statements


def setup_source_database():
    """Ejecuta sql/source_setup.sql y puebla grammy_source.grammy_awards desde el CSV

    si la tabla se encuentra vacía (Reconciliación y preparación de fuente - sección 6.2).
    """
    settings = config.mysql_settings()
    # Conectar sin especificar base de datos inicial para poder crear grammy_source y music_dw
    conn = connect(settings, None)
    try:
        with conn.cursor() as cursor:
            setup_sql = (config.sql_dir() / "source_setup.sql").read_text(encoding="utf-8")
            for stmt in split_sql_statements(setup_sql):
                cursor.execute(stmt)
            conn.commit()

            # Verificar si grammy_awards ya contiene datos
            cursor.execute("USE grammy_source")
            cursor.execute("SELECT COUNT(*) FROM grammy_awards")
            count = cursor.fetchone()[0]

            csv_path = config.grammy_csv_path()
            if count == 0:
                print(f"[source_prep] Poblando grammy_source.grammy_awards desde {csv_path}...")
                df = pd.read_csv(csv_path)
                columns = ["year", "title", "published_at", "updated_at", "category", "nominee", "artist", "workers", "img", "winner"]
                sql = f"INSERT INTO grammy_awards ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
                rows = [tuple(None if pd.isna(val) else str(val) for val in row) for row in df[columns].itertuples(index=False, name=None)]
                
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    cursor.executemany(sql, rows[i:i + batch_size])
                conn.commit()
                print(f"[source_prep] Éxito: {len(rows)} filas insertadas y reconciliadas con el CSV.")
            else:
                print(f"[source_prep] grammy_source.grammy_awards ya contiene {count} filas. No se requiere repoblar.")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_source_database()
