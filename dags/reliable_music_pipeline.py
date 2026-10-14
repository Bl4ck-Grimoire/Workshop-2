"""DAG de orquestación de Apache Airflow 3.1.8 usando TaskFlow API (punto 6.11).

Estructura de la arquitectura requerida:
  Spotify Branch: extract_spotify -> validate_spotify_raw
  Grammy Branch:  extract_grammys -> validate_grammys_raw
  Convergencia:   transform_and_integrate (depende de ambas validaciones raw)
                  -> validate_prepared
                  -> load_dw (gated por validate_prepared)

Políticas de confiabilidad aplicadas:
  - Paso de metadatos/rutas por XCom, nunca DataFrames masivos en memoria.
  - Reintentos selectivos:
      * Tareas operacionales/transitorias (extracciones, conexión a BD): retries=2
      * Tareas determinísticas de calidad/transformación: retries=0 (falla controlada inmediata)
"""
from datetime import datetime, timedelta, timezone

try:
    from airflow.sdk import dag, task
except ImportError:
    from airflow.decorators import dag, task

import pandas as pd

from src import extract, load, transform, validation

# Configuración por defecto para el DAG
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id="reliable_music_pipeline",
    default_args=DEFAULT_ARGS,
    description="Batch ETL pipeline integrating Spotify and Grammy data into a trusted DW",
    schedule=None,  # Disparo manual o batch programado
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "data_engineering", "spotify", "grammy", "reliable"],
)
def reliable_music_pipeline():

    @task(
        task_id="extract_spotify",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def task_extract_spotify(batch_id: str) -> dict:
        """Extrae los datos crudos de Spotify desde el CSV fuente sin alterar el archivo."""
        return extract.extract_spotify(batch_id)

    @task(
        task_id="validate_spotify_raw",
        retries=0,  # Falla determinística de calidad de datos: no reintentar a ciegas
    )
    def task_validate_spotify_raw(spotify_info: dict) -> dict:
        """Compuerta de calidad Great Expectations para Spotify Raw."""
        df = pd.read_parquet(spotify_info["path"])
        result = validation.validate_spotify_raw(df, source=spotify_info["source"])
        return result

    @task(
        task_id="extract_grammys",
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def task_extract_grammys(batch_id: str) -> dict:
        """Extrae los datos crudos de Grammy desde la tabla operacional grammy_source.grammy_awards en MySQL."""
        return extract.extract_grammys(batch_id)

    @task(
        task_id="validate_grammys_raw",
        retries=0,  # Falla determinística de calidad de datos
    )
    def task_validate_grammys_raw(grammy_info: dict) -> dict:
        """Compuerta de calidad Great Expectations para Grammy Raw."""
        df = pd.read_parquet(grammy_info["path"])
        result = validation.validate_grammys_raw(df, source=grammy_info["source"])
        return result

    @task(
        task_id="transform_and_integrate",
        retries=0,  # Transformación determinística
    )
    def task_transform_and_integrate(spotify_info: dict, grammy_info: dict, batch_id: str) -> dict:
        """Limpia, estandariza, armoniza e integra ambas fuentes construyendo las tablas preparadas del star schema."""
        return transform.transform_and_integrate(
            spotify_path=spotify_info["path"],
            grammy_path=grammy_info["path"],
            batch_id=batch_id,
        )

    @task(
        task_id="validate_prepared",
        retries=0,  # Compuerta crítica de datos preparados
    )
    def task_validate_prepared(prepared_info: dict) -> dict:
        """Compuerta de calidad Great Expectations para las tablas preparadas del Data Warehouse."""
        tables = transform.read_prepared(prepared_info["prepared_dir"])
        return validation.validate_prepared(tables, source=prepared_info["prepared_dir"])

    @task(
        task_id="load_dw",
        retries=1,
        retry_delay=timedelta(seconds=10),
    )
    def task_load_dw(prepared_info: dict, validation_result: dict, batch_id: str) -> dict:
        """Carga transaccional e idempotente (Safe Rerun / Truncate-and-load) al Data Warehouse en MySQL."""
        return load.load_dw(
            prepared_dir=prepared_info["prepared_dir"],
            validation=validation_result,
            batch_id=batch_id,
        )

    # Identificador único de batch por ejecución
    current_batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

    # 1. Rama Spotify
    spotify_meta = task_extract_spotify(batch_id=current_batch_id)
    spotify_val = task_validate_spotify_raw(spotify_info=spotify_meta)

    # 2. Rama Grammy
    grammy_meta = task_extract_grammys(batch_id=current_batch_id)
    grammy_val = task_validate_grammys_raw(grammy_info=grammy_meta)

    # 3. Convergencia: La transformación NO inicia hasta que ambas compuertas raw pasen
    prepared_meta = task_transform_and_integrate(
        spotify_info=spotify_meta,
        grammy_info=grammy_meta,
        batch_id=current_batch_id,
    )
    spotify_val >> prepared_meta
    grammy_val >> prepared_meta

    # 4. Compuerta de calidad de datos preparados
    prepared_val = task_validate_prepared(prepared_info=prepared_meta)

    # 5. Carga al Data Warehouse (bloqueada si validate_prepared falla)
    task_load_dw(
        prepared_info=prepared_meta,
        validation_result=prepared_val,
        batch_id=current_batch_id,
    )


dag_instance = reliable_music_pipeline()
