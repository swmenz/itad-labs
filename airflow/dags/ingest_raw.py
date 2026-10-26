"""DAG ЛР № 2: ожидание HTTP-источника Movie Industry и загрузка в слой raw SeaweedFS."""

from __future__ import annotations

from datetime import datetime
from urllib.parse import urlsplit

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor

from ingestion.load_raw import load_to_raw

# Основной статический источник: набор Movie Industry (Daniel Grijalva).
# Домен должен совпадать с host в AIRFLOW_CONN_SOURCE_HTTP_CONN (.env).
SOURCE_URL = "https://raw.githubusercontent.com/danielgrijalva/movie-stats/master/movies.csv"
SOURCE_FILENAME = "movies.csv"
DATASET_SLUG = "movie_industry"
HTTP_CONN_ID = "source_http_conn"
S3_CONN_ID = "s3_conn"

parsed_source = urlsplit(SOURCE_URL)
sensor_endpoint = parsed_source.path or "/"
if parsed_source.query:
    sensor_endpoint = f"{sensor_endpoint}?{parsed_source.query}"

with DAG(
    dag_id="ingest_raw",
    description="Проверка доступности файла movies.csv по HTTP и идемпотентная загрузка в raw.",
    # Фиксированная логическая дата: запуск «с чистого листа» создаёт ровно одну партицию
    # ingested_on=2026-10-24, которую читает ноутбук ЛР № 3.
    start_date=datetime(2026, 10, 24),
    end_date=datetime(2026, 10, 24),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["lab-02", "raw", "movie-industry"],
) as dag:
    wait_for_primary_source = HttpSensor(
        task_id="wait_for_primary_source",
        http_conn_id=HTTP_CONN_ID,
        endpoint=sensor_endpoint,
        method="HEAD",
        poke_interval=60,
        timeout=600,
    )

    load_to_raw_task = PythonOperator(
        task_id="load_to_raw",
        python_callable=load_to_raw,
        op_kwargs={
            "source_url": SOURCE_URL,
            "source_filename": SOURCE_FILENAME,
            "dataset_slug": DATASET_SLUG,
            "s3_conn_id": S3_CONN_ID,
        },
    )

    wait_for_primary_source >> load_to_raw_task
