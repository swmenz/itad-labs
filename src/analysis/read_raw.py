"""Чтение raw-объектов напрямую из SeaweedFS S3 средствами DuckDB без создания таблиц."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from urllib.parse import urlsplit

import duckdb


def reader_for(object_path: str) -> str:
    """Определить табличную функцию чтения DuckDB по расширению файла."""
    suffix = Path(urlsplit(object_path).path or object_path).suffix.lower()
    supported_readers = {
        ".csv": "read_csv_auto",
        ".json": "read_json_auto",
        ".jsonl": "read_json_auto",
        ".ndjson": "read_json_auto",
        ".parquet": "read_parquet",
    }
    try:
        return supported_readers[suffix]
    except KeyError as error:
        msg = f"Неподдерживаемое расширение файла для DuckDB: {suffix or 'без расширения'}."
        raise ValueError(msg) from error


def configure_s3(connection: duckdb.DuckDBPyConnection) -> None:
    """Настроить S3-параметры DuckDB для работы с локальным SeaweedFS."""
    raw_endpoint = os.getenv("S3_ENDPOINT", "localhost:8333")
    parsed_endpoint = urlsplit(raw_endpoint) if "://" in raw_endpoint else None
    endpoint = parsed_endpoint.netloc if parsed_endpoint else raw_endpoint
    use_ssl = parsed_endpoint.scheme == "https" if parsed_endpoint else False

    access_key = os.getenv("S3_ACCESS_KEY", "s3admin")
    secret_key = os.getenv("S3_SECRET_KEY", "s3admin123")

    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")

    s3_params: dict[str, object] = {
        "s3_endpoint": endpoint,
        "s3_access_key_id": access_key,
        "s3_secret_access_key": secret_key,
        "s3_region": "us-east-1",
        "s3_url_style": "path",
        "s3_use_ssl": use_ssl,
    }
    for param_name, param_value in s3_params.items():
        connection.execute(f"SET {param_name} = ?", [param_value])


def main() -> None:
    """Вывести первые строки объекта из бакета raw в консоль."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--path",
        required=True,
        help="Полный S3 URI объекта (например, s3://raw/movie_industry/ingested_on=.../movies.csv)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Число выводимых строк предпросмотра (по умолчанию 5).",
    )
    args = parser.parse_args()

    reader_fn = reader_for(args.path)
    connection = duckdb.connect(":memory:")
    configure_s3(connection)

    cursor = connection.execute(
        f"SELECT * FROM {reader_fn}(?) LIMIT ?",
        [args.path, args.limit],
    )
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    print("Поля:", ", ".join(columns))
    for idx, row in enumerate(rows, start=1):
        print(f"[{idx}] {row}")


if __name__ == "__main__":
    main()
