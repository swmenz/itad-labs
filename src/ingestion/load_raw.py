"""Идемпотентная загрузка исходного HTTP-файла в бакет raw объектного хранилища SeaweedFS."""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

RAW_BUCKET = "raw"


def load_to_raw(
    *,
    source_url: str,
    source_filename: str,
    dataset_slug: str,
    s3_conn_id: str,
    **context: Any,
) -> str:
    """Скачать исходный файл без изменения байтов и записать в raw с партицией ingested_on."""
    logical_date = context["ds"]
    object_key = f"{dataset_slug}/ingested_on={logical_date}/{source_filename}"
    target_uri = f"s3://{RAW_BUCKET}/{object_key}"

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)

    if not s3_hook.check_for_bucket(bucket_name=RAW_BUCKET):
        raise AirflowException(
            f"Бакет '{RAW_BUCKET}' не найден в SeaweedFS. "
            "Запустите инфраструктуру командой 'docker compose up -d', чтобы бакеты были созданы."
        )

    if s3_hook.check_for_key(key=object_key, bucket_name=RAW_BUCKET):
        logging.info(
            "Объект %s уже существует в слое raw — повторное скачивание и перезапись пропущены.",
            target_uri,
        )
        return target_uri

    logging.info("Загрузка исходного файла из %s", source_url)
    with requests.get(
        source_url,
        headers={"Accept-Encoding": "identity"},
        stream=True,
        timeout=(15, 120),
    ) as response:
        response.raise_for_status()
        raw_bytes = response.raw.read(decode_content=False)

    s3_hook.load_bytes(
        bytes_data=raw_bytes,
        key=object_key,
        bucket_name=RAW_BUCKET,
        replace=False,
    )

    digest = hashlib.sha256(raw_bytes).hexdigest()
    logging.info(
        "Сохранён объект %s (%d байт, SHA-256: %s)",
        target_uri,
        len(raw_bytes),
        digest,
    )
    return target_uri
