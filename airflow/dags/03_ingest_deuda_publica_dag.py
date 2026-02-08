import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator

from deuda_publica.api_client import BASE_URL, fetch_page
from deuda_publica.raw_blob_writer import RawBlobWriter

logger = logging.getLogger(__name__)

SOURCE_NAME = "deuda_publica"

# creamos hash unico a partir de los datos de la request para poder identificarla y no guardar duplicados
def _build_request_hash(*, endpoint, params, page, execution_date):
    signature = {
        "endpoint": endpoint,
        "params": params,
        "page": page,
        "execution_date": execution_date }
    raw = json.dumps(signature, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# arma metadatos de la ingesta (fecha, fuente, timestamp y hash) que se guarda junto al archivo RAW
def _build_metadata(*, execution_date, request_hash):
    return {
        "execution_date": execution_date,
        "source": SOURCE_NAME,
        "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
        "request_hash": request_hash }

# Lee la API por páginas y guarda cada resultado como RAW en ADLS hasta que ya no haya datos
def ingest(**context):
    execution_date = context["ds"]
    writer = RawBlobWriter()

    page = 1
    while True:
        params = {"page": page}
        payload = fetch_page(page)
        results = payload.get("results") or []

        if not results:
            logger.info("Página %s sin resultados. Corte.", page)
            break

        request_hash = _build_request_hash(endpoint=BASE_URL, params=params, page=page, execution_date=execution_date)
        metadata = _build_metadata(execution_date=execution_date, request_hash=request_hash)

        writer.write_page(
            execution_date=execution_date,
            request_hash=request_hash,
            page=page,
            payload=payload,
            metadata=metadata,
        )

        page += 1


with DAG(
    dag_id="03_ingest_deuda_publica_raw",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["raw", "odmh"],
) as dag:
    PythonOperator(
        task_id="ingest_raw_deuda_publica",
        python_callable=ingest,
        provide_context=True,
    )
