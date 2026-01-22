# Imports estándar Python
import json
import logging
import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

# Librerías externas (Azure SDK)
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

# modulos del proyecto (deuda_publica)
from audit.audit_logger import AuditLogger

from deuda_publica.raw_blob_writer import RawBlobWriter
from deuda_publica.api_client import fetch_page

logger = logging.getLogger(__name__)

# Constantes del loader RAW
MAX_PAGES = 166  # para pruebas
RAW_CONTAINER = "raw"
SOURCE_SYSTEM = "odmh_hacienda_py"

# Loader RAW de Deuda Pública
class RawDeudaPublicaLoader:
    def __init__(self, execution_date: str, *, container: str = RAW_CONTAINER, max_pages: int = MAX_PAGES, overwrite: bool = False, ) -> None:
        self.execution_date = _validate_execution_date(execution_date)
        self.container = container
        self.max_pages = int(max_pages)
        self.overwrite = overwrite

        self._blob_service = _build_blob_service_client()

    # esta funcion ejecuta la ingesta completa del raw - (una corrida es una auditoria)
    def load_all(self) -> None:
        audit = AuditLogger()

        pipeline_run_id = audit.start_run(
            dag_id="02_ingest_deuda_publica_raw",
            task_id="ingest_raw_deuda_publica",
            run_id=self.execution_date,
            execution_date=self.execution_date,
            source="odmh_api",
            target="adls_raw",)

        records_loaded = 0

        try:
            # inicializa writer de ADLS (responsabilidad separada)
            writer = RawBlobWriter(
                blob_service=self._blob_service,
                container=self.container,
                overwrite=self.overwrite,)

            # asegura que el container exista
            container_client = writer.ensure_container()

            # itera página por página consumiendo la API
            for page in range(1, self.max_pages + 1):
                data = fetch_page(page)
                results = data.get("results") or []

                if not results:
                    logger.info("Página %s sin resultados. Corte.", page)
                    break

                blob_path = self._raw_blob_path(page)
                request_hash = self._request_hash(page)

                # salta si ya existe y overwrite=False
                if not self.overwrite and self._blob_exists(container_client, blob_path):
                    logger.info("RAW ya existe (skip): %s", blob_path)
                    continue

                payload_bytes = _json_bytes(data)
                metadata = self._build_metadata(page=page, request_hash=request_hash)

                # escritura en ADLS delegada al writer
                writer.write_json(
                    container_client=container_client,
                    blob_path=blob_path,
                    payload=payload_bytes,
                    metadata=metadata,)

                records_loaded += len(results)

                logger.info(
                    "Página %s guardada en ADLS (%s registros) -> %s",
                    page,
                    len(results),
                    blob_path,)

            # auditoría OK
            audit.finish_success(pipeline_run_id, records_loaded)

        except Exception as e:
            # auditoría FAIL
            audit.finish_failure(pipeline_run_id, str(e))
            raise



    # Construye el path RAW particionado por execution_date y page
    def _raw_blob_path(self, page: int) -> str:
        return f"deuda_publica/execution_date={self.execution_date}/page={page:03d}.json"

    # Genera un hash único de la request para idempotencia
    def _request_hash(self, page: int) -> str:
        raw = f"{SOURCE_SYSTEM}|endpoint=deuda_publica|page={page}|execution_date={self.execution_date}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # Construye metadata técnica para el blob RAW
    def _build_metadata(self, *, page: int, request_hash: str) -> Dict[str, str]:
        ingestion_ts = datetime.now(timezone.utc).isoformat()
        return {
            "source_system": SOURCE_SYSTEM,
            "execution_date": self.execution_date,
            "page_number": str(page),
            "request_hash": request_hash,
            "ingestion_timestamp_utc": ingestion_ts,
        }

    # Verifica si un blob existe en el container
    @staticmethod
    def _blob_exists(container_client, blob_path: str) -> bool:
        return container_client.get_blob_client(blob_path).exists()

# Construcción del cliente Azure Blob
def _build_blob_service_client() -> BlobServiceClient:
    conn_str = _get_env("AZURE_STORAGE_CONNECTION_STRING")
    if conn_str:
        return BlobServiceClient.from_connection_string(conn_str)

    tenant_id = _require_env("AZURE_TENANT_ID")
    client_id = _require_env("AZURE_CLIENT_ID")
    client_secret = _require_env("AZURE_CLIENT_SECRET")
    account_name = _require_env("AZURE_STORAGE_ACCOUNT")

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret, )

    account_url = f"https://{account_name}.blob.core.windows.net"
    return BlobServiceClient(account_url=account_url, credential=credential)

# Validación de execution_date
def _validate_execution_date(execution_date: str) -> str:
    try:
        datetime.strptime(execution_date, "%Y-%m-%d")
        return execution_date
    except ValueError as exc:
        raise ValueError(f"execution_date inválida (esperado YYYY-MM-DD): {execution_date}") from exc


# Serialización JSON a bytes
def _json_bytes(data: Dict[str, Any]) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("utf-8")

# lectura segura de variables de entorno
def _get_env(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None

# variable de entorno obligatoria
def _require_env(name: str) -> str:
    value = _get_env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
