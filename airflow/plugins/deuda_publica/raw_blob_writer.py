import json
import logging
import os
from typing import Any, Dict, Optional

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)

# guardar los datos raw (payload mas metadata) en adls gen2 con estructura particionada e idempotencia
class RawBlobWriter:
    # Maneja exclusivamente escritura RAW en ADLS Gen2
    def __init__(self):
        self._account_name = _require_env("ADLS_ACCOUNT_NAME")
        self._tenant_id = _require_env("ADLS_TENANT_ID")
        self._client_id = _require_env("ADLS_CLIENT_ID")
        self._client_secret = _require_env("ADLS_CLIENT_SECRET")
        self._container = _require_env("ADLS_RAW_CONTAINER")

        self._service_client = _build_datalake_service_client(
            account_name=self._account_name,
            tenant_id=self._tenant_id,
            client_id=self._client_id,
            client_secret=self._client_secret,
        )
        self._file_system_client = _ensure_file_system(
            service_client=self._service_client,
            container=self._container,
        )

    # Escribe un archivo JSON RAW (payload + metadata) con idempotencia
    def write_page(self, *, execution_date, request_hash, page, payload, metadata):
        directory_path = (f"deuda_publica/execution_date={execution_date}/request_hash={request_hash}")
        file_name = f"page={page:04d}.json"

        directory_client = self._ensure_directory(directory_path)
        file_client = directory_client.get_file_client(file_name)

        if _file_exists(file_client):
            logger.info("RAW ya existe (skip): %s/%s/%s", self._container, directory_path, file_name)
            return False

        raw_content = {"metadata": metadata, "payload": payload}
        payload_bytes = json.dumps(raw_content, ensure_ascii=False).encode("utf-8")

        try:
            file_client.create_file()
            file_client.upload_data(payload_bytes, overwrite=True)
            logger.info("RAW escrito: %s/%s/%s", self._container, directory_path, file_name)
        
            return True
        
        except ResourceExistsError:
            logger.info("RAW ya existe (skip): %s/%s/%s", self._container, directory_path, file_name)
            return False

    # Crea directorios si no existen
    def _ensure_directory(self, directory_path):
        normalized = directory_path.strip("/")
        current = ""

        for segment in normalized.split("/"):
            current = f"{current}/{segment}" if current else segment
            dir_client = self._file_system_client.get_directory_client(current)

            try:
                dir_client.create_directory()
            except ResourceExistsError:
                pass

        return self._file_system_client.get_directory_client(normalized)

# Construye y devuelve un cliente autenticado de ADLS Gen2 usando Service Principal (tenant/client/secret) para conectarse a la cuenta de Data Lake
def _build_datalake_service_client(*, account_name, tenant_id, client_id, client_secret):
    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)
    account_url = f"https://{account_name}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=account_url, credential=credential)

# Obtiene el cliente del contenedor en ADLS y, si no existe, lo crea de forma segura antes de devolverlo
def _ensure_file_system(*, service_client, container):
    file_system_client = service_client.get_file_system_client(container)
    try:
        file_system_client.get_file_system_properties()
    except ResourceNotFoundError:
        try:
            service_client.create_file_system(container)
        except ResourceExistsError:
            pass
        file_system_client = service_client.get_file_system_client(container)
    return file_system_client

# Comprueba si el archivo ya existe en ADLS consultando sus metadatos; se usa para evitar duplicar o sobrescribir archivos (idempotencia del pipeline)
def _file_exists(file_client):
    try:
        file_client.get_file_properties()
        return True
    except ResourceNotFoundError:
        return False

# Lee una variable de entorno por nombre y devuelve su valor limpio, o None si no existe o está vacía
def _get_env(name):
    value = os.getenv(name)
    return value.strip() if value and value.strip() else None

# Obtiene una variable de entorno obligatoria y lanza error si no está definida, para evitar correr el pipeline con config incompleta
def _require_env(name):
    value = _get_env(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value
