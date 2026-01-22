import logging
from datetime import datetime, timezone
from typing import Dict
from azure.storage.blob import ContentSettings
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)


class RawBlobWriter:
    # Maneja exclusivamente escritura en ADLS
    def __init__(self, blob_service, container: str, overwrite: bool):
        self.blob_service = blob_service
        self.container = container
        self.overwrite = overwrite

    # Garantiza que el container exista
    def ensure_container(self):
        container_client = self.blob_service.get_container_client(self.container)
        try:
            container_client.get_container_properties()
        except ResourceNotFoundError:
            container_client.create_container()
            logger.info("Container creado: %s", self.container)
        return container_client

    # Escribe un archivo RAW en ADLS
    def write_json(self, *, container_client, blob_path: str, payload: bytes, metadata: Dict[str, str]):
        container_client.upload_blob(
            name=blob_path,
            data=payload,
            overwrite=self.overwrite,
            metadata=metadata,
            content_settings=ContentSettings(content_type="application/json; charset=utf-8"),)
