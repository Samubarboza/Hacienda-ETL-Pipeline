import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

DATASET_NAME = "deuda_publica"
DEFAULT_RAW_BASE_PATH = "deuda_publica"
DEFAULT_LOGS_BASE_PATH = "logs"
MAX_ERROR_DETAILS = 50


class RawDeudaPublicaValidator:
    def __init__(
        self,
        storage_account_url: str,
        file_system_name: str,
        credential: Dict[str, str],
        raw_base_path: str = DEFAULT_RAW_BASE_PATH,
        logs_base_path: str = DEFAULT_LOGS_BASE_PATH,
    ) -> None:
        self.raw_base_path = raw_base_path
        self.logs_base_path = logs_base_path

        self.credential = ClientSecretCredential(
            tenant_id=credential["tenant_id"],
            client_id=credential["client_id"],
            client_secret=credential["client_secret"],
        )

        self.service_client = DataLakeServiceClient(
            account_url=storage_account_url,
            credential=self.credential,
        )
        self.fs_client = self.service_client.get_file_system_client(file_system=file_system_name)

    def validate(self, *, execution_date: Optional[str]) -> Dict[str, Any]:
        started_at = datetime.now(timezone.utc)
        errors: List[str] = []
        invalid_files: List[Dict[str, str]] = []
        invalid_file_names = set()

        checks = {
            "execution_date_not_null": False,
            "raw_path_exists": False,
            "json_files_present": False,
            "json_valid": False,
            "has_metadata_and_payload": False,
            "payload_not_empty": False,
        }

        metrics = {
            "files_total": 0,
            "json_files": 0,
            "validated_files": 0,
            "invalid_files": 0,
        }

        raw_path: Optional[str] = None

        def add_error(message: str, file_name: Optional[str] = None) -> None:
            if len(errors) < MAX_ERROR_DETAILS:
                errors.append(message)
            if file_name:
                invalid_file_names.add(file_name)
                if len(invalid_files) < MAX_ERROR_DETAILS:
                    invalid_files.append({"file": file_name, "error": message})

        metadata_missing = 0
        payload_empty = 0
        json_invalid = 0
        metadata_execution_date_missing = 0

        try:
            if execution_date and str(execution_date).strip():
                checks["execution_date_not_null"] = True
                raw_path = f"{self.raw_base_path}/execution_date={execution_date}"

                try:
                    paths = list(self.fs_client.get_paths(path=raw_path, recursive=True))
                except Exception as exc:
                    paths = []
                    add_error(f"No se pudo listar el path RAW: {exc}")

                if paths:
                    checks["raw_path_exists"] = True
                    files = [p for p in paths if not p.is_directory]
                    metrics["files_total"] = len(files)

                    json_files = [p for p in files if p.name.lower().endswith(".json")]
                    metrics["json_files"] = len(json_files)

                    if json_files:
                        checks["json_files_present"] = True

                        for file in json_files:
                            file_name = file.name
                            file_client = self.fs_client.get_file_client(file_name)

                            try:
                                content = file_client.download_file().readall()
                            except Exception as exc:
                                json_invalid += 1
                                add_error(f"No se pudo leer el archivo: {exc}", file_name=file_name)
                                continue

                            if not content:
                                json_invalid += 1
                                add_error("Archivo JSON vacio", file_name=file_name)
                                continue

                            try:
                                data = json.loads(content)
                            except (json.JSONDecodeError, UnicodeDecodeError, TypeError):
                                json_invalid += 1
                                add_error("JSON invalido", file_name=file_name)
                                continue

                            metrics["validated_files"] += 1

                            if not isinstance(data, dict):
                                metadata_missing += 1
                                add_error("JSON no contiene objeto raiz", file_name=file_name)
                                continue

                            metadata = data.get("metadata")
                            payload = data.get("payload")

                            if metadata is None or payload is None:
                                metadata_missing += 1
                                add_error("Faltan metadata o payload", file_name=file_name)
                                continue

                            metadata_execution_date = None
                            if isinstance(metadata, dict):
                                metadata_execution_date = metadata.get("execution_date")

                            if not metadata_execution_date:
                                metadata_execution_date_missing += 1
                                add_error("metadata.execution_date es nulo", file_name=file_name)

                            if _payload_is_empty(payload):
                                payload_empty += 1
                                add_error("payload vacio", file_name=file_name)
                    else:
                        add_error(f"No hay archivos JSON en {raw_path}")
                else:
                    add_error(f"No existe el path RAW para execution_date={execution_date}")
            else:
                add_error("execution_date es nulo o vacio")
        except Exception as exc:
            add_error(f"Error inesperado en validacion: {exc}")

        checks["json_valid"] = json_invalid == 0 and metrics["json_files"] > 0
        checks["has_metadata_and_payload"] = metadata_missing == 0 and metrics["json_files"] > 0
        checks["payload_not_empty"] = payload_empty == 0 and metrics["json_files"] > 0
        checks["execution_date_not_null"] = bool(execution_date and str(execution_date).strip()) and metadata_execution_date_missing == 0

        metrics["invalid_files"] = len(invalid_file_names)

        ended_at = datetime.now(timezone.utc)
        status = "SUCCESS" if not errors else "FAILED"
        error_message = "; ".join(errors[:5]) if errors else None

        return {
            "event_type": "raw_validation",
            "dataset_name": DATASET_NAME,
            "execution_date": execution_date,
            "raw_path": raw_path,
            "status": status,
            "started_at_utc": started_at,
            "ended_at_utc": ended_at,
            "checks": checks,
            "metrics": metrics,
            "errors": errors,
            "invalid_files": invalid_files,
            "error_message": error_message,
        }

    def write_audit_log(
        self,
        *,
        report: Dict[str, Any],
        dag_id: Optional[str],
        task_id: Optional[str],
        run_id: Optional[str],
    ) -> str:
        execution_date = report.get("execution_date") or "unknown"
        safe_run_id = run_id or f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

        log_dir = f"{self.logs_base_path}/{DATASET_NAME}/execution_date={execution_date}"
        self._ensure_directory(log_dir)

        log_path = f"{log_dir}/run_id={safe_run_id}.json"

        payload = dict(report)
        payload["log_path"] = log_path
        payload["dag_id"] = dag_id
        payload["task_id"] = task_id
        payload["run_id"] = run_id
        payload["started_at_utc"] = _to_iso(payload.get("started_at_utc"))
        payload["ended_at_utc"] = _to_iso(payload.get("ended_at_utc"))
        payload["log_written_at_utc"] = datetime.now(timezone.utc).isoformat()

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        file_client = self.fs_client.get_file_client(log_path)
        file_client.upload_data(data, overwrite=True)

        return log_path

    def _ensure_directory(self, directory_path: str) -> None:
        normalized = directory_path.strip("/")
        current = ""

        for segment in normalized.split("/"):
            current = f"{current}/{segment}" if current else segment
            dir_client = self.fs_client.get_directory_client(current)

            try:
                dir_client.create_directory()
            except ResourceExistsError:
                pass


def _payload_is_empty(payload: Any) -> bool:
    if payload is None:
        return True
    if isinstance(payload, (list, dict, str)):
        return len(payload) == 0
    return False


def _to_iso(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value
