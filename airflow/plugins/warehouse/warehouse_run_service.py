import json
from datetime import datetime, timezone
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from audit.audit_logger import AuditLogger


class WarehouseRunService:
    def __init__(self, *, dataset_name, layer_name, curated_check_task_id, warehouse_load_task_id, warehouse_model_task_id):
        self.dataset_name = dataset_name
        self.layer_name = layer_name
        self.curated_check_task_id = curated_check_task_id
        self.warehouse_load_task_id = warehouse_load_task_id
        self.warehouse_model_task_id = warehouse_model_task_id

    def check_curated_exists(self, **context):
        execution_date = context["ds"]
        curated_file_system = Variable.get("AZURE_CURATED_FILE_SYSTEM_NAME", default_var="curated")

        fs_client = self._build_fs_client(curated_file_system)
        curated_path = f"{self.dataset_name}/execution_date={execution_date}"

        try:
            paths = list(fs_client.get_paths(path=curated_path, recursive=False))
        except Exception as exc:
            raise AirflowException(
                f"Unable to read curated path: {curated_path}. Error: {exc}"
            ) from exc

        if not paths:
            raise AirflowException(f"Curated path does not exist: {curated_path}")

        expected_tables = {"prestamo", "tramo", "acreedor", "deudor", "ley", "deuda_corte"}
        found_tables = set()

        for item in paths:
            suffix = item.name.replace(curated_path + "/", "")
            segment = suffix.split("/")[0]
            if segment:
                found_tables.add(segment)

        missing = sorted(expected_tables - found_tables)
        if missing:
            raise AirflowException(
                f"Curated execution_date is incomplete. Missing tables: {missing}. Path: {curated_path}"
            )

        return {"curated_path": curated_path}

    def run_warehouse_load(self, **context):
        execution_date = context["ds"]
        storage_account = self._get_storage_account_name()
        curated_container = Variable.get("AZURE_CURATED_FILE_SYSTEM_NAME", default_var="curated")
        sas_token = Variable.get("AZURE_STORAGE_SAS_TOKEN", default_var="")

        sql = self._load_sql_script("20_load_warehouse_from_curated.sql")
        hook = MsSqlHook(mssql_conn_id="sqlserver_hacienda")

        row = hook.get_first(
            sql,
            parameters=(execution_date, storage_account, curated_container, sas_token),
        )

        if not row:
            raise AirflowException("Warehouse load script did not return metrics row.")

        rows_read, rows_inserted, rows_updated = row

        return {
            "rows_read": int(rows_read or 0),
            "rows_inserted": int(rows_inserted or 0),
            "rows_updated": int(rows_updated or 0),
        }

    def audit_warehouse_run(self, **context):
        dag_run = context["dag_run"]
        execution_date = context["ds"]
        airflow_run_id = context.get("run_id")
        dag_id = dag_run.dag_id

        ti = context["ti"]
        metrics = ti.xcom_pull(task_ids=self.warehouse_load_task_id) or {}

        rows_read = int(metrics.get("rows_read", 0) or 0)
        rows_inserted = int(metrics.get("rows_inserted", 0) or 0)
        rows_updated = int(metrics.get("rows_updated", 0) or 0)

        tracked_tasks = {
            self.curated_check_task_id,
            self.warehouse_model_task_id,
            self.warehouse_load_task_id,
        }

        failed_tis = [
            task_instance
            for task_instance in dag_run.get_task_instances()
            if task_instance.task_id in tracked_tasks
            and task_instance.state in {"failed", "upstream_failed"}
        ]

        status = "FAILED" if failed_tis else "SUCCESS"
        message = "warehouse load completed"
        if failed_tis:
            first_failed = sorted(
                failed_tis,
                key=lambda x: x.end_date or datetime.now(timezone.utc),
            )[0]
            message = f"Task failed: {first_failed.task_id}"

        started_at = dag_run.start_date or datetime.now(timezone.utc)
        ended_at = datetime.now(timezone.utc)

        payload = {
            "event_type": "warehouse_load",
            "layer": self.layer_name,
            "dataset_name": self.dataset_name,
            "execution_date": execution_date,
            "dag_id": dag_id,
            "task_id": self.warehouse_load_task_id,
            "airflow_run_id": airflow_run_id,
            "rows_read": rows_read,
            "inserted": rows_inserted,
            "updated": rows_updated,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "status": status,
            "message": message,
            "started_at_utc": started_at.isoformat(),
            "ended_at_utc": ended_at.isoformat(),
        }

        logs_file_system = Variable.get("AZURE_FILE_SYSTEM_NAME", default_var="raw")
        fs_client = self._build_fs_client(logs_file_system)

        log_dir = f"logs/{self.dataset_name}/layer={self.layer_name}/execution_date={execution_date}"
        self._ensure_directory(fs_client, log_dir)
        log_path = f"{log_dir}/run_id={self._safe_run_id(airflow_run_id)}.json"

        fs_client.get_file_client(log_path).upload_data(
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            overwrite=True,
        )

        details = {
            "layer": self.layer_name,
            "rows_read": rows_read,
            "inserted": rows_inserted,
            "updated": rows_updated,
            "rows_inserted": rows_inserted,
            "rows_updated": rows_updated,
            "message": message,
            "adls_log_path": log_path,
        }

        AuditLogger().log_etl_run(
            dag_id=dag_id,
            task_id=self.warehouse_load_task_id,
            run_id=airflow_run_id,
            execution_date=execution_date,
            status=status,
            started_at_utc=started_at,
            ended_at_utc=ended_at,
            dataset_name=self.dataset_name,
            stage=self.layer_name,
            source="adls_curated",
            target="sql_warehouse",
            error_message=message if status == "FAILED" else None,
            details=details,
            raw_path=f"{self.dataset_name}/execution_date={execution_date}",
        )

        if status != "SUCCESS":
            raise AirflowException(message)

    @staticmethod
    def _get_storage_account_name():
        raw_value = Variable.get("azure_storage_account", default_var=None)
        if raw_value:
            account = (
                raw_value.replace("https://", "")
                .replace(".dfs.core.windows.net", "")
                .replace(".blob.core.windows.net", "")
                .replace("/", "")
                .strip()
            )
            if account:
                return account

        account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")
        if "https://" in account_url:
            return account_url.replace("https://", "").split(".")[0]

        raise AirflowException("Unable to resolve storage account name from Airflow variables.")

    @staticmethod
    def _build_fs_client(file_system_name):
        storage_account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")

        azure_conn = BaseHook.get_connection("azure_datalake_conn")
        credential = ClientSecretCredential(
            tenant_id=azure_conn.extra_dejson["tenant_id"],
            client_id=azure_conn.login,
            client_secret=azure_conn.password,
        )

        service_client = DataLakeServiceClient(account_url=storage_account_url, credential=credential)
        return service_client.get_file_system_client(file_system=file_system_name)

    @staticmethod
    def _ensure_directory(fs_client, directory_path):
        normalized = directory_path.strip("/")
        current = ""

        for segment in normalized.split("/"):
            current = f"{current}/{segment}" if current else segment
            dir_client = fs_client.get_directory_client(current)
            try:
                dir_client.create_directory()
            except ResourceExistsError:
                pass

    @staticmethod
    def _safe_run_id(value):
        if not value:
            return f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        return (
            value.replace("/", "_")
            .replace(":", "_")
            .replace("+", "_")
            .replace("=", "_")
        )

    @staticmethod
    def _load_sql_script(file_name):
        container_path = Path("/opt/airflow/sql/warehouse") / file_name
        local_path = Path(__file__).resolve().parents[2] / "sql" / "warehouse" / file_name

        target = container_path if container_path.exists() else local_path
        if not target.exists():
            raise AirflowException(f"SQL script not found: {file_name}")

        return target.read_text(encoding="utf-8")
