import json
from datetime import datetime, timezone

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from audit.audit_logger import AuditLogger


def _safe_run_id(value):
    if not value:
        return f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    return (
        value.replace("/", "_")
        .replace(":", "_")
        .replace("+", "_")
        .replace("=", "_")
    )


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


def _to_utc_from_ms(epoch_ms):
    if not epoch_ms:
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)


class WarehouseRunAuditor:
    def __init__(self, *, dataset_name, layer_name, databricks_task_id):
        self.dataset_name = dataset_name
        self.layer_name = layer_name
        self.databricks_task_id = databricks_task_id

    def audit_from_context(self, context):
        execution_date = context.get("ds")
        dag = context.get("dag")
        dag_id = dag.dag_id if dag else context.get("dag_id")
        airflow_run_id = context.get("run_id")

        ti = context["ti"]
        parent_run_id = self._extract_databricks_parent_run_id(ti)
        metrics = self._extract_metrics_from_databricks(parent_run_id)

        payload = {
            "event_type": "warehouse_load",
            "layer": self.layer_name,
            "dataset_name": self.dataset_name,
            "execution_date": execution_date,
            "dag_id": dag_id,
            "task_id": self.databricks_task_id,
            "airflow_run_id": airflow_run_id,
            "databricks_run_id": metrics.get("databricks_run_id"),
            "databricks_run_page_url": metrics.get("databricks_run_page_url"),
            "rows_read": int(metrics.get("rows_read", 0) or 0),
            "rows_inserted": int(metrics.get("rows_inserted", 0) or 0),
            "rows_updated": int(metrics.get("rows_updated", 0) or 0),
            "rows_written": int(metrics.get("rows_written", 0) or 0),
            "status": metrics.get("status", "FAILED"),
            "message": metrics.get("message"),
            "started_at_utc": metrics.get("started_at_utc") or datetime.now(timezone.utc),
            "ended_at_utc": metrics.get("ended_at_utc") or datetime.now(timezone.utc),
            "audited_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        adls_log_path = self._write_adls_audit_log(
            payload,
            execution_date=execution_date,
            airflow_run_id=airflow_run_id,
        )
        payload["adls_log_path"] = adls_log_path

        self._write_sql_audit_log(payload)

        if payload["status"] != "SUCCESS":
            raise AirflowException(payload.get("message") or "Warehouse load failed")

    def _extract_databricks_parent_run_id(self, ti):
        run_id = ti.xcom_pull(task_ids=self.databricks_task_id)

        if not run_id:
            run_id = ti.xcom_pull(task_ids=self.databricks_task_id, key="run_id")

        if isinstance(run_id, dict):
            run_id = run_id.get("run_id")

        if isinstance(run_id, str):
            run_id = int(run_id) if run_id.isdigit() else None

        if isinstance(run_id, int):
            return run_id
        return None

    def _extract_metrics_from_databricks(self, run_id):
        result = {
            "databricks_run_id": run_id,
            "databricks_run_page_url": None,
            "status": "FAILED",
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_written": 0,
            "message": "No Databricks run_id found.",
            "started_at_utc": datetime.now(timezone.utc),
            "ended_at_utc": datetime.now(timezone.utc),
        }

        if not run_id:
            return result

        hook = DatabricksHook(databricks_conn_id="databricks_hacienda")
        run_info = hook.get_run(run_id)

        state = run_info.get("state", {})
        tasks = run_info.get("tasks") or []
        task_info = tasks[0] if tasks else {}
        task_state = task_info.get("state", {})

        task_run_id = task_info.get("run_id") or run_id
        result_state = task_state.get("result_state") or state.get("result_state")
        state_message = task_state.get("state_message") or state.get("state_message")

        result["databricks_run_page_url"] = run_info.get("run_page_url")
        result["message"] = state_message or "Run completed"
        result["status"] = "SUCCESS" if result_state == "SUCCESS" else "FAILED"
        result["started_at_utc"] = _to_utc_from_ms(run_info.get("start_time"))
        result["ended_at_utc"] = _to_utc_from_ms(run_info.get("end_time"))

        try:
            output = hook.get_run_output(task_run_id)
        except Exception:
            output = {}

        notebook_result = ((output or {}).get("notebook_output") or {}).get("result")
        if notebook_result:
            try:
                metrics = json.loads(notebook_result)
                rows_read = int(metrics.get("rows_read", metrics.get("records_read", 0)) or 0)
                rows_inserted = int(metrics.get("rows_inserted", metrics.get("inserted", 0)) or 0)
                rows_updated = int(metrics.get("rows_updated", metrics.get("updated", 0)) or 0)
                rows_written = int(
                    metrics.get("rows_written", rows_inserted + rows_updated) or 0
                )

                result["rows_read"] = rows_read
                result["rows_inserted"] = rows_inserted
                result["rows_updated"] = rows_updated
                result["rows_written"] = rows_written

                metric_status = metrics.get("status")
                if metric_status in {"SUCCESS", "FAILED"}:
                    result["status"] = metric_status

                metric_message = metrics.get("message")
                if metric_message:
                    result["message"] = metric_message
            except (TypeError, ValueError, json.JSONDecodeError):
                pass

        return result

    def _write_adls_audit_log(self, payload, *, execution_date, airflow_run_id):
        storage_account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")
        file_system_name = Variable.get("AZURE_FILE_SYSTEM_NAME")

        azure_conn = BaseHook.get_connection("azure_datalake_conn")
        credential = ClientSecretCredential(
            tenant_id=azure_conn.extra_dejson["tenant_id"],
            client_id=azure_conn.login,
            client_secret=azure_conn.password,
        )

        service_client = DataLakeServiceClient(account_url=storage_account_url, credential=credential)
        fs_client = service_client.get_file_system_client(file_system=file_system_name)

        safe_run_id = _safe_run_id(airflow_run_id)
        log_dir = f"logs/{self.dataset_name}/layer={self.layer_name}/execution_date={execution_date}"
        _ensure_directory(fs_client, log_dir)

        log_path = f"{log_dir}/run_id={safe_run_id}.json"

        body = dict(payload)
        body["logged_at_utc"] = datetime.now(timezone.utc).isoformat()

        content = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
        fs_client.get_file_client(log_path).upload_data(content, overwrite=True)

        return log_path

    def _write_sql_audit_log(self, payload):
        details = {
            "event_type": payload.get("event_type"),
            "layer": payload.get("layer"),
            "rows_read": payload.get("rows_read"),
            "rows_inserted": payload.get("rows_inserted"),
            "rows_updated": payload.get("rows_updated"),
            "rows_written": payload.get("rows_written"),
            "databricks_run_id": payload.get("databricks_run_id"),
            "databricks_run_page_url": payload.get("databricks_run_page_url"),
            "message": payload.get("message"),
            "adls_log_path": payload.get("adls_log_path"),
        }

        error_message = payload.get("message") if payload.get("status") != "SUCCESS" else None

        AuditLogger().log_etl_run(
            dag_id=payload.get("dag_id"),
            task_id=payload.get("task_id"),
            run_id=payload.get("airflow_run_id"),
            execution_date=payload.get("execution_date"),
            status=payload.get("status"),
            started_at_utc=payload.get("started_at_utc"),
            ended_at_utc=payload.get("ended_at_utc"),
            dataset_name=self.dataset_name,
            stage=self.layer_name,
            source="adls_curated",
            target="sql_warehouse",
            error_message=error_message,
            details=details,
            raw_path=None,
        )
