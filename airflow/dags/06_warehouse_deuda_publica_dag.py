import json
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.trigger_rule import TriggerRule
from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

from audit.audit_logger import AuditLogger


DAG_ID = "06_warehouse_deuda_publica"
DATASET_NAME = "deuda_publica"
LAYER_NAME = "warehouse"
CURATED_CHECK_TASK_ID = "check_curated_exists"
WAREHOUSE_LOAD_TASK_ID = "run_warehouse_load"


def _get_storage_account_name() -> str:
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


def _build_fs_client(file_system_name: str):
    storage_account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")

    azure_conn = BaseHook.get_connection("azure_datalake_conn")
    credential = ClientSecretCredential(
        tenant_id=azure_conn.extra_dejson["tenant_id"],
        client_id=azure_conn.login,
        client_secret=azure_conn.password,
    )

    service_client = DataLakeServiceClient(account_url=storage_account_url, credential=credential)
    return service_client.get_file_system_client(file_system=file_system_name)


def _ensure_directory(fs_client, directory_path: str) -> None:
    normalized = directory_path.strip("/")
    current = ""

    for segment in normalized.split("/"):
        current = f"{current}/{segment}" if current else segment
        dir_client = fs_client.get_directory_client(current)
        try:
            dir_client.create_directory()
        except ResourceExistsError:
            pass


def _safe_run_id(value: str) -> str:
    if not value:
        return f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    return (
        value.replace("/", "_")
        .replace(":", "_")
        .replace("+", "_")
        .replace("=", "_")
    )


def _load_sql_script(file_name: str) -> str:
    container_path = Path("/opt/airflow/sql/warehouse") / file_name
    local_path = Path(__file__).resolve().parents[1] / "sql" / "warehouse" / file_name

    target = container_path if container_path.exists() else local_path
    if not target.exists():
        raise AirflowException(f"SQL script not found: {file_name}")

    return target.read_text(encoding="utf-8")


def check_curated_exists(**context):
    execution_date = context["ds"]
    curated_file_system = Variable.get("AZURE_CURATED_FILE_SYSTEM_NAME", default_var="curated")

    fs_client = _build_fs_client(curated_file_system)

    curated_path = f"{DATASET_NAME}/execution_date={execution_date}"

    try:
        paths = list(fs_client.get_paths(path=curated_path, recursive=False))
    except Exception as exc:
        raise AirflowException(f"Unable to read curated path: {curated_path}. Error: {exc}") from exc
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


def run_warehouse_load(**context):
    execution_date = context["ds"]
    storage_account = _get_storage_account_name()
    curated_container = Variable.get("AZURE_CURATED_FILE_SYSTEM_NAME", default_var="curated")
    sas_token = Variable.get("AZURE_STORAGE_SAS_TOKEN", default_var="")

    sql = _load_sql_script("20_load_warehouse_from_curated.sql")
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


def audit_warehouse_run(**context):
    dag_run = context["dag_run"]
    execution_date = context["ds"]
    airflow_run_id = context.get("run_id")
    dag_id = dag_run.dag_id

    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids=WAREHOUSE_LOAD_TASK_ID) or {}

    rows_read = int(metrics.get("rows_read", 0) or 0)
    rows_inserted = int(metrics.get("rows_inserted", 0) or 0)
    rows_updated = int(metrics.get("rows_updated", 0) or 0)

    tracked_tasks = {
        CURATED_CHECK_TASK_ID,
        "create_warehouse_model",
        WAREHOUSE_LOAD_TASK_ID,
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
        "layer": LAYER_NAME,
        "dataset_name": DATASET_NAME,
        "execution_date": execution_date,
        "dag_id": dag_id,
        "task_id": WAREHOUSE_LOAD_TASK_ID,
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

    # ADLS JSON audit log
    logs_file_system = Variable.get("AZURE_FILE_SYSTEM_NAME", default_var="raw")
    fs_client = _build_fs_client(logs_file_system)

    log_dir = f"logs/{DATASET_NAME}/layer={LAYER_NAME}/execution_date={execution_date}"
    _ensure_directory(fs_client, log_dir)
    log_path = f"{log_dir}/run_id={_safe_run_id(airflow_run_id)}.json"

    fs_client.get_file_client(log_path).upload_data(
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        overwrite=True,
    )

    # SQL audit log
    details = {
        "layer": LAYER_NAME,
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
        task_id=WAREHOUSE_LOAD_TASK_ID,
        run_id=airflow_run_id,
        execution_date=execution_date,
        status=status,
        started_at_utc=started_at,
        ended_at_utc=ended_at,
        dataset_name=DATASET_NAME,
        stage=LAYER_NAME,
        source="adls_curated",
        target="sql_warehouse",
        error_message=message if status == "FAILED" else None,
        details=details,
        raw_path=f"{DATASET_NAME}/execution_date={execution_date}",
    )

    if status != "SUCCESS":
        raise AirflowException(message)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["warehouse", "sqlserver", "curated"],
    template_searchpath="/opt/airflow/sql",
) as dag:

    check_curated_input = PythonOperator(
        task_id=CURATED_CHECK_TASK_ID,
        python_callable=check_curated_exists,
        provide_context=True,
    )

    create_warehouse_model = MsSqlOperator(
        task_id="create_warehouse_model",
        mssql_conn_id="sqlserver_hacienda",
        sql="warehouse/10_create_warehouse_model.sql",
    )

    run_warehouse_load_task = PythonOperator(
        task_id=WAREHOUSE_LOAD_TASK_ID,
        python_callable=run_warehouse_load,
        provide_context=True,
    )

    audit_warehouse = PythonOperator(
        task_id="audit_warehouse_run",
        python_callable=audit_warehouse_run,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )

    check_curated_input >> create_warehouse_model >> run_warehouse_load_task >> audit_warehouse
