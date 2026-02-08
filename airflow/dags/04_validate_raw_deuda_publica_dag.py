from datetime import datetime, timezone

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from audit.audit_logger import AuditLogger
from deuda_publica.raw_validator import RawDeudaPublicaValidator

def run_validation(**context):
    execution_date = context.get("ds")
    dag = context.get("dag")
    task = context.get("task")
    dag_id = dag.dag_id if dag else context.get("dag_id")
    task_id = task.task_id if task else context.get("task_id")
    run_id = context.get("run_id")

    storage_account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")
    file_system_name = Variable.get("AZURE_FILE_SYSTEM_NAME")

    azure_conn = BaseHook.get_connection("azure_datalake_conn")

    credential = {
        "tenant_id": azure_conn.extra_dejson["tenant_id"],
        "client_id": azure_conn.login,
        "client_secret": azure_conn.password,
    }

    validator = RawDeudaPublicaValidator(
        storage_account_url=storage_account_url,
        file_system_name=file_system_name,
        credential=credential,
    )

    report = validator.validate(execution_date=execution_date)

    adls_error = None
    sql_error = None

    try:
        log_path = validator.write_audit_log(
            report=report,
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
        )
        report["log_path"] = log_path
    except Exception as exc:
        adls_error = exc
        report["status"] = "FAILED"
        report.setdefault("errors", []).append(f"Fallo escritura log ADLS: {exc}")
        report["error_message"] = "; ".join(report.get("errors", [])[:5])

    try:
        AuditLogger().log_etl_run(
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            execution_date=execution_date,
            status=report.get("status"),
            started_at_utc=report.get("started_at_utc"),
            ended_at_utc=report.get("ended_at_utc") or datetime.now(timezone.utc),
            dataset_name=report.get("dataset_name"),
            stage="raw_validation",
            source="adls_raw",
            target="adls_raw",
            error_message=report.get("error_message"),
            details=report,
            raw_path=report.get("raw_path"),
        )
    except Exception as exc:
        sql_error = exc

    if adls_error:
        raise AirflowException(f"Fallo escritura log ADLS: {adls_error}")
    if sql_error:
        raise AirflowException(f"Fallo auditoria SQL: {sql_error}")
    if report.get("status") != "SUCCESS":
        raise AirflowException(report.get("error_message") or "Validacion RAW fallida")

with DAG(
    dag_id="04_validate_raw_deuda_publica",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["raw", "validation", "odmh"],
) as dag:

    validate_raw = PythonOperator(
        task_id="validate_raw_ingestion",
        python_callable=run_validation,
        provide_context=True,
    )
