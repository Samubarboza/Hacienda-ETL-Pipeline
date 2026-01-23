from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from deuda_publica.raw_validator import RawDeudaPublicaValidator
from airflow.models import Variable
from airflow.hooks.base import BaseHook

def run_validation(**context):
    execution_date = context["ds"]

    storage_account_url = Variable.get("AZURE_STORAGE_ACCOUNT_URL")
    file_system_name = Variable.get("AZURE_FILE_SYSTEM_NAME")

    azure_conn = BaseHook.get_connection("azure_datalake_conn")

    credential = {
        "tenant_id": azure_conn.extra_dejson["tenant_id"],
        "client_id": azure_conn.login,
        "client_secret": azure_conn.password,
    }

    RawDeudaPublicaValidator(storage_account_url=storage_account_url, file_system_name=file_system_name, credential=credential, ).validate_or_fail()

with DAG(
    dag_id="03_validate_raw_deuda_publica",
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
