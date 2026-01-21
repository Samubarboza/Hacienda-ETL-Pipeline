from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from deuda_publica.raw_validator import RawDeudaPublicaValidator

def run_validation(**context):
    execution_date = context["ds"]  # YYYY-MM-DD (execution_date lógico)
    RawDeudaPublicaValidator(execution_date=execution_date).validate_or_fail()

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
    )
