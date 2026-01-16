from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from deuda_publica.raw_loader import RawDeudaPublicaLoader

def ingest(**context):
    execution_date = context["ds"]
    loader = RawDeudaPublicaLoader(execution_date)
    loader.load_all()

with DAG(
    dag_id="03_ingest_deuda_publica_raw",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["raw", "odmh"]
) as dag:

    PythonOperator(
        task_id="ingest_raw_deuda_publica",
        python_callable=ingest
    )
