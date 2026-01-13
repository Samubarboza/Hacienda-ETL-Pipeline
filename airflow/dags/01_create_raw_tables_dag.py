from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

with DAG(
    dag_id="01_create_raw_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bootstrap", "raw"],
    template_searchpath="/opt/airflow/sql",
) as dag:

    create_raw_table = MsSqlOperator(
        task_id="create_raw_deuda_publica",
        mssql_conn_id="sqlserver_hacienda",
        sql="raw/01_create_raw_deuda_publica.sql",
    )
