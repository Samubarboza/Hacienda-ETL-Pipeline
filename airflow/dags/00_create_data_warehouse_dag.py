from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

DAG_ID = "00_create_data_warehouse"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    template_searchpath="/opt/airflow/sql",
    tags=["bootstrap", "warehouse"],
) as dag:

    create_data_warehouse = SQLExecuteQueryOperator(
        task_id="create_data_warehouse",
        conn_id="sqlserver_hacienda",
        sql="warehouse/00_create_data_warehouse.sql",
        autocommit=True,
    )
