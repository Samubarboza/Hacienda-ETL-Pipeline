from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime

with DAG(
    dag_id="01_create_schemas",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bootstrap", "sqlserver"],
    template_searchpath="/opt/airflow/sql",
) as dag:

    create_schemas = MsSqlOperator(
        task_id="create_schemas",
        mssql_conn_id="sqlserver_hacienda",
        sql="schemas/01_create_schemas.sql",
    )
    
    create_audit_schema = MsSqlOperator(
    task_id="create_audit_schema",
    mssql_conn_id="sqlserver_hacienda",
    sql="schemas/02_create_audit_schema.sql",
    )

    create_schemas >> create_audit_schema

