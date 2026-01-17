from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

with DAG(
    dag_id="05_staging_deuda_publica_databricks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["staging", "databricks"],
    template_searchpath="/opt/airflow/sql",
) as dag:

    create_stg_tables = SQLExecuteQueryOperator(
        task_id="create_stg_tables",
        conn_id="sqlserver_hacienda",
        sql="staging/10_create_stg_deuda_publica.sql",
        hook_params={"schema": "hacienda_dw"},
    )

    run_databricks = DatabricksSubmitRunOperator(
    task_id="run_raw_to_stg",
    databricks_conn_id="databricks_hacienda",
    json={
    "run_name": "raw_to_stg_deuda_publica",
    "tasks": [
        {
            "task_key": "transform_raw_to_stg",
            "warehouse_id": "9f5aee4d22b5dec7",
            "notebook_task": {
                "notebook_path": "/Workspace/Users/samubjunior95@gmail.com/01_raw_to_stg_deuda_publica",
                "base_parameters": {
                    "execution_date": "{{ ds }}",
                    "jdbc_host": "sqlserver",
                    "jdbc_port": "1433",
                    "jdbc_db": "hacienda_dw",
                    "jdbc_user": "sa",
                    "jdbc_pass": "{{ var.value.sqlserver_password }}"
                }
            }
        }
    ]
}
)

    create_stg_tables >> run_databricks