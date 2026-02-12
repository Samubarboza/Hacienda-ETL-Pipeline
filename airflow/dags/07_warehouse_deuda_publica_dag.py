from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.trigger_rule import TriggerRule

from warehouse.warehouse_audit_service import WarehouseRunAuditor


DAG_ID = "07_warehouse_deuda_publica"
DATASET_NAME = "deuda_publica"
LAYER_NAME = "warehouse"
WAREHOUSE_MODEL_TASK_ID = "create_warehouse_model"
WAREHOUSE_LOAD_DATABRICKS_TASK_ID = "run_curated_to_warehouse_databricks"
AUDIT_TASK_ID = "audit_warehouse_run"

DATABRICKS_JOB_JSON = {
    "run_name": "curated_to_warehouse_deuda_publica",
    "tasks": [
        {
            "task_key": "curated_to_warehouse",
            "existing_cluster_id": "0124-102720-469koryx",
            "notebook_task": {
                "notebook_path": "/Users/samu_junior95@hotmail.com/03_curated_to_warehouse_deuda_publica",
                "base_parameters": {
                    "execution_date": "{{ ds }}",
                },
            },
        }
    ],
}


def audit_warehouse_run(**context):
    auditor = WarehouseRunAuditor(
        dataset_name=DATASET_NAME,
        layer_name=LAYER_NAME,
        databricks_task_id=WAREHOUSE_LOAD_DATABRICKS_TASK_ID,
    )
    auditor.audit_from_context(context)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["warehouse", "sqlserver", "databricks"],
    template_searchpath="/opt/airflow/sql",
) as dag:

    create_warehouse_model = MsSqlOperator(
        task_id=WAREHOUSE_MODEL_TASK_ID,
        mssql_conn_id="sqlserver_hacienda",
        sql="warehouse/10_create_warehouse_model.sql",
    )

    run_curated_to_warehouse_databricks = DatabricksSubmitRunOperator(
        task_id=WAREHOUSE_LOAD_DATABRICKS_TASK_ID,
        databricks_conn_id="databricks_hacienda",
        json=DATABRICKS_JOB_JSON,
    )

    audit_warehouse = PythonOperator(
        task_id=AUDIT_TASK_ID,
        python_callable=audit_warehouse_run,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_warehouse_model >> run_curated_to_warehouse_databricks >> audit_warehouse
