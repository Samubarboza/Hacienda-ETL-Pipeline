from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.utils.trigger_rule import TriggerRule

from warehouse.warehouse_run_service import WarehouseRunService


DAG_ID = "07_warehouse_deuda_publica"
DATASET_NAME = "deuda_publica"
LAYER_NAME = "warehouse"
CURATED_CHECK_TASK_ID = "check_curated_exists"
WAREHOUSE_MODEL_TASK_ID = "create_warehouse_model"
WAREHOUSE_LOAD_TASK_ID = "run_warehouse_load"
AUDIT_TASK_ID = "audit_warehouse_run"

warehouse_service = WarehouseRunService(
    dataset_name=DATASET_NAME,
    layer_name=LAYER_NAME,
    curated_check_task_id=CURATED_CHECK_TASK_ID,
    warehouse_load_task_id=WAREHOUSE_LOAD_TASK_ID,
    warehouse_model_task_id=WAREHOUSE_MODEL_TASK_ID,
)

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
        python_callable=warehouse_service.check_curated_exists,
        provide_context=True,
    )

    create_warehouse_model = MsSqlOperator(
        task_id=WAREHOUSE_MODEL_TASK_ID,
        mssql_conn_id="sqlserver_hacienda",
        sql="warehouse/10_create_warehouse_model.sql",
    )

    run_warehouse_load_task = PythonOperator(
        task_id=WAREHOUSE_LOAD_TASK_ID,
        python_callable=warehouse_service.run_warehouse_load,
        provide_context=True,
    )

    audit_warehouse = PythonOperator(
        task_id=AUDIT_TASK_ID,
        python_callable=warehouse_service.audit_warehouse_run,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )

    check_curated_input >> create_warehouse_model >> run_warehouse_load_task >> audit_warehouse
