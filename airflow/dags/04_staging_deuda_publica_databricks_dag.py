from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime

# dag transformacion de datos del datalake raw - a staging
with DAG(
    dag_id="05_staging_deuda_publica_databricks",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["staging", "databricks", "azure"],
) as dag:

    run_raw_to_staging = DatabricksSubmitRunOperator(
        task_id="run_raw_to_staging_parquet",
        databricks_conn_id="databricks_hacienda",
        json={
            "run_name": "raw_to_staging_deuda_publica",
            "tasks": [
                {
                    "task_key": "transform_raw_to_staging",
                    "existing_cluster_id": "0124-102720-469koryx",
                    "notebook_task": {
                        "notebook_path": "/Users/samu_junior95@hotmail.com/01_raw_to_stg_deuda_publica",
                        "base_parameters": {
                            "execution_date": "2026-01-22",
                            "raw_container": "raw",
                            "staging_container": "staging",
                            "storage_account": "{{ var.value.azure_storage_account }}",
                            "dataset": "deuda_publica"
                        }
                    }
                }
            ]
        }
    )
