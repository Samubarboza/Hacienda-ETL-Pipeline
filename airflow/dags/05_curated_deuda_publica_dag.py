from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.trigger_rule import TriggerRule

from curated.curated_audit_service import CuratedRunAuditor

# Constantes de configuración del DAG curated que definen identificadores de flujo, dataset, capa y task de Databricks
DAG_ID = "05_curated_deuda_publica"
DATASET_NAME = "deuda_publica"
LAYER_NAME = "curated"
DATABRICKS_TASK_ID = "run_staging_to_curated"

# Configuración del job de Databricks que define qué notebook correr, en qué cluster y con qué parámetros para transformar de staging a curated
DATABRICKS_JOB_JSON = {
    "run_name": "staging_to_curated_deuda_publica",
    "tasks": [
        {
            "task_key": "transform_staging_to_curated",
            "existing_cluster_id": "0124-102720-469koryx",
            "notebook_task": {
                "notebook_path": "/Users/samu_junior95@hotmail.com/02_stg_to_curated_deuda_publica",
                "base_parameters": {
                    "execution_date": "{{ ds }}",
                    "staging_container": "staging",
                    "curated_container": "curated",
                    "storage_account": "{{ var.value.azure_storage_account | replace('https://', '') | replace('.dfs.core.windows.net', '') | replace('.blob.core.windows.net', '') | replace('/', '') }}",
                    "dataset": DATASET_NAME,
                },
            },
        }
    ],
}

# Función de task que instancia el CuratedRunAuditor y ejecuta la auditoría de la corrida curated usando el context de Airflow
def audit_curated_run(**context):
    auditor = CuratedRunAuditor(
        dataset_name=DATASET_NAME,
        layer_name=LAYER_NAME,
        databricks_task_id=DATABRICKS_TASK_ID,
    )
    auditor.audit_from_context(context)

# Define el DAG de Airflow que ejecuta el job de Databricks para pasar de staging a curated y luego corre la task de auditoría al finalizar
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["curated", "databricks", "azure"],
) as dag:
    run_staging_to_curated = DatabricksSubmitRunOperator(
        task_id=DATABRICKS_TASK_ID,
        databricks_conn_id="databricks_hacienda",
        json=DATABRICKS_JOB_JSON,
    )

    audit_run = PythonOperator(
        task_id="audit_curated_run",
        python_callable=audit_curated_run,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    run_staging_to_curated >> audit_run
