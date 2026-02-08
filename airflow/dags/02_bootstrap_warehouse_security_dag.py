from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


BOOTSTRAP_SQL = """USE hacienda_dw;

IF NOT EXISTS (
    SELECT 1
    FROM sys.symmetric_keys
    WHERE name = '##MS_DatabaseMasterKey##'
)
BEGIN
    CREATE MASTER KEY
    ENCRYPTION BY PASSWORD = 'TempStrongPassword_OnlyBootstrap_123!';
END;
"""


with DAG(
    dag_id="02_bootstrap_warehouse_security",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    bootstrap_warehouse_security = SQLExecuteQueryOperator(
        task_id="bootstrap_warehouse_security",
        conn_id="sqlserver_hacienda",
        sql=BOOTSTRAP_SQL,
    )
