from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

class AuditLogger:
    def __init__(self):
        self.hook = MsSqlHook(mssql_conn_id="sqlserver_hacienda")

    def start_run(self, dag_id, task_id, run_id, execution_date, source, target):
        sql = """
        USE hacienda_dw;
        INSERT INTO audit.pipeline_runs (
            pipeline_name,
            stage,
            dataset_name,
            execution_date,
            status,
            started_at_utc,
            error_message
        )
        OUTPUT INSERTED.audit_id
        VALUES (%s, %s, %s, %s, 'STARTED', %s, NULL)
        """
        return self.hook.get_first(sql, parameters=(dag_id, task_id, run_id, execution_date, datetime.utcnow()))[0]

    def finish_success(self, pipeline_run_id, records_loaded):
        sql = """
        USE hacienda_dw;
        UPDATE audit.pipeline_runs
        SET status = 'SUCCESS',
            ended_at_utc = %s
        WHERE audit_id = %s
        """
        self.hook.run(sql, parameters=(datetime.utcnow(), pipeline_run_id))

    def finish_failure(self, pipeline_run_id, error_message):
        sql = """
        USE hacienda_dw;
        UPDATE audit.pipeline_runs
        SET status = 'FAILED',
            ended_at_utc = %s,
            error_message = %s
        WHERE audit_id = %s
        """
        self.hook.run(sql, parameters=(datetime.utcnow(), error_message, pipeline_run_id))