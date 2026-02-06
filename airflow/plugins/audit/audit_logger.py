import json
from datetime import datetime
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

class AuditLogger:
    def __init__(self):
        self.hook = MsSqlHook(mssql_conn_id="sqlserver_hacienda")
        self._etl_run_log_columns = None

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

    def log_etl_run(
        self,
        *,
        dag_id,
        task_id,
        run_id,
        execution_date,
        status,
        started_at_utc,
        ended_at_utc,
        dataset_name,
        stage,
        source=None,
        target=None,
        error_message=None,
        details=None,
        raw_path=None,
    ):
        columns = self._get_etl_run_log_columns()
        if not columns:
            raise RuntimeError("No se encontraron columnas para audit.etl_run_log")

        execution_date_value = execution_date
        if isinstance(execution_date, str):
            try:
                execution_date_value = datetime.strptime(execution_date, "%Y-%m-%d").date()
            except ValueError:
                execution_date_value = execution_date

        details_json = None
        if details is not None:
            try:
                details_json = json.dumps(details, ensure_ascii=False, default=str)
            except TypeError:
                details_json = json.dumps(str(details), ensure_ascii=False)

        values = {
            "pipeline_name": dag_id,
            "pipeline": dag_id,
            "dag_id": dag_id,
            "task_id": task_id,
            "run_id": run_id,
            "execution_date": execution_date_value,
            "status": status,
            "stage": stage,
            "dataset_name": dataset_name,
            "dataset": dataset_name,
            "source": source,
            "target": target,
            "started_at_utc": started_at_utc,
            "ended_at_utc": ended_at_utc,
            "error_message": error_message,
            "details_json": details_json,
            "details": details_json,
            "raw_path": raw_path,
        }

        required_fields = ["status", "stage", "dataset_name", "execution_date", "started_at_utc", "ended_at_utc"]
        missing_required = [name for name in required_fields if values.get(name) is None]
        if missing_required:
            raise ValueError(f"Faltan campos obligatorios para audit.etl_run_log: {missing_required}")

        insert_cols = [col for col in columns if col in values and values[col] is not None]
        if not insert_cols:
            raise RuntimeError("audit.etl_run_log no tiene columnas compatibles para insertar")

        placeholders = ", ".join(["%s"] * len(insert_cols))
        cols_sql = ", ".join(insert_cols)
        params = tuple(values[col] for col in insert_cols)

        sql = f"""
        USE hacienda_dw;
        INSERT INTO audit.etl_run_log ({cols_sql})
        VALUES ({placeholders})
        """
        self.hook.run(sql, parameters=params)

    def _get_etl_run_log_columns(self):
        if self._etl_run_log_columns is not None:
            return self._etl_run_log_columns

        sql = """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'audit'
          AND TABLE_NAME = 'etl_run_log'
        """
        rows = self.hook.get_records(sql)
        self._etl_run_log_columns = [row[0] for row in rows] if rows else []
        return self._etl_run_log_columns
