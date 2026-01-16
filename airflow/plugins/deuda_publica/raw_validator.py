from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.exceptions import AirflowException
from datetime import datetime, timezone
from deuda_publica.audit_log import AuditLogger

DATASET_NAME = "deuda_publica"
TOTAL_PAGES = 166 # para pruebas modificamos el total page a 167

class RawDeudaPublicaValidator:
    def __init__(self, execution_date: str, mssql_conn_id: str = "sqlserver_hacienda"):
        self.execution_date = execution_date
        self.hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
        self.audit = AuditLogger(mssql_conn_id=mssql_conn_id)

    def _get_loaded_pages(self) -> set[int]:
        sql = """
        USE hacienda_dw;
        
        SELECT DISTINCT page_number
        FROM raw.raw_deuda_publica
        WHERE execution_date = %s
        """
        rows = self.hook.get_records(sql, parameters=(self.execution_date,))
        return {int(r[0]) for r in rows}

    def _get_row_count(self) -> int:
        sql = """
        USE hacienda_dw;
        
        SELECT COUNT(*)
        FROM raw.raw_deuda_publica
        WHERE execution_date = %s
        """
        return int(self.hook.get_first(sql, parameters=(self.execution_date,))[0])

    def validate_or_fail(self) -> None:
        started = datetime.now(timezone.utc)

        self.audit.ensure_table()

        status = "SUCCESS"
        error_message = None
        missing_pages_str = None

        try:
            loaded_pages = self._get_loaded_pages()
            rows_loaded = self._get_row_count()

            if rows_loaded <= 0:
                raise AirflowException("RAW vacío para execution_date (rows_loaded=0).")

            expected_pages = set(range(1, TOTAL_PAGES + 1))
            missing_pages = sorted(list(expected_pages - loaded_pages))

            if missing_pages:
                missing_pages_str = ",".join(map(str, missing_pages[:50]))
                extra = "" if len(missing_pages) <= 50 else f"...(+{len(missing_pages)-50})"
                raise AirflowException(
                    f"RAW incompleto: faltan {len(missing_pages)} páginas. Ej: {missing_pages_str}{extra}"
                )

            ended = datetime.now(timezone.utc)
            self.audit.log_run(
                pipeline_name="airflow_raw_validation",
                dataset_name=DATASET_NAME,
                execution_date=self.execution_date,
                status=status,
                started_at_utc=started.isoformat(),
                ended_at_utc=ended.isoformat(),
                pages_expected=TOTAL_PAGES,
                pages_loaded=len(loaded_pages),
                rows_loaded=rows_loaded,
                missing_pages=None,
                error_message=None,
            )

        except Exception as e:
            status = "FAILED"
            error_message = str(e)
            ended = datetime.now(timezone.utc)

            # Intento de auditoría incluso si falla
            try:
                loaded_pages = self._get_loaded_pages()
                rows_loaded = self._get_row_count()
                self.audit.log_run(
                    pipeline_name="airflow_raw_validation",
                    dataset_name=DATASET_NAME,
                    execution_date=self.execution_date,
                    status=status,
                    started_at_utc=started.isoformat(),
                    ended_at_utc=ended.isoformat(),
                    pages_expected=TOTAL_PAGES,
                    pages_loaded=len(loaded_pages),
                    rows_loaded=rows_loaded,
                    missing_pages=missing_pages_str,
                    error_message=error_message,
                )
            finally:
                raise
