from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

DDL = """
USE hacienda_dw;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'etl_run_log' AND s.name = 'raw'
)
BEGIN
    CREATE TABLE raw.etl_run_log (
        run_id           UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        pipeline_name    VARCHAR(100) NOT NULL,
        dataset_name     VARCHAR(100) NOT NULL,
        execution_date   DATE NOT NULL,

        status           VARCHAR(20) NOT NULL, -- SUCCESS / FAILED
        started_at_utc   DATETIME2 NOT NULL,
        ended_at_utc     DATETIME2 NOT NULL,

        pages_expected   INT NOT NULL,
        pages_loaded     INT NOT NULL,
        rows_loaded      INT NOT NULL,
        missing_pages    NVARCHAR(MAX) NULL,
        error_message    NVARCHAR(MAX) NULL
    );

    CREATE INDEX ix_etl_run_log_execdate
        ON raw.etl_run_log (execution_date, dataset_name);
END
"""

class AuditLogger:
    def __init__(self, mssql_conn_id: str = "sqlserver_hacienda"):
        self.hook = MsSqlHook(mssql_conn_id=mssql_conn_id)

    def ensure_table(self) -> None:
        self.hook.run(DDL)

    def log_run(
        self,
        pipeline_name: str,
        dataset_name: str,
        execution_date: str,
        status: str,
        started_at_utc: str,
        ended_at_utc: str,
        pages_expected: int,
        pages_loaded: int,
        rows_loaded: int,
        missing_pages: str | None,
        error_message: str | None,
    ) -> None:
        sql = """
        USE hacienda_dw;
        
        INSERT INTO raw.etl_run_log (
            pipeline_name, dataset_name, execution_date,
            status, started_at_utc, ended_at_utc,
            pages_expected, pages_loaded, rows_loaded,
            missing_pages, error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.hook.run(
            sql,
            parameters=(
                pipeline_name, dataset_name, execution_date,
                status, started_at_utc, ended_at_utc,
                pages_expected, pages_loaded, rows_loaded,
                missing_pages, error_message,
            ),
        )
