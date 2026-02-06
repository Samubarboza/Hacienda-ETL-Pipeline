USE hacienda_dw;

-- creamos el schema de auditoria
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'audit'
)
BEGIN
    EXEC('CREATE SCHEMA audit');
END;

-- creamos la tabla minima de auditoria para runs ETL
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'etl_run_log'
        AND s.name = 'audit'
)
BEGIN
    CREATE TABLE audit.etl_run_log (
        run_id         VARCHAR(250) NOT NULL,
        dag_id         VARCHAR(250) NOT NULL,
        task_id        VARCHAR(250) NOT NULL,
        execution_date DATE         NOT NULL,
        status         VARCHAR(20)  NOT NULL,
        message        VARCHAR(2000) NULL,
        created_at     DATETIME2    NOT NULL
            CONSTRAINT df_audit_etl_run_log_created_at
            DEFAULT (SYSDATETIME())
    );
END;

-- creamos la tabla de auditoria de ejecuciones del pipeline
IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'pipeline_runs'
        AND s.name = 'audit'
)
BEGIN
    CREATE TABLE audit.pipeline_runs (
        audit_id BIGINT IDENTITY(1,1) PRIMARY KEY,

        pipeline_name      VARCHAR(100) NOT NULL,
        stage              VARCHAR(50)  NOT NULL,
        dataset_name       VARCHAR(100) NOT NULL,
        execution_date     DATE         NOT NULL,

        status             VARCHAR(20)  NOT NULL, -- STARTED | SUCCESS | FAILED

        pages_expected     INT          NULL,
        pages_loaded       INT          NULL,
        missing_pages      VARCHAR(500) NULL,

        started_at_utc     DATETIME2    NOT NULL,
        ended_at_utc       DATETIME2    NULL,

        error_message      VARCHAR(2000) NULL,

        created_at_utc     DATETIME2 NOT NULL
            CONSTRAINT df_audit_pipeline_runs_created_at
            DEFAULT (SYSUTCDATETIME())
    );
END;
