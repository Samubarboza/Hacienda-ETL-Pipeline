USE hacienda_dw;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE t.name = 'raw_deuda_publica'
        AND s.name = 'raw'
)
BEGIN
    CREATE TABLE raw.raw_deuda_publica (
        raw_id BIGINT IDENTITY(1,1) PRIMARY KEY,
        payload NVARCHAR(MAX) NOT NULL,
        source_system VARCHAR(50) NOT NULL,
        execution_date DATE NOT NULL,
        ingestion_timestamp DATETIME2 NOT NULL
            DEFAULT SYSUTCDATETIME(),
        page_number INT NOT NULL,
        request_hash CHAR(64) NOT NULL
    );

    CREATE UNIQUE INDEX ux_raw_deuda_publica_idempotency
        ON raw.raw_deuda_publica (request_hash);
END
