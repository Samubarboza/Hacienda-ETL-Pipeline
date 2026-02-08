USE hacienda_dw;
SET NOCOUNT ON;

DECLARE @execution_date DATE = %s;
DECLARE @storage_account NVARCHAR(128) = %s;
DECLARE @curated_container NVARCHAR(128) = %s;
DECLARE @sas_token NVARCHAR(MAX) = %s;

DECLARE @sql NVARCHAR(MAX);
DECLARE @base_path NVARCHAR(4000);
DECLARE @rows_read BIGINT = 0;
DECLARE @rows_inserted BIGINT = 0;
DECLARE @rows_updated BIGINT = 0;

DECLARE @actions TABLE (action_name NVARCHAR(10));

IF @execution_date IS NULL
    THROW 50000, 'execution_date is required.', 1;

IF @storage_account IS NULL OR LTRIM(RTRIM(@storage_account)) = ''
    THROW 50000, 'storage_account is required.', 1;

IF @curated_container IS NULL OR LTRIM(RTRIM(@curated_container)) = ''
    SET @curated_container = 'curated';

IF LEFT(ISNULL(@sas_token, ''), 1) = '?'
    SET @sas_token = SUBSTRING(@sas_token, 2, LEN(@sas_token));

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_scoped_credentials
    WHERE name = 'curated_adls_cred'
)
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM sys.symmetric_keys
        WHERE name = '##MS_DatabaseMasterKey##'
    )
        THROW 50000, 'Database master key is required before creating curated_adls_cred.', 1;

    IF @sas_token IS NULL OR LTRIM(RTRIM(@sas_token)) = ''
        THROW 50000, 'Missing SAS token. Set Airflow variable AZURE_STORAGE_SAS_TOKEN or pre-create curated_adls_cred.', 1;

    SET @sql = N'CREATE DATABASE SCOPED CREDENTIAL curated_adls_cred '
        + N'WITH IDENTITY = ''SHARED ACCESS SIGNATURE'', SECRET = '''
        + REPLACE(@sas_token, '''', '''''') + N''';';
    EXEC sp_executesql @sql;
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.external_data_sources
    WHERE name = 'curated_adls_ds'
)
BEGIN
    SET @sql = N'CREATE EXTERNAL DATA SOURCE curated_adls_ds '
        + N'WITH (TYPE = BLOB_STORAGE, LOCATION = ''https://'
        + REPLACE(@storage_account, '''', '''''')
        + N'.blob.core.windows.net'', CREDENTIAL = curated_adls_cred);';
    EXEC sp_executesql @sql;
END;

SET @base_path =
    @curated_container
    + N'/deuda_publica/execution_date='
    + CONVERT(NVARCHAR(10), @execution_date, 23)
    + N'/';

CREATE TABLE #src_prestamo (
    numero_prestamo VARCHAR(50) NOT NULL,
    tipo_prestamo VARCHAR(100) NULL,
    fuente_deuda VARCHAR(50) NULL,
    duracion VARCHAR(50) NULL,
    moneda_base_prestamo VARCHAR(10) NULL,
    garantia_publica VARCHAR(50) NULL,
    gestion_tramo VARCHAR(50) NULL,
    execution_date DATE NOT NULL
);

CREATE TABLE #src_tramo (
    numero_prestamo VARCHAR(50) NOT NULL,
    numero_tramo INT NOT NULL,
    moneda_tramo VARCHAR(10) NULL,
    categoria_interes_tramo VARCHAR(100) NULL,
    tasa_interes DECIMAL(18,6) NULL,
    grupo_tasa_interes VARCHAR(100) NULL,
    grupo_vencimiento VARCHAR(100) NULL,
    execution_date DATE NOT NULL
);

CREATE TABLE #src_acreedor (
    nombre_acreedor VARCHAR(200) NOT NULL,
    tipo_acreedor VARCHAR(100) NULL,
    pais_acreedor VARCHAR(200) NULL,
    execution_date DATE NOT NULL
);

CREATE TABLE #src_deudor (
    deudor VARCHAR(200) NOT NULL,
    nombre_beneficiario VARCHAR(200) NULL,
    categoria_deudor VARCHAR(100) NULL,
    descripcion_tipo_administracion VARCHAR(200) NULL,
    institucion_pagadora VARCHAR(200) NULL,
    agencias_implantacion VARCHAR(200) NULL,
    execution_date DATE NOT NULL
);

CREATE TABLE #src_ley (
    ley_numero VARCHAR(200) NOT NULL,
    fecha_ley DATE NULL,
    execution_date DATE NOT NULL
);

CREATE TABLE #src_deuda_corte (
    numero_prestamo VARCHAR(50) NOT NULL,
    numero_tramo INT NOT NULL,
    fecha_corte DATE NOT NULL,
    mes_corte INT NULL,
    anio_corte INT NULL,
    categoria_deuda VARCHAR(100) NULL,
    fuente_deuda VARCHAR(50) NULL,
    situacion VARCHAR(50) NULL,
    monto_prestamo DECIMAL(18,2) NULL,
    monto_tramo DECIMAL(18,2) NULL,
    fecha_inicial_programada DATE NULL,
    fecha_final_programada DATE NULL,
    fecha_limite_desembolso DATE NULL,
    execution_date DATE NOT NULL
);

SET @sql = N'
INSERT INTO #src_prestamo
SELECT
    CAST(src.numero_prestamo AS VARCHAR(50)),
    CAST(src.tipo_prestamo AS VARCHAR(100)),
    CAST(src.fuente_deuda AS VARCHAR(50)),
    CAST(src.duracion AS VARCHAR(50)),
    CAST(src.moneda_base_prestamo AS VARCHAR(10)),
    CAST(src.garantia_publica AS VARCHAR(50)),
    CAST(src.gestion_tramo AS VARCHAR(50)),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'prestamo/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @sql = N'
INSERT INTO #src_tramo
SELECT
    CAST(src.numero_prestamo AS VARCHAR(50)),
    CAST(src.numero_tramo AS INT),
    CAST(src.moneda_tramo AS VARCHAR(10)),
    CAST(src.categoria_interes_tramo AS VARCHAR(100)),
    CAST(src.tasa_interes AS DECIMAL(18,6)),
    CAST(src.grupo_tasa_interes AS VARCHAR(100)),
    CAST(src.grupo_vencimiento AS VARCHAR(100)),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'tramo/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @sql = N'
INSERT INTO #src_acreedor
SELECT
    CAST(src.nombre_acreedor AS VARCHAR(200)),
    CAST(src.tipo_acreedor AS VARCHAR(100)),
    CAST(src.pais_acreedor AS VARCHAR(200)),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'acreedor/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @sql = N'
INSERT INTO #src_deudor
SELECT
    CAST(src.deudor AS VARCHAR(200)),
    CAST(src.nombre_beneficiario AS VARCHAR(200)),
    CAST(src.categoria_deudor AS VARCHAR(100)),
    CAST(src.descripcion_tipo_administracion AS VARCHAR(200)),
    CAST(src.institucion_pagadora AS VARCHAR(200)),
    CAST(src.agencias_implantacion AS VARCHAR(200)),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'deudor/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @sql = N'
INSERT INTO #src_ley
SELECT
    CAST(src.ley_numero AS VARCHAR(200)),
    CAST(src.fecha_ley AS DATE),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'ley/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @sql = N'
INSERT INTO #src_deuda_corte
SELECT
    CAST(src.numero_prestamo AS VARCHAR(50)),
    CAST(src.numero_tramo AS INT),
    CAST(src.fecha_corte AS DATE),
    CAST(src.mes_corte AS INT),
    CAST(src.anio_corte AS INT),
    CAST(src.categoria_deuda AS VARCHAR(100)),
    CAST(src.fuente_deuda AS VARCHAR(50)),
    CAST(src.situacion AS VARCHAR(50)),
    CAST(src.monto_prestamo AS DECIMAL(18,2)),
    CAST(src.monto_tramo AS DECIMAL(18,2)),
    CAST(src.fecha_inicial_programada AS DATE),
    CAST(src.fecha_final_programada AS DATE),
    CAST(src.fecha_limite_desembolso AS DATE),
    CAST(src.execution_date AS DATE)
FROM OPENROWSET(
    BULK ''' + @base_path + N'deuda_corte/*.parquet'',
    DATA_SOURCE = ''curated_adls_ds'',
    FORMAT = ''PARQUET''
) AS src;';
EXEC sp_executesql @sql;

SET @rows_read =
    (SELECT COUNT(*) FROM #src_prestamo)
    + (SELECT COUNT(*) FROM #src_tramo)
    + (SELECT COUNT(*) FROM #src_acreedor)
    + (SELECT COUNT(*) FROM #src_deudor)
    + (SELECT COUNT(*) FROM #src_ley)
    + (SELECT COUNT(*) FROM #src_deuda_corte);

DELETE FROM @actions;
;WITH src AS (
    SELECT
        numero_prestamo,
        tipo_prestamo,
        fuente_deuda,
        duracion,
        moneda_base_prestamo,
        garantia_publica,
        gestion_tramo,
        ROW_NUMBER() OVER (PARTITION BY numero_prestamo ORDER BY execution_date DESC) AS rn
    FROM #src_prestamo
)
MERGE warehouse.dim_prestamo AS tgt
USING (
    SELECT numero_prestamo, tipo_prestamo, fuente_deuda, duracion, moneda_base_prestamo, garantia_publica, gestion_tramo
    FROM src WHERE rn = 1
) AS src
ON tgt.numero_prestamo = src.numero_prestamo
WHEN MATCHED AND (
    ISNULL(tgt.tipo_prestamo, '') <> ISNULL(src.tipo_prestamo, '') OR
    ISNULL(tgt.fuente_deuda, '') <> ISNULL(src.fuente_deuda, '') OR
    ISNULL(tgt.duracion, '') <> ISNULL(src.duracion, '') OR
    ISNULL(tgt.moneda_base_prestamo, '') <> ISNULL(src.moneda_base_prestamo, '') OR
    ISNULL(tgt.garantia_publica, '') <> ISNULL(src.garantia_publica, '') OR
    ISNULL(tgt.gestion_tramo, '') <> ISNULL(src.gestion_tramo, '')
)
THEN UPDATE SET
    tipo_prestamo = src.tipo_prestamo,
    fuente_deuda = src.fuente_deuda,
    duracion = src.duracion,
    moneda_base_prestamo = src.moneda_base_prestamo,
    garantia_publica = src.garantia_publica,
    gestion_tramo = src.gestion_tramo,
    last_execution_date = @execution_date,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        numero_prestamo, tipo_prestamo, fuente_deuda, duracion,
        moneda_base_prestamo, garantia_publica, gestion_tramo,
        last_execution_date, created_at_utc, updated_at_utc
    )
    VALUES (
        src.numero_prestamo, src.tipo_prestamo, src.fuente_deuda, src.duracion,
        src.moneda_base_prestamo, src.garantia_publica, src.gestion_tramo,
        @execution_date, SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

DELETE FROM @actions;
;WITH src AS (
    SELECT
        numero_prestamo,
        numero_tramo,
        moneda_tramo,
        categoria_interes_tramo,
        tasa_interes,
        grupo_tasa_interes,
        grupo_vencimiento,
        ROW_NUMBER() OVER (PARTITION BY numero_prestamo, numero_tramo ORDER BY execution_date DESC) AS rn
    FROM #src_tramo
)
MERGE warehouse.dim_tramo AS tgt
USING (
    SELECT numero_prestamo, numero_tramo, moneda_tramo, categoria_interes_tramo, tasa_interes, grupo_tasa_interes, grupo_vencimiento
    FROM src WHERE rn = 1
) AS src
ON tgt.numero_prestamo = src.numero_prestamo AND tgt.numero_tramo = src.numero_tramo
WHEN MATCHED AND (
    ISNULL(tgt.moneda_tramo, '') <> ISNULL(src.moneda_tramo, '') OR
    ISNULL(tgt.categoria_interes_tramo, '') <> ISNULL(src.categoria_interes_tramo, '') OR
    ISNULL(tgt.tasa_interes, 0) <> ISNULL(src.tasa_interes, 0) OR
    ISNULL(tgt.grupo_tasa_interes, '') <> ISNULL(src.grupo_tasa_interes, '') OR
    ISNULL(tgt.grupo_vencimiento, '') <> ISNULL(src.grupo_vencimiento, '')
)
THEN UPDATE SET
    moneda_tramo = src.moneda_tramo,
    categoria_interes_tramo = src.categoria_interes_tramo,
    tasa_interes = src.tasa_interes,
    grupo_tasa_interes = src.grupo_tasa_interes,
    grupo_vencimiento = src.grupo_vencimiento,
    last_execution_date = @execution_date,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        numero_prestamo, numero_tramo, moneda_tramo, categoria_interes_tramo,
        tasa_interes, grupo_tasa_interes, grupo_vencimiento,
        last_execution_date, created_at_utc, updated_at_utc
    )
    VALUES (
        src.numero_prestamo, src.numero_tramo, src.moneda_tramo, src.categoria_interes_tramo,
        src.tasa_interes, src.grupo_tasa_interes, src.grupo_vencimiento,
        @execution_date, SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

DELETE FROM @actions;
;WITH src AS (
    SELECT
        nombre_acreedor,
        tipo_acreedor,
        pais_acreedor,
        ROW_NUMBER() OVER (PARTITION BY nombre_acreedor ORDER BY execution_date DESC) AS rn
    FROM #src_acreedor
)
MERGE warehouse.dim_acreedor AS tgt
USING (
    SELECT nombre_acreedor, tipo_acreedor, pais_acreedor
    FROM src WHERE rn = 1
) AS src
ON tgt.nombre_acreedor = src.nombre_acreedor
WHEN MATCHED AND (
    ISNULL(tgt.tipo_acreedor, '') <> ISNULL(src.tipo_acreedor, '') OR
    ISNULL(tgt.pais_acreedor, '') <> ISNULL(src.pais_acreedor, '')
)
THEN UPDATE SET
    tipo_acreedor = src.tipo_acreedor,
    pais_acreedor = src.pais_acreedor,
    last_execution_date = @execution_date,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        nombre_acreedor, tipo_acreedor, pais_acreedor,
        last_execution_date, created_at_utc, updated_at_utc
    )
    VALUES (
        src.nombre_acreedor, src.tipo_acreedor, src.pais_acreedor,
        @execution_date, SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

DELETE FROM @actions;
;WITH src AS (
    SELECT
        deudor,
        nombre_beneficiario,
        categoria_deudor,
        descripcion_tipo_administracion,
        institucion_pagadora,
        agencias_implantacion,
        ROW_NUMBER() OVER (PARTITION BY deudor ORDER BY execution_date DESC) AS rn
    FROM #src_deudor
)
MERGE warehouse.dim_deudor AS tgt
USING (
    SELECT deudor, nombre_beneficiario, categoria_deudor, descripcion_tipo_administracion, institucion_pagadora, agencias_implantacion
    FROM src WHERE rn = 1
) AS src
ON tgt.deudor = src.deudor
WHEN MATCHED AND (
    ISNULL(tgt.nombre_beneficiario, '') <> ISNULL(src.nombre_beneficiario, '') OR
    ISNULL(tgt.categoria_deudor, '') <> ISNULL(src.categoria_deudor, '') OR
    ISNULL(tgt.descripcion_tipo_administracion, '') <> ISNULL(src.descripcion_tipo_administracion, '') OR
    ISNULL(tgt.institucion_pagadora, '') <> ISNULL(src.institucion_pagadora, '') OR
    ISNULL(tgt.agencias_implantacion, '') <> ISNULL(src.agencias_implantacion, '')
)
THEN UPDATE SET
    nombre_beneficiario = src.nombre_beneficiario,
    categoria_deudor = src.categoria_deudor,
    descripcion_tipo_administracion = src.descripcion_tipo_administracion,
    institucion_pagadora = src.institucion_pagadora,
    agencias_implantacion = src.agencias_implantacion,
    last_execution_date = @execution_date,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        deudor, nombre_beneficiario, categoria_deudor, descripcion_tipo_administracion,
        institucion_pagadora, agencias_implantacion,
        last_execution_date, created_at_utc, updated_at_utc
    )
    VALUES (
        src.deudor, src.nombre_beneficiario, src.categoria_deudor, src.descripcion_tipo_administracion,
        src.institucion_pagadora, src.agencias_implantacion,
        @execution_date, SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

DELETE FROM @actions;
;WITH src AS (
    SELECT
        ley_numero,
        fecha_ley,
        ROW_NUMBER() OVER (PARTITION BY ley_numero ORDER BY execution_date DESC) AS rn
    FROM #src_ley
)
MERGE warehouse.dim_ley AS tgt
USING (
    SELECT ley_numero, fecha_ley
    FROM src WHERE rn = 1
) AS src
ON tgt.ley_numero = src.ley_numero
WHEN MATCHED AND ISNULL(tgt.fecha_ley, '19000101') <> ISNULL(src.fecha_ley, '19000101')
THEN UPDATE SET
    fecha_ley = src.fecha_ley,
    last_execution_date = @execution_date,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ley_numero, fecha_ley,
        last_execution_date, created_at_utc, updated_at_utc
    )
    VALUES (
        src.ley_numero, src.fecha_ley,
        @execution_date, SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

DELETE FROM @actions;
;WITH src AS (
    SELECT
        numero_prestamo,
        numero_tramo,
        fecha_corte,
        mes_corte,
        anio_corte,
        categoria_deuda,
        fuente_deuda,
        situacion,
        monto_prestamo,
        monto_tramo,
        fecha_inicial_programada,
        fecha_final_programada,
        fecha_limite_desembolso,
        execution_date,
        ROW_NUMBER() OVER (
            PARTITION BY numero_prestamo, numero_tramo, fecha_corte, execution_date
            ORDER BY execution_date DESC
        ) AS rn
    FROM #src_deuda_corte
),
resolved AS (
    SELECT
        p.prestamo_key,
        t.tramo_key,
        s.numero_prestamo,
        s.numero_tramo,
        s.fecha_corte,
        s.mes_corte,
        s.anio_corte,
        s.categoria_deuda,
        s.fuente_deuda,
        s.situacion,
        s.monto_prestamo,
        s.monto_tramo,
        s.fecha_inicial_programada,
        s.fecha_final_programada,
        s.fecha_limite_desembolso,
        s.execution_date
    FROM src s
    INNER JOIN warehouse.dim_prestamo p
        ON p.numero_prestamo = s.numero_prestamo
    INNER JOIN warehouse.dim_tramo t
        ON t.numero_prestamo = s.numero_prestamo
       AND t.numero_tramo = s.numero_tramo
    WHERE s.rn = 1
)
MERGE warehouse.fact_deuda_corte AS tgt
USING resolved AS src
ON tgt.numero_prestamo = src.numero_prestamo
AND tgt.numero_tramo = src.numero_tramo
AND tgt.fecha_corte = src.fecha_corte
AND tgt.execution_date = src.execution_date
WHEN MATCHED AND (
    ISNULL(tgt.mes_corte, 0) <> ISNULL(src.mes_corte, 0) OR
    ISNULL(tgt.anio_corte, 0) <> ISNULL(src.anio_corte, 0) OR
    ISNULL(tgt.categoria_deuda, '') <> ISNULL(src.categoria_deuda, '') OR
    ISNULL(tgt.fuente_deuda, '') <> ISNULL(src.fuente_deuda, '') OR
    ISNULL(tgt.situacion, '') <> ISNULL(src.situacion, '') OR
    ISNULL(tgt.monto_prestamo, 0) <> ISNULL(src.monto_prestamo, 0) OR
    ISNULL(tgt.monto_tramo, 0) <> ISNULL(src.monto_tramo, 0) OR
    ISNULL(tgt.fecha_inicial_programada, '19000101') <> ISNULL(src.fecha_inicial_programada, '19000101') OR
    ISNULL(tgt.fecha_final_programada, '19000101') <> ISNULL(src.fecha_final_programada, '19000101') OR
    ISNULL(tgt.fecha_limite_desembolso, '19000101') <> ISNULL(src.fecha_limite_desembolso, '19000101')
)
THEN UPDATE SET
    prestamo_key = src.prestamo_key,
    tramo_key = src.tramo_key,
    mes_corte = src.mes_corte,
    anio_corte = src.anio_corte,
    categoria_deuda = src.categoria_deuda,
    fuente_deuda = src.fuente_deuda,
    situacion = src.situacion,
    monto_prestamo = src.monto_prestamo,
    monto_tramo = src.monto_tramo,
    fecha_inicial_programada = src.fecha_inicial_programada,
    fecha_final_programada = src.fecha_final_programada,
    fecha_limite_desembolso = src.fecha_limite_desembolso,
    updated_at_utc = SYSUTCDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        prestamo_key, tramo_key,
        numero_prestamo, numero_tramo,
        fecha_corte, mes_corte, anio_corte,
        categoria_deuda, fuente_deuda, situacion,
        monto_prestamo, monto_tramo,
        fecha_inicial_programada, fecha_final_programada, fecha_limite_desembolso,
        execution_date,
        created_at_utc, updated_at_utc
    )
    VALUES (
        src.prestamo_key, src.tramo_key,
        src.numero_prestamo, src.numero_tramo,
        src.fecha_corte, src.mes_corte, src.anio_corte,
        src.categoria_deuda, src.fuente_deuda, src.situacion,
        src.monto_prestamo, src.monto_tramo,
        src.fecha_inicial_programada, src.fecha_final_programada, src.fecha_limite_desembolso,
        src.execution_date,
        SYSUTCDATETIME(), SYSUTCDATETIME()
    )
OUTPUT $action INTO @actions;

SELECT @rows_inserted = @rows_inserted + COUNT(*) FROM @actions WHERE action_name = 'INSERT';
SELECT @rows_updated = @rows_updated + COUNT(*) FROM @actions WHERE action_name = 'UPDATE';

SELECT
    CAST(@rows_read AS BIGINT) AS rows_read,
    CAST(@rows_inserted AS BIGINT) AS rows_inserted,
    CAST(@rows_updated AS BIGINT) AS rows_updated;
