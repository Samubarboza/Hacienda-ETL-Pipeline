USE hacienda_dw;

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'warehouse'
)
BEGIN
    EXEC('CREATE SCHEMA warehouse');
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'dim_prestamo'
)
BEGIN
    CREATE TABLE warehouse.dim_prestamo (
        prestamo_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        numero_prestamo VARCHAR(50) NOT NULL,
        tipo_prestamo VARCHAR(100) NULL,
        fuente_deuda VARCHAR(50) NULL,
        duracion VARCHAR(50) NULL,
        moneda_base_prestamo VARCHAR(10) NULL,
        garantia_publica VARCHAR(50) NULL,
        gestion_tramo VARCHAR(50) NULL,
        last_execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_dim_prestamo_numero UNIQUE (numero_prestamo)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'dim_tramo'
)
BEGIN
    CREATE TABLE warehouse.dim_tramo (
        tramo_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        numero_prestamo VARCHAR(50) NOT NULL,
        numero_tramo INT NOT NULL,
        moneda_tramo VARCHAR(10) NULL,
        categoria_interes_tramo VARCHAR(100) NULL,
        tasa_interes DECIMAL(18,6) NULL,
        grupo_tasa_interes VARCHAR(100) NULL,
        grupo_vencimiento VARCHAR(100) NULL,
        last_execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_dim_tramo_nk UNIQUE (numero_prestamo, numero_tramo)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'dim_acreedor'
)
BEGIN
    CREATE TABLE warehouse.dim_acreedor (
        acreedor_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        nombre_acreedor VARCHAR(200) NOT NULL,
        tipo_acreedor VARCHAR(100) NULL,
        pais_acreedor VARCHAR(200) NULL,
        last_execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_dim_acreedor_nombre UNIQUE (nombre_acreedor)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'dim_deudor'
)
BEGIN
    CREATE TABLE warehouse.dim_deudor (
        deudor_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        deudor VARCHAR(200) NOT NULL,
        nombre_beneficiario VARCHAR(200) NULL,
        categoria_deudor VARCHAR(100) NULL,
        descripcion_tipo_administracion VARCHAR(200) NULL,
        institucion_pagadora VARCHAR(200) NULL,
        agencias_implantacion VARCHAR(200) NULL,
        last_execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_dim_deudor_nombre UNIQUE (deudor)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'dim_ley'
)
BEGIN
    CREATE TABLE warehouse.dim_ley (
        ley_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        ley_numero VARCHAR(200) NOT NULL,
        fecha_ley DATE NULL,
        last_execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_dim_ley_numero UNIQUE (ley_numero)
    );
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'warehouse' AND t.name = 'fact_deuda_corte'
)
BEGIN
    CREATE TABLE warehouse.fact_deuda_corte (
        deuda_corte_key BIGINT IDENTITY(1,1) PRIMARY KEY,
        prestamo_key BIGINT NOT NULL,
        tramo_key BIGINT NOT NULL,
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
        execution_date DATE NOT NULL,
        created_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT uq_fact_deuda_corte_nk UNIQUE (numero_prestamo, numero_tramo, fecha_corte, execution_date),
        CONSTRAINT fk_fact_deuda_corte_prestamo FOREIGN KEY (prestamo_key) REFERENCES warehouse.dim_prestamo(prestamo_key),
        CONSTRAINT fk_fact_deuda_corte_tramo FOREIGN KEY (tramo_key) REFERENCES warehouse.dim_tramo(tramo_key)
    );

    CREATE INDEX ix_fact_deuda_corte_execution_date ON warehouse.fact_deuda_corte (execution_date);
END;
