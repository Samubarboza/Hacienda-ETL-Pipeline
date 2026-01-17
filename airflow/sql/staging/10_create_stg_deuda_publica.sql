-- STG: entidades normalizadas (3FN) + auditoría
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_prestamo' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_prestamo (
        numero_prestamo        VARCHAR(50) NOT NULL,
        tipo_prestamo          VARCHAR(100) NULL,
        fuente_deuda           VARCHAR(50)  NULL,
        duracion               VARCHAR(50)  NULL,
        moneda_base_prestamo   VARCHAR(10)  NULL,
        garantia_publica       VARCHAR(50)  NULL,
        gestion_tramo          VARCHAR(50)  NULL,

        execution_date         DATE NOT NULL,
        ingestion_timestamp    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_prestamo PRIMARY KEY (numero_prestamo, execution_date)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_tramo' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_tramo (
        numero_prestamo          VARCHAR(50) NOT NULL,
        numero_tramo             INT NOT NULL,
        moneda_tramo             VARCHAR(10)  NULL,
        categoria_interes_tramo  VARCHAR(50)  NULL,
        tasa_interes             DECIMAL(18,6) NULL,
        grupo_tasa_interes       VARCHAR(100) NULL,
        grupo_vencimiento        VARCHAR(100) NULL,

        execution_date           DATE NOT NULL,
        ingestion_timestamp      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_tramo PRIMARY KEY (numero_prestamo, numero_tramo, execution_date)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_acreedor' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_acreedor (
        nombre_acreedor       VARCHAR(200) NOT NULL,
        tipo_acreedor         VARCHAR(100) NULL,
        pais_acreedor         VARCHAR(200) NULL,

        execution_date        DATE NOT NULL,
        ingestion_timestamp   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_acreedor PRIMARY KEY (nombre_acreedor, execution_date)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_deudor' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_deudor (
        deudor                        VARCHAR(200) NOT NULL,
        nombre_beneficiario           VARCHAR(200) NULL,
        categoria_deudor              VARCHAR(100) NULL,
        descripcion_tipo_administracion VARCHAR(200) NULL,
        institucion_pagadora          VARCHAR(200) NULL,
        agencias_implantacion         VARCHAR(200) NULL,

        execution_date                DATE NOT NULL,
        ingestion_timestamp           DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_deudor PRIMARY KEY (deudor, execution_date)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_ley' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_ley (
        ley_numero          VARCHAR(200) NOT NULL,
        fecha_ley           DATE NULL,

        execution_date      DATE NOT NULL,
        ingestion_timestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_ley PRIMARY KEY (ley_numero, execution_date)
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE t.name='stg_deuda_corte' AND s.name='stg')
BEGIN
    CREATE TABLE stg.stg_deuda_corte (
        numero_prestamo         VARCHAR(50) NOT NULL,
        numero_tramo            INT NOT NULL,
        fecha_corte             DATE NOT NULL,
        mes_corte               INT NULL,
        anio_corte              INT NULL,

        categoria_deuda         VARCHAR(100) NULL,
        fuente_deuda            VARCHAR(50) NULL,
        situacion               VARCHAR(50) NULL,

        monto_prestamo          DECIMAL(18,2) NULL,
        monto_tramo             DECIMAL(18,2) NULL,

        -- Fechas operativas
        fecha_inicial_programada DATE NULL,
        fecha_final_programada   DATE NULL,
        fecha_limite_desembolso  DATE NULL,

        execution_date          DATE NOT NULL,
        ingestion_timestamp     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT pk_stg_deuda_corte PRIMARY KEY (numero_prestamo, numero_tramo, fecha_corte, execution_date)
    );

    CREATE INDEX ix_stg_deuda_corte_fecha ON stg.stg_deuda_corte (fecha_corte);
END;