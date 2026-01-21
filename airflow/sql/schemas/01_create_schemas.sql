-- schemas base del Data Warehouse
USE hacienda_dw;

-- Schema STG: datos transformados/intermedios
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');

-- Schema MART: datos finales para analítica y BI
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'mart')
    EXEC('CREATE SCHEMA mart');
