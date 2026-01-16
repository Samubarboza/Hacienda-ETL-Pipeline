IF NOT EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'hacienda_dw'
)
BEGIN
    CREATE DATABASE hacienda_dw;
END
