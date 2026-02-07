# Hacienda ETL Pipeline

Production-oriented ETL pipeline for `deuda_publica`, orchestrated in Airflow, transformed in Databricks, and persisted in ADLS Gen2.

## Current Status
The project is under active development and currently delivers an end-to-end flow up to the `curated` layer.

## High-Level Architecture

![Current ETL Flow](docs/diagrams/etl_flow_current.svg)

### Flow Summary
`ODMH API -> ADLS Raw -> Raw Validation -> ADLS Staging -> ADLS Curated -> Audit (ADLS JSON + SQL)`

## Implemented Scope (Current)
- Ingestion from source API to ADLS raw.
- Raw quality validation with fail-fast behavior.
- Raw-to-staging transformation in Databricks.
- Staging-to-curated transformation in Databricks (no KPI aggregations).
- Technical audit persisted in ADLS logs and SQL (`audit.etl_run_log`).

## Repository Structure
```text
.
├── airflow/
│   ├── dags/
│   │   ├── 00_create_data_warehouse_dag.py
│   │   ├── 01_create_schemas_dag.py
│   │   ├── 02_ingest_deuda_publica_dag.py
│   │   ├── 03_validate_raw_deuda_publica_dag.py
│   │   ├── 04_staging_deuda_publica_databricks_dag.py
│   │   └── 05_curated_deuda_publica_dag.py
│   ├── plugins/
│   │   ├── audit/
│   │   ├── curated/
│   │   └── deuda_publica/
│   └── sql/
│       ├── schemas/
│       └── warehouse/
├── docker/
├── docker-compose.yml
└── README.md
```

## DAG Catalog

| DAG ID | Layer / Purpose | Main Output |
|---|---|---|
| `00_create_data_warehouse` | Bootstrap DB | `hacienda_dw` database |
| `01_create_schemas` | Bootstrap schemas and audit tables | `stg`, `mart`, `audit.*` |
| `02_ingest_deuda_publica_raw` | API ingestion to raw | `raw/deuda_publica/execution_date=YYYY-MM-DD/...` |
| `03_validate_raw_deuda_publica` | Raw data quality checks | Validation status + logs |
| `04_staging_deuda_publica_databricks` | Raw -> Staging transform | `staging/deuda_publica/execution_date=YYYY-MM-DD/...` |
| `05_curated_deuda_publica` | Staging -> Curated transform + audit | `curated/deuda_publica/execution_date=YYYY-MM-DD/...` + audit logs |

## Data Layers

### Raw
- Input from source API.
- JSON files partitioned by `execution_date`.
- No business transformation.

### Staging
- Databricks transformation from raw.
- Intermediate normalized entities.
- Partitioned by `execution_date`.

### Curated
- Business-rule standardization.
- Null-handling based on domain rules.
- No KPI/aggregation layer yet.

## Audit & Observability

### ADLS JSON Logs
- Raw validation logs:
  - `logs/deuda_publica/execution_date=YYYY-MM-DD/...`
- Curated run logs:
  - `logs/deuda_publica/layer=curated/execution_date=YYYY-MM-DD/...`

### SQL Audit
- `audit.etl_run_log`
- `audit.pipeline_runs`

## Runtime Prerequisites
- Docker + Docker Compose.
- Airflow with Microsoft SQL Server and Databricks providers.
- Databricks workspace + active cluster.
- ADLS Gen2 account and containers.
- SQL Server reachable from Airflow.

## Local Startup
```bash
docker compose up -d --build
```
Airflow UI: `http://localhost:8080`

## Airflow Setup

### Connections (`Admin -> Connections`)
1. `sqlserver_hacienda`
- Type: `Microsoft SQL Server`
- Host/Port/Schema/Login/Password according to your target environment.

2. `databricks_hacienda`
- Type: `Databricks`
- Host: Databricks workspace URL.
- Login: `token`
- Password: PAT token.

3. `azure_datalake_conn`
- Login: Service Principal `client_id`
- Password: Service Principal `client_secret`
- Extra JSON:
```json
{"tenant_id": "<tenant-id>"}
```

### Variables (`Admin -> Variables`)
1. `AZURE_STORAGE_ACCOUNT_URL`  
Example: `https://<storage-account>.dfs.core.windows.net`

2. `AZURE_FILE_SYSTEM_NAME`  
Main file system used for raw validation and audit logs.

3. `azure_storage_account`  
Storage account name only (no protocol).

## Databricks Setup
- Ensure the cluster configured in DAGs exists and is running.
- Ensure notebook paths in DAGs are valid in your workspace.
- Use workspace-neutral paths in your own environment, for example:
  - `/Workspace/Shared/etl/01_raw_to_stg_deuda_publica`
  - `/Workspace/Shared/etl/02_stg_to_curated_deuda_publica`
- Ensure RBAC/access to ADLS for read/write operations in `raw`, `staging`, `curated`, and `logs`.

## Recommended Execution Order
1. `00_create_data_warehouse`
2. `01_create_schemas`
3. `02_ingest_deuda_publica_raw`
4. `03_validate_raw_deuda_publica`
5. `04_staging_deuda_publica_databricks`
6. `05_curated_deuda_publica`

## Validation Checklist
After running the flow for a given `execution_date`:
1. Raw files exist in `raw/deuda_publica/execution_date=YYYY-MM-DD/`
2. Validation DAG succeeds (or fails with explicit quality reason)
3. Staging data exists in `staging/deuda_publica/execution_date=YYYY-MM-DD/`
4. Curated data exists in `curated/deuda_publica/execution_date=YYYY-MM-DD/`
5. ADLS audit logs were written
6. SQL audit row was inserted into `audit.etl_run_log`

## Troubleshooting
1. `Path not found` during curated run
- Check staging data exists for that execution date.

2. Databricks ADLS OAuth errors
- Check cluster auth configuration and Service Principal permissions.

3. SQL audit insertion errors
- Run `01_create_schemas` and verify `audit.etl_run_log` exists.

## Security Note
Do not store secrets, tokens, or personal identifiers in source-controlled documentation.
