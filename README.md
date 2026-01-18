# Initial Configuration - Hacienda ETL Pipeline

## 1. SQL Server Connection in Airflow

Access the Airflow UI at `http://localhost:8080`

**Admin → Connections → Create New Connection**

Fill in with these values:

| Field | Value |
|-------|-------|
| Connection ID | `sqlserver_hacienda` |
| Connection Type | `Microsoft SQL Server` |
| Host | `sqlserver` |
| Schema | `master` |
| Login | `sa` |
| Password | `StrongPassword123Abc` |
| Port | `1433` |
| Extra | (empty) |

**Save the connection**

---

## 2. Databricks Connection in Airflow

**Admin → Connections → Create New Connection**

Fill in with these values:

| Field | Value |
|-------|-------|
| Connection ID | `databricks_hacienda` |
| Connection Type | `Databricks` |
| Host | `https://dbc-75d5300e-e786.cloud.databricks.com` |
| Login | `token` |
| Password | `<token-databricks>` |

**Save the connection**

---

## 3. Variables in Airflow

**Admin → Variables**

Find or create the variable `sqlserver_password`:

| Field | Value |
|-------|-------|
| Key | `sqlserver_password` |
| Val | `StrongPassword123Abc` |

**Save the variable**

---

## 4. Environment Variables

Create or verify the `.env` file in the project root:
```env
SQLSERVER_SA_PASSWORD=StrongPassword123Abc
```

---

## 5. Verify Connections

From the Airflow UI:

- **Admin → Connections**
- Verify that both connections appear in green

If they appear in red, check that the values are correct.

---

## 6. Next Step

Once connections and variables are configured, you can run the DAGs from the UI.