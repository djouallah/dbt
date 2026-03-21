# dbt in a Pure Python Notebook — Demo with DuckDB + CI/CD on Microsoft Fabric

A working demo showing how to run **dbt entirely inside a Microsoft Fabric Python notebook** using **DuckDB**, with a full CI/CD pipeline via GitHub Actions and the [Fabric CLI](https://microsoft.github.io/fabric-cli/).

No Fabric Data Warehouse. No ODBC. No Spark. Just dbt + DuckDB running in a standard Python notebook, writing Delta Lake tables directly to OneLake.

> **Not officially supported.** The official dbt adapter for Fabric is [dbt-fabric](https://github.com/microsoft/dbt-fabric) (connects via ODBC to Fabric Warehouse). This repo uses **dbt-duckdb** inside a notebook as a lightweight alternative — useful when you want SQL transformations without provisioning a warehouse.

## What this demo shows

- Running dbt models inside a Fabric Python notebook using the **`dbtRunner` Python API** (no shell commands, no `!dbt run`)
- Using **DuckDB** as the execution engine, reading from and writing to **OneLake Delta tables**
- A complete **CI/CD pipeline** from code push to production refresh:
  1. GitHub Actions runs `dbt run` + `dbt test` locally against DuckDB (Azurite for blob storage)
  2. Fabric CLI deploys the notebook and lakehouse
  3. The notebook is executed synchronously to build Delta tables
  4. A Direct Lake semantic model is deployed and refreshed in Power BI
  5. A Fabric Data Pipeline is deployed and scheduled

## Architecture

```
GitHub Push
    │
    ▼
GitHub Actions CI
    ├── dbt run   (DuckDB, local)
    └── dbt test  (DuckDB, local)
    │
    ▼
deploy.py (Fabric CLI + Power BI API)
    ├── fab deploy  → Notebook + Lakehouse
    ├── Copy dbt files → OneLake
    ├── fab job run → Notebook executes dbt, writes Delta tables
    ├── fab deploy  → Semantic Model
    ├── Power BI API → Refresh semantic model
    └── fab deploy  → Data Pipeline + cron schedule
```

## Stack

| Layer | Tool |
|-------|------|
| Transformations | dbt-core + dbt-duckdb |
| Execution | Python notebook (Fabric) |
| Storage | OneLake (Delta Lake) |
| Serving | Direct Lake semantic model (Power BI) |
| CI | GitHub Actions |
| Deploy | Fabric CLI (`ms-fabric-cli`) |

## Schema layout

- **`mart`** — Power BI-facing tables: `dim_calendar`, `dim_duid`, `fct_summary`
- **`landing`** — Intermediate tables: `fct_scada`, `fct_price`, staging models

## Notebook pattern

The notebook calls dbt via the Python API — no shell escaping, no path issues:

```python
import os
from dbt.cli.main import dbtRunner

os.chdir('/lakehouse/default/Files/dbt')
dbtRunner().invoke(["run", "--profiles-dir", "."])
```

## Configuration

- `deploy_config.yml` — all deployment targets (workspace, lakehouse, model IDs)
- `parameter.yml` — environment-specific value substitution for `fab deploy`
- `profiles.yml` / `dbt_project.yml` — dbt config for DuckDB + DuckLake

## Requirements

- [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) (`pip install ms-fabric-cli`)
- Azure AD app registration with Fabric API permissions

## CI/CD setup (GitHub Actions)

Set as GitHub secrets:
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`

Push to `main` — runs CI tests, deploys to Fabric, publishes dbt docs to GitHub Pages.

## Manual deploy

```bash
az login
python deploy.py
```

Run a specific step only:

```bash
python deploy.py semantic_model
```
