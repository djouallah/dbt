# dbt + DuckLake on Microsoft Fabric


> **Note:** The official dbt adapter for Fabric is [dbt-fabric](https://github.com/microsoft/dbt-fabric) (connects via ODBC to Fabric Warehouse). This repo is side project, using **dbt-duckdb** with **DuckLake** inside a notebook for lightweight SQL transformations that write directly to OneLake, Notice it is a single writer, and does not support conccurency unless you use Postgresql which is out of scope


> **Limitation:** it is a single writer, and does not support conccurency unless you use Postgresql which is out of scope 

Run **dbt** inside a **Microsoft Fabric Python notebook** using **DuckDB** and **DuckLake** to build and manage Delta Lake tables on OneLake, with full CI/CD via GitHub Actions and the [Fabric CLI](https://microsoft.github.io/fabric-cli/).

DuckLake acts as the Delta Lake catalog — dbt models materialize as DuckLake-managed tables, and `delta_export()` writes the final Delta metadata and Parquet files so Fabric sees them as native Delta tables. The result is a complete pipeline from raw CSVs to a Direct Lake semantic model in Power BI.


## How it works

1. **DuckLake as catalog** — dbt attaches a DuckLake database backed by a SQLite metadata store. All table materializations go through DuckLake, which manages Parquet files and Delta transaction logs under `Tables/{schema}/{table}/`.

2. **delta_export() for Delta metadata** — After dbt finishes, `delta_export()` finalizes the `_delta_log/` entries so Fabric and Direct Lake can read the tables natively. This runs as a dbt `on-run-end` hook.

3. **DuckLake compaction** — `on-run-end` also calls `ducklake_rewrite_data_files`, `ducklake_merge_adjacent_files`, and `ducklake_cleanup_old_files` to optimize file layout before export.

4. **Notebook execution** — The Fabric notebook calls dbt via the `dbtRunner` Python API, pointing at OneLake (`abfss://`) paths for production. The notebook is deployed and executed by Fabric CLI as part of CI/CD.

## Architecture

```
GitHub Push
    │
    ▼
GitHub Actions CI
    ├── dbt run   (DuckDB + DuckLake, Azurite blob storage)
    └── dbt test  (validates Delta table row counts)
    │
    ▼
deploy.py (Fabric CLI + Power BI API)
    ├── fab create  → Lakehouse (with schemas)
    ├── fab deploy  → Notebook
    ├── Copy dbt/   → OneLake Files
    ├── fab job run → Notebook runs dbt, DuckLake writes Delta tables
    ├── delta_export() finalizes Delta metadata
    ├── fab deploy  → Semantic Model (Direct Lake, GUIDs swapped)
    ├── Power BI API → Refresh semantic model
    └── fab deploy  → Data Pipeline + cron schedule
```

## Stack

| Layer | Tool |
|-------|------|
| Transformations | dbt-core + dbt-duckdb |
| Delta catalog | DuckLake (DuckDB extension) |
| Delta metadata export | delta_export (DuckDB extension) |
| Execution | Python notebook (Fabric) |
| Storage | OneLake (Delta Lake / Parquet) |
| Serving | Direct Lake semantic model (Power BI) |
| CI | GitHub Actions |
| Deploy | Fabric CLI (`ms-fabric-cli`) |

## Schema layout

The included models are a working example — swap in your own sources and transformation logic.

- **`landing`** — Staging and incremental fact tables (source ingestion, deduplication)
- **`mart`** — Power BI-facing dimensions and facts (joined, aggregated, ready for Direct Lake)

## DuckLake configuration

In `profiles.yml`, each target attaches DuckLake as the database:

```yaml
extensions:
  - ducklake
  - delta_export
attach:
  - path: "ducklake:sqlite:{{ METADATA_LOCAL_PATH }}"
    alias: ducklake
    options:
      data_path: "{{ ROOT_PATH }}/Tables"
      data_inlining_row_limit: 0
```

In `dbt_project.yml`, on-run hooks manage compaction and export:

```yaml
on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "CALL ducklake.set_option('target_file_size', '128MB')"
  - "CALL ducklake.set_option('expire_older_than', '2 day')"

on-run-end:
  - "CALL ducklake_rewrite_data_files('ducklake')"
  - "CALL ducklake_merge_adjacent_files('ducklake')"
  - "CALL ducklake_cleanup_old_files('ducklake', cleanup_all => true);"
  - "CALL delta_export()"
```

## Environments

| Target | Storage | Metadata | Use case |
|--------|---------|----------|----------|
| `dev` | Local filesystem (`/tmp/Tables`) | Local SQLite | Development |
| `ci` | Azurite (`az://dbt/Tables`) | Local SQLite | GitHub Actions |
| `prod` | OneLake (`abfss://...`) | Lakehouse SQLite | Fabric notebook |

## Configuration

- `deploy_config.yml` — Workspace ID, schedule, and settings per environment
- `profiles.yml` — dbt targets with DuckLake attach config
- `dbt_project.yml` — Model config, DuckLake hooks, variable defaults

## CI/CD setup (GitHub Actions)

Set as GitHub secrets:
- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`

Push to `main` runs CI tests and publishes dbt docs to GitHub Pages. Push to `production` deploys to Fabric.

## Manual deploy

```bash
az login
python deploy.py --env main
```

## Requirements

- [Microsoft Fabric CLI](https://microsoft.github.io/fabric-cli/) (`pip install ms-fabric-cli`)
- Azure AD app registration with Fabric API permissions
