# dbt + DuckLake on Microsoft Fabric


> **Note:** The official dbt adapter for Fabric is [dbt-fabric](https://github.com/microsoft/dbt-fabric) (connects via ODBC to Fabric Warehouse). This repo is side project, using **dbt-duckdb** with **DuckLake** inside a notebook for lightweight SQL transformations that write directly to OneLake, Notice it is a single writer, and does not support conccurency unless you use Postgresql which is out of scope


> **Limitation:** still a single-writer model — DuckLake cannot run multiple concurrent writers against the same SQLite catalog. The notebook now acquires an exclusive OneLake lease on `metadata.db` so accidental concurrent runs fail fast instead of corrupting the catalog. See [Concurrency safety](#concurrency-safety) below. For true multi-writer concurrency you'd need a Postgres catalog, which is out of scope. 

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

## Concurrency safety

The notebook protects the DuckLake catalog (`metadata.db`) with an exclusive OneLake lease so that an accidental second run cannot corrupt it.

Each run does:

1. Acquire an infinite lease on `Files/metadata.db` via the Azure Data Lake SDK (token from `notebookutils.credentials.getToken("storage")`).
2. Download `metadata.db` to local `/tmp/metadata.db` and point dbt at it (`METADATA_LOCAL_PATH`).
3. Run dbt. The lease is held the whole time.
4. Upload the modified file back under the lease, then release.

If a second notebook starts while the first is running, `acquire_lease` fails immediately with `LeaseAlreadyPresent` and the second run aborts — no silent corruption.

**Stale-lease auto-heal.** Azure leases are either 15–60 s fixed or infinite, so a 12 h "soft" expiry is implemented with file metadata. On acquire, the notebook stamps `acquired_at = <utc iso>` into the file's blob metadata. On a lease conflict, the next run reads that timestamp; if it's older than 12 hours (or missing/unparseable), the notebook calls `break_lease` and re-acquires. So a crashed/killed run self-heals within 12 hours with no manual intervention.

**Manual unstick** (if you need to recover before the 12 h window):

```python
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeLeaseClient
from azure.core.credentials import AccessToken, TokenCredential
import notebookutils

class StaticToken(TokenCredential):
    def __init__(self, t): self.t = t
    def get_token(self, *_, **__): return AccessToken(self.t, 9999999999)

workspace_id = "<ws-guid>"
lakehouse_id = "<lh-guid>"
file = (
    DataLakeServiceClient("https://onelake.dfs.fabric.microsoft.com",
                          credential=StaticToken(notebookutils.credentials.getToken("storage")))
    .get_file_system_client(workspace_id)
    .get_file_client(f"{lakehouse_id}/Files/metadata.db")
)
DataLakeLeaseClient(file).break_lease(lease_break_period=0)
```

Only the SQLite catalog is leased — Delta data files under `Tables/` are not. That's fine: DuckLake's atomicity comes from the catalog, so protecting `metadata.db` is sufficient to prevent split-brain writes.

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
