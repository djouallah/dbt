# AEMO Electricity — dbt + DuckLake

Downloads Australian electricity market data (AEMO NEM), transforms with dbt-duckdb + DuckLake, and exports to Delta Lake on Microsoft Fabric.

**Single writer only.** No concurrent runs — metadata sync and archive log use read-modify-overwrite on parquet with no locking.

## Quick Start

```bash
pip install duckdb dbt-duckdb
dbt run   # downloads data, transforms, and exports to Delta Lake
dbt test
```

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `ROOT_PATH` | `abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}` | Storage root (uses IDs, not names) |
| `DBT_SCHEMA` | `aemo` | Target schema |
| `download_limit` | `2` | Max files to download per source per run |
| `process_limit` | `500` | Max files to process per model per run |
| `daily_source` | `aemo` | `aemo` (live) or `github` (historical backfill) |
| `METADATA_LOCAL_PATH` | `/tmp/ducklake_metadata.db` | Local path for DuckLake SQLite metadata DB |

Layout under `ROOT_PATH`: `/Tables` (DuckLake data), `/Files/csv` (archives), `/Files/csv_archive_log.parquet`, `/Files/metadata/` (metadata sync).

## Deployment to Microsoft Fabric

`deploy_to_fabric.py` deploys everything (lakehouse, notebook, pipeline, schedule, semantic model) via the Fabric REST API. It is a one-off operation — once deployed, the pipeline runs on its own.

**Note:** Update `WORKSPACE_ID` and `TENANT_ID` in `deploy_to_fabric.py` with your own values before running. Using IDs rather than names is more rigorous and avoids ambiguity.

```bash
python deploy_to_fabric.py              # deploy everything
python deploy_to_fabric.py semantic_model   # just the semantic model
```

See the [blog post](https://datamonkeysite.com/2026/03/05/building-a-data-pipeline-using-vscode-and-claude-out-of-thin-air/) for a full walkthrough, and [CLAUDE.md](CLAUDE.md) for architecture, model details, and implementation notes.
