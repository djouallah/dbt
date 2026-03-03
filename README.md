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
| `ROOT_PATH` | `abfss://duckrun@...dbt.Lakehouse` | Storage root — `abfss://...` or `s3://bucket/prefix` |
| `DBT_SCHEMA` | `aemo` | Target schema |
| `download_limit` | `2` | Max files to download per source per run |
| `process_limit` | `500` | Max files to process per model per run |
| `daily_source` | `aemo` | `aemo` (live) or `github` (historical backfill) |
| `METADATA_LOCAL_PATH` | `/tmp/ducklake_metadata.db` | Local path for DuckLake SQLite metadata DB |

Layout under `ROOT_PATH`: `/Tables` (DuckLake data), `/Files/csv` (archives), `/Files/csv_archive_log.parquet`, `/Files/metadata/` (metadata sync).

See [CLAUDE.md](CLAUDE.md) for full architecture, model details, and implementation notes.
