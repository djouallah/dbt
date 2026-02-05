# AEMO Electricity dbt Project

A dbt project that downloads and transforms Australian electricity market data from AEMO (Australian Energy Market Operator) using DuckDB and exports to Delta Lake for Microsoft Fabric.

## Prerequisites

- Python 3.x
- dbt-duckdb
- DuckDB
- ducklake-delta-exporter

Install dependencies:
```bash
pip install duckdb dbt-duckdb ducklake-delta-exporter
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DBT_SCHEMA` | Target schema name (e.g., `power`) |
| `FABRIC_WORKSPACE` | Microsoft Fabric workspace name |
| `FABRIC_LAKEHOUSE` | Microsoft Fabric lakehouse name |
| `DUCKLAKE_METADATA_PATH` | Path to DuckLake metadata SQLite database |
| `download_limit` | Number of files to download per source |

## Usage

Run the dbt project:
```bash
dbt run
dbt test
```

Export to Delta Lake:
```python
from ducklake_delta_exporter import generate_latest_delta_log
generate_latest_delta_log('/lakehouse/default/Files/metadata.db')
```

## Models

### Staging
- `stg_csv_archive_log` - Tracks downloaded CSV files and triggers data downloads

### Dimensions
- `dim_calendar` - Date dimension
- `dim_duid` - Dispatch Unit Identifier (generator) reference data

### Facts
- `fct_scada` - Historical SCADA generation data
- `fct_scada_today` - Intraday SCADA data
- `fct_price` - Historical electricity prices
- `fct_price_today` - Intraday prices
- `fct_summary` - Aggregated summary metrics

## Data Sources

Data is downloaded from AEMO's public APIs. Configure `daily_source` in `dbt_project.yml`:
- `aemo` (default) - Direct from AEMO, no rate limits
- `github` - Historical archive for backfilling

# Limitation

you need a filesystem to store sqlite metadata, fuse works fine too
