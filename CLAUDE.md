# CLAUDE.md — Project Memory for aemo_electricity

## Project Overview
- **Name:** aemo_electricity (v1.0.0)
- **Purpose:** Downloads and transforms Australian electricity market data from AEMO (Australian Energy Market Operator) using DuckDB, exports to Delta Lake for Microsoft Fabric
- **Stack:** dbt-duckdb, DuckLake, Microsoft Fabric, Azure (abfss://)
- **Database:** DuckDB in-memory with ducklake extension attached
- **Run:** `dbt run` then `dbt test`, export via `ducklake-delta-exporter`

## Architecture & Data Flow
1. `download()` macro fetches data from AEMO NEM Web (`nemweb.com.au`) and GitHub (`djouallah/aemo_fabric`)
2. Files archived to Azure Fabric as partitioned ZIPs under `abfss://` paths
3. `csv_archive_log.parquet` on abfss:// tracks all downloads (standalone, not a DuckLake table)
4. Fact models read from archives incrementally, processing only new files
5. Dimensions provide reference data (calendar dates, generator units with coordinates)
6. `fct_summary` rolls up intraday + daily SCADA and price data
7. Delta Lake exporter writes final tables to Microsoft Fabric

## Directory Structure
```
dbt/
├── dbt_project.yml          # Project config (vars: daily_source)
├── profiles.yml             # DuckDB + DuckLake connection
├── models/
│   ├── sources.yml          # (no active sources — log is standalone parquet)
│   ├── staging/
│   │   ├── stg_csv_archive_log.sql   # View, runs download() in pre-hook
│   │   └── schema.yml
│   ├── dimensions/
│   │   ├── dim_calendar.sql          # Date dim: 2018-04-01 to 2026-12-31
│   │   ├── dim_duid.sql              # Generator unit reference (DUID, Region, Fuel, coords)
│   │   └── schema.yml
│   └── marts/
│       ├── fct_scada.sql             # Daily SCADA dispatch data
│       ├── fct_scada_today.sql       # Intraday SCADA (5-min intervals)
│       ├── fct_price.sql             # Daily regional electricity prices
│       ├── fct_price_today.sql       # Intraday prices (5-min intervals)
│       ├── fct_summary.sql           # Rollup combining SCADA + price data
│       └── schema.yml
├── macros/
│   ├── download.sql                  # Core data ingestion (371 lines)
│   ├── parse_filename.sql            # Extract filename from path
│   ├── get_csv_archive_path.sql      # Construct Azure Fabric archive path
│   ├── get_archive_paths.sql         # Build dynamic file paths per source type
│   ├── get_metadata_path.sql         # Construct metadata sync path on abfss://
│   ├── import_metadata.sql           # on-run-start: restore metadata DB blob from abfss://
│   └── export_metadata.sql           # on-run-end: upload metadata DB blob to abfss://
├── tests/
│   ├── assert_all_daily_files_processed_price.sql
│   └── assert_all_daily_files_processed_scada.sql
└── dbt.ipynb                         # Jupyter notebook for execution in Fabric
```

## Models (8 total)

| Model | Layer | Materialization | Unique Key |
|-------|-------|-----------------|------------|
| stg_csv_archive_log | staging | view | — |
| dim_calendar | dimensions | table | date |
| dim_duid | dimensions | table | DUID |
| fct_scada | marts | incremental | file, DUID, SETTLEMENTDATE, INTERVENTION |
| fct_scada_today | marts | incremental | file, DUID, SETTLEMENTDATE |
| fct_price | marts | incremental | file, REGIONID, SETTLEMENTDATE, INTERVENTION |
| fct_price_today | marts | incremental | file, REGIONID, SETTLEMENTDATE, INTERVENTION |
| fct_summary | marts | incremental (append) | — |

## Macros (7)
- **download()** (`macros/download.sql`) — Core orchestrator. Downloads daily reports, intraday SCADA, intraday prices, and DUID reference data. Archives to Fabric. Reads/writes `csv_archive_log.parquet` on abfss:// (standalone, not DuckLake). Configurable via `daily_source` var ('aemo' or 'github').
- **parse_filename()** (`macros/parse_filename.sql`) — Extracts filename without extension from a full path. Used in all fact models.
- **get_csv_archive_path()** (`macros/get_csv_archive_path.sql`) — Builds the abfss:// path to the CSV archive using env vars.
- **get_archive_paths()** (`macros/get_archive_paths.sql`) — Queries stg_csv_archive_log and constructs ZIP file paths for each source_type.
- **get_metadata_path()** (`macros/get_metadata_path.sql`) — Builds the abfss:// path to the metadata blob folder.
- **import_metadata()** (`macros/import_metadata.sql`) — on-run-start hook. Downloads the DuckLake SQLite metadata DB blob from abfss://, DETACHes the empty DuckLake, and REATTACHes with the restored file. Skips if no blob exists (first run).
- **export_metadata()** (`macros/export_metadata.sql`) — on-run-end hook. DETACHes DuckLake (WAL checkpoint), then uploads the SQLite file as a blob to abfss:// using `FORMAT BLOB`.

## Incremental Strategies
- **dim_calendar:** One-time load. After first run, uses `WHERE 1=0` to skip inserts. `full_refresh: false`.
- **dim_duid:** Smart refresh — checks for new DUIDs first, only does full rebuild when new ones found. `full_refresh: false`.
- **Fact tables (scada, price, scada_today, price_today):** Incremental by file. Pre-hooks set DuckDB VARIABLEs with paths to unprocessed files only.
- **fct_summary:** Append strategy. Incremental appends new intraday data after max cutoff timestamp. Full refresh recalculates from daily facts.

## Tests (16 total)
- **Singular tests (2):** Assert all downloaded daily files appear in fct_price and fct_scada
- **Schema tests (14):** unique + not_null on dimension keys; not_null on fact keys (DUID, REGIONID, SETTLEMENTDATE, file); accepted_values on source_type

## Key Configuration

### Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| DBT_SCHEMA | aemo | Target schema name |
| FABRIC_WORKSPACE | duckrun | Microsoft Fabric workspace |
| FABRIC_LAKEHOUSE | dbt | Microsoft Fabric lakehouse |

| download_limit | 2 | Max files to download per source per run |
| daily_source | aemo | Data source: 'aemo' (live) or 'github' (historical) |

### DuckDB Extensions
parquet, azure, httpfs, json, sqlite, ducklake, zipfs (community)

### dbt_project.yml Model Config
- `dimensions/` → materialized: table
- `marts/` → materialized: incremental

## Data Domain
- **Domain:** Australian NEM (National Electricity Market)
- **Regions:** WA1, QLD1, NSW1, VIC1, SA1, TAS1
- **Data types:** SCADA dispatch (unit-level MW generation), regional electricity prices (RRP), generator reference data (fuel source, coordinates)
- **Granularity:** 5-minute dispatch intervals
- **Time coverage:** 2018-04-01 to 2026-12-31
- **Sources:** AEMO NEM Web (nemweb.com.au), GitHub archive (djouallah/aemo_fabric), WA AEMO (data.wa.aemo.com.au)

## Data Sources Downloaded by download() Macro
| Source Type | Origin | Partition Pattern | Content |
|-------------|--------|-------------------|---------|
| daily | AEMO or GitHub | /daily/year=YYYY/source_file=name/ | PUBLIC_DAILY*.zip (SCADA + price) |
| scada_today | nemweb.com.au | /scada_today/day=YYYYMMDD/source_file=name/ | PUBLIC_DISPATCHSCADA*.zip |
| price_today | nemweb.com.au | /price_today/day=YYYYMMDD/source_file=name/ | PUBLIC_DISPATCHIS_*.zip |
| duid_data | GitHub | /duid/ | duid_data.csv |
| duid_facilities | WA AEMO | /duid/ | facilities.csv |
| duid_wa_energy | GitHub | /duid/ | WA_ENERGY.csv |
| duid_geo_data | GitHub | /duid/ | geo_data.csv |

## DuckLake Metadata Sync
DuckLake needs a local filesystem for its SQLite metadata DB — it can't run directly on object storage (abfss://). To make metadata portable across ephemeral environments (e.g., Fabric notebook sessions), the entire SQLite file is synced as a binary blob using `FORMAT BLOB`:
- **on-run-start:** `import_metadata()` checks for a blob at `abfss://.../Files/metadata/data_0.db`. If found, DETACHes the empty DuckLake, downloads the blob via `COPY (read_blob(...)) TO ... (FORMAT BLOB)`, then REATTACHes DuckLake to the restored file.
- **on-run-end:** `export_metadata()` DETACHes DuckLake (checkpoints WAL), then uploads the SQLite file as a blob to abfss:// via `COPY (read_blob(...)) TO ... (FORMAT BLOB)`.
- **First run:** No blob exists, import is skipped, DuckLake starts fresh. After the run, the SQLite file is exported for future sessions.
- **Subsequent runs:** The full SQLite file (with complete DuckLake history) is restored from blob, dbt runs normally, updated file is exported back.

## Key Patterns & Conventions
- Pre-hooks on models create schemas/tables and set DuckDB VARIABLEs with file paths
- CSV files read directly from ZIP archives using DuckDB's `read_csv()` with `ignore_errors=true, null_padding=true`
- File-based incremental: each fact model tracks which files it has processed via the `file` column
- DuckDB `VARIABLE` system used for dynamic SQL path construction in pre-hooks
- `{{ parse_filename(...) }}` used consistently across all fact models to extract source identifiers
- No external dbt packages — self-contained project
