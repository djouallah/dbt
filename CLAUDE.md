# CLAUDE.md — Project Memory for aemo_electricity

## Project Overview
- **Name:** aemo_electricity (v1.0.0)
- **Purpose:** Downloads and transforms Australian electricity market data from AEMO (Australian Energy Market Operator) using DuckDB, exports to Delta Lake for Microsoft Fabric
- **Stack:** dbt-duckdb, DuckLake, Microsoft Fabric, Azure (abfss://)
- **Database:** DuckDB in-memory with DuckLake extension attached (SQLite metadata catalog)
- **Run:** `dbt run` then `dbt test`; delta_export runs automatically in on-run-end hook
- **Blog:** https://datamonkeysite.com/2026/03/05/building-a-data-pipeline-using-vscode-and-claude-out-of-thin-air/

## Architecture & Data Flow
1. `download()` macro fetches data from AEMO NEM Web (`nemweb.com.au`) and GitHub (`djouallah/aemo_fabric`)
2. Files archived to OneLake as partitioned ZIPs under `abfss://` paths
3. `csv_archive_log.parquet` on abfss:// tracks all downloads (standalone, not a DuckLake table)
4. Fact models read from archives incrementally, processing only new files
5. Dimensions provide reference data (calendar dates, generator units with coordinates)
6. `fct_summary` rolls up intraday + daily SCADA and price data
7. `delta_export()` converts DuckLake tables to Delta Lake on OneLake (on-run-end)

## Directory Structure
```
dbt/
├── dbt_project.yml          # Project config + on-run-start/end hooks
├── profiles.yml             # DuckDB + DuckLake connection (dev/ci/prod targets)
├── deploy_to_fabric.py      # Deployment script (Fabric REST API)
├── model.bim                # AI-generated Power BI semantic model (TMSL, Direct Lake)
├── dbt.ipynb                # Jupyter notebook for execution in Fabric
├── models/
│   ├── sources.yml
│   ├── staging/
│   │   ├── stg_csv_archive_log.sql   # View over archive log parquet
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
│   ├── download.sql                  # Core data ingestion
│   ├── parse_filename.sql            # Extract filename from path
│   ├── get_csv_archive_path.sql      # Construct abfss:// archive path
│   ├── get_archive_paths.sql         # Build dynamic file paths per source type
│   ├── get_root_path.sql             # Construct ROOT_PATH
│   └── generate_schema_name.sql      # Custom schema routing (no prefix)
├── tests/                            # 7 singular tests
│   ├── assert_all_daily_files_processed_price.sql
│   ├── assert_all_daily_files_processed_scada.sql
│   ├── assert_all_today_files_processed_price.sql
│   ├── assert_all_today_files_processed_scada.sql
│   ├── assert_summary_covers_all_scada_days.sql
│   ├── assert_summary_delta_row_count.sql
│   └── assert_summary_has_valid_joins.sql
└── .github/
    └── workflows/
        └── dbt-ci.yml               # CI: Azurite + dbt run/test + docs to GitHub Pages
```

## Models (8 total)

| Model | Layer | Materialization | Unique Key |
|-------|-------|-----------------|------------|
| stg_csv_archive_log | staging | view | -- |
| dim_calendar | dimensions | table | date |
| dim_duid | dimensions | table | DUID |
| fct_scada | marts | incremental | file, DUID, SETTLEMENTDATE, INTERVENTION |
| fct_scada_today | marts | incremental | file, DUID, SETTLEMENTDATE |
| fct_price | marts | incremental | file, REGIONID, SETTLEMENTDATE, INTERVENTION |
| fct_price_today | marts | incremental | file, REGIONID, SETTLEMENTDATE, INTERVENTION |
| fct_summary | marts | incremental (append) | -- |

## Schema Layout
- **`aemo`** schema: dim_calendar, dim_duid, fct_summary (Power BI-facing)
- **`raw`** schema: fct_scada, fct_scada_today, fct_price, fct_price_today, stg_csv_archive_log (intermediate)
- Default schema from `profiles.yml` is `aemo` (env var `DBT_SCHEMA`)
- `dbt_project.yml` sets `+schema: raw` on staging/ and marts/ folders
- `fct_summary.sql` overrides back to `schema='aemo'`
- `generate_schema_name.sql` uses schema name directly (no prefix)

## Macros (6)
- **download()** (`macros/download.sql`) -- Core orchestrator. Downloads daily reports, intraday SCADA, intraday prices, and DUID reference data. Archives to OneLake. Reads/writes `csv_archive_log.parquet` on abfss:// (standalone, not DuckLake).
- **parse_filename()** (`macros/parse_filename.sql`) -- Extracts filename without extension from a full path.
- **get_csv_archive_path()** (`macros/get_csv_archive_path.sql`) -- Builds the abfss:// path to the CSV archive.
- **get_archive_paths()** (`macros/get_archive_paths.sql`) -- Queries stg_csv_archive_log and constructs ZIP file paths for each source_type.
- **get_root_path()** (`macros/get_root_path.sql`) -- Constructs the ROOT_PATH from env vars.
- **generate_schema_name()** (`macros/generate_schema_name.sql`) -- Custom schema routing: uses schema name as-is, no prefix.

## Incremental Strategies
- **dim_calendar:** One-time load. After first run, uses `WHERE 1=0` to skip inserts. `full_refresh: false`.
- **dim_duid:** Smart refresh -- checks for new DUIDs first, only does full rebuild when new ones found. `full_refresh: false`.
- **Fact tables (scada, price, scada_today, price_today):** Incremental by file. Pre-hooks set DuckDB VARIABLEs with paths to unprocessed files only.
- **fct_summary:** Append strategy. Incremental appends new intraday data after max cutoff timestamp. Full refresh recalculates from daily facts.

## Tests (35 total)
- **Singular tests (7):** Assert all downloaded files appear in respective fact tables; assert fct_summary covers all scada days, has valid joins, and delta row count matches.
- **Schema tests (28):** unique + not_null on dimension keys; not_null on fact keys (DUID, REGIONID, SETTLEMENTDATE, file); accepted_values on source_type and REGIONID; relationships from fct_scada/fct_summary DUID to dim_duid.

## Key Configuration

### Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| ROOT_PATH | `abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}` | Storage root (uses IDs, not names) |
| DBT_SCHEMA | aemo | Target schema name |
| METADATA_LOCAL_PATH | /tmp/ducklake_metadata.db | Local path for DuckLake SQLite metadata DB |
| download_limit | 2 | Max files to download per source per run |
| process_limit | 500 | Max files to process per model per run |

### DuckDB Extensions
parquet, azure, httpfs, json, sqlite, ducklake, zipfs (community), delta_export (community)

### dbt_project.yml Hooks
```yaml
on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "CALL ducklake.set_option('target_file_size', '128MB')"
  - "{{ download() }}"

on-run-end:
  - "CALL ducklake_rewrite_data_files('ducklake')"
  - "CALL ducklake_merge_adjacent_files('ducklake')"
  - "CALL delta_export()"
```

### Profiles (3 targets)
| Target | Use | Storage |
|--------|-----|---------|
| dev | Local development | In-memory, local filesystem |
| ci | GitHub Actions | In-memory, Azurite (Azure Storage emulator) |
| prod | Fabric notebook | In-memory, OneLake (abfss://) |

## DuckLake Metadata
- DuckLake SQLite metadata DB lives at `METADATA_LOCAL_PATH`
- In Fabric notebooks: `/lakehouse/default/Files/metadata.db` (Files section of OneLake lakehouse)
- `/lakehouse/default/` is a local FUSE mount of OneLake -- the SQLite file persists across notebook runs without any special sync logic
- SQLite chosen over DuckDB for the metadata DB because it flushes more reliably on OneLake's FUSE filesystem
- **Single-writer limitation:** DuckLake with a file-based DB is single-writer. Pipeline concurrency is set to 1.

## Deployment

### Option 1: Manual
Upload `dbt.ipynb` to a Fabric workspace, create and attach a lakehouse, run it.

### Option 2: Script (`deploy_to_fabric.py`)
7 steps: lakehouse, files, initial_load, notebook, semantic_model, pipeline, schedule. Uses `az login` (AzureCliCredential) -- no service principal needed. Clones `production` branch for deployment.

### Deploy Idempotency Rules
- Lakehouse is never recreated if it already exists.
- All other steps (notebook, semantic_model, pipeline, schedule) are safe to re-run — updating with same content is harmless.
- `schedule` skips update if interval and enabled flag match.
- `initial_load` only runs on first deploy (new lakehouse). Gated by `first_deploy` output.

### CI/CD
- **Main branch** (`.github/workflows/dbt-ci.yml`): Azurite + `dbt run --target ci` + `dbt test --target ci`. No docs.
- **Production branch** (`.github/workflows/deploy.yml`): Full deploy DAG + dbt docs to GitHub Pages.
- GitHub Pages source must be set to "GitHub Actions" (not "Deploy from a branch").

## Semantic Model (`model.bim`)
- AI-generated TMSL (Tabular Model Scripting Language) format
- Pure Direct Lake mode: `DirectLakeOnOneLakeCreatedInDesktop` annotation (no SQL endpoint fallback)
- `{{ONELAKE_URL}}` placeholder substituted at deploy time
- 3 visible tables (dim_calendar, dim_duid, fct_summary), hidden raw tables
- 2 relationships (fct_summary -> dim_duid, fct_summary -> dim_calendar)
- 5 DAX measures (Total MW, Total MWh, Avg Price, Generator Count, Latest Update)

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

## Key Patterns & Conventions
- Pre-hooks on models set DuckDB VARIABLEs with file paths
- CSV files read directly from ZIP archives using DuckDB's `read_csv()` with `ignore_errors=true, null_padding=true`
- File-based incremental: each fact model tracks which files it has processed via the `file` column
- DuckDB `VARIABLE` system used for dynamic SQL path construction in pre-hooks
- `{{ parse_filename(...) }}` used consistently across all fact models to extract source identifiers
- No external dbt packages -- self-contained project
- delta_export community extension (https://github.com/djouallah/delta_export) converts DuckLake tables to Delta Lake
