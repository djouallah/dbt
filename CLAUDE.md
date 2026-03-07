# CLAUDE.md — aemo_electricity

## Quick Reference
- **Stack:** dbt-duckdb, DuckLake, Delta Lake, Microsoft Fabric
- **Run:** `dbt build --target dev --profiles-dir .`
- **Schemas:** `mart` (dim_calendar, dim_duid, fct_summary) / `landing` (facts, staging)
- **Blog:** https://datamonkeysite.com/2026/03/05/building-a-data-pipeline-using-vscode-and-claude-out-of-thin-air/

## Architecture
1. `stg_csv_archive_log.py` (Python model) downloads data from AEMO + GitHub, archives as gzipped CSVs on OneLake
2. DUID reference data skipped if downloaded < 24 hours ago
3. Fact models read from archives incrementally (file-based), dimensions are smart-refresh
4. `fct_summary` rolls up daily + intraday SCADA and price data
5. On-run-end: DuckLake compaction → `delta_export()` writes Delta Lake logs for Fabric/Power BI

## Models (8)
| Model | Schema | Materialization |
|-------|--------|-----------------|
| stg_csv_archive_log | landing | table (Python) |
| dim_calendar | mart | incremental (one-time) |
| dim_duid | mart | incremental (smart refresh) |
| fct_scada, fct_price | landing | incremental (by file) |
| fct_scada_today, fct_price_today | landing | incremental (by file) |
| fct_summary | mart | incremental (append) |

## Macros (4)
- `parse_filename.sql`, `get_csv_archive_path.sql`, `get_root_path.sql`, `generate_schema_name.sql`

## Key Env Vars
`ROOT_PATH` (abfss:// storage root), `DBT_SCHEMA` (default: mart), `METADATA_LOCAL_PATH`, `download_limit` (default: 2), `process_limit` (default: 500)

## Profiles: dev (local), ci (Azurite), prod (OneLake)

## Deployment
- **Manual:** Upload `dbt.ipynb` to Fabric
- **Script:** `deploy_to_fabric.py` (7 steps, uses `az login`)
- **CI:** main → Azurite tests; production → deploy + docs to GitHub Pages

## Semantic Model (`model.bim`)
TMSL format, Direct Lake mode, `{{ONELAKE_URL}}` placeholder, 3 visible tables, 5 DAX measures

## Key Patterns
- Pre-hooks set DuckDB VARIABLEs with file paths for incremental processing
- CSVs read from gzipped archives via `read_csv()` with `ignore_errors=true`
- DuckLake metadata in SQLite (not DuckDB) for reliable FUSE writes
- Single-writer only (pipeline concurrency = 1)
- `delta_export` extension: https://github.com/djouallah/delta_export
