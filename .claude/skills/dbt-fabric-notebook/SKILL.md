---
name: dbt-fabric-notebook
description: Guide for running dbt inside Microsoft Fabric Python notebooks using DuckDB, DuckLake, and Delta Lake export. Trigger when user asks about dbt in Fabric, dbt-duckdb in notebooks, or DuckLake with Fabric.
---

# Running dbt inside a Microsoft Fabric Python Notebook

This skill explains how to run a full dbt pipeline inside a Microsoft Fabric Python notebook using DuckDB as the compute engine and DuckLake for Delta Lake table management.

## Architecture

```
Fabric Notebook (ephemeral Python session)
  -> pip install duckdb, dbt-duckdb
  -> dbt run (DuckDB in-memory + DuckLake extension)
       -> on-run-start: set DuckLake options
       -> stg_csv_archive_log.py (Python model): downloads + archives data
       -> Transforms with dbt models (incremental by file)
       -> DuckLake writes Parquet to abfss://.../<lakehouse>/Tables/
       -> on-run-end: DuckLake maintenance + CALL delta_export()
  -> dbt test
  -> Fabric / Power BI reads Delta tables natively
```

Key insight: DuckDB runs **in-memory** inside the notebook. DuckLake stores data as Parquet on OneLake (abfss://) and keeps metadata in a local SQLite file. The `delta_export` DuckDB extension converts DuckLake metadata into Delta Lake transaction logs — no separate Python package needed. Everything runs inside `dbt run`.

## Notebook Template (2 cells)

The deploy script generates the notebook with env vars baked into the run command. The notebook only needs two cells:

### Cell 1 — Install dependencies
```python
!pip install -q duckdb==1.4.4
!pip install -q dbt-duckdb
import sys
sys.exit(0)  # Restart kernel to pick up new packages
```

### Cell 2 — Run dbt
```python
import os
os.environ['ROOT_PATH']           = 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse'
os.environ['METADATA_LOCAL_PATH'] = '/lakehouse/default/Files/metadata.db'
os.environ['download_limit']      = '100'
os.environ['process_limit']       = '1000'
!cd /lakehouse/default/Files/dbt && dbt run --target prod --profiles-dir . && dbt test --target prod --profiles-dir .
```

| Variable | Purpose |
|----------|---------|
| `ROOT_PATH` | abfss:// path to the Fabric lakehouse root |
| `METADATA_LOCAL_PATH` | Local path for DuckLake SQLite metadata DB. `/lakehouse/default/` is a FUSE mount of OneLake — the SQLite file persists across notebook runs |
| `download_limit` | Max files to download per source per run (default: 2) |
| `process_limit` | Max files to process per model per run (default: 1000) |

Note: `DBT_SCHEMA` defaults to `mart` in profiles.yml. Override only if you need a different schema name.

## Data Ingestion via Python Model

Data download is handled by `stg_csv_archive_log.py` — a **dbt Python model** (materialized as table). This replaces the old `download()` macro approach. The Python model:

- Downloads ZIPs from AEMO/GitHub using `urllib.request`
- Extracts CSVs, gzip-compresses them, uploads to OneLake via DuckDB `COPY ... TO ... (FORMAT BLOB)`
- Tracks all downloads in `csv_archive_log.parquet` on abfss://
- Uses `ThreadPoolExecutor` for parallel downloads (batch_size=7, max_workers=8)
- Returns the full log as a DuckDB relation (no pandas/numpy dependency)

This runs as a regular dbt model in dependency order — no on-run-start hook needed.

## profiles.yml Configuration

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ':memory:'
      database: ducklake
      threads: 1
      schema: "{{ env_var('DBT_SCHEMA', 'mart') }}"
      config_options:
        allow_unsigned_extensions: true
      settings:
        preserve_insertion_order: false
      extensions:
        - parquet
        - sqlite
        - httpfs
        - json
        - ducklake
        - name: delta_export
          repo: community
      attach:
        - path: "ducklake:sqlite:{{ env_var('METADATA_LOCAL_PATH', '/tmp/metadata.db') }}"
          alias: ducklake
          options:
            data_path: "{{ env_var('ROOT_PATH', '/tmp') }}/Tables"
            data_inlining_row_limit: 0
```

Key points:
- `path: ':memory:'` — DuckDB runs fully in-memory, no local database file
- `database: ducklake` — tells dbt to target the attached DuckLake catalog
- `config_options.allow_unsigned_extensions: true` — must be set at connection creation time (not `settings`) to allow community extensions
- `settings.preserve_insertion_order: false` — reduces memory usage by allowing DuckDB to reorder results
- `data_path` points to `abfss://.../<lakehouse>/Tables` where Parquet data lives
- `data_inlining_row_limit: 0` — disables data inlining (small writes go to Parquet, not metadata)

### CI target with Azurite
For CI testing, add a separate target with Azure extension and Azurite connection string:
```yaml
    ci:
      type: duckdb
      path: ':memory:'
      database: ducklake
      threads: 1
      schema: "{{ env_var('DBT_SCHEMA', 'mart') }}"
      config_options:
        allow_unsigned_extensions: true
      settings:
        preserve_insertion_order: false
        azure_storage_connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
      secrets:
        - type: azure
          provider: config
          connection_string: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
      extensions:
        - parquet
        - sqlite
        - azure
        - json
        - ducklake
        - name: delta_export
          repo: community
      attach:
        - path: "ducklake:sqlite:{{ env_var('METADATA_LOCAL_PATH', '/tmp/metadata.db') }}"
          alias: ducklake
          options:
            data_path: "{{ env_var('ROOT_PATH', 'az://dbt') }}/Tables"
            data_inlining_row_limit: 0
```

The CI target uses `az://` (Azurite) instead of `abfss://` (OneLake), and requires both `settings.azure_storage_connection_string` and a `secrets` block for DuckDB's Azure extension to authenticate with the emulator.

## dbt_project.yml — Delta Export & DuckLake Maintenance

```yaml
on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "CALL ducklake.set_option('target_file_size', '128MB')"

on-run-end:
  - "CALL ducklake_rewrite_data_files('ducklake')"
  - "CALL ducklake_merge_adjacent_files('ducklake')"
  - "CALL delta_export()"
```

`delta_export` is a **community extension** — installed via `profiles.yml` extensions list (no manual INSTALL/LOAD needed). At run end, DuckLake maintenance compacts data files before `delta_export()` writes Delta Lake transaction logs.

**Why delta_export is mandatory:** DuckLake is not natively supported by Spark, Power BI, or any other engine in Microsoft Fabric. Without `delta_export`, the Parquet files written by DuckLake are invisible to the rest of the platform. The Delta Lake transaction logs (`_delta_log/`) make the tables readable as standard Delta tables by Power BI, Spark, SQL Analytics, and any Delta-compatible tool.

### DuckLake Maintenance (on-run-end)

| Call | Purpose |
|------|---------|
| `ducklake_rewrite_data_files('ducklake')` | Rewrites files with deletes (threshold controlled by `rewrite_delete_threshold`) |
| `ducklake_merge_adjacent_files('ducklake')` | Merges small Parquet files into larger ones (target size from `target_file_size`) |
| `delta_export()` | Writes Delta Lake `_delta_log/` so Fabric/Power BI reads tables natively |

### DuckLake Options

| Option | Value | How to Set | Why |
|--------|-------|------------|-----|
| `data_inlining_row_limit` | 0 | ATTACH option in profiles.yml | Disable storing small inserts in metadata DB |
| `rewrite_delete_threshold` | 0 | `set_option` in on-run-start (not an ATTACH param) | Rewrite all files with any deletes |
| `target_file_size` | 128MB | `set_option` in on-run-start (not an ATTACH param) | Target Parquet file size for insert and compaction (default 512MB, reduced to prevent OOM) |

Note: `target_file_size` controls both `merge_adjacent_files` merge target and auto-splitting of large inserts. It cannot be set via ATTACH options — must use `set_option`.

## Schema Separation (mart + landing)

Separate intermediate tables from Power BI-facing tables using dbt schema config:

- **`mart`** schema (default from `DBT_SCHEMA`): dim_calendar, dim_duid, fct_summary — exposed to Power BI
- **`landing`** schema: fct_scada, fct_scada_today, fct_price, fct_price_today, stg_csv_archive_log — intermediate

**dbt_project.yml:**
```yaml
models:
  aemo_electricity:
    staging:
      +materialized: view
      +schema: landing
    dimensions:
      +materialized: table
    marts:
      +materialized: incremental
      +schema: landing
```

Override specific models back to `mart`: `{{ config(schema='mart') }}` in `fct_summary.sql`.

**Custom schema macro required** (`macros/generate_schema_name.sql`):
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```
Without this, dbt prefixes the target schema (e.g., `mart_landing` instead of `landing`).

## Semantic Model (model.bim)

Deploy via Fabric REST API using **TMSL format (model.bim)** — TMDL create is NOT supported by the API.

Key requirements for Direct Lake on OneLake (no SQL endpoint):
- `PBI_ProTooling` annotation with `DirectLakeOnOneLakeCreatedInDesktop`
- `sourceLineageTag` on all tables (`[schema].[table]`) and columns
- `relyOnReferentialIntegrity` on all relationships
- `PBI_RemovedChildren` on expression listing excluded tables with correct schema prefix
- Partition `schemaName` must match the lakehouse schema folder name

## Key Patterns

### Metadata persistence in Fabric
DuckLake's SQLite metadata DB needs a local filesystem. In Fabric notebooks, use `/lakehouse/default/Files/` — this is a FUSE mount of OneLake that persists across notebook runs without any special sync logic. Set `METADATA_LOCAL_PATH` to a file in that folder (e.g., `/lakehouse/default/Files/metadata.db`).

### Incremental by file with process_limit
Track which source files have been processed using a `file` column in each fact table. Pre-hooks use DuckDB `SET VARIABLE` to pass only unprocessed file paths, capped by `process_limit`:

```sql
{{ config(
    materialized='incremental',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
    pre_hook="SET VARIABLE paths = (
      SELECT list(...)
      FROM (
        SELECT source_filename
        FROM {{ ref('stg_csv_archive_log') }}
        WHERE source_type = 'daily'
        {% if is_incremental() %}
          AND source_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
        {% endif %}
        LIMIT {{ env_var('process_limit', '1000') }}
      )
    )"
) }}
```

### Reading CSVs from ZIP archives
DuckDB can read CSVs directly from ZIP files on remote storage:

```sql
SELECT * FROM read_csv(
    getvariable('paths'),
    ignore_errors=true,
    null_padding=true
)
```

## Reference
See [reference.md](reference.md) for full code examples.
