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
       -> on-run-start: installs delta_export extension, downloads data
       -> Transforms with dbt models (incremental by file)
       -> DuckLake writes Parquet to abfss://.../<lakehouse>/Tables/
       -> on-run-end: CALL delta_export() writes Delta Lake _delta_log
  -> Fabric / Power BI reads Delta tables natively
```

Key insight: DuckDB runs **in-memory** inside the notebook. DuckLake stores data as Parquet on OneLake (abfss://) and keeps metadata in a local SQLite file. The `delta_export` DuckDB extension converts DuckLake metadata into Delta Lake transaction logs — no separate Python package needed. Everything runs inside `dbt run`.

## Notebook Template (4 cells)

### Cell 1 — Install dependencies
```python
!pip install -q duckdb==1.4.4
!pip install -q dbt-duckdb
import sys
sys.exit(0)  # Restart kernel to pick up new packages
```

### Cell 2 — Environment variables
```python
import os
os.environ['DBT_SCHEMA']          = 'my_schema'
os.environ['ROOT_PATH']           = 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse'
os.environ['download_limit']      = '10'
os.environ['process_limit']       = '500'
os.environ['METADATA_LOCAL_PATH'] = '/synfs/nb_resource/builtin/metadata.db'
```

| Variable | Purpose |
|----------|---------|
| `DBT_SCHEMA` | Target schema name in DuckLake |
| `ROOT_PATH` | abfss:// path to the Fabric lakehouse root |
| `download_limit` | Max files to download per source per run |
| `process_limit` | Max files to process per model per run (default: 500) |
| `METADATA_LOCAL_PATH` | Local path for DuckLake SQLite metadata DB. Use `/synfs/nb_resource/builtin/` in Fabric — this persists across notebook sessions |

### Cell 3 — Clone dbt project
```python
!git clone https://github.com/<your-repo>.git /tmp/dbt
```

### Cell 4 — Run dbt
```python
!cd /tmp/dbt && dbt run && dbt test
```

Delta export happens automatically via the `on-run-end` hook — no separate step needed.

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
      schema: "{{ env_var('DBT_SCHEMA', 'default') }}"
      config_options:
        allow_unsigned_extensions: true
      settings:
        preserve_insertion_order: false
      extensions:
        - parquet
        - sqlite
        - azure
        - httpfs
        - json
        - ducklake
        - name: zipfs
          repo: community
      attach:
        - path: "ducklake:sqlite:{{ env_var('METADATA_LOCAL_PATH', '/tmp/ducklake_metadata.db') }}"
          alias: ducklake
          options:
            data_path: "{{ env_var('ROOT_PATH') }}/Tables"
            data_inlining_row_limit: 0
```

Key points:
- `path: ':memory:'` — DuckDB runs fully in-memory, no local database file
- `database: ducklake` — tells dbt to target the attached DuckLake catalog
- `config_options.allow_unsigned_extensions: true` — must be set at connection creation time (not `settings`) to allow unsigned extensions like `delta_export`
- `settings.preserve_insertion_order: false` — reduces memory usage by allowing DuckDB to reorder results
- `data_path` points to `abfss://.../<lakehouse>/Tables` where Parquet data lives
- `data_inlining_row_limit: 0` — disables data inlining (small writes go to Parquet, not metadata)
- Extensions: `azure` for abfss://, `httpfs` for HTTP downloads, `zipfs` for reading CSVs from ZIPs

## dbt_project.yml — Delta Export & DuckLake Config

```yaml
on-run-start:
  - "INSTALL delta_export FROM 'https://djouallah.github.io/delta_export'"
  - "LOAD delta_export"
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "{{ download() }}"   # your data ingestion macro

on-run-end:
  - "CALL delta_export()"
```

The `delta_export` extension is installed and loaded at run start (requires `allow_unsigned_extensions` in `config_options`). At run end, `CALL delta_export()` writes Delta Lake transaction logs alongside the Parquet files so Fabric/Power BI reads them as native Delta tables.

| DuckLake Option | Value | How to Set | Why |
|-----------------|-------|------------|-----|
| `data_inlining_row_limit` | 0 | ATTACH option in profiles.yml | Disable storing small inserts in metadata DB |
| `rewrite_delete_threshold` | 0 | `set_option` in on-run-start hook (not an ATTACH param) | Disable automatic file rewriting on deletes |

## Key Patterns

### Metadata persistence in Fabric
DuckLake's SQLite metadata DB needs a local filesystem. In Fabric notebooks, use `/synfs/nb_resource/builtin/` — this is the notebook resource folder that persists across sessions. Set `METADATA_LOCAL_PATH` to a file in that folder.

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
        FROM {{ ref('stg_archive_log') }}
        WHERE source_type = 'daily'
        {% if is_incremental() %}
          AND source_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
        {% endif %}
        LIMIT {{ env_var('process_limit', '500') }}
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
