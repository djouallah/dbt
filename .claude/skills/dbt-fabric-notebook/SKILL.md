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
os.environ['download_limit']      = '100'
os.environ['process_limit']       = '100'
os.environ['METADATA_LOCAL_PATH'] = '/synfs/nb_resource/builtin/metadata.db'
```

| Variable | Purpose |
|----------|---------|
| `DBT_SCHEMA` | Target schema name in DuckLake |
| `ROOT_PATH` | abfss:// path to the Fabric lakehouse root |
| `download_limit` | Max files to download per source per run |
| `process_limit` | Max files to process per model per run (default: 100) |
| `METADATA_LOCAL_PATH` | Local path for DuckLake SQLite metadata DB. Use `/synfs/nb_resource/builtin/` in Fabric — this persists across notebook sessions |

### Cell 3 — Clone dbt project

**Public repo:**
```python
!git clone --branch production --single-branch https://github.com/<your-repo>.git /tmp/dbt
```

**Private repo (production):**
```python
pat = mssparkutils.credentials.getSecret('https://<vault-name>.vault.azure.net/', 'github-pat')
!git clone --branch production --single-branch https://{pat}@github.com/<your-repo>.git /tmp/dbt
```

For private repos, store a GitHub fine-grained PAT (with `Contents: Read` permission) in Azure Key Vault. Grant the Fabric workspace identity `Key Vault Secrets User` access.

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
        - name: delta_export
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
- `config_options.allow_unsigned_extensions: true` — must be set at connection creation time (not `settings`) to allow community extensions
- `settings.preserve_insertion_order: false` — reduces memory usage by allowing DuckDB to reorder results
- `data_path` points to `abfss://.../<lakehouse>/Tables` where Parquet data lives
- `data_inlining_row_limit: 0` — disables data inlining (small writes go to Parquet, not metadata)
- Extensions: `azure` for abfss://, `httpfs` for HTTP downloads, `zipfs` for reading CSVs from ZIPs

## dbt_project.yml — Delta Export & DuckLake Maintenance

```yaml
on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "CALL ducklake.set_option('target_file_size', '128MB')"
  - "{{ download() }}"   # your data ingestion macro

on-run-end:
  - "CALL ducklake_rewrite_data_files('ducklake')"
  - "CALL ducklake_merge_adjacent_files('ducklake')"
  - "CALL delta_export()"
```

`delta_export` is now a **community extension** — installed via `profiles.yml` extensions list (no manual INSTALL/LOAD needed). At run end, DuckLake maintenance compacts data files before `delta_export()` writes Delta Lake transaction logs.

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
        LIMIT {{ env_var('process_limit', '100') }}
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
