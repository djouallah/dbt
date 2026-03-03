---
name: dbt-fabric-notebook
description: Guide for running dbt inside Microsoft Fabric Python notebooks using DuckDB, DuckLake, and Delta Lake export. Trigger when user asks about dbt in Fabric, dbt-duckdb in notebooks, or DuckLake with Fabric.
---

# Running dbt inside a Microsoft Fabric Python Notebook

This skill explains how to run a full dbt pipeline inside a Microsoft Fabric Python notebook using DuckDB as the compute engine and DuckLake for Delta Lake table management.

## Architecture

```
Fabric Notebook (ephemeral Python session)
  -> pip install duckdb, dbt-duckdb, ducklake-delta-exporter
  -> dbt run (DuckDB in-memory + DuckLake extension)
       -> Downloads data via HTTP/HTTPS into Azure (abfss://)
       -> Transforms with dbt models (incremental by file)
       -> DuckLake writes Parquet to abfss://.../<lakehouse>/Tables/
  -> ducklake-delta-exporter converts metadata to Delta Lake _delta_log
  -> Fabric / Power BI reads Delta tables natively
```

Key insight: DuckDB runs **in-memory** inside the notebook. DuckLake stores data as Parquet on OneLake (abfss://) and keeps metadata in a local SQLite file. The `ducklake-delta-exporter` converts DuckLake metadata into Delta Lake transaction logs so Fabric and Power BI can read the tables.

## Notebook Template (6 cells)

### Cell 1 — Install dependencies
```python
!pip install -q duckdb==1.4.4
!pip install -q dbt-duckdb ducklake-delta-exporter --upgrade
import sys
sys.exit(0)  # Restart kernel to pick up new packages
```

### Cell 2 — Imports
```python
import os
import duckdb
```

### Cell 3 — Environment variables
```python
os.environ['DBT_SCHEMA']         = 'my_schema'
os.environ['ROOT_PATH']          = 'abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse'
os.environ['download_limit']     = '10'
os.environ['METADATA_LOCAL_PATH'] = '/synfs/nb_resource/builtin/metadata.db'
```

| Variable | Purpose |
|----------|---------|
| `DBT_SCHEMA` | Target schema name in DuckLake |
| `ROOT_PATH` | abfss:// path to the Fabric lakehouse root |
| `download_limit` | Max files to download per source per run |
| `METADATA_LOCAL_PATH` | Local path for DuckLake SQLite metadata DB. Use `/synfs/nb_resource/builtin/` in Fabric — this persists across notebook sessions |

### Cell 4 — Clone dbt project
```python
!git clone https://github.com/<your-repo>.git /tmp/dbt
```

### Cell 5 — Run dbt
```python
!cd /tmp/dbt && dbt run && dbt test
```

### Cell 6 — Export to Delta Lake
```python
from ducklake_delta_exporter import generate_latest_delta_log
generate_latest_delta_log(os.environ['METADATA_LOCAL_PATH'])
```

This converts the DuckLake metadata into Delta Lake `_delta_log/` entries so Fabric sees proper Delta tables.

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
- `data_path` points to `abfss://.../<lakehouse>/Tables` where Parquet data lives
- `data_inlining_row_limit: 0` — disables data inlining (small writes go to Parquet, not metadata)
- Extensions: `azure` for abfss://, `httpfs` for HTTP downloads, `zipfs` for reading CSVs from ZIPs

## DuckLake Configuration

Disable features that add overhead for append-heavy pipelines:

```yaml
# In dbt_project.yml
on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "{{ download() }}"   # your data ingestion macro
```

| DuckLake Option | Value | How to Set | Why |
|-----------------|-------|------------|-----|
| `data_inlining_row_limit` | 0 | ATTACH option in profiles.yml | Disable storing small inserts in metadata DB |
| `rewrite_delete_threshold` | 0 | `set_option` in on-run-start hook | Disable automatic file rewriting on deletes |

## Key Patterns

### Metadata persistence in Fabric
DuckLake's SQLite metadata DB needs a local filesystem. In Fabric notebooks, use `/synfs/nb_resource/builtin/` — this is the notebook resource folder that persists across sessions. Set `METADATA_LOCAL_PATH` to a file in that folder.

### Incremental by file
Track which source files have been processed using a `file` column in each fact table. Pre-hooks use DuckDB `SET VARIABLE` to pass only unprocessed file paths:

```sql
{{ config(
    materialized='incremental',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE'],
    pre_hook="SET VARIABLE paths = (
      SELECT list(...)
      FROM {{ ref('stg_archive_log') }}
      WHERE source_type = 'daily'
      {% if is_incremental() %}
        AND source_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
      {% endif %}
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

### Delta Lake export
After `dbt run`, call `generate_latest_delta_log()` with the path to the DuckLake SQLite metadata file. This writes Delta Lake transaction logs alongside the Parquet files so Fabric/Power BI can read them as native Delta tables.

## Reference
See [reference.md](reference.md) for full code examples.
