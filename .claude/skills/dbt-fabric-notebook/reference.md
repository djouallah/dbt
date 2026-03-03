# Reference: dbt in Fabric Notebook — Full Code Examples

## Complete profiles.yml

```yaml
aemo_electricity:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ':memory:'
      database: ducklake
      threads: 1
      schema: "{{ env_var('DBT_SCHEMA', 'aemo') }}"
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

## Complete dbt_project.yml

```yaml
name: 'aemo_electricity'
version: '1.0.0'
profile: 'aemo_electricity'

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

vars:
  # download_limit: max files per source per run (default: 2)

on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "{{ download() }}"

models:
  aemo_electricity:
    dimensions:
      +materialized: table
    marts:
      +materialized: incremental
```

## Incremental Fact Model with Pre-Hook

Example from fct_scada — reads only unprocessed files:

```sql
{{ config(
    materialized='incremental',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE', 'INTERVENTION'],
    pre_hook="SET VARIABLE scada_daily_paths = (
      SELECT list(
        'zip://' || '{{ get_csv_archive_path() }}' || '/daily/year='
        || substring(source_filename, 14, 4)
        || '/source_file=' || source_filename
        || '/data_0.zip/*.CSV'
      )
      FROM {{ ref('stg_csv_archive_log') }}
      WHERE source_type = 'daily'
      {% if is_incremental() %}
        AND source_filename NOT IN (
          SELECT DISTINCT split_part(file, '.', 1) FROM {{ this }}
        )
      {% endif %}
    )"
) }}

SELECT
    {{ parse_filename('filename') }} as file,
    DUID,
    CAST(SETTLEMENTDATE AS TIMESTAMP) AS SETTLEMENTDATE,
    CAST(INITIALMW AS DOUBLE) AS INITIALMW,
    CAST(INTERVENTION AS INT) AS INTERVENTION,
    CAST(SETTLEMENTDATE AS DATE) AS DATE
FROM read_csv(
    getvariable('scada_daily_paths'),
    header=true,
    skip=1,
    ignore_errors=true,
    null_padding=true
)
WHERE DUID IS NOT NULL
  AND getvariable('scada_daily_paths') IS NOT NULL
```

## Dimension with Smart Refresh

Example from dim_duid — only rebuilds when new DUIDs appear:

```sql
{{ config(
    materialized='incremental',
    unique_key='DUID',
    full_refresh=false
) }}

{% if is_incremental() %}
  -- Check if new DUIDs exist in source files
  {% set new_duid_check %}
    SELECT count(*) FROM read_csv_auto(...)
    WHERE DUID NOT IN (SELECT DUID FROM {{ this }})
  {% endset %}
  -- If no new DUIDs, return empty result
  {% if run_query(new_duid_check).columns[0].values()[0] == 0 %}
    SELECT * FROM {{ this }} WHERE 1=0
  {% else %}
    -- Full rebuild with DELETE + INSERT
  {% endif %}
{% else %}
  -- First run: full load
{% endif %}
```

## Environment Variables Summary

| Variable | Example Value | Required |
|----------|---------------|----------|
| `DBT_SCHEMA` | `aemo` | Yes |
| `ROOT_PATH` | `abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse` | Yes |
| `download_limit` | `10` | No (default: 2) |
| `METADATA_LOCAL_PATH` | `/synfs/nb_resource/builtin/metadata.db` | Yes (for Fabric) |

## DuckLake Options Reference

| Option | Default | Set Via | Description |
|--------|---------|---------|-------------|
| `data_inlining_row_limit` | 0 | ATTACH parameter or `set_option` | Rows below this limit are inlined in metadata instead of Parquet |
| `rewrite_delete_threshold` | 0.95 | `set_option` only (not ATTACH) | Fraction of deleted rows before file rewrite (0 = disabled) |
| `parquet_compression` | snappy | `set_option` | Compression: snappy, zstd, gzip, lz4 |
| `target_file_size` | 512MB | `set_option` | Target Parquet file size |

## Execution Flow Diagram

```
Fabric Notebook
  |
  v
pip install duckdb dbt-duckdb ducklake-delta-exporter
  |
  v
Set env vars (ROOT_PATH, METADATA_LOCAL_PATH, etc.)
  |
  v
git clone <dbt-project> /tmp/dbt
  |
  v
dbt run
  |-- on-run-start: set_option(rewrite_delete_threshold, 0)
  |-- on-run-start: download() macro
  |     |-- Load csv_archive_log.parquet from abfss://
  |     |-- Download new files from AEMO / GitHub
  |     |-- Archive ZIPs to abfss://ROOT_PATH/Files/csv/
  |     |-- Update csv_archive_log.parquet
  |
  |-- Models (dependency order):
  |     |-- stg_csv_archive_log (VIEW)
  |     |-- dim_calendar, dim_duid (TABLE)
  |     |-- fct_scada, fct_price (incremental by file)
  |     |-- fct_scada_today, fct_price_today (incremental by file)
  |     |-- fct_summary (append)
  |
  v
dbt test
  |
  v
generate_latest_delta_log(METADATA_LOCAL_PATH)
  |-- Reads DuckLake SQLite metadata
  |-- Writes _delta_log/ JSON files to abfss://ROOT_PATH/Tables/
  |
  v
Fabric / Power BI reads Delta tables from OneLake
```
