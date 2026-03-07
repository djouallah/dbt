# Reference: dbt in Fabric Notebook — Full Code Examples

## Complete profiles.yml (3 targets)

```yaml
aemo_electricity:
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
    prod:
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
        - azure
        - httpfs
        - json
        - ducklake
        - name: delta_export
          repo: community
      attach:
        - path: "ducklake:sqlite:{{ env_var('METADATA_LOCAL_PATH', '/tmp/ducklake_metadata.db') }}"
          alias: ducklake
          options:
            data_path: "{{ env_var('ROOT_PATH') }}/Tables"
            data_inlining_row_limit: 0
```

Key differences between targets:
- **dev**: Local filesystem, `httpfs` for downloads, no `azure` extension needed
- **ci**: Azurite emulator (`az://`), `azure` extension + connection string in both `settings` and `secrets`
- **prod**: OneLake (`abfss://`), `azure` + `httpfs` extensions

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
  # process_limit: max files per model per run (default: 1000)

on-run-start:
  - "CALL ducklake.set_option('rewrite_delete_threshold', 0)"
  - "CALL ducklake.set_option('target_file_size', '128MB')"

on-run-end:
  - "CALL ducklake_rewrite_data_files('ducklake')"
  - "CALL ducklake_merge_adjacent_files('ducklake')"
  - "CALL delta_export()"

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

Note: `stg_csv_archive_log.py` overrides to `materialized: table` in its `dbt.config()`. `fct_summary.sql` overrides back to `schema='mart'`.

## Incremental Fact Model with Pre-Hook and process_limit

Example from fct_scada — reads only unprocessed files, capped by process_limit:

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
      FROM (
        SELECT source_filename
        FROM {{ ref('stg_csv_archive_log') }}
        WHERE source_type = 'daily'
        {% if is_incremental() %}
          AND source_filename NOT IN (
            SELECT DISTINCT split_part(file, '.', 1) FROM {{ this }}
          )
        {% endif %}
        LIMIT {{ env_var('process_limit', '1000') }}
      )
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

## Python Model for Data Ingestion

`stg_csv_archive_log.py` handles all data downloads as a dbt Python model:

```python
def model(dbt, session):
    dbt.config(materialized="table", schema="landing")

    # Downloads ZIPs from AEMO/GitHub, extracts CSVs, archives to OneLake
    # Tracks downloads in csv_archive_log.parquet on abfss://
    # Uses ThreadPoolExecutor for parallel downloads
    # Returns DuckDB relation (no pandas/numpy)

    return session.sql("SELECT * FROM _csv_archive_log")
```

Key pattern: the Python model uses `session.sql()` for all DuckDB operations and returns a DuckDB relation directly — no pandas or numpy dependencies needed. File uploads to OneLake use `COPY ... TO ... (FORMAT BLOB)` through DuckDB.

## Environment Variables Summary

| Variable | Example Value | Default | Required |
|----------|---------------|---------|----------|
| `DBT_SCHEMA` | `mart` | `mart` | No |
| `ROOT_PATH` | `abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse` | `/tmp` | Yes (for Fabric) |
| `download_limit` | `100` | `2` | No |
| `process_limit` | `1000` | `1000` | No |
| `METADATA_LOCAL_PATH` | `/lakehouse/default/Files/metadata.db` | `/tmp/metadata.db` | Yes (for Fabric) |

## DuckDB / DuckLake Options Reference

| Option | Default | Set Via | Description |
|--------|---------|---------|-------------|
| `allow_unsigned_extensions` | false | `config_options` in profiles.yml (connection creation time) | Allow loading community extensions |
| `preserve_insertion_order` | true | `settings` in profiles.yml (runtime SET) | Set false to reduce memory usage |
| `data_inlining_row_limit` | 0 | ATTACH parameter in profiles.yml | Rows below this limit are inlined in metadata instead of Parquet |
| `rewrite_delete_threshold` | 0.95 | `set_option` only (not ATTACH param) | Fraction of deleted rows before file rewrite (0 = rewrite all) |
| `target_file_size` | 512MB | `set_option` only (not ATTACH param) | Target Parquet file size for insert and compaction. `merge_adjacent_files` reads this automatically. Reduce to prevent OOM (e.g., 128MB) |
| `parquet_compression` | snappy | `set_option` | Compression: snappy, zstd, gzip, lz4 |

## Execution Flow Diagram

```
Fabric Notebook
  |
  v
pip install duckdb dbt-duckdb
  |
  v
Set env vars (ROOT_PATH, METADATA_LOCAL_PATH, process_limit, etc.)
  |
  v
dbt run --target prod --profiles-dir .
  |-- on-run-start: set_option(rewrite_delete_threshold, 0)
  |-- on-run-start: set_option(target_file_size, '128MB')
  |
  |-- Models (dependency order):
  |     |-- stg_csv_archive_log (Python model, TABLE)
  |     |     |-- Downloads new files from AEMO / GitHub
  |     |     |-- Archives gzipped CSVs to abfss://ROOT_PATH/Files/csv/
  |     |     |-- Updates csv_archive_log.parquet
  |     |-- dim_calendar, dim_duid (TABLE)
  |     |-- fct_scada, fct_price (incremental by file, LIMIT process_limit)
  |     |-- fct_scada_today, fct_price_today (incremental by file, LIMIT process_limit)
  |     |-- fct_summary (delete+insert with daily priority, append intraday)
  |
  |-- on-run-end: ducklake_rewrite_data_files (compact deleted data)
  |-- on-run-end: ducklake_merge_adjacent_files (merge small files, target 128MB)
  |-- on-run-end: CALL delta_export()
  |     |-- Reads DuckLake SQLite metadata
  |     |-- Writes _delta_log/ JSON files to abfss://ROOT_PATH/Tables/
  |
  v
dbt test --target prod --profiles-dir .
  |
  v
Fabric / Power BI reads Delta tables from OneLake
```
