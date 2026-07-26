-- Staging table over the archive log the shared download notebook writes to the landing
-- lakehouse (Files/csv_raw_archive_log.parquet). Every engine reads the log with SQL.
-- INCREMENTAL (append), not table/view: the DuckDB Iceberg catalog supports neither CREATE
-- VIEW nor the table materialization's temp-table RENAME, but it does CREATE TABLE AS +
-- INSERT. `append` (not `insert`) because `insert` is a duckrun-only strategy — dbt-duckdb
-- (the iceberg target) has no insert macro; append works on both and the WHERE below keeps
-- it idempotent (only rows for files not already logged).
{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='landing'
) }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    csv_filename
FROM read_parquet('{{ get_root_path() }}/csv_raw_archive_log.parquet')
{% if is_incremental() %}
WHERE (source_type, source_filename) NOT IN (SELECT source_type, source_filename FROM {{ this }})
{% endif %}
