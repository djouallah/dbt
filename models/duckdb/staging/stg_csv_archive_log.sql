-- Staging table over the archive log the shared download notebook writes to the landing
-- lakehouse (Files/csv_raw_archive_log.parquet). Every engine reads the log with SQL.
-- INCREMENTAL, not table/view: the DuckDB Iceberg catalog supports neither CREATE VIEW nor
-- the table materialization's temp-table RENAME, but it does CREATE TABLE AS + INSERT.
-- Insert-only on both targets, keyed on (source_type, source_filename) — duckrun spells that
-- 'insert' (a duckrun-only strategy) and iceberg spells it merge + when_matched do_nothing,
-- since dbt-duckdb has no insert macro. See the fct_price.sql header for the full reasoning.
-- The WHERE below stays: it keeps the merge source to just the unlogged rows, and reading
-- {{ this }} in the body puts the read and the commit on one snapshot.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    unique_key=['source_type', 'source_filename'],
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
