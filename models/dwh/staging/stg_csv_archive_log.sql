-- Table over the EXISTING archive log the (unchanged) file-landing pipeline writes to the
-- lakehouse: Files/csv_archive_log.parquet. This dbt-fabric project does not download or
-- land anything — it reads the files the existing pipeline already put in OneLake. Keeping
-- this as stg_csv_archive_log preserves every ref('stg_csv_archive_log') (models, tests,
-- check_new_daily) unchanged from the duckrun version, where it was a Python model.
-- Materialized as a TABLE (not a view) so it's a physical OneLake Delta table the neutral
-- delta_scan test reader can see — same as every engine's fct_*/dim_* output.
{{ config(materialized='table', schema='landing') }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    csv_filename
FROM OPENROWSET(
    BULK '{{ get_root_path() }}/csv_raw_archive_log.parquet',
    FORMAT = 'PARQUET'
) AS log
