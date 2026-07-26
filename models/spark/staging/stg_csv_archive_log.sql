-- Spark table over the archive log the shared download notebook writes to the landing
-- lakehouse. Fabric Spark 3.5 has no read_files(), so read parquet with the path datasource
-- syntax (parquet.`path`) — the form the benchmark_direct_query repo uses.
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
FROM parquet.`{{ get_root_path() }}/csv_raw_archive_log.parquet`
