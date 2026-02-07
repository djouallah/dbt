{{ config(
    materialized='view',
    schema='staging'
) }}

{%- set log_path = get_root_path() ~ '/Files/csv_archive_log.parquet' -%}

SELECT
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag
FROM read_parquet('{{ log_path }}')
