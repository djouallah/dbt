{{ config(
    materialized='table',
    pre_hook=[
        "CREATE SCHEMA IF NOT EXISTS ducklake.{{ env_var('DBT_SCHEMA', 'aemo') }}",
        "CREATE TABLE IF NOT EXISTS ducklake.{{ env_var('DBT_SCHEMA', 'aemo') }}.csv_archive_log (source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR, archived_at TIMESTAMP, row_count BIGINT, source_url VARCHAR, etag VARCHAR)",
        "{{ download() }}"
    ]
) }}

{# 
  Pre-hooks:
  1. Create schema if not exists
  2. Create csv_archive_log table if not exists (no seed needed)
  3. Run download() on this model's connection (which has DuckLake attached)
#}

SELECT 
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag
FROM ducklake.{{ env_var('DBT_SCHEMA', 'aemo') }}.csv_archive_log
