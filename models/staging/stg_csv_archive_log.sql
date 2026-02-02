{{ config(
    materialized='table',
    pre_hook="{{ download() }}"
) }}

{# 
  Pre-hook runs download() on this model's connection (which has DuckLake attached).
  The download macro inserts into the seed table (csv_archive_log).
  This model then reads from it and becomes the source for other models.
#}

SELECT 
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag
FROM {{ ref('csv_archive_log') }}
