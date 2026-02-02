{{ config(
    materialized='incremental',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE','INTERVENTION'],
    pre_hook="SET VARIABLE scada_today_paths = (SELECT COALESCE(NULLIF(list('zip://' || '{{ get_csv_archive_path() }}' || '/scada_today/day=' || substring(source_filename, 22, 8) || '/source_file=' || source_filename || '/data_0.zip/*.CSV'), []), ['']) FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = 'scada_today'{% if is_incremental() %} AND source_filename NOT IN (SELECT DISTINCT split_part(file, '.', 1) FROM {{ this }}){% endif %})"
) }}

{% set csv_archive_path = get_csv_archive_path() %}

WITH scada_staging AS (
  SELECT *
  FROM read_csv(
    getvariable('scada_today_paths'),
    skip = 1,
    header = 0,
    all_varchar = 1,
    columns = {
      'I': 'VARCHAR',
      'DISPATCH': 'VARCHAR',
      'UNIT_SCADA': 'VARCHAR',
      'xx': 'VARCHAR',
      'SETTLEMENTDATE': 'timestamp',
      'DUID': 'VARCHAR',
      'SCADAVALUE': 'double',
      'LASTCHANGED': 'timestamp'
    },
    filename = 1,
    null_padding = true,
    ignore_errors = 1,
    auto_detect = false,
    hive_partitioning = false
  )
  WHERE I = 'D' AND SCADAVALUE != 0
)

SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  {{ parse_filename('filename') }} AS file,
  SETTLEMENTDATE,
  LASTCHANGED,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM scada_staging
