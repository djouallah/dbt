



WITH source_files AS (
  SELECT source_filename
  FROM "ducklake"."aemo"."csv_archive_log"
  WHERE source_type = 'scada_today'
  
),

file_paths AS (
  SELECT list('zip://' || '/lakehouse/default/Files/csv' || '/scada_today/day=' || substring(source_filename, 22, 8) || '/source_file=' || source_filename || '/data_0.zip/*.CSV') as paths
  FROM source_files
),

scada_staging AS (
  SELECT *
  FROM read_csv(
    (SELECT COALESCE(paths, ['']) FROM file_paths),
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
  
    split_part(split_part(filename, '/', -1), '.', 1)
 AS file,
  SETTLEMENTDATE,
  LASTCHANGED,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM scada_staging