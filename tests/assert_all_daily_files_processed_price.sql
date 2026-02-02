-- Test: All downloaded daily files should be processed in fct_price
-- Returns rows where a downloaded file is missing from fct_price

SELECT
  source_filename
FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = 'daily'
  AND source_filename NOT IN (
    SELECT DISTINCT file
    FROM {{ ref('fct_price') }}
  )
