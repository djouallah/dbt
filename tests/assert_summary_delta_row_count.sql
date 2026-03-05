-- Test: fct_summary row count should match its Delta Lake export
-- Validates that delta_export() from the previous dbt run kept Delta in sync with DuckLake
-- Returns a row if counts differ, with details to help diagnose the mismatch

SELECT
  'fct_summary' AS table_name,
  ducklake_count,
  delta_count,
  ducklake_count - delta_count AS difference,
  CASE
    WHEN ducklake_count > delta_count THEN 'Delta is missing ' || (ducklake_count - delta_count) || ' rows'
    WHEN delta_count > ducklake_count THEN 'Delta has ' || (delta_count - ducklake_count) || ' extra rows'
  END AS diagnosis
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('fct_summary') }}) AS ducklake_count,
    (SELECT COUNT(*) FROM delta_scan('{{ env_var("ROOT_PATH", "/tmp") }}/Tables/{{ env_var("DBT_SCHEMA", "aemo") }}/fct_summary')) AS delta_count
)
WHERE ducklake_count != delta_count
