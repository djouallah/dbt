-- Test: fct_summary row count should match its Delta Lake export
-- Returns a row if counts differ, indicating delta_export missed data

SELECT
  ducklake_count,
  delta_count
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('fct_summary') }}) AS ducklake_count,
    (SELECT COUNT(*) FROM delta_scan('{{ env_var("ROOT_PATH", "/tmp") }}/Tables/{{ env_var("DBT_SCHEMA", "aemo") }}/fct_summary')) AS delta_count
)
WHERE ducklake_count != delta_count
