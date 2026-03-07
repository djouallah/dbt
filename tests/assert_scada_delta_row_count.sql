-- fct_scada row count should match its Delta Lake export
SELECT
  'fct_scada' AS table_name,
  ducklake_count,
  delta_count,
  ducklake_count - delta_count AS difference
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('fct_scada') }}) AS ducklake_count,
    (SELECT COUNT(*) FROM delta_scan('{{ env_var("ROOT_PATH", "/tmp") }}/Tables/landing/fct_scada')) AS delta_count
)
WHERE ducklake_count != delta_count
