{{ config(severity='warn') }}
-- fct_scada_today row count should match its Delta Lake export
SELECT
  'fct_scada_today' AS table_name,
  ducklake_count,
  delta_count,
  ducklake_count - delta_count AS difference
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('fct_scada_today') }}) AS ducklake_count,
    (SELECT COUNT(*) FROM delta_scan('{{ env_var("ROOT_PATH", "/tmp") }}/Tables/landing/fct_scada_today')) AS delta_count
)
WHERE ducklake_count != delta_count
