-- depends_on: {{ ref('fct_scada') }}
-- fct_summary row count should match its Delta Lake export
{%- if execute -%}
  {%- set stats = run_query("
    SELECT
      (SELECT COUNT(*) FROM " ~ ref('fct_summary') ~ ") AS ducklake_count,
      (SELECT COUNT(*) FROM delta_scan('" ~ env_var('ROOT_PATH', '/tmp') ~ "/Tables/" ~ env_var('DBT_SCHEMA', 'mart') ~ "/fct_summary')) AS delta_count,
      (SELECT COUNT(DISTINCT date) FROM " ~ ref('fct_summary') ~ ") AS summary_days,
      (SELECT COUNT(DISTINCT DATE) FROM " ~ ref('fct_scada') ~ " WHERE INTERVENTION = 0) AS scada_days
  ") -%}
  {{ log("fct_summary: ducklake=" ~ stats.rows[0][0] ~ " delta=" ~ stats.rows[0][1] ~ " diff=" ~ (stats.rows[0][0] - stats.rows[0][1]) ~ " | summary_days=" ~ stats.rows[0][2] ~ " scada_days=" ~ stats.rows[0][3], info=true) }}
{%- endif -%}

SELECT 1
FROM (
  SELECT
    (SELECT COUNT(*) FROM {{ ref('fct_summary') }}) AS ducklake_count,
    (SELECT COUNT(*) FROM delta_scan('{{ env_var("ROOT_PATH", "/tmp") }}/Tables/{{ env_var("DBT_SCHEMA", "mart") }}/fct_summary')) AS delta_count
)
WHERE ducklake_count != delta_count
