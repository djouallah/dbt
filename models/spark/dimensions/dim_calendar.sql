-- Calendar dimension. Spark builds the date range with sequence()+explode() (the
-- DuckDB generate_series() equivalent). Insert-only merge on date, with the NOT IN filter
-- below kept so the merge source stays empty on a steady-state run. Was 'append' with an
-- inert unique_key (append ignores it); ~3k rows, so the key match costs nothing.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key='date',
    skip_matched_step=true
) }}

SELECT
  CAST(d AS DATE) AS date,
  CAST(YEAR(d) AS INT) AS year,
  CAST(MONTH(d) AS INT) AS month
FROM (
  SELECT explode(sequence(to_date('2018-04-01'), to_date('2026-12-31'), interval 1 day)) AS d
)
{% if is_incremental() %}
WHERE CAST(d AS DATE) NOT IN (SELECT date FROM {{ this }})
{% endif %}
