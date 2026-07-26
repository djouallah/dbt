-- Calendar dimension. Spark builds the date range with sequence()+explode() (the
-- DuckDB generate_series() equivalent). Idempotent via the file-level style NOT IN filter.
{{ config(
    materialized='incremental',
    unique_key='date',
    incremental_strategy='append'
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
