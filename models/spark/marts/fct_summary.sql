-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). Same logic as the DuckDB/DWH versions,
-- in Spark SQL: strftime -> date_format, TIMESTAMPTZ -> TIMESTAMP. merge upsert on the key.
-- New daily files -> backfill missing dates from the daily facts; otherwise top up from the
-- intraday facts after the cutoff. `dbt run --full-refresh` reconciles within-date gaps.
{#-- materialized='incremental', NOT 'table': as a table is_incremental() was always false, so this
     model took the full-refresh branch (whole history, every run) while duckrun/iceberg took the
     backfill-missing-dates branch and dwh took its append branch — three different definitions of
     the same table. Incremental puts all three on the same branch logic. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{% if is_incremental() %}

{%- set has_new_daily_query -%}
SELECT
  (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
  (SELECT COUNT(DISTINCT date) FROM {{ this }}) as summary_days
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set result = run_query(has_new_daily_query) -%}
  {%- set has_new_daily = result and result.rows[0][0] > result.rows[0][1] -%}
{%- else -%}
  {%- set has_new_daily = true -%}
{%- endif -%}

{% if has_new_daily %}

-- New daily data: backfill ONLY the dates missing from the summary.
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}) as cutoff,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  cutoff
FROM daily_summary

{% else %}

-- No new daily data: append intraday after the cutoff.
WITH max_cutoff AS (
  SELECT MAX(cutoff) as cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.DATE as date,
    s.SETTLEMENTDATE,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
)

SELECT
  date,
  CAST(date_format(SETTLEMENTDATE, 'HHmm') AS INT) AS time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  CAST(MAX(SETTLEMENTDATE) OVER () AS TIMESTAMP) AS cutoff
FROM incremental_data

{% endif %}

{% else %}

-- Full refresh from daily + intraday.
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL

  UNION ALL

  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  greatest(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMP))
  ) AS cutoff
FROM daily_summary
ORDER BY date

{% endif %}
