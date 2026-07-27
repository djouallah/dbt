-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). Same logic as the DuckDB/DWH versions,
-- in Spark SQL: strftime -> date_format, TIMESTAMPTZ -> TIMESTAMP.
--
-- Determinism contract (see the duckdb version for the full story): every run recomputes,
-- with the SAME SQL as a full refresh, exactly the dates whose stored content could be
-- stale, and replaces them wholesale — delete+insert keyed on `date` (dbt-fabricspark's
-- delete+insert: delete target rows whose unique_key appears in the batch, insert the
-- batch). Incremental == full-refresh by construction for every date it touches; no
-- cutoff watermark, no run history dependence.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    file_format='delta',
    unique_key=['date'],
    schema='mart'
) }}

{# Full-history rebuild lever: REBUILD_SUMMARY=1 in the env (CI workflow_dispatch input)
   or --vars 'rebuild_summary: true'. Same write path, source just emits every date. #}
{%- set rebuild = var('rebuild_summary', false) or env_var('REBUILD_SUMMARY', '0') == '1' -%}
{%- set scoped = is_incremental() and not rebuild -%}

WITH
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation: missing dates,
-- the newest daily date (its daily file may have just superseded intraday rows), and
-- intraday dates. Everything older was last written from its daily file by this same
-- date-replace logic and is immutable.
rebuild_dates AS (
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
  UNION
  SELECT MAX(s.DATE) FROM {{ ref('fct_scada') }} s
  UNION
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada_today') }} s
),
{% endif %}

daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(date_format(s.SETTLEMENTDATE, 'HHmm') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  -- INNER joins: `WHERE p.INTERVENTION = 0` always discarded null-price rows anyway.
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.DATE IN (SELECT date FROM rebuild_dates)
    {% endif %}
  GROUP BY ALL

  UNION ALL

  -- Intraday tail: intervals beyond the daily horizon. Every date here is in
  -- rebuild_dates by construction.
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
  -- Provenance column only — no read path depends on it anymore. Kept to match the
  -- other engines' schema.
  greatest(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMP)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMP))
  ) AS cutoff
FROM daily_summary
ORDER BY date
