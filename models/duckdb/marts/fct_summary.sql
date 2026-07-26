-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Insert-only merge on the (date, time, DUID) grain — one operation, one commit.
-- The OneLake Iceberg REST catalog allows only ONE add-snapshot update per commit
-- ("Only one instance of each update type is allowed per request"), so a merge with
-- a WHEN MATCHED UPDATE branch (delete files + data files in one commit) is rejected
-- with BadRequest 400. WHEN MATCHED DO NOTHING keeps every run a single append
-- snapshot: the daily branch backfills dates missing from the summary, the intraday
-- branch tops up after the cutoff, and re-runs dedupe on the key instead of
-- double-appending. Consequences: once a key exists (e.g. from intraday), the daily
-- value never overwrites it — same measurement, so acceptable — and within-date key
-- gaps aren't revisited; `dbt run --full-refresh` is the reconciliation lever.
-- assert_fct_summary_grain is the duplicate tripwire.
{{ config(
    materialized='incremental',
    incremental_strategy='insert' if target.name == 'duckrun' else 'merge',
    unique_key=['date', 'time', 'DUID'],
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
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

-- New daily data found: backfill ONLY the dates missing from the summary. Computing
-- full history here OOMs the 7GB CI runner — the merge materializes its source as a
-- temp relation before joining the target, so the source must stay small. Within-date
-- gaps (a key missing for a date the summary already has) need `--full-refresh`.
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}) as cutoff,
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
    {% if is_incremental() %}
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
    {% endif %}
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

-- No new daily data: append intraday after cutoff
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
  CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT) AS time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  CAST(MAX(SETTLEMENTDATE) OVER () AS TIMESTAMPTZ) AS cutoff
FROM incremental_data

{% endif %}

{% else %}

-- Full refresh from daily + today data
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
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
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
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
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary
ORDER BY date

{% endif %}
