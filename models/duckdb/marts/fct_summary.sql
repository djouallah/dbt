-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Determinism contract: fct_summary must be a pure function of its inputs — same
-- fct_scada/fct_price/dim_duid/_today tables => same summary, on every engine,
-- regardless of that engine's run history. The old three-branch incremental
-- (backfill-missing-dates | append-intraday-after-cutoff, matches never touched)
-- violated this: a date first written by the intraday path kept whatever gaps that
-- engine's schedule left, forever, and the four engines fossilized different tables
-- from identical inputs.
--
-- Now every run recomputes, with the SAME SQL as a full refresh, exactly the dates
-- whose stored content could be stale, and replaces them wholesale via delete+insert
-- keyed on `date` (a date-partition overwrite): incremental == full-refresh by
-- construction for every date it touches. On the Iceberg REST catalog the DELETE and
-- INSERT are separate commits, so the one-snapshot-per-commit limit that forbade an
-- updating merge does not apply (dim_calendar has proven this path on all engines).
--
-- Known edge: a rebuilt date whose recomputation yields ZERO rows leaves its stale
-- rows in place (no key in the batch, nothing deleted). assert_fct_summary_matches_
-- recomputation trips on it; REBUILD_SUMMARY=1 (or --full-refresh) clears it.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date'],
    schema='mart'
) }}

{# Full-history rebuild lever: REBUILD_SUMMARY=1 in the env (CI workflow_dispatch input,
   forwarded into Fabric notebooks) or --vars 'rebuild_summary: true'. Same delete+insert
   write path, source just emits every date — no DROP, table object preserved. #}
{%- set rebuild = var('rebuild_summary', false) or env_var('REBUILD_SUMMARY', '0') == '1' -%}
{%- set scoped = is_incremental() and not rebuild -%}

WITH
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation:
--   * missing from the summary entirely (first sight of a daily file, or catch-up);
--   * the newest daily date (its daily file may have just superseded intraday rows);
--   * intraday dates (in flux until their daily file lands).
-- Everything older is immutable: it was last written from its daily file by this same
-- date-replace logic, so recomputing it would reproduce it byte-for-byte.
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
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  -- INNER joins: `WHERE p.INTERVENTION = 0` always discarded null-price rows anyway,
  -- so the old LEFT JOINs were inner joins in disguise — say what we do.
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
  -- rebuild_dates by construction, so no extra scoping predicate is needed.
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
  -- Provenance column only — no read path depends on it anymore. Kept (and kept
  -- populated) to avoid a schema change that would force a table DROP on dwh.
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary
ORDER BY date
