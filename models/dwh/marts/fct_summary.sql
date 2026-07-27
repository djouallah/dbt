-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Determinism contract (see the duckdb version for the full story): every run recomputes,
     with the SAME SQL as a full rebuild, exactly the dates whose stored content could be
     stale, and replaces them wholesale via delete+insert keyed on [date] — a native keyed
     DELETE + INSERT, no DROP. Incremental == full-rebuild by construction for every date
     it touches; no cutoff watermark, no run-history dependence, no runner-decided branch.

     We deliberately do NOT use --full-refresh on this engine: on dbt-fabric that DROPs +
     recreates the table (a Sch-M DDL swap that deadlocks Fabric's background stats
     maintenance, loses grants, and rebinds Direct Lake every run). The full-history
     rebuild lever is REBUILD_SUMMARY=1 / --vars 'rebuild_summary: true', which keeps the
     same delete+insert write path and just emits every date. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['date'],
    schema='mart'
) }}
{#-- cluster_by was REMOVED: on a CLUSTER BY table Fabric runs automatic background
     clustering/compaction that holds a lock on the table, and every fct_summary write then
     deadlocked against it *reproducibly* — the retry deadlocked too, same process id.
     Dropping cluster_by is what stops the deadlocks; the summary is small and date/DUID
     filtering is fine without physical clustering. Do not re-add it here. --#}

{%- set rebuild = var('rebuild_summary', false) or env_var('REBUILD_SUMMARY', '0') == '1' -%}
{%- set scoped = is_incremental() and not rebuild -%}

WITH
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation; everything older
-- is settled. The trailing window must stay >= the window
-- assert_fct_summary_matches_recomputation checks (see the duckdb version).
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.[DATE] AS [date] FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.[DATE] NOT IN (SELECT DISTINCT [date] FROM {{ this }})
  UNION
  -- Recently settled: incomplete until the daily file lands, which is several days later
  -- if the pipeline missed a run — so a window, not just the newest daily date.
  SELECT DISTINCT s.[DATE] FROM {{ ref('fct_scada') }} s
  WHERE s.[DATE] >= (SELECT DATEADD(DAY, -6, MAX([DATE])) FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux until their daily file lands.
  SELECT DISTINCT s.[DATE] FROM {{ ref('fct_scada_today') }} s
),
{% endif %}
scada_cutoff AS (
  SELECT MAX(SETTLEMENTDATE) AS c FROM {{ ref('fct_scada') }}
),
cutoff_calc AS (
  -- T-SQL has no GREATEST: max of (daily max, intraday max) via UNION ALL.
  SELECT MAX(v) AS cutoff FROM (
    SELECT MAX(SETTLEMENTDATE) AS v FROM {{ ref('fct_scada') }}
    UNION ALL
    SELECT COALESCE(MAX(SETTLEMENTDATE), CAST('1900-01-01' AS DATETIME2(6))) FROM {{ ref('fct_scada_today') }}
  ) u
),
daily_summary AS (
  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
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
    AND s.[DATE] IN (SELECT [date] FROM rebuild_dates)
    {% endif %}
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID

  UNION ALL

  -- Intraday tail: intervals beyond the daily horizon. Every date here is in
  -- rebuild_dates by construction.
  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT c FROM scada_cutoff)
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID
)

SELECT
  [date],
  [time],
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  -- Provenance column only — no read path depends on it anymore. Kept (and kept
  -- populated) to avoid a schema change that would force a DROP here.
  (SELECT cutoff FROM cutoff_calc) AS cutoff
FROM daily_summary
