-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Power BI-facing summary at (date, time, DUID). Same logic as the DuckDB/DWH versions,
-- in Spark SQL: strftime -> date_format, TIMESTAMPTZ -> TIMESTAMP.
--
-- Determinism contract (see the duckdb version for the full story): the defect was in what
-- the SOURCE emitted — only wholly-missing dates, so an incomplete date could never be
-- repaired. Now every run emits the COMPLETE recomputation for exactly the dates whose
-- stored content could still be stale, and the merge reconciles that batch key by key.
-- No cutoff watermark, no dependence on this engine's run history.
--
-- The intraday branch is gated on dispatch_duids because the two branches read AEMO tables
-- with DIFFERENT UNIT UNIVERSES: 26 non-scheduled units publish SCADA telemetry but have
-- zero rows in fct_scada ever, so rows written for them became permanent orphans once the
-- date settled and merge could not delete them. See the duckdb version for the full story.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['date', 'time', 'DUID'],
    schema='mart'
) }}

{# Full-history rebuild lever is plain `--full-refresh` (CI adds that step when the
   rebuild_summary input is set) — never a var that makes this branch emit all history,
   which would hand the merge a 143M-row source. #}
{# Closes with `%}`, NOT `-%}`: a right-strip swallows the newlines after this tag and
   glues WITH onto the `-- depends_on` comment line above, commenting the keyword out. #}
{%- set scoped = is_incremental() %}

WITH
-- The unit universe the DAILY branch can reproduce. Deliberately UNBOUNDED, not a trailing
-- window: fct_scada is append-only, so this set only ever GROWS and can never orphan a row
-- it previously admitted. Outside the `scoped` block on purpose — a --full-refresh runs the
-- intraday branch too and must apply the identical filter.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation; everything older
-- is settled. The window's old floor (>= what assert_fct_summary_matches_recomputation
-- checked) died with that test -- shrinking it is now silent (see the duckdb version).
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
  UNION
  -- Recently settled: incomplete until the daily file lands, which is several days later
  -- if the pipeline missed a run — so a window, not just the newest daily date.
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada') }} s
  WHERE s.DATE >= (SELECT date_sub(MAX(DATE), 6) FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux until their daily file lands.
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
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
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
