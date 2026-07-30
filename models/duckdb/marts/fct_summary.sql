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
-- The defect was in WHAT THE SOURCE EMITTED, not in how it was written: the source only
-- ever offered wholly-missing dates, so a date that existed but was incomplete could
-- never be repaired by any write strategy. Now every run emits the COMPLETE
-- recomputation — the same SQL as a full refresh — for exactly the dates whose stored
-- content could still be stale, and the write reconciles that batch key by key.
--
-- Write strategy stays each target's proven one; only the source changed:
--   duckrun -> merge (update matched + insert new) on the grain. delta_rs prunes target
--     files from the source's own stats, so a ~3-date batch touches only those files.
--     This is the ONE model here that is not incremental_strategy='insert': the facts can be
--     insert-only because a stored row is final, but a re-emitted summary row may carry
--     REVISED mw/price and must overwrite what is stored. Insert-only would keep the stale
--     value, which is exactly the fossilization this header is about.
--     NOT delete+insert: this adapter implements that as a fenced FULL-TABLE overwrite
--     (it materializes every surviving target row plus the batch into a DuckDB temp
--     table, then overwrites), which would rewrite all 143M rows on every run.
--   iceberg -> merge with WHEN MATCHED DO NOTHING, unchanged. The OneLake Iceberg REST
--     catalog rejects a matched-UPDATE branch (BadRequest 400: only one add-snapshot
--     update per commit). Insert-only suffices because every input is append-only: a
--     (date, time, DUID) value is final once produced, and craters are missing keys,
--     which insert repairs. (Emitting deletes is NOT the blocker — XTable converts
--     Iceberg positional deletes to Delta deletion vectors fine.)
--
-- Neither merge path DELETES a stored row the recomputation no longer produces, and that
-- is NOT free just because the inputs are append-only — the assumption this header used
-- to make. No input row has to disappear: a row's PRODUCING BRANCH switches from intraday
-- to daily when the daily file lands, and the two branches read different AEMO tables
-- covering DIFFERENT UNIT UNIVERSES. fct_scada (DISPATCH_UNIT_SOLUTION) has 644 DUIDs;
-- fct_scada_today (DISPATCH_UNIT_SCADA) has 406, of which 26 non-scheduled units
-- (ROWALLAN, WAUBRAWF, ROYALLA1, GERMCRK, ...) have ZERO rows in fct_scada across all
-- 369M — they publish telemetry but are never dispatched. The intraday branch wrote them;
-- once the date settled nothing could reproduce them, so they became permanent orphans:
-- 5,861 rows on one date, every date, on all three merge engines. dwh only looked healthy
-- because delete+insert on [date] can retract; it was not more correct.
-- Hence dispatch_duids below: the intraday branch may only emit units the daily branch
-- will be able to reproduce. assert_fct_summary_matches_recomputation applies the SAME
-- filter and is the tripwire; the two must always change together.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    merge_clauses=none if target.name == 'duckrun' else {'when_matched': [{'action': 'do_nothing'}]},
    schema='mart'
) }}

{# Full-history rebuild lever here is plain `--full-refresh` (a streaming overwrite);
   REBUILD_SUMMARY=1 makes CI add that step. Deliberately NOT a var that makes the
   incremental branch emit all history: that would hand the merge a 143M-row source. #}
{# Closes with `%}`, NOT `-%}`: a right-strip swallows the newlines after this tag and
   glues WITH onto the `-- depends_on` comment line above, commenting the keyword out
   (the compiled SQL then starts at `daily_summary AS (` and the parser errors there). #}
{%- set scoped = is_incremental() %}

WITH
-- The unit universe the DAILY branch can reproduce. Gates the intraday branch so it never
-- emits a unit that will be unreproducible once the date settles (see the header).
-- Deliberately UNBOUNDED, not a trailing window: fct_scada is append-only, so this set only
-- ever GROWS and can never orphan a row it previously admitted. A rolling window would
-- reintroduce the same bug from the other side — a unit ageing out of the window turns its
-- already-written intraday rows into orphans, which merge still cannot delete.
-- Outside the `scoped` block on purpose: a --full-refresh runs the intraday branch too and
-- must apply the identical filter.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation. Everything older
-- is settled: its daily file has landed and been folded in, so recomputing it would
-- reproduce it exactly.
--
-- The trailing window must stay >= the window assert_fct_summary_matches_recomputation
-- checks, or CI can go permanently red on drift this model is not allowed to repair.
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.DATE AS date FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.DATE NOT IN (SELECT DISTINCT date FROM {{ this }})
  UNION
  -- Recently settled: a date first written from the intraday feed is incomplete until
  -- its daily file lands, which is several days later if the pipeline missed a run — so
  -- a window, not just the newest daily date.
  SELECT DISTINCT s.DATE FROM {{ ref('fct_scada') }} s
  WHERE s.DATE >= (SELECT MAX(DATE) - INTERVAL 6 DAY FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux: the intraday feed keeps extending these until their daily file lands.
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
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
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
