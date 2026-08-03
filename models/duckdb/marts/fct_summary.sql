-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Determinism contract: same inputs => same summary, on every engine, regardless of that
-- engine's run history. Every run emits the COMPLETE recomputation -- the same SQL as a full
-- refresh -- for exactly the dates whose stored content could still be stale, and the write
-- reconciles that batch key by key. A partial top-up would fossilize gaps forever.
--
-- Insert-only on BOTH targets, which is a real limitation and not a preference: the OneLake
-- Iceberg REST catalog rejects a matched-UPDATE branch (BadRequest 400), and the duckdb tree runs
-- one config for both, so duckrun gives up the update it could do. Consequence: a re-emitted row
-- carrying REVISED mw/price does NOT overwrite what is stored -- craters (missing keys) are
-- repaired, changed values are not. spark and dwh do update, so a revision would show up as a
-- value difference between the engine pairs; the repair lever on this side is a full rebuild.
-- Not delete+insert on duckrun: that adapter implements it as a fenced full-table overwrite,
-- i.e. 143M rows every run.
--
-- No merge path DELETES a row the recomputation stops producing, which is why dispatch_duids
-- below gates the intraday branch to units the daily branch can reproduce.
-- That gate is now UNGUARDED: assert_fct_summary_matches_recomputation mirrored the same filter
-- and failed by construction if the two drifted apart, and it was deleted when the suite was cut
-- back to a grain check. Treat any edit to dispatch_duids as load-bearing -- nothing will catch a
-- mistake in it. Full story: LEARNINGS.md, "Two branches of one model, two different unit
-- universes"; CLAUDE.md, "fct_summary must be a pure function of its inputs".
--
-- sort_by='auto' was HERE and was REMOVED. It worked, and it is still not coming back: duckrun's
-- picker is a greedy single pass over statistical sketches -- its own limitations.md calls it "a
-- naive, lightly-tested heuristic ... not guaranteed to shrink anything" and warns it "can
-- occasionally pick a worse key than the default" -- so it is a coin-flip re-thrown on EVERY batch,
-- in a benchmark whose whole value is that two dispatches of one commit differ by nothing but what
-- was changed. What it picked was `date`. Measured, run 30796667149 (auto) against 30752070535
-- (none), both 64 vCores, both full loads, both 143,980,961 rows, one config line apart:
-- fct_summary 985.5 -> 777.2 MB, 27 -> 25 row groups, warm 6,300 -> 3,498 ms, hot 5,420 -> 3,056 ms,
-- and etl CU 22,624 -> 26,980 -- that +19% is the profiling pass, which re-executes this model's
-- whole query to sample it before the write executes it again.
-- So the layout win is real and the way it was obtained is not worth keeping: `sort_by=['date']`
-- states the same key outright, costs no profiling pass and cannot change its mind between runs.
-- That is the change to make if the win is wanted -- not this one.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
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
-- The window used to have a hard floor: it had to stay >= the window
-- assert_fct_summary_matches_recomputation checked, or CI went permanently red on drift this
-- model is not allowed to repair. That test is gone, so the constraint is gone with it -- and so
-- is the alarm. Shrinking this window now silently reduces what can be repaired.
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
