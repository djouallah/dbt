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
-- sort_by is the `sorted` DISPATCH INPUT, off by default, and it now DECLARES the write layout
-- rather than delegating it: the named key ['date','time'] plus explicit geometry —
-- max_row_group_size 48M rows and target_file_size_mb 1024, so the 143.98M-row table should land
-- near 1 file / 3 row groups (readHeavyForPBI's 1 GB bin, without the spark profile). It spent one
-- era as 'auto' so the input measured what the adapter's picker does out of the box; the picker
-- kept choosing `date, time` and paid +19% ETL CU for the profiling pass against a named key's
-- +3.7%, so the key it picked is now simply written down. DUID was measured worth ~16% of size
-- (652.6 vs 777.2 MB below) and deliberately NOT taken: date,time only, the safe pick.
-- DUCKRUN-ONLY without breaking the one-config-for-both rule, for the same reason partition_by
-- was: `sort_by` and the geometry keys occur ZERO times in dbt-duckdb's adapter and macro package,
-- so on iceberg they are parsed into the manifest and read by nobody. Both targets still run
-- byte-identical model code and there is still no `target.name` in this tree. Off they render to
-- `none`, which is what every run before the input did.
--
-- ⚠️ THE GEOMETRY KEYS ARE INERT AS OF duckrun 0.4.43, AND THE FAILURE IS SILENT. Measured on run
-- 30955591822 (sorted=true, this config at 48M/1024): 651.1 MB, 3 files, 19 row groups — the
-- adaptive estimator's layout (log: "row-group geometry was sized for ~14,911,911 rows", 8M floor
-- x 18 = the 19 RG), not the declared one. The plugin and engine honor the values fine
-- (_geometry_config -> WriterProperties, no 16M clamp; duckrun's own parquet_layout CI pins the
-- same seam directly and gets 3 RG) — but the dbt materialization macro `_delta_core.sql` builds
-- its plugin config dict as a fixed key list that carries `sort_by` and NOT these two, so they
-- die between the manifest and the plugin. sort_by is unaffected (the 651 MB proves it sorted).
-- They start working when a duckrun release adds the two keys to that dict; the notebook installs
-- duckrun unpinned from PyPI, so no change is needed here — just re-measure after the release.
--
-- sort_by is honored on this model despite the adapter docs calling it inert on the delta_rs
-- merge path: the merge here is insert-only, so the engine seam routes it to a DuckDB anti-join
-- committed as a plain append, and that path forwards sort_by and the geometry -- as does the
-- first-build overwrite, where an explicit row-group size bypasses the adaptive planner.
--
-- FOUR MEASURED POINTS, all 64 vCores, all full loads, all 143,980,961 rows. Every row-group
-- count is the adaptive planner's — the 48M cap has never reached a write (see the ⚠️ above):
--   none                    985.5 MB  4f/27RG  cold 23,491  warm 6,300  hot 5,420  etl 22,624
--   auto -> `date, time`    777.2 MB  4f/25RG  cold 27,740  warm 3,498  hot 3,056  etl 26,991
--   ['date','time','DUID']  652.6 MB  3f/26RG  cold 24,523  warm 3,141  hot 3,572  etl 23,465
--   ['date','time','DUID']  651.1 MB  3f/19RG  (geometry declared 48M/1GB and silently dropped)
-- (runs 30752070535, 30796667149, 30805417412, 30955591822. The key is spelled in this config,
-- durably; fabric_run.py's sort_by_auto scrape still exists but matches nothing unless someone
-- hand-sets 'auto' again.)
--
-- READ THAT TABLE CAREFULLY, because the obvious reading is wrong. auto did NOT pick `date` alone —
-- it picked `date, time`, the same first two columns the query suite argues for. So the picker got
-- the direction right on its own, and the entire 652.6-vs-777.2 gap (~16% of size) is attributable
-- to the THIRD key, `DUID`. That contradicts the reasoning this file used to carry, which said DUID
-- adds no run-length because it appears once per (date, time) — true, and beside the point: a
-- sorted string column with ascending dictionary ids compresses far better than the same values in
-- arbitrary order, and DUID is the widest low-cardinality column here. The run-length argument
-- under-weighted it.
--
-- WHY DATE, TIME IS THE RIGHT DIRECTION, read off benchmark/xmla_compare.py rather than guessed —
-- and now independently agreed with by the picker: dim_calendar[year]/[month] filters or groups 9
-- of the 25 queries while fct_summary[DUID] is a filter in 2, and both relationships are
-- relyOnReferentialIntegrity so a year filter propagates onto date. Hence date-first: a DUID-first
-- key would give DUID runs of ~209k rows and destroy date monotonicity, hurting 9 queries to help
-- 2. `time` second earns its place through `price`, which is RRP per (SETTLEMENTDATE, REGIONID) --
-- 5 regions -- so one (date, time) holds ~156 rows carrying at most 5 distinct prices, collapsed
-- into a 156-row window and smeared over ~45,036 rows under a bare ['date']. Read the mechanism as
-- Direct Lake, not file skipping: VertiPaq inherits the parquet row order when it transcodes, so a
-- sorted table gives longer RLE runs in the resident columns -- which is why warm and hot move and
-- not only cold.
--
-- The picker question is CLOSED — it never added DUID — and the key is now written down as the
-- `date, time` it kept choosing, minus the profiling pass that cost +19% ETL. DUID's ~16% of size
-- is deliberately left on the table (the safe pick). What the input measures once the geometry
-- keys actually land (⚠️ above) is the declared layout against the adaptive default: does
-- collapsing ~25 row groups to ~3 move cold/warm/hot the way Direct Lake's segment story says.
--
-- Not the trailing ORDER BY below doing any of this: that reaches no stored table on any engine
-- (CLAUDE.md, "fairness invariant"), and at 143M rows with spilling it demonstrably did not reach
-- duckrun's write either -- adding a sort key changed the parquet, which it could not have done if
-- the order were already there.
--
-- Two costs, both expected. The write does a real ORDER BY of 143M rows (no profiling pass any
-- more -- that went with 'auto'), so duckrun's ETL CU still rises, just less than auto's did. And
-- with this on, the duckrun/iceberg pair differs by more than the writer -- the pair CLAUDE.md
-- calls the sharpest comparison on the dashboard. There is no fix: dbt-duckdb has no sort or
-- geometry config at all, so iceberg cannot follow.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    sort_by=(['date', 'time'] if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(48000000 if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    target_file_size_mb=(1024 if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
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
