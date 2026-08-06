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
-- rather than delegating it: the named key ['date','DUID','time'] plus explicit geometry —
-- max_row_group_size 6000000 and target_file_size_mb 1024. ALL THREE DEFAULTS WERE MEASURED, and
-- the first two REPLACE an earlier bet that the data killed. Read the retraction before changing
-- them back.
--
-- 16M was chosen to copy V-Order's segment shape (spark readHeavyForPBI writes 9-11 row groups of
-- ~16.0M rows; 143,980,961 = 8 x 16M + 15.98M, exactly 9 full segments) on the theory that segment
-- parity was V-Order's structural edge. IT IS THE WORST GEOMETRY MEASURED. Sorted `date,time`,
-- warm ms by segment count: 8 RG 5,793 (n=1) · 9 RG 4,141-6,311 (n=6, median 5,425) · 19 RG 5,725
-- (n=1) · 24 RG 3,221 (n=1) · 25 RG 2,783 (n=1) · 72 RG 2,655 (n=1). A step between 19 and 24, not
-- a curve, and 72 buys nothing over 24 while costing 4.5% more bytes. 6M = 24 segments sits at the
-- knee. The 9-RG arm is the only well-sampled point and it anchors the SLOW side, so this is not a
-- noise artefact — but where exactly the knee falls between 19 and 24 is unresolved (three n=1
-- points).
-- Also dead: a "wave model" predicting cost ~ ceil(segments/threads), which fit 9 RG at N=8 to
-- within 2%. Run 31077710594 wrote EXACTLY 8 segments (avg_row_group 17,997,620) to test it and
-- landed at warm 5,793 — inside the 9-RG range, upper half. One pool-of-8 wave is not faster than
-- two. Do not re-derive it from the 9-RG number alone; that fit was a coincidence on one point.
--
-- ['date','DUID','time'] over ['date','time'] is a SIZE decision, not a speed one, and it is the
-- one thing in this whole experiment that separated. n=4 per arm at 6M, interleaved so capacity
-- weather hits both: size 543.03 MB vs 778.3 (-30%, and 543.03 on ALL FOUR runs — zero variance);
-- cold 26,364 vs 25,973 (p=0.57), warm 4,559 vs 4,584 (p=0.83), hot 3,728 vs 4,401 (p=0.74), ETL
-- CU 24,681 vs 24,051 with overlapping ranges. So: latency and ETL are INDISTINGUISHABLE and the
-- bytes are free. It is NOT faster — that is the fourth demonstration here that a smaller file does
-- not buy query time. Anyone reading -30% as a speed claim has it backwards.
-- Where the bytes go, per column, MB (`date,time` -> `date,DUID,time`): price 207.6 -> 125.8,
-- DUID 150.2 -> 1.9, time 3.3 -> 22.1, mw 417.3 -> 393.2. DUID collapses because a unit's whole
-- day becomes contiguous; `time` pays for it and the trade is worth ~148 MB.
-- price is a PER-REGION series, so it also compresses under `date,time,price` — 207.6 -> 25.0,
-- measured on run 31081276252 — but the two wins are MUTUALLY EXCLUSIVE and no fourth key gets
-- both: cheap DUID needs DUID-major (one unit's whole day contiguous), cheap price needs time-major
-- (one instant's whole region contiguous), and a table has one order. `date,price,DUID,time` just
-- reproduces `date,time,price`, since fixing price also fixes the interval. 543 MB is the frontier.
-- `mw` is the wall: 393-417 MB across every sort tried (a 6% range) and 72% of the best total. The
-- next size lever is its encoding or type, not row order.
-- THE 1 GB FILE CAP IS LOAD-BEARING, NOT A MIRROR OF binSize: delta-rs rolls files on in-flight
-- buffered bytes and TRUNCATES the current row group at the cap (measured: a cap of 1.15x one
-- RG's bytes wrote groups at 0.43x the declared rows), so chasing spark's 9 FILES with a small
-- cap would shred the 16M segments. ~777 MB under a 1 GB cap = one file, and max_row_group_size
-- alone cuts the groups. File count never separated engines in the CU data (dwh ships 78 files,
-- iceberg 357); segments did.
-- It spent one era as 'auto' so the input measured what the adapter's picker does out of the
-- box; the picker kept choosing `date, time` and paid +19% ETL CU for the profiling pass against
-- a named key's +3.7%, so a NAMED key is written down rather than 'auto'. Which named key is a
-- separate question and the picker never answered it — it only ever offered `date, time`.
-- DUID was refused once on n=1 evidence: run 30955591822 (`date,time,DUID`, 19 RG) posted the
-- worst duckrun CU (2,247.8) and warm (8,071 ms) in the table, and this comment used to cite that
-- as the reason its ~16% of size was "deliberately left on the table". n=4 per arm retired that —
-- one draw from a distribution whose within-config spread runs 25-100%. Note the retired run is
-- also a DIFFERENT KEY: DUID trailing (648.1 MB) is not DUID in the middle (543.0 MB).
-- DUCKRUN-ONLY without breaking the one-config-for-both rule, for the same reason partition_by
-- was: `sort_by` and the geometry keys occur ZERO times in dbt-duckdb's adapter and macro package,
-- so on iceberg they are parsed into the manifest and read by nobody. Both targets still run
-- byte-identical model code and there is still no `target.name` in this tree. Off they render to
-- `none`, which is what every run before the input did.
--
-- THE GEOMETRY KEYS NEED duckrun >= 0.4.44, AND ON 0.4.43 THE FAILURE WAS SILENT. Measured on run
-- 30955591822 (0.4.43, sorted=true, this config at 48M/1024): 651.1 MB, 3 files, 19 row groups —
-- the adaptive estimator's layout (log: "row-group geometry was sized for ~14,911,911 rows", 8M
-- floor x 18 = the 19 RG), not the declared one. The plugin and engine honored the values all
-- along (_geometry_config -> WriterProperties, no 16M clamp); what 0.4.43 was missing is the hop
-- between them — `_delta_core.sql` forwards model config to the plugin as a fixed key dict, and it
-- carried `sort_by` but neither geometry key (sort_by is why the 651 MB still came out sorted).
-- 0.4.44 forwards them and regression-tests the whole class; the notebook installs duckrun
-- unpinned from PyPI, so dispatches from 2026-08-05 get the declared layout with no change here.
--
-- sort_by is honored on this model despite the adapter docs calling it inert on the delta_rs
-- merge path: the merge here is insert-only, so the engine seam routes it to a DuckDB anti-join
-- committed as a plain append, and that path forwards sort_by and the geometry -- as does the
-- first-build overwrite, where an explicit row-group size bypasses the adaptive planner.
--
-- FOUR MEASURED POINTS, all 64 vCores, all full loads, all 143,980,961 rows. Every row-group
-- count is the adaptive planner's — a declared cap first reaches a write with 0.4.44 (see above):
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
-- is deliberately left on the table (the safe pick). What the input measures now is the declared
-- layout against the adaptive default: does collapsing ~25 row groups to ~9 full 16M segments —
-- spark readHeavyForPBI's exact geometry, minus V-Order — move cold/warm/hot the way Direct
-- Lake's segment story says.
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
    sort_by=(env_var('DUCKDB_SORT_BY', 'date,DUID,time').split(',')
             if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    max_row_group_size=(env_var('DUCKDB_ROW_GROUP_SIZE', '6000000') | int
                        if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
    target_file_size_mb=(env_var('DUCKDB_FILE_SIZE_MB', '1024') | int
                         if env_var('DUCKDB_SORTED', 'false') == 'true' else none),
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
