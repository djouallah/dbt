# Learnings

A running record of things that took real time to work out. Facts and measurements, not
recommendations — the rules that follow from them live in [CLAUDE.md](CLAUDE.md).

## What actually OOM-killed the duckrun facts: a MERGE reads the whole target, not the batch

The duckrun facts went `append` → keyed write → `append` again → keyed write over a handful of
commits. The middle detour cost the most time, and the reason is one property of delta-rs MERGE:
even an **insert-only** merge plans a join against the whole pinned target, so its cost scales
with the target's *partition span* rather than the size of the batch, and its join state is not
fully spillable. It also splits the memory budget with DuckDB (a 30% merge share).

`fct_scada` is 369,205,022 rows. The first symptom was a OneLake GET that sat 212s and failed;
after a pruning predicate was added, the next run died mid-merge with **no dbt error line at
all** — just a leaked-semaphore warning, which means the process was killed rather than a query
failing. Neither symptom names memory, which is why this took two CI runs to read correctly.

duckrun 0.4.34 removed the cause: `incremental_strategy='insert'` no longer calls delta-rs MERGE.
It anti-joins the batch against the target's key columns **in DuckDB** (projection pushdown,
spills like any other DuckDB query) and hands delta-rs a commit carrying `add` actions only.
Measured on 20M rows × 14 columns over 12 monthly partitions, inserting 200k rows of which 100k
keys are new, post-write maintenance excluded from both:

| | wall | process RSS growth |
|---|---:|---:|
| DuckDB anti-join + append | **0.9s** | **+84 MB** |
| delta-rs insert-only MERGE | 6.7s | +8,397 MB |

Identical rows out of both. The memory column is the finding: +8.4 GB to insert 100k rows into a
20M-row table, and that growth tracks the *target*, not the batch.

Two things that were true only of the detour, and are worth not re-deriving:

- The interim `append` was fenced only because the adapter found the relation name in the
  **rendered** model SQL (`reads_self`). At one point the only occurrence was a passing mention
  of `{{ this }}` inside a prose comment — dbt renders comments, so the fence was real but one
  rewording away from silently becoming a last-writer-wins append. `insert`'s append is fenced
  unconditionally, on the version its anti-join read.
- `incremental_predicates=['target.month_key = source.month_key']` prunes nothing in a delta-rs
  merge (measured: 60/60 files scanned, partitioned or not) but **does** prune on the anti-join
  path, because `engine.probe_filters` reads the batch and folds *literal* partition values into
  the probe. Identical predicate text, opposite behaviour — the difference is which component
  computes the literals. Hence the file-literal macro is now iceberg's alone.

## Reading CSV with an explicit schema in Spark SQL

This was the single biggest time sink. The data needs an explicit schema because AEMO rows are
ragged: one PUBLIC_DAILY file holds many report types with different column counts. Without a
schema, Spark takes the column count from the first line (a short `C,NEMP.WORLD,…` row) and
silently truncates every wider row.

Every route that can carry a schema is blocked somewhere:

| Route | Carries a schema? | Outcome |
|---|---|---|
| `SELECT * FROM csv.\`path\`` | No — infers | Truncates ragged rows, silently |
| `CREATE TEMPORARY VIEW … USING csv` | Yes | Legal SQL, but see below |
| `CREATE VIEW … USING csv` | — | Doesn't exist; persistent `CREATE VIEW` is `AS query` only |
| `CREATE TABLE … USING csv` with a schema | Yes | Fabric: "External tables with … schema … are not supported" |
| `read_files('path', schema => …)` | Yes | Databricks-only. Not in Apache Spark 4.1 or 4.2 |
| dbt Python model (`spark.read.csv`) | Yes | dbt-fabricspark has no `python_model` macro |
| `text.` + `from_csv` | Yes | Works. Row-at-a-time parsing |

`USING <datasource>` is wired up **only** for `CREATE TEMPORARY VIEW`. That single fact is why
the workaround took so long to find: temp views are the one form that carries a schema, and a
temp view is exactly what a stored view definition may not reference.

A notebook has none of this trouble — `spark.read.format("csv").schema(user_schema).load(paths)`
is right there. The constraint is dbt, not Spark.

### Where the temp view does work

`dbt-fabricspark`'s incremental materialization builds `<model>__dbt_tmp` as a **persistent
view** only when it has to merge into an existing relation. For a first build or
`--full-refresh` it runs a bare `CREATE TABLE AS SELECT` with no tmp relation at all
(`incremental.sql`, the `existing_relation is none` and `should_full_refresh()` branches).

A CTAS executes immediately, so it *may* reference a temp view; a persistent view's definition
is stored and re-resolved later, so it may not (`INVALID_TEMP_OBJ_REFERENCE`). `pre_hooks` run
before the main statement. So a `pre_hook` that creates the temp view plus a CTAS that reads it
works — verified on a schema-enabled lakehouse, no `REQUIRES_SINGLE_PART_NAMESPACE`.

That splits the model in two: the rebuild path gets a real CSV scan, the incremental path keeps
`from_csv`. Acceptable because the incremental path only ever reads the current run's new files.

### What the row-at-a-time parse actually cost

Measured from the Spark driver log on a full `fct_price` rebuild:

- `text.` + `from_csv` over 130 columns, filter applied after the parse:
  **1,773 tasks, median 150s, min 120s, max 253s** — about 74 CPU-hours. Killed at 30+ minutes,
  projecting ~70.
- Same rebuild through `USING csv` with an explicit schema: **318s**.

Two separate causes were stacked: no vectorization or column pruning from `text.`, and the
`I='D' AND UNIT='DREGION'` filter running *after* `from_csv`, so every DISPATCH/TRADING row in
the file was fully parsed into a 130-field struct and then discarded.

Filtering the raw line first (`WHERE value LIKE 'D,DREGION,%'`) fixes the second cause without
fixing the first — `WHERE` is evaluated ahead of the `SELECT` list, so the plan becomes
Scan → Filter → Project. Useful on the path where the temp view isn't available.

## One trailing space split the engines in half, for over a year

`fct_summary` on `dwh` held ~250 more rows per date than a recomputation produced. It looked
like write-path drift on dwh. It was the opposite: dwh was the only engine that was right.

`dim_duid.DUID` contained `'ERB01 '` — one trailing space, straight from the AEMO registration
CSV. One row out of 689. The engines then disagreed about what `s.DUID = d.DUID` means:

| dialect | `'ERB01' = 'ERB01 '` |
|---|---|
| T-SQL | **TRUE** — ANSI pads on comparison |
| DuckDB | FALSE |
| Spark | FALSE |

So `dwh` joined the unit and carried it into `fct_summary` — 113,959 rows, from 2025-03-04
onward. `duckrun`, `iceberg` and `spark` dropped it silently and had **never** included it.
A real generating unit was missing from three of four outputs for more than a year.

Nothing failed. Nothing warned. The only symptom was a row-count gap that pointed at the wrong
engine, because the neutral reader is DuckDB and therefore inherits DuckDB's equality.

Two things this cost, worth remembering separately from the bug:

- **Reading the count as the cause.** `spark` and `dwh` both reported `Got 7 results` and were
  assumed to share a failure. They did not: spark's was ±0.0001 rounding, dwh's was 258 whole
  rows. The counts matched only because both differ on the same 7-day window. Tightening the
  row-count gate while loosening the sums is what finally separated them.
- **Not querying the warehouse in its own dialect.** Reading dwh's `dim_duid` through Delta with
  `WHERE DUID = 'ERB01'` returned nothing, which looked like the row was absent; the same query
  in T-SQL returned 1. Both statements were true — DuckDB will not match a padded key. The
  divergence *was* the finding, and it was invisible until the same question was asked twice in
  two dialects. `dbt show --target dwh --inline "…"` reaches the warehouse from a laptop.

Guarded by `tests/assert_duid_has_no_whitespace.sql`: no DUID may contain whitespace anywhere.
It is a one-line assertion at the point the value enters, and it would have caught this on the
day the CSV changed instead of a year later via a row-count gap on the wrong engine. The class
is general — any string join key crossing these four engines needs the same guarantee, because
padding is the one difference the dialects will not agree on and will never report.

## A neutral reader cannot grade a writer's rounding

`assert_fct_summary_matches_recomputation` failed on `spark` and `dwh` while `duckrun` passed,
across every run, with `Got 7 results` — one row per date in its 7-day window. It survived a
full rebuild of `fct_price` and a second pass at `fct_summary`, so it was not drift.

What the numbers said, measured with local `duckrun.connect()` against both lakehouses:

- Row counts per date **identical** — 65,680 vs 65,680, all seven dates.
- Sums apart by ~0.011 on ~6.9M — about 2 parts per billion, `actual` always the larger.
- Joining the two `fct_summary` tables key-for-key: ~146 rows per date differ out of ~65,000,
  every row joins, no key mismatch.
- The distribution of those differences is **only two values**: `+0.0001` × 132 and
  `-0.0001` × 14. Nothing else. 118 × 0.0001 = 0.0118, exactly the reported delta.

One unit in the last place of `DECIMAL(18,4)`, every time. Sample rows:

| DUID | spark | duckrun |
|---|---|---|
| CROOKWF3 | 2.5876 | 2.5875 |
| BW04 | 526.3875 | 526.3874 |
| WALGRV1 | **-0.3007** | **-0.3006** |

Every underlying double is an exact tie at the 5th decimal (2.58755, 526.38745, −0.30065).
Spark moves away from zero on all of them — note the negative going *more* negative, which is
away-from-zero, not "up". DuckDB lands on the even digit. **Spark casts `DOUBLE → DECIMAL` with
HALF_UP, DuckDB with HALF_EVEN**, and T-SQL is a third implementation, which is why `dwh` fails
alongside `spark`. The 132/14 split is the signature: HALF_UP moves on every tie, HALF_EVEN only
when the neighbour is odd.

Ruled out with measurements along the way, each of which looked plausible first:

- **Duplicate rows from `append`.** `fct_scada` for one date: 163,295 rows, 2 source files,
  **0** duplicate `(DUID, SETTLEMENTDATE)` keys and **0** keys with differing values — identical
  on both engines. (At the time `fct_scada`/`fct_price` did use `append` on duckrun and spark,
  with the file-level `NOT IN` filter as the only guard — which assumes dbt is the only writer.
  That assumption is what later moved every fact model onto a keyed insert-only strategy; the
  measurement above still stands, and is the evidence that the merge keys are unique.)
- **A stored-type mismatch.** Both lakehouses: `mw`/`price` `DECIMAL(18,4)`, `INITIALMW`/`RRP`
  `DOUBLE`, `SETTLEMENTDATE` `TIMESTAMP WITH TIME ZONE`. Byte-identical schemas.
- **The spark CSV rewrite.** `dwh` shares no model code with `models/spark/` and fails with the
  same count. That alone exonerates any spark-side change.

The real lesson is about the test, not the engines. Its header promises the stored table
"EXACTLY equal a clean recomputation of the model's full-refresh logic" — but it recomputes in
the **reader's** dialect, not the **writer's**. For spark, `fct_summary` *is* `f(inputs)`;
it is Spark's `f`. A DuckDB reader cannot reproduce Spark's rounding, so the assertion can only
ever hold for the engine that shares the reader's dialect. No rebuild fixes it, and
`REBUILD_SUMMARY=1` recomputes the same values and fails identically.

Row counts are dialect-independent and are the guard that actually matters — the bug this test
exists for was three different *row counts* across four engines. Sums need a tolerance, since
real drift is orders of magnitude above 0.011. Making the outputs genuinely byte-identical is
possible (`FLOOR(x*10000+0.5)/10000` is IEEE-754 arithmetic and agrees everywhere; only the
`DECIMAL` cast's tie rule diverges) but costs three model changes plus a one-off full rebuild on
every engine, and leaves older history mixed until then.

Method note: this took several CI round trips before the obvious move — `duckrun.connect()`
against both lakehouses from a laptop, which answered every one of the above in minutes. The
tables are reachable locally; reach for that before instrumenting a workflow.

## Two branches of one model, two different unit universes

`assert_fct_summary_matches_recomputation` went red on `duckrun`, `iceberg` and `spark`, two
rows each. `dwh` passed. The obvious reading — "dwh is the reference and three engines drifted"
— is wrong twice over: the test never compares engines, and dwh was not more correct.

The test grades each engine against **its own lakehouse**: recompute `fct_summary` from that
item's `fct_scada`/`fct_price`/`dim_duid` and diff it against that item's stored `fct_summary`.
Red means a table disagrees with its own inputs. Measured for 2026-07-27:

| | rows |
|---|---|
| recomputed from that lakehouse's own inputs | 64,718 |
| duckrun / iceberg / spark stored | 70,579 (+5,861) |
| dwh stored | 64,718 |

The surplus is **28 DUIDs that have zero rows in `fct_scada` across all 369,368,318 of them,
ever** — ROWALLAN, WAUBRAWF, ROYALLA1, GERMCRK, CLUNY, PALOONA, BUTLERSG, CAPTL_WF and 20 more.
Non-scheduled units: they publish SCADA telemetry but are never dispatched.

The model reads two AEMO tables whose unit coverage differs:

| branch | source | distinct DUIDs |
|---|---|---|
| daily | `fct_scada` — DISPATCH_UNIT_SOLUTION | 644 |
| intraday | `fct_scada_today` — DISPATCH_UNIT_SCADA | 406 |

While a date is fresh the intraday branch writes those units. When the daily file lands, the
date's recomputation comes only from the daily branch, which **cannot** reproduce them. Nothing
disappeared from any input — the row's *producing branch switched*, and the branches do not
cover the same units. The model header asserted this could not happen "while the inputs stay
append-only". Append-only was never the relevant property.

Every engine writes them. On 2026-07-28, still inside the intraday window, **dwh held 5,016 of
those rows too**. dwh only looked healthy because `delete+insert` on `unique_key=['date']`
rewrites a whole date and so can retract; the merge engines key on `['date','time','DUID']`,
which inserts but never deletes. Same model, same rows written, different ability to take them
back. (dwh has since moved to `merge` on that same full grain, so no engine retracts now and the
`dispatch_duids` gate is the only thing holding those units out.)

Scale: 11,540 orphan rows over two dates, identical on all three merge engines, and it re-fires
**every day** a date crosses the daily horizon. The last green run before it was timing luck —
07-27's daily file had not landed yet.

The fix gates the intraday branch on `SELECT DISTINCT DUID FROM fct_scada`, deliberately
**unbounded**. `fct_scada` is append-only, so that set only ever grows and can never orphan a
row it previously admitted. A trailing window would reintroduce the identical bug from the other
side — a unit ageing out turns its already-written rows into orphans, which merge still cannot
delete. Cost of the unbounded scan: 644 rows out of 369M, 1m44s from a laptop over the WAN,
which is the pessimistic bound since CI runs co-located with OneLake.

Two method notes, both of which changed the answer:

- **Sampling one date gave the wrong DUID list.** Deriving it from 07-27's surplus produced 26
  units. The authoritative query — `DUID NOT IN (SELECT DISTINCT DUID FROM fct_scada)` over the
  whole table — produced **28**: `BUTLERSG` and `CAPTL_WF` appear only on 07-28. Two of 28 would
  have survived a purge built from the sample.
- **`duckrun.connect(..., read_only=True)` from a laptop reproduced CI's numbers exactly**
  (64,718 / 70,579 and 62,244 / 62,552), which made the whole diagnosis a local exercise. Set
  `SET TimeZone='UTC'` first — CI writes under UTC, and `CAST(SETTLEMENTDATE AS TIMESTAMPTZ)`
  renders in the session zone, so a `+10` laptop reads shifted timestamps.

Before pushing, the rendered DuckDB-dialect SQL was **executed** against empty dummy tables in a
local DuckDB — models and the test, both `is_incremental()` states. That catches column and
syntax errors the render check in [CLAUDE.md](CLAUDE.md) cannot, because rendering only proves
the Jinja produced text.

## `--full-refresh` is not a rebuild lever on every engine

`REBUILD_SUMMARY=1` fans out to three different mechanisms: `fabric_build.py` appends
`dbt run --select fct_summary --full-refresh` for **both** duckrun and iceberg, `ci.yml` adds its
own `--full-refresh` step for spark, and the dwh model reads the env var and emits every date
through its ordinary `delete+insert`. Only dwh's route is documented as special. The other three
were assumed equivalent. Two of them are not.

**iceberg cannot full-refresh at all.** In the same job the ordinary run passed
`PASS=14 WARN=0 ERROR=0`; the appended rebuild step then failed:

```
Runtime Error in model fct_summary
TransactionContext Error: Failed to commit: Failed to commit Iceberg transaction:
Table fct_summary__dbt_tmp does not exist
```

This is **not** an Iceberg limitation, and conflating the two costs a diagnosis. Probed directly
against the OneLake Iceberg REST catalog through `duckrun.IcebergSession`:

```
OK   CREATE TABLE mart.zz_drop_probe AS SELECT 1 AS x
OK   DROP TABLE mart.zz_drop_probe
OK   DROP TABLE IF EXISTS mart.zz_not_there
```

[DuckDB's docs](https://duckdb.org/docs/current/core_extensions/iceberg/writing) agree —
`CREATE`/`DROP TABLE`, `ALTER … RENAME TO`, `INSERT`, `UPDATE`, `DELETE` and `MERGE INTO` are all
supported against an attached REST catalog. What fails is **how dbt-duckdb materializes a full
refresh**: it builds `<model>__dbt_tmp` and swaps it in, and that relation is not visible to the
Iceberg transaction at commit time. The catalog's capabilities and the adapter's materialization
strategy are separate layers; the error names the second.

Consequence: the failed swap leaves a `mart.fct_summary__dbt_backup` behind, holding a full copy.
And the repair lever recorded for the merge engines is fiction for this one — the ordinary
incremental path is unaffected, so there is no in-band way to rebuild that table.

**duckrun can be killed by it.** The 143,753,905-row full refresh died with **no dbt error line
at all**:

```
Duckrun adapter: merge spill cap: 36.65 GiB (60% of 61.09 GiB available RAM)
resource_tracker: There appear to be 2 leaked semaphore objects to clean up at shutdown
[fabric_run] duckrun success=False returncode=1
```

No error line means the process was killed, not that a query failed — the same signature as the
memory-bound merge already recorded in [CLAUDE.md](CLAUDE.md). A separate Fabric-side failure hit
the same leg first: `PythonComputeClientException … isRetriable: False`, the job dying *before
the payload ran*, which is infrastructure and not attributable to the model.

Net: `--full-refresh` works on spark, is forbidden on dwh (it DROPs and recreates), fails
outright on iceberg, and is a coin flip on duckrun at this table size. Firing it for all four
from one flag is what turned a rebuild that only dwh needed into two failed legs — duckrun and
iceberg had both already been rebuilt from scratch by the preceding run and needed nothing.

## "The table exists" can be a lie, in two different engines

Two incidents, same shape: the existence check consults metadata, the metadata disagrees with
storage, `is_incremental()` goes true, and the model emits SQL against a table that isn't there.

- **duckrun / `dbt_delta`**: `landing/fct_scada` held parquet but no `_delta_log`. The adapter's
  OneLake REST listing returns every immediate sub-directory name with no `_delta_log` check, so
  it was cached as an existing relation. `Catalog Error: Table with name fct_scada does not
  exist!`, pointing at line 5 of the compiled SQL. The same store read through
  `duckrun.connect()` listed it correctly — that discrepancy is what identified it.
  Filed as [djouallah/duckrun#19](https://github.com/djouallah/duckrun/issues/19).
- **spark / `dbt_spark`**: `landing/fct_price` gone from storage, entry still in the Spark
  catalog. `[DELTA_TABLE_NOT_FOUND] Delta table … doesn't exist.`

Both error messages name the model and a line in the compiled SQL, so they read as SQL bugs.
Neither mentions discovery, `_delta_log`, or the directory that caused it.

**Deleting the folder is not the same as dropping the table.** dbt asks the catalog, not the
filesystem. Removing `Tables/landing/fct_price` left the Spark catalog entry intact and the
failure survived two more runs. `DROP TABLE IF EXISTS landing.fct_price` is what clears it.

## dbt-fabricspark high concurrency

`high_concurrency` defaults to **True**. In that mode the adapter opens one HC session — one
Spark REPL — **per dbt thread**, each a separate `POST /highConcurrencySessions`.

Fabric packs at most **five REPLs per Livy session**. At `threads: 8` that spilled into a second
Livy session, i.e. a second Spark application, separately started and separately billed, for one
`dbt run`. Dropped to `threads: 4`; the driver log then showed exactly four REPLs in one
application.

Other details from the same log:

- `sessionTag` is documented as a packing *hint*, not a lock — "rapid concurrent calls … might
  create multiple Livy sessions". dbt opens its thread connections simultaneously.
- With `reuse_session: false` (the default) the tag is a fresh uuid **per process**, so
  `|| dbt retry` gets a different tag and cold-starts a new Spark application rather than
  reusing the warm one.
- Each REPL submits to its own scheduler pool and none are configured, so they all fall back to
  the default pool — no fair share between threads.
- Executors scaled 1 → 9 over a run. The "initial executors = 1" in `profiles.yml` is a
  dynamic-allocation floor, not a cap.
- The HC acquire payload *does* accept `numExecutors`, `executorCores` etc., and the adapter
  forwards them from `spark_config`. Whether Fabric honours them over the pool settings is
  untested here.

## Cancelling a GitHub job does not stop Fabric

The workflow used to have every job run `gh run cancel` on the whole run when it failed, to stop
a doomed run burning Fabric compute. It doesn't do that: the Livy session and the notebook job
keep running workspace-side after the GitHub job dies. What the cancel actually produced was a
run where every leg reads `cancelled` — including the one that failed — and no artifacts from
legs still mid-flight. Removed.

To actually stop the compute, kill the session in the Fabric monitoring hub; HC jobs appear
there as `HC_<LakehouseName>_<LivySessionId>`.

## The Spark driver log is worth pulling

dbt's client log shows only the outcome. The driver `stderr` from the Livy session shows task
counts and durations, REPL ids, executor scaling, and the actual stage that is slow — that is
where the 1,773 × 150s number came from. Nothing else available made the cost visible.

Noise to ignore in it:

- `ERROR OneLakeUtil: OneSecurity failed to resolve table name` — fires constantly, including on
  `parquet.abfss://…` and `text.abfss://…` pseudo-paths that are not tables. Non-fatal.
- `WARN OptimisticTransaction: Change in the table id detected` — a `table` materialization
  replacing itself legitimately mints a new metadata id.

Fabric redacts SAS signatures in these logs (`sig=X`), but the log still carries workspace,
lakehouse and tenant GUIDs. It is gitignored; keep it that way.

## Jinja traps hit while editing the models

Beyond the documented `-%}` trap:

- **Jinja comments do not nest.** Writing the trimming tokens inside a `{# … #}` comment closes
  it early and leaks the prose into the SQL.
- A trimming comment between `FROM text.\`path\`` and a following `WHERE` eats the newline and
  renders `` …`path`WHERE value LIKE … ``. Same family as the `depends_on` trap, different
  keyword.

Both were caught by the render check in CLAUDE.md before reaching CI, which is the argument for
running it every time. Render **every** branch: `is_incremental()` both ways *and* the empty vs
non-empty `spark_new_files` list, since they select different SQL.
