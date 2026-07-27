# Learnings

A running record of things that took real time to work out. Facts and measurements, not
recommendations — the rules that follow from them live in [CLAUDE.md](CLAUDE.md).

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
  on both engines. (`fct_scada`/`fct_price` do use `append`, not `merge`, on duckrun and spark;
  the file-level `NOT IN` filter is what keeps that safe, and it assumes dbt is the only writer.)
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
