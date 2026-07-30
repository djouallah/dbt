# Working in this repo

One dbt project, four engines (`duckrun`, `iceberg`, `dwh`, `spark`), one landed copy of the
data. The thesis is *the engine doesn't matter, the output does* — so the models are written
per dialect (`models/duckdb`, `models/dwh`, `models/spark`, gated by `+enabled` in
`dbt_project.yml`) and every leg runs `dbt build`, so each engine writes and tests its own output
in one DAG walk. CI's final word is `stats.py`, which reads all four items through Delta on
OneLake and puts every shared table side by side.

**The test suite covers the mart and nothing else** — `fct_summary`, `dim_duid`, `dim_calendar`.
The facts and the staging view carry descriptions, no assertions: the grain and
files-processed tests over `fct_price`/`fct_scada` were deleted deliberately, so an input defect
is now only visible where it surfaces in the summary. Adding a test on a fact model is a reversal
of that decision, not an oversight being corrected.

**And the suite in `tests/` runs on the duckdb-family targets only** — `data_tests: +enabled` in
`dbt_project.yml` gates it, and it is DuckDB SQL, so it cannot render on dwh or spark. Those two
are graded by their generic column tests plus the mart parity table. That is the trade made when
the separate neutral-reader test job was removed: the assertions used to run against all four items
through one duckrun reader. If a determinism question about dwh or spark comes up, the reader still
exists — point `duckrun.connect(<abfss Tables path>, read_only=True)` at the item from a laptop and
run the test body by hand.

The traps below have all been hit for real. Each one cost a CI run or worse.
[LEARNINGS.md](LEARNINGS.md) records the longer investigations behind some of them — measured
numbers, and the routes that were tried and did not work.

## Verify locally before you push — CI is the last check, not the first

CI here is slow, serialized on a concurrency group, and burns paid Fabric compute. It is not a
syntax checker. Before pushing a model change, render it and read the SQL:

```bash
python - <<'EOF'
import re, jinja2
class T:
    def __init__(s, n): s.name = n
MODELS = [("models/duckdb/marts/fct_summary.sql", "duckrun"),
          ("models/duckdb/marts/fct_summary.sql", "iceberg"),
          ("models/spark/marts/fct_summary.sql",  "spark"),
          ("models/dwh/marts/fct_summary.sql",    "dwh")]
for path, tgt in MODELS:
    src = open(path, encoding="utf-8").read()
    for inc in (True, False):
        for reb in ("0", "1"):
            out = jinja2.Environment().from_string(src).render(
                config=lambda **k: "", ref=lambda n: f"tbl_{n}", this="tgt",
                is_incremental=lambda: inc, var=lambda n, d=None: d,
                env_var=lambda n, d=None: reb if n == "REBUILD_SUMMARY" else d,
                target=T(tgt))
            first = next((l for l in out.splitlines()
                          if l.strip() and not l.strip().startswith("--")), "")
            glued = [l for l in out.splitlines() if re.search(r"--.*\bWITH\b", l)]
            ok = first.strip().upper().startswith("WITH") and not glued
            print(f"{'ok ' if ok else 'BAD'} {tgt:8s} incr={inc!s:5s} REBUILD={reb} "
                  f"-> {first.strip()[:60]!r}")
EOF
```

It prints a verdict per branch, not a wall of header comments — the thing you're checking is
that SQL starts at a bare `WITH`, never glued onto a `--` line. Render **every** branch:
`is_incremental()` both ways, each target, and any env-var switch. A branch you didn't render
is a branch you didn't test. On the spark daily models that also means both an empty and a
non-empty `spark_new_files` list, and asserting on the rendered `pre_hook` — they select
genuinely different SQL, not just different text.

Two Jinja bugs this has caught that the `-%}` rule alone does not describe: a trimming comment
between `FROM text.\`path\`` and a following `WHERE` glues them into `` …`path`WHERE ``, and
Jinja comments **do not nest** — writing the trimming tokens inside a `{# … #}` closes it early
and leaks the prose into the SQL.

**Rendering only proves the Jinja produced text — go one step further and *execute* it.** For
the DuckDB-family models and the singular tests, create empty dummy tables carrying just the
columns the SQL references (`tbl_fct_scada`, `tbl_fct_price`, `tbl_dim_duid`, …), point `ref()`
at them, and run every rendered branch through a local `duckdb.connect()`. It costs seconds, needs
no credentials, and catches the column and syntax errors a render check cannot see. It will not
cover the spark or dwh dialects — those are structurally identical here, so CI remains their
first real check.

When a build does fail, the job uploads `target/` as an artifact. Read the *compiled* SQL
instead of guessing at the error:

```bash
gh run download <run-id> -R djouallah/dbt -n dbt-target-dwh -D /tmp/t
cat /tmp/t/compiled/aemo_electricity/models/dwh/marts/fct_summary.sql
```

## Jinja whitespace control will comment out your SQL

Every model starts with `-- depends_on:` line comments. A tag closed with `-%}` strips the
newlines *after* it, so the next SQL keyword gets pulled onto that comment line and vanishes:

```
-- depends_on: [dbt_dwh].[landing].[fct_price_today]WITH
```

The parser then reports an error at the *first CTE name*, which sends you hunting for a SQL
problem that doesn't exist. Real symptom seen: `Incorrect syntax near 'scada_cutoff'`.

**Rule:** the last Jinja tag before SQL closes with `%}`, never `-%}`. The spark
`fct_price`/`fct_scada` models carry the same warning inline — heed it rather than tidying it
away.

## Incremental write strategies are per engine, and not interchangeable

**Nothing writes with `append` any more, and nothing should go back to it.** Append has no
write-time key check, so the only thing preventing duplicate rows was the *file selection* —
`new_source_files` on dwh, `spark_new_files` on spark, the `SET VARIABLE` pre-hook on duckdb.
That list is computed **before** the write. Two overlapping runs (a re-dispatch, a `dbt retry`
racing a scheduled run) both see a file as new and both append it. The file lists all stay —
they are what keeps the write's source small — but the key match is now the guard underneath.

The duckrun facts did spend one commit on `append` plus a hand-built OCC fence, because the
`insert` of the day was a delta-rs merge that OOM-killed `fct_scada`. Do not resurrect that
shape: the fence depended on the adapter spotting `{{ this }}` in the *rendered* SQL, so a
reworded comment silently downgraded it to a last-writer-wins append. duckrun 0.4.34 removed the
reason it existed.

Every fact model is **insert-only**: the data is append-only, so a matched row never needs
updating. The **standing rule for the duckdb tree** is that duckrun and iceberg run byte-identical
model code — where the two adapters disagree, the project writes what iceberg can take, even when
duckrun offers something better. That is why the facts say `merge` + `do_nothing` rather than
duckrun's own `insert`, which is the same operation and would keep the full memory share.

| target | strategy | why not something else |
|---|---|---|
| `duckrun` **and** `iceberg` | `merge` + `when_matched: do_nothing` on the facts **and on `fct_summary`** — **one config, zero `target.name` in the whole tree**. | The models/duckdb tree renders for both, so it is written in dbt-duckdb's spelling, which **duckrun accepts verbatim since 0.4.35** (before that it raised on `do_nothing`, `_specs_from_merge_clauses` — that raise was this project's reason to branch, and it is gone). Requires duckrun ≥ 0.4.35. On duckrun that clause list is *routed* — an insert-only shape never removes a row, so `engine.merge_delta_clauses` diverts it to a DuckDB anti-join over the key columns plus an add-only append, always fenced to the version the anti-join read. Cost tracks the batch, not the target's partition span: 0.9s/+84MB against 6.7s/+8,397MB for the delta-rs merge on a 20M-row table, which is what OOM-killed `fct_scada`. One accepted cost versus spelling it `insert`: the merge path has already called `set_merge_memory_limit`, so the routed anti-join computes under DuckDB's 0.3 merge share instead of the full write share — correct, just more spill-prone (`_store_merge` docstring). On iceberg it is a real delta-rs-free MERGE, and it must stay insert-only: the OneLake REST catalog rejects a matched-UPDATE branch with `BadRequest 400`, and *omitting* `when_matched` is not the same thing — dbt-duckdb defaults it to update-by-name and draws that 400. Not `delete+insert`: on duckrun that is a fenced **full-table overwrite** (every surviving row plus the batch into a DuckDB temp table, then overwrite) — a full rewrite of 143M rows *every run*. The price of one config on `fct_summary`: duckrun **gives up the matched UPDATE it is capable of**, so a re-emitted row with a revised `mw`/`price` no longer overwrites on either duckdb target — craters are filled, changed values are not. spark and dwh do update, so a revision shows up as a value gap between the pairs, not a row-count gap. |
| `spark` | `merge` + `skip_matched_step=true` | dbt-fabricspark honours `skip_matched_step`, which omits the WHEN MATCHED branch entirely — genuinely insert-only, and it cannot hit a multiple-source-row match error because there is no matched clause. Requires `file_format='delta'`. `merge` and `append` take the identical path in that materialization (persistent `__dbt_tmp` view, then one DML), so switching strategy does not disturb the CSV read. |
| `dwh` | `merge` on the facts **and** on `fct_summary` | Insert-only is **not expressible** here — the opposite limitation from iceberg: dbt-fabric merge is `default__get_merge_sql`, which always emits `WHEN MATCHED THEN UPDATE SET <every column>` (`merge_update_columns=[]` is falsy and falls through to all columns). For append-only facts that branch is a semantic no-op — a matched row is rewritten with its own values — so it is correct, just not free. On `fct_summary` that forced update is exactly what is wanted, and matches duckrun/spark. It was `delete+insert` on `['[date]']`, which replaced whole dates and therefore **retracted** rows the recomputation no longer produced — the one write path here that could, which is why dwh's row count could differ from the other three on identical inputs and why it silently passed the intraday-unit bug. Repair is `REBUILD_SUMMARY=1`, not a per-date wipe. If the leg gets slow, fall back to `delete+insert` on `unique_key=['[file]']`. Bracket every key column: dbt interpolates them raw into the ON clause and `file`/`date` are reserved words. Never `--full-refresh` here: on dbt-fabric that DROPs and recreates, which deadlocks Fabric's background stats maintenance, loses grants, and rebinds Direct Lake. Use `REBUILD_SUMMARY=1` instead. |

Concurrency is not equal across the four. duckrun, iceberg and spark check the commit, so a real
overlap **fails loudly** instead of duplicating. Fabric Warehouse does not: under snapshot
isolation two transactions overlapping in time can still both insert. Merge shrinks that window
from *[compile-time file list → write]*, which is unbounded, down to the transaction overlap, and
T-SQL offers nothing stronger without application locks Fabric DW lacks.
`assert_fct_summary_grain` is the detector for the remainder, and it is the **only** assertion left
on `fct_summary` — the fact grain tests, the recomputation test, the crater test and the join test
were all deleted when the suite was cut back to uniqueness. Three consequences worth holding onto:

- **A duplicate in `fct_scada` / `fct_price` is no longer caught where it enters**, only if it
  happens to surface as a duplicated `(date, time, DUID)` in the summary. One that lands on a
  distinct grain key is invisible. Nothing else would have caught it either: the recomputation
  test recomputed *from* those same facts, so a duplicated source row agreed with itself.
- **It does not cover dwh**, the one engine whose write path can actually duplicate — under
  snapshot isolation, without a commit check. It is a singular test, so `data_tests: +enabled`
  admits it on duckrun and iceberg only. Run it by hand against `dbt_dwh` after any run that could
  have overlapped (a re-dispatch, a `dbt retry` racing a scheduled run):
  `duckrun.connect(<dbt_dwh Tables path>, read_only=True)` plus the body of
  `tests/assert_fct_summary_grain.sql`.
- **It is deliberately incurious about everything else.** No join to `dim_duid`, no recomputation,
  no expectation about intervals per day or which dates exist. That is what makes it immune to a
  short AEMO day or a half-drained backlog — and it is also why a wrong `mw`, a NULL `price` or a
  missing date now passes silently.

Full table, no date window, no `heavy` tag. A window would encode an assumption about *where*
duplicates live (recent writes), which is the source knowledge this test is meant to be free of —
verified against an 8-year-old duplicate, which the earlier 30-day version missed. A tag would
exclude it from every leg and leave `fct_summary` with no assertion at all.

Before changing a strategy, read the adapter's own source rather than assuming the name means
what it does elsewhere. duckrun's lives in `dbt/adapters/duckrun/delta_plugin.py`; the Fabric ones
in `dbt/include/fabric{,spark}/macros/materializations/models/incremental/`.

### A keyed write reads the target — the literal file predicate is what bounds it

**Current state first, history second:** the duckdb facts declare **no `partition_by` and carry no
`month_key`**, and this is not a preference — **dbt-duckdb cannot express partitioning at all**
(the string appears nowhere in its materializations; a `partition_by` is silently dropped, see
[LEARNINGS.md](LEARNINGS.md)). So `partition_by` could only ever be duckrun-only, which makes it
incompatible by construction with one body for both targets. What bounds the target read instead is
`macros/pending_file_predicate.sql` — a literal `file IN (…)`, measured at **0 of 60 files scanned**
where every column-to-column predicate scanned all 60. Read the rest of this section as *why
partitioning was tried and what it cost*, not as a description of the models.

**Deleting the `month_key` column forced one rebuild**, and note the actual cause: `merge` happily
writes into whatever partitioning a table already has, but duckrun refuses a batch that is *missing
a column the target has* (`delta_plugin.py:645-656` → `insert: … Missing: ['month_key']`). So the
four duckrun fact tables were `DROP TABLE`d (never a folder delete) and rebuilt. That rebuild is
the whole cost of the rule, and `fct_scada` is 369M rows of it. Do not repeat the mistake of
blaming `merge` or partitioning for it.

Why partitioning was introduced at all: moving the facts off `append` made every run scan the
target looking for key collisions. The other three engines absorbed it (dwh 48s, iceberg 49s,
spark 95s on `fct_scada`). duckrun did not, twice: first a OneLake GET that sat 212s and failed,
then — after adding a pruning predicate — a run that died mid-merge with **no dbt error at all**,
just a leaked-semaphore warning. No error line means the process was killed, not a query that
failed. `fct_scada` is 369,205,022 rows and a **delta-rs merge** is memory bound: it plans a join
against the whole pinned target and its join state is not fully spillable. The routed anti-join
(duckrun ≥ 0.4.34) sidesteps that — it runs in DuckDB and spills like any other query — which is
what made the partitioning droppable rather than load-bearing. The duckrun AEMO reference models
(`tests/integration_tests/aemo/models/marts/fct_{price,scada}.sql`) still carry
`partition_by=['month_key']` plus `incremental_predicates=['target.month_key = source.month_key']`,
and that remains the right shape for a **single-target** duckrun project — just not for this one.

Two things that cost real time here, worth not rediscovering:

- **Partitioning is set at table creation.** `_store_overwrite` passes `partition_by` through
  (`delta_plugin.py` 253→301); `_store_merge` does not, because a merge writes into whatever
  partitioning already exists. Adding `partition_by` to a live table does nothing — the table
  has to be dropped and rebuilt. All four duckrun facts were dropped for this. `_store_insert`
  **does** forward it (`delta_plugin.py:598,662`), because that path commits an append and its
  probe filters need to know which column is the partition — so the existing layout is preserved
  and no rebuild was needed to move back onto `insert`.
- **A column-to-column predicate does not prune target FILES — in a MERGE.** Measured against
  delta-rs on a 60-file table merging one new file: key only, `target.DATE = source.DATE`,
  `target.month_key = source.month_key`, and even the same with the table partitioned, all
  scanned 60/60; only a *literal* filter reached 0/60. This is why the predicate carries file
  **names** and not a comparison. duckrun's routed anti-join additionally folds its own literals in
  (`engine.probe_filters`: an exact `IN` list for a **declared** partition equality, min/max bounds
  for every other join key, so `file` gets its range for free) — so on that side the pruning is
  belt-and-braces, and it is the reason dropping the partition column was affordable.

`macros/pending_file_predicate.sql` is the literal-value version, built from the pending file
names known at compile time (`IN (...)` up to 200 files, else `BETWEEN min AND max`), and it now
serves **both** duckdb targets. Because `file` leads every fact's `unique_key` the predicate is
*implied* by the merge ON clause, so it removes no match the key would have made; on a deliberate
duplicate it still scanned the 1 file that could collide and inserted 0 rows.

Write it with dbt's `DBT_INTERNAL_DEST` alias, never `target.` — dbt-duckdb builds its ON clause
with `DBT_INTERNAL_DEST` and knows no `target` alias, so `target.file` fails the iceberg leg
outright. duckrun accepts the same text because `_merge_predicates` rewrites
`DBT_INTERNAL_DEST`/`_SOURCE` to `target`/`source` **before** `_merge_source_keys` parses it, so
one spelling genuinely serves both. Do not "fix" it to `target.`.

## `fct_summary` must be a pure function of its inputs

It once held three different row counts across four engines while every input table was in
exact parity. Cause: the incremental source only ever offered dates missing *entirely*, so a
date that existed but was incomplete could never be repaired by any write strategy — each
engine's run history got fossilized into its table.

**Nothing tests this any more.** `assert_fct_summary_matches_recomputation` — which recomputed the
model's full-refresh logic over a trailing 7-day window and demanded exact equality with the stored
table — was deleted along with the crater and join tripwires, when the suite was cut back to a
uniqueness check that reads `fct_summary` alone and assumes nothing about the source. So the rules
below are now conventions held by code review, not by CI. Every failure mode this section
describes is one CI used to catch and no longer does; the only surviving signals are the grain
test and a row-count difference between engines in the `summary` parity table.

Rules that keep it honest:

- The incremental source emits the **complete recomputation** for every date that could still
  be stale — never a partial top-up.
- The stale set is: dates absent from the target, plus a **trailing 7-day window**, plus dates
  still in the intraday feed. The window is not "the newest daily date": if a run is missed,
  two daily files land at once and the older one's craters would be unreachable. There is no
  longer a test whose window has to be kept ≤ this one; the pairing constraint died with it.
- **Both branches must cover the same unit universe.** The daily branch reads `fct_scada`
  (DISPATCH_UNIT_SOLUTION, 644 DUIDs); the intraday branch reads `fct_scada_today`
  (DISPATCH_UNIT_SCADA, 406). 28 non-scheduled units appear only in the second — zero rows in
  `fct_scada` across all 369M, ever. Ungated, the intraday branch wrote them, and when the date
  crossed the daily horizon nothing could reproduce them: 11,540 permanent orphans, re-firing
  daily. The intraday branch is therefore gated on a `dispatch_duids` CTE. That gate used to be
  mirrored by an identical filter in `assert_fct_summary_matches_recomputation`, so changing one
  without the other failed by construction; the test is gone, so the gate is now unguarded — treat
  any edit to it as load-bearing. Keep that set **unbounded**
  (`SELECT DISTINCT DUID FROM fct_scada`): the table is append-only so the set only grows and can
  never orphan a row it admitted, whereas a trailing window recreates the bug from the other side.
  Note this class of drift is invisible to "the inputs are append-only" reasoning — no input row
  vanishes, the row's *producing branch* changes.
- Repair lever, and it is **not uniform** — do not assume `--full-refresh` works everywhere:
  `REBUILD_SUMMARY=1` on dwh (never `--full-refresh`, it DROPs); `--full-refresh` on spark and
  duckrun, but on duckrun that is a 143M-row rebuild that has been killed outright (no dbt error,
  just a leaked-semaphore warning). On **iceberg it fails every time** —
  `Failed to commit Iceberg transaction: Table fct_summary__dbt_tmp does not exist`. That is
  dbt-duckdb's swap materialization, *not* an Iceberg limit: `CREATE`/`DROP`/`RENAME`/`MERGE` all
  work against that catalog when issued directly. `fabric_build.py` fires the rebuild step for
  duckrun **and** iceberg from one flag, so `REBUILD_SUMMARY=1` breaks the iceberg leg and leaves
  a `fct_summary__dbt_backup` behind. See [LEARNINGS.md](LEARNINGS.md).
  This lever carries more weight now that both duckdb targets are insert-only on `fct_summary`: a
  **revised** `mw`/`price` cannot be repaired by any incremental run there, only by a rebuild.

## Where the DuckDB fold runs

Always in a Fabric notebook, via `fabric_run.py` → `duckrun.run_python` → `fabric_build.py`.
There is no runner-side branch and nothing decides placement.

Two attempts at deciding it are already buried, so don't dig up a third:

1. *"Did a new daily file land this run?"* — describes the download, not the backlog. A
   from-scratch lakehouse has ~3000 files outstanding with nothing new landed; that reads as 0
   and puts the whole archive on a 7GB runner.
2. *Count pending files per engine* (`pending_files.py`, deleted) — measured the right thing but
   had to read the backlog through the very tables the build was about to write. When
   `landing.fct_scada` in `dbt_delta` went unreadable, the probe threw, the aborted DuckDB
   transaction poisoned every later probe, and it fell back to its sentinel anyway.

Both failed the same way: placement is a prediction made before the build, and a wrong one is
paid for by the leg that can least afford it. Fabric handles a fold of any size; the runner
handles a small one slightly cheaper. That trade was never worth a decision that could be wrong.

`fabric_build.py` stays location-agnostic — it resolves its own token either side — so you can
still run it by hand to reproduce a CI failure. That is a debugging affordance, not a CI path.

## Facts that are easy to get wrong

- **XTable *does* convert Iceberg positional deletes** into Delta deletion vectors. Emitting
  deletes is not what forces `iceberg` to stay insert-only; the REST catalog's 400 on
  matched-UPDATE is.
- **Livy compute is workspace-side.** Change the workspace Spark pool to resize a session. The
  HC acquire payload does accept `numExecutors`/`executorCores` and the adapter forwards them
  from `spark_config`, so "cannot" is untested rather than proven — but nothing here sets them,
  and the observed 1-executor launch is the pool's dynamic-allocation floor, not a cap (it
  scaled to 9 under load).
- **Deleting a table's folder does not delete the table.** dbt asks the catalog, not storage.
  A `Tables/<schema>/<name>` directory removed by hand leaves the entry behind, `is_incremental()`
  stays true, and the model emits DML against nothing —
  `[DELTA_TABLE_NOT_FOUND]` on spark, `Catalog Error … does not exist` on duckrun. Use
  `DROP TABLE IF EXISTS <schema>.<name>`. A directory holding parquet with no `_delta_log` is
  the same trap from the other direction.
- **String join keys must be whitespace-clean, and only a test can guarantee it.** T-SQL pads on
  comparison (`'ERB01' = 'ERB01 '` is TRUE); DuckDB and Spark do not. One trailing space in
  `dim_duid.DUID` put a real unit in `dwh` and in none of the other three, for a year, silently
  — and the row-count gap it produced accused the one engine that was correct.
  `assert_duid_has_no_whitespace` guards it — but it is a singular test, so it now runs on the
  duckdb targets only, and T-SQL padding is a *dwh* pathology. The guard sits on the two engines
  that cannot exhibit the bug. `dim_duid` is built from the same source everywhere, so a dirty key
  will still trip it on duckrun; treat that as the alarm for all four. Any new string key crossing
  engines needs the same.
- **A DuckDB assertion cannot grade another engine's rounding.** `DOUBLE → DECIMAL` tie-breaking
  differs per dialect — Spark HALF_UP, DuckDB HALF_EVEN, T-SQL a third — so a test asserting a
  DuckDB recomputation *exactly* equals a stored value can only ever pass for `duckrun`. The
  symptom is ±0.0001 on a few hundred rows and no row-count difference at all. No test does this
  any more (the recomputation test is deleted, and the surviving grain check compares a table to
  itself), so it cannot bite in CI — but it bites anyone who reintroduces a value comparison, or
  points a DuckDB reader at `dbt_spark` / `dbt_dwh` by hand. Row counts are dialect-independent;
  assert those exactly and give any sum a tolerance. See [LEARNINGS.md](LEARNINGS.md).
- **A green engine is not a reference, and no test is cross-engine.** This was already true when
  `assert_fct_summary_matches_recomputation` existed — it recomputed from *the same item's* inputs
  and diffed against *that item's* stored table, asserting self-consistency and never agreement
  with another engine — and it is more true now that the only assertion left compares a table to
  itself. The `summary` parity table is the sole cross-engine signal in the whole workflow.
  So "three red, one green" does not mean the green one is right: dwh passed the
  intraday-unit bug purely because `delete+insert` on `[date]` can retract rows, while holding
  5,016 of the very same rows on the still-open date. Read a lone green leg as "this write path
  can retract", not as ground truth, and diff it against the others before believing it. (That
  strategy is gone — dwh now merges on the full grain like duckrun and spark, so no engine
  retracts and this particular asymmetry cannot recur.)
- **Query the lakehouses directly before instrumenting CI.** `duckrun.connect(<abfss Tables
  path>, read_only=True)` works from a laptop against any of the four items and answers
  schema/row/value questions in minutes. Several CI round trips were spent not doing this.
- **V-Order is set once, in the session, and only affects files written after it.** The spark
  target now carries `spark.sql.parquet.vorder.default: "true"` under `spark_config.conf` in
  `profiles.yml`; the adapter copies `conf` verbatim into both the singleton and the
  high-concurrency Livy payload (`concurrent_livy.py` `_build_acquire_payload`), so that is the
  only place it belongs — there is no model-level equivalent and no way to retrofit it. Parquet
  already on disk stays un-V-Ordered until the rows are rewritten, so an incremental leg flips
  over slowly: `stats.py`'s `vorder` column is the only honest report of where it actually got to,
  and `benchmark/README.md`'s snapshot table predates the change. A `·` on the other three is
  correct rather than a regression, but for two different reasons: delta-rs and DuckDB have no
  V-Order encoder at all, whereas Fabric Warehouse does — it is off by default on new warehouses
  and toggled at the warehouse level (`ALTER DATABASE`), not from anything in this repo.
- **`threads` on the spark target must stay ≤ 4.** dbt-fabricspark defaults to high concurrency
  and opens one Spark REPL per thread; Fabric packs at most five REPLs per Livy session, so more
  threads means a second Spark application, separately billed, for one `dbt run`.
- **Spark cannot read CSV with an explicit schema from a path in SQL.** `USING csv` exists only
  on `CREATE TEMPORARY VIEW`, and a temp view is unreachable from the persistent `__dbt_tmp`
  view the incremental path builds. It *is* reachable from the bare `CREATE TABLE AS SELECT`
  that the first-build and `--full-refresh` paths use, which is why `fct_price`/`fct_scada`
  carry two different reads. See [LEARNINGS.md](LEARNINGS.md) for the routes already ruled out —
  `csv.\`path\``, external CSV tables, `read_files()`, Python models — so they don't get retried.
- Scripts writing to `$GITHUB_ENV` / `$GITHUB_STEP_SUMMARY` must keep stdout clean —
  diagnostics go to stderr, and library chatter gets fenced with `redirect_stdout(sys.stderr)`.

## CI etiquette

- Cancel superseded runs immediately (`gh run cancel <id>`) — spark and Fabric legs cost money.
- **Nothing runs on push. Both workflows are `workflow_dispatch` only.** Pushing to `main` used to
  trigger the four Fabric legs, which meant any code change — a script, a workflow file, a comment —
  spent paid capacity nobody asked for, and a batch of edits queued several such runs on the
  concurrency group. `paths-ignore` did not fix that: it is per-PUSH, not per-file, so a commit
  touching a doc *and* anything else still ran. Commit and push freely; start a build with
  `gh workflow run dbt` when you actually want one.
- Jobs no longer cancel the run when they fail, and no matrix is `fail-fast`. Every leg runs to
  its own conclusion, so `gh run view <id> --json jobs` reads straight: `failure` means that
  leg failed. Cancelling never saved the Fabric compute anyway — the notebook or Livy session
  keeps running workspace-side after the GitHub job dies — it only erased the evidence.
- `summary` has no `if: always()`. It compares all four engines side by side, so it runs only
  when every leg is green; a summary with holes in it reads as drift that isn't there.
- Every leg is `dbt build` — the engine tests its own output, in the same DAG walk that wrote it.
  This replaced a separate test job that graded all four items with one neutral duckrun reader.
  What was bought: a failure stops at the node that broke, and four jobs disappeared. What was
  paid: the two singular tests in `tests/` are DuckDB SQL gated to the duckdb targets, so dwh and
  spark run only `unique`/`not_null` on the two dimension keys. Read a green duckrun as "duckrun
  is self-consistent", never as "all four agree" — the mart parity table in `summary` is the only
  thing that compares engines to each other.
- **The suite is two singular tests and four generic ones, on purpose.** `fct_summary` is asserted
  for grain uniqueness and nothing else; `dim_duid`/`dim_calendar` keep `unique` + `not_null` on
  their keys, plus the whitespace check. Everything that read an upstream table or encoded an
  expectation about the source was deleted. A red CI leg now means a duplicate key, a null key or
  a padded DUID — nothing else. Anything subtler surfaces as a ⚠️ in the parity table or not at all.
- **The `heavy` tag is gone from the project** — nothing carries it, so `--exclude tag:heavy` was
  removed from every invocation. It was on the assertions that scanned `fct_summary` whole, and
  those were deleted; a selector matching zero nodes only emits a warning and misdescribes what
  ran. Do not re-add the flag without re-adding a tagged test. Related: never pass `--select` or
  `--exclude` to `dbt retry` — it rejects them and replays the selection from `run_results`, which
  is why `base` in `fabric_build.py` can be shared between `build` and `retry` only while it holds
  no selection flag.
- The DuckDB legs stop retrying once the only failures are data tests (`_only_tests_failed` in
  `fabric_build.py`). The retry ladder is for transient OneLake commit conflicts, which are a
  property of the write; a failed assertion is deterministic and would just re-scan on Fabric
  compute to reach the same verdict.
- `summary` runs `stats.py` and nothing else, over **every shared table** in pipeline order —
  the staging view, the four facts, then `dim_calendar`/`dim_duid`/`fct_summary`. It was briefly
  cut to the three mart tables on the argument that the facts are inputs whose rows are implied by
  the summary's; that was wrong in the one situation the dashboard exists for. When `fct_summary`
  disagrees across engines, the fact counts on the rows above it are what separate "an input
  differs" from "the summary logic differs", and a mart-only table shows the symptom while hiding
  the cause. Totals are unscoped again, so they cover anything an item holds beyond this list.
  `summary.py` (the four-engine test dashboard) is deleted — its input was the `rr-<engine>.json`
  artifacts the test matrix uploaded, and there is no test matrix.

## The query benchmark is a second workflow, and it only reads

`benchmark/` + `.github/workflows/benchmark.yml` ("Direct Lake benchmark") ask the question `ci.yml`
does not: the parity table says the four engines hold the *same rows*, this measures how long Power BI
takes to **query** them. Ported from `djouallah/duckrun`'s `parquet_layout.yml`.
[benchmark/README.md](benchmark/README.md) has the detail; what matters when touching this repo:

- **`workflow_dispatch` only, and nothing depends on it.** Never triggered by a push, never a gate.
  Do not add `schedule`, `push`, `workflow_run`, `repository_dispatch` or a `needs:` from another
  workflow — not even a nightly, not even behind an `if:`. This is a standing instruction, not a
  default to be weighed against convenience. The benchmark's dehydrate→query cycles are
  **interactive CU** on shared Fabric capacity, which is the class of usage a capacity admin sees
  and asks about; a run nobody chose to start is the one that causes trouble. A human dispatches
  it, or it does not run.
- **The dispatch defaults are tuned for capacity cost, not statistical strength**: `cold_repeats=1`,
  `runs=3`, `gap_seconds=600`. That is one measured sample per query per tier, so both spreads are
  0 and `render_summary`'s >25%-cold-spread noise filter flags nothing —
  a default run is a smoke test with timings, not a defensible ranking. `cold_repeats=3 runs=5` (the
  previous defaults) is what a quotable result costs; raise them per dispatch, don't raise the
  defaults back.
- **Deploy models, run queries, report timings — that is the whole scope.** Upstream had to *build*
  the layouts it compared; here the four engines' own `mart.fct_summary` already are four layouts, at
  row-count parity. So there is no build phase, and deliberately **no stats phase either**: physical
  layout is `stats.py`'s job in `summary`, and re-deriving it here would be a second, slower reader of
  the same Delta logs. The only endpoints touched are the Fabric control plane and XMLA. Keep it that
  way — the moment this writes a table into a lakehouse, `stats.py`'s unscoped `get_stats()` starts
  counting it and the parity dashboard reads it as drift.
- **It shares `ci.yml`'s concurrency group (`onelake-<ref>`) deliberately.** Not for correctness, but
  because a concurrent dbt build contends for the same capacity, and capacity contention is the one
  thing a wall-clock benchmark cannot absorb. So a benchmark dispatch queues behind a `dbt` dispatch
  rather than racing it. Do not give it its own group to make it start sooner.
- **All four engines are Direct Lake, `dwh` included, and there is ONE `.bim`.** Requires
  duckrun ≥ 0.4.36 — the benchmark job pins that floor, above the repo's dbt floor of 0.4.35.
  `deploy()` now takes two independent knobs and `benchmark/engines.py` decides both:
  `lakehouse=`/`warehouse=` (from `KIND`) says **which** item holds the tables, `mode=` (from `MODE`,
  via `DEPLOY_MODE`) says **how** it is read. `mode="direct_lake"` rewrites every table to an entity
  partition over the item's OneLake root and stamps `directLakeBehavior: directLakeOnly`, so a query
  Direct Lake cannot serve **fails** instead of silently falling back to DirectQuery and logging a
  pushdown time that reads as a bad layout.
  **This reverses what used to be written here.** `dwh` was DirectQuery because before `mode=` a
  warehouse item could only be read that way, which forced a second hand-authored
  `fct_summary_dq.SemanticModel` kept in lockstep with the first, and made the dwh leg hot-only, gave
  it no reframe at deploy, and scoped it out of every COLD table. A warehouse's `Tables` are Delta in
  OneLake like any other item's — `stats.py` has always read them that way — so none of that was ever
  about the storage. The second template is **deleted**: `mode="direct_query"` on the remaining bim
  reproduces it exactly.
  The DirectQuery machinery all survives and is driven by `MODE`, never by engine name — the hot-only
  degradation in `bench_model`, the cold-tier scoping in `render_report._totals`, the
  `_(DirectQuery)_` verdict tag. Flipping an engine back is a one-line change in `MODE`, and
  `test_templates.py` pins the current setting so that stays deliberate. What the default gives up:
  the only DirectQuery data point, i.e. "is Direct Lake over the warehouse faster than DirectQuery
  over it?" — flip `MODE["dwh"]` for one run to ask it.
  If anyone reintroduces a hand-authored DirectQuery bim rather than using `mode=`, the old trap is
  still live: such a file must contain neither the camelCase Direct-Lake mode token nor a
  `onelake.dfs` URL **anywhere, prose included** — `_is_directlake_bim()` greps the raw bytes, so a
  `description` string naming the mode flips the model and makes deploy attempt a reframe it cannot
  serve. That mistake was made, and caught by a test, once already.
- **The models carry every shared table, not just the mart three** — the same eight `stats.py`
  reports on, in the schemas dbt writes them to (`mart` for `fct_summary`/`dim_duid`/`dim_calendar`,
  `landing` for the raw facts and the archive log), with one `raw`-tier query per raw table so none of
  them is dead weight. Two invariants a test pins, both of which fail *silently* rather than loudly:
  the table set must match `stats.py`'s `TABLES` (add a model there and the benchmark stops covering
  it), and **only `fct_summary`'s relationships may set `relyOnReferentialIntegrity`** — that flag
  permits an inner join, and the raw facts genuinely carry DUIDs missing from `dim_duid` (that is what
  `duid_probe` exists for), so asserting it there would make the benchmark measure fewer rows on the
  tables it is comparing. The wide facts are a deliberate column subset; `fct_price` alone has ~130.
- **The fastest engine wins a row, by any margin — there is no tie band, and do not re-add one.**
  A per-query gap inside the measured spread used to be called a tie. In the side-by-side table
  `best` was computed best-vs-*second*-best, so on a four-engine run iceberg beating spark by 2ms
  printed `tie` on a row where dwh was 4× slower than both — and every row read `tie`, i.e. "all
  four are equal", the opposite of what the row showed. `best` is now argmin, full stop. Spread is
  still measured and reported per query; it just no longer decides who won.
  `render_summary.verify_verdicts` had always compared strictly, so the band was also a divergence
  between the verdict and its own fatal orientation guard. One thing that survives and still
  surprises: the aggregate verdict follows the **summed totals**, not the win count, so
  "duckrun 1.00× faster (duckrun wins 5, spark wins 14)" is possible and correct — it lost most
  queries and won the expensive one.
- **The reference engine is `BENCH_ENGINES[0]`, explicitly.** Upstream picked the base by name
  (`endswith('_auto_sort')`, else the shortest). With one model per engine the shortest name is
  `aemo_dwh`, so inheriting that heuristic would silently make the DirectQuery leg the thing every
  ratio is measured against. `test_verdicts.py` pins this.
- **`benchmark/`'s pytest suite is the only CI check in this repo that touches no Fabric.** It is a
  `needs:` gate on the paid job. Run it before pushing anything under `benchmark/`:
  `python -m pytest benchmark/ -q`. Everything `test_templates.py` asserts would otherwise fail at
  *deploy* time, after ADOMD.NET is installed and the workspace resolved. The render layer is pure
  JSON → markdown, so a past run's `run-report` artifact re-renders offline with
  `RUN_REPORT=<file> python benchmark/render_report.py` — no credentials.
- Scout with `engines=duckrun,spark runs=1 cold=false cold_repeats=1 gap_seconds=0` before spending a
  full run: it exercises deploy → XMLA → render end to end in minutes rather than an hour of capacity.
