# Working in this repo

One dbt project, four engines (`duckrun`, `iceberg`, `dwh`, `spark`), one landed copy of the
data. The thesis is *the engine doesn't matter, the output does* — so the models are written
per dialect (`models/duckdb`, `models/dwh`, `models/spark`, gated by `+enabled` in
`dbt_project.yml`) and one neutral DuckDB suite grades all four outputs by reading them through
Delta on OneLake.

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

Every fact model is **insert-only** where the adapter can express it: the data is append-only,
so a matched row never needs updating.

| target | strategy | why not something else |
|---|---|---|
| `duckrun` | `insert` on the facts, `merge` on `fct_summary` | Since **0.4.34** `insert` is *not* a delta-rs merge: the adapter anti-joins the batch against the target's key columns **in DuckDB** and commits a plain append (add actions only, nothing rewritten), so cost tracks the batch instead of the target's partition span, and the append is **always** fenced to the version the anti-join read — no dependence on the `reads_self` heuristic. Measured 0.9s/+84MB against 6.7s/+8,397MB for the delta-rs equivalent on a 20M-row table. Not `merge` + `when_matched do_nothing`: duckrun's clause translator accepts only `update`/`delete` and **raises** on `do_nothing` (`_specs_from_merge_clauses`) — the one thing keeping duckrun and iceberg from sharing a single config, filed as a feature request. Not `delete+insert`: in this adapter that is a fenced **full-table overwrite** — every surviving target row plus the batch into a DuckDB temp table, then overwrite. On a 143M-row table that is a full rewrite *every run*. |
| `iceberg` | `merge` + `when_matched: do_nothing` | The OneLake Iceberg REST catalog rejects a matched-UPDATE branch: `BadRequest 400`, one add-snapshot update per commit. Omitting `when_matched` is not the same as insert-only — dbt-duckdb defaults it to update-by-name and draws the 400. |
| `spark` | `merge` + `skip_matched_step=true` | dbt-fabricspark honours `skip_matched_step`, which omits the WHEN MATCHED branch entirely — genuinely insert-only, and it cannot hit a multiple-source-row match error because there is no matched clause. Requires `file_format='delta'`. `merge` and `append` take the identical path in that materialization (persistent `__dbt_tmp` view, then one DML), so switching strategy does not disturb the CSV read. |
| `dwh` | `merge` on the facts, `delete+insert` on `fct_summary` | Insert-only is **not expressible** here: dbt-fabric merge is `default__get_merge_sql`, which always emits `WHEN MATCHED THEN UPDATE SET <every column>` (`merge_update_columns=[]` is falsy and falls through to all columns). For append-only data that branch is a semantic no-op — a matched row is rewritten with its own values — so it is correct, just not free. If the leg gets slow, fall back to `delete+insert` on `unique_key=['[file]']`. Bracket every key column: dbt interpolates them raw into the ON clause and `file`/`date` are reserved words. Never `--full-refresh` here: on dbt-fabric that DROPs and recreates, which deadlocks Fabric's background stats maintenance, loses grants, and rebinds Direct Lake. Use `REBUILD_SUMMARY=1` instead. |

Concurrency is not equal across the four. duckrun, iceberg and spark check the commit, so a real
overlap **fails loudly** instead of duplicating. Fabric Warehouse does not: under snapshot
isolation two transactions overlapping in time can still both insert. Merge shrinks that window
from *[compile-time file list → write]*, which is unbounded, down to the transaction overlap, and
T-SQL offers nothing stronger without application locks Fabric DW lacks.
`assert_fct_price_grain` / `assert_fct_scada_grain` are the detector for the remainder — both are
scoped to a rolling 30-day window and deliberately **not** tagged `heavy`, because the CI test job
runs `--exclude tag:heavy` and a tagged tripwire would never fire.

Before changing a strategy, read the adapter's own source rather than assuming the name means
what it does elsewhere. duckrun's lives in `dbt/adapters/duckrun/delta_plugin.py`; the Fabric ones
in `dbt/include/fabric{,spark}/macros/materializations/models/incremental/`.

### A keyed write reads the target, and on duckrun that means the table must be PARTITIONED

Moving the facts off `append` made every run scan the target looking for key collisions. The
other three engines absorbed it (dwh 48s, iceberg 49s, spark 95s on `fct_scada`). duckrun did
not, twice: first a OneLake GET that sat 212s and failed, then — after adding a pruning
predicate — a run that died mid-merge with **no dbt error at all**, just a leaked-semaphore
warning. No error line means the process was killed, not a query that failed. `fct_scada` is
369,205,022 rows and a **delta-rs merge** is memory bound: it plans a join against the whole
pinned target and its join state is not fully spillable. duckrun 0.4.34's `insert` sidesteps that
entirely — the anti-join runs in DuckDB and spills like any other query — but the target read is
still a read, so the partitioning below is what keeps it bounded rather than optional.

**The fix is `partition_by`, not a cleverer predicate.** Both duckrun AEMO reference models
(`tests/integration_tests/aemo/models/marts/fct_{price,scada}.sql`) carry:

```jinja
partition_by=['month_key'],
incremental_predicates=['target.month_key = source.month_key'],
```

with `month_key = YEAR*100 + MONTH` in the SELECT. All four `models/duckdb/marts/fct_*` models
now do the same on the duckrun branch.

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
  scanned 60/60; only a *literal* filter reached 0/60. That is why the **iceberg** leg needs
  `macros/pending_file_predicate.sql`. It is **not** true of duckrun's `insert`: `engine.probe_filters`
  reads the batch and folds *literal* values into the probe — an exact `"month_key" IN (202601, …)`
  for a **declared** partition equality, min/max bounds for every other join key (so `file` gets its
  range for free). Same predicate text, opposite outcome, because a different component computes
  the literals. Declaring the equality is required: without it duckrun will not prune that column,
  since only a declared equality makes the filter result-neutral.

`macros/pending_file_predicate.sql` is the literal-value version, built from the pending file
names known at compile time (`IN (...)` up to 200 files, else `BETWEEN min AND max`). It now
serves the **iceberg** branch only. Because `file` leads every fact's `unique_key` the predicate
is *implied* by the merge ON clause, so it removes no match the key would have made; on a
deliberate duplicate it still scanned the 1 file that could collide and inserted 0 rows.

Write that one with dbt's `DBT_INTERNAL_DEST` alias, never `target.` — dbt-duckdb builds its ON
clause with `DBT_INTERNAL_DEST` and knows no `target` alias, so `target.file` fails the iceberg
leg outright. duckrun is the opposite: its own aliases *are* `target`/`source`, which is why the
partition predicate above is written that way and is duckrun-only.

## `fct_summary` must be a pure function of its inputs

It once held three different row counts across four engines while every input table was in
exact parity. Cause: the incremental source only ever offered dates missing *entirely*, so a
date that existed but was incomplete could never be repaired by any write strategy — each
engine's run history got fossilized into its table.

Rules that keep it honest:

- The incremental source emits the **complete recomputation** for every date that could still
  be stale — never a partial top-up.
- The stale set is: dates absent from the target, plus a **trailing 7-day window**, plus dates
  still in the intraday feed. The window is not "the newest daily date": if a run is missed,
  two daily files land at once and the older one's craters would be unreachable.
- **The rebuild window must be ≥ the window `assert_fct_summary_matches_recomputation`
  checks.** A test that inspects a date the model may not repair holds CI red until someone
  runs `--full-refresh` by hand. Widen both together or neither.
- **Both branches must cover the same unit universe.** The daily branch reads `fct_scada`
  (DISPATCH_UNIT_SOLUTION, 644 DUIDs); the intraday branch reads `fct_scada_today`
  (DISPATCH_UNIT_SCADA, 406). 28 non-scheduled units appear only in the second — zero rows in
  `fct_scada` across all 369M, ever. Ungated, the intraday branch wrote them, and when the date
  crossed the daily horizon nothing could reproduce them: 11,540 permanent orphans, re-firing
  daily. The intraday branch is therefore gated on a `dispatch_duids` CTE, and
  `assert_fct_summary_matches_recomputation` applies the **identical** filter — the two change
  together or the test fails by construction. Keep that set **unbounded**
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
  `assert_duid_has_no_whitespace` guards it; any new string key crossing engines needs the same.
- **The neutral reader cannot grade another engine's rounding.** `DOUBLE → DECIMAL` tie-breaking
  differs per dialect — Spark HALF_UP, DuckDB HALF_EVEN, T-SQL a third — so a test asserting a
  DuckDB recomputation *exactly* equals a stored value can only ever pass for `duckrun`. The
  symptom is ±0.0001 on a few hundred rows and no row-count difference at all. Row counts are
  dialect-independent; assert those exactly and give the sums a tolerance. See
  [LEARNINGS.md](LEARNINGS.md).
- **A green engine is not a reference, and the tests are not cross-engine.**
  `assert_fct_summary_matches_recomputation` recomputes from *the same item's* inputs and diffs
  against *that item's* stored table — it asserts self-consistency, never agreement with another
  engine. So "three red, one green" does not mean the green one is right: dwh passed the
  intraday-unit bug purely because `delete+insert` on `[date]` can retract rows, while holding
  5,016 of the very same rows on the still-open date. Read a lone green leg as "this write path
  can retract", not as ground truth, and diff it against the others before believing it.
- **Query the lakehouses directly before instrumenting CI.** `duckrun.connect(<abfss Tables
  path>, read_only=True)` works from a laptop against any of the four items and answers
  schema/row/value questions in minutes. Several CI round trips were spent not doing this.
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
- Pushing to `main` triggers a run. If you want a dispatch with inputs instead, cancel the push
  run first; the concurrency group is not `cancel-in-progress`.
- Jobs no longer cancel the run when they fail, and no matrix is `fail-fast`. Every leg runs to
  its own conclusion, so `gh run view <id> --json jobs` reads straight: `failure` means that
  leg failed. Cancelling never saved the Fabric compute anyway — the notebook or Livy session
  keeps running workspace-side after the GitHub job dies — it only erased the evidence.
- `summary` has no `if: always()`. It compares all four engines side by side, so it runs only
  when every leg is green; a summary with holes in it reads as drift that isn't there.
- Build jobs never run tests — the engine must not grade its own homework. Testing is a
  separate job with one neutral reader.
