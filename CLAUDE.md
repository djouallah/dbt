# Working in this repo

One dbt project, four engines (`duckrun`, `iceberg`, `dwh`, `spark`), one landed copy of the
data. The thesis is *the engine doesn't matter, the output does* — so the models are written
per dialect (`models/duckdb`, `models/dwh`, `models/spark`, gated by `+enabled` in
`dbt_project.yml`) and every leg runs `dbt build`, so each engine writes and tests its own output
in one DAG walk. `stats.py` reads all four items through Delta on OneLake and puts every shared table
side by side — it is the only cross-engine check there is, and it is **no longer part of the build**:
it is the dispatch-only `Table layout` workflow, because it costs ~10 minutes to report something that
only changes when the tables are rewritten.

**The test suite covers the mart and nothing else** — `fct_summary`, `dim_duid`, `dim_calendar`.
The facts and the staging view carry descriptions, no assertions: the grain and
files-processed tests over `fct_price`/`fct_scada` were deleted deliberately, so an input defect
is now only visible where it surfaces in the summary. Adding a test on a fact model is a reversal
of that decision, not an oversight being corrected.

**And `tests/` is written per dialect, exactly like `models/`** — `tests/duckdb`, `tests/dwh`,
`tests/spark`, each holding the same two singular tests in its own SQL, with `data_tests` in
`dbt_project.yml` enabling one folder per target. All four engines therefore run the same six
assertions (two singular, four generic) against the output they just wrote.

**Put the gate on the folder key, never on `aemo_electricity`.** This was a live bug: a generic
test declared in `models/_*.yml` gets fqn `['aemo_electricity', '<test_name>']` — no folder
segment, because the patch files sit at the root of `models/` — so a project-level `+enabled`
matches it too. `data_tests: aemo_electricity: +enabled: "{{ target.type in ['duckrun','duckdb'] }}"`
therefore disabled the four `unique`/`not_null` tests along with the DuckDB-SQL singular ones, and
**dwh and spark ran zero tests** for as long as it stood — while this file and `ci.yml` both said
the generic ones still applied. `dbt build --target dwh` was `dbt run` wearing a hat. Check a
gating change with `dbt parse --target <name>` and read the manifest's `disabled` block; the
adapters all install locally and parse needs no credentials, so it costs seconds and no capacity.

A cross-engine reader still exists if a determinism question comes up that the in-leg tests cannot
answer — point `duckrun.connect(<abfss Tables path>, read_only=True)` at any of the four items from
a laptop. It is no longer the only way to grade dwh and spark.

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

For the **T-SQL and Spark dialects** there is one offline check short of CI: `sqlglot.parse_one(sql,
dialect='tsql'|'spark')`. It is a parser, not an engine — it will not tell you that `LEN()` ignores
trailing spaces or that Fabric DW lacks a function — but it catches the syntax class of error, and
it is the only thing that does without spending capacity. Worth running over the `tests/dwh` and
`tests/spark` bodies wrapped in their adapter's own test wrapper: both wrappers put the test SQL
inside a subquery on its own line (`fabric__get_test_sql` a CTE, `fabricspark__get_test_sql` a
`from ( … ) dbt_internal_test`), so a leading `--` comment block is safe there — unlike a **view**
model on dwh, which dbt-fabric wraps in `EXEC('create view … as <sql>')` and where the same comment
would swallow the SELECT.

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
- **It now covers dwh** — the one engine whose write path can actually duplicate, under snapshot
  isolation without a commit check, and therefore the one that most needed it. `tests/dwh/`
  carries the T-SQL spelling and the dwh leg runs it in the same `dbt build` that wrote the table,
  so a re-dispatch or a `dbt retry` racing a scheduled run is caught in the leg rather than by
  someone remembering to check afterwards. It was unreachable there until the folder-key gating
  fix; the by-hand recipe (`duckrun.connect(<dbt_dwh Tables path>, read_only=True)` plus the test
  body) still works and is now a debugging affordance, not the only coverage.
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
  scaled to 9 under load). A Fabric **Environment** is the one lever that was proven to override
  compute — its `dynamicExecutorAllocation` was accepted at 4-9 even with the workspace's
  `pool.customizeComputeEnabled` set to `false`, so that flag does not gate environment-level
  compute despite its name — but nothing here uses an environment any more; see the
  tried-and-reverted note below before reaching for one.
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
  `assert_duid_has_no_whitespace` guards it, and it now exists in all three dialects — which
  matters, because the padding is a *dwh* pathology and the guard used to sit only on the two
  engines that cannot exhibit it. The T-SQL copy has two spellings that are load-bearing rather
  than stylistic: it matches with `LIKE '%[<tab><lf><cr><space>]%'` because LIKE does **not** pad
  (`DUID <> LTRIM(RTRIM(DUID))` is a comparison, comparisons pad, so it is always FALSE for exactly
  the trailing-space case), and it reports `DATALENGTH`, because `LEN()` ignores trailing spaces and
  would print the padded and clean values as the same length. Any new string key crossing engines
  needs the same three copies.
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
- **Set the resource profile from `spark_config.conf`, not the individual key. Measured.** Three
  probe runs on 2026-07-31 read the effective SQLConf from inside the REPLs dbt was actually using
  (`SET <key>` in an `on-run-start` hook and a model `pre_hook`, so master and a packed worker
  both reported). Master and worker agreed in every run:

  | run | `resourceProfile` | `vorder.default` | canary |
  |---|---|---|---|
  | 30599066885 / 30599860363 — profile not set | `writeHeavy` | **`false`** (conf asks `"true"`) | `alive` |
  | 30600482604 — `resourceProfile: readHeavyForPBI` in conf | `readHeavyForPBI` | **`true`** | `alive` |

  Two things this pins down. **Delivery was never the problem** — the canary is a made-up key no
  profile defines and it arrives intact on both REPLs, in every run. And the **resource profile
  outranks individual keys**: `writeHeavy` defines `spark.sql.parquet.vorder.default = false` and
  is applied after the session conf, so it clobbered that key while leaving the canary alone.
  Asking for the *profile* instead binds, and V-Order follows it.
  **`profiles.yml` therefore sets the profile and nothing else** — the explicit
  `spark.sql.parquet.vorder.default` was removed, because on its own it did nothing and alongside
  the profile it made the two indistinguishable. The open question that arrangement answers is
  whether `readHeavyForPBI` carries `vorder=true` unaided; it should, since the profile defines the
  key. If a run ever reads `readHeavyForPBI` with `vorder=false`, put the key back *below* the
  profile line and record it. Do not re-add it "to be safe" — that is what hid the mechanism.
  Two dead hypotheses, recorded so they are not retried: the adapter is not dropping the conf, and
  REPL packing is not either (the canary reads `alive` on the worker, which is a packed acquire).
  Two earlier claims in this file were wrong and are retracted: that the conf was "inert / has
  never been in force" (delivery works; only the one key was losing), and that the conf was "the
  only thing switching V-Order on at all" (it switched nothing on until the profile changed).
  **Cost, and it is not free:** `readHeavyForPBI` changes write layout for the whole spark leg, not
  just V-Order. Judge it in the *Table layout* workflow before treating it as settled — and note
  this is the same profile the reverted Fabric Environment was built to get, now obtained with one
  line in `profiles.yml` and no environment, so no starter-pool penalty.
  To re-measure, re-add the probe: `git show df1e5ec -- macros/probe_spark_conf.sql`.
  The old `add.tags {"VORDER": "true"}` observation — small writes tagged, `mart/fct_summary`
  `tags: {}` on all 19 files — was read as "V-Order works but leaks on the large write, size cutoff
  unexplained". That reading assumed the key was in force, and it was not, so there is no cutoff to
  explain and the tags on the small writes came from something else. Whether the large write now
  V-Orders under `readHeavyForPBI` is **open and worth checking**, since it is the only table
  `benchmark/` queries: read `_delta_log/*.json` for `"VORDER": "true"` in the `add` actions.
  `stats.py`'s `vorder` column cannot answer it — see the bullet below on why.
  **Do not read the Spark UI Environment tab to check any of this.** It renders the SparkContext
  conf captured at application launch and never shows a `spark.sql.*` value applied afterwards to a
  SparkSession, so it cannot distinguish "dropped" from "live but invisible" — it was the instrument
  that made this look like an adapter bug for three runs. The in-session `SET <key>` read is the
  only authoritative one.
  Levers if the profile ever stops being enough: `+tblproperties:
  {delta.parquet.vorder.enabled: "true"}` — the docs say `INSERT`/`UPDATE`/`MERGE` honour it,
  dbt-fabricspark emits it from `create_table_as`, and it is also the key `stats.py` reads — or
  `OPTIMIZE … VORDER` as a post_hook on `fct_summary` alone. A third: re-assert the conf per REPL
  with a `SET` statement, which runs after the profile is applied and therefore wins.
- **The adapter is not what drops it — do not go looking there again.** `credentials.py:65` holds
  `spark_config` untouched, `__post_init__` (`credentials.py:203-207`) only asserts `name` is
  present, and `concurrent_livy.py:195-228` copies `conf` verbatim into the
  `POST …/highConcurrencySessions` body, where `conf` is a **documented** field of
  `HighConcurrencySessionRequest`, and the canary proves it arrives. The adapter's real defects are
  about *observability*, not delivery, and they are what made this take three runs to work out: the
  acquire payload is never logged (`concurrent_livy.py:136` logs only the `sessionTag`) and
  `spark_config` is excluded from `_connection_keys()`, so nothing short of reading the adapter
  source tells you what was sent; and non-whitelisted `spark_config` keys are dropped silently in
  the HC path (`concurrent_livy.py:200-219`) while the singleton path forwards the whole dict
  verbatim. Filed as
  [dbt-fabricspark#257](https://github.com/microsoft/dbt-fabricspark/issues/257).
- **REPL packing does not strip `conf` — hypothesis tested and dead.** `high_concurrency` defaults
  to **True** and `threads: 4` fires **five** acquires under one `sessionTag` (4 workers + dbt's
  master connection — [dbt-fabricspark#242](https://github.com/microsoft/dbt-fabricspark/issues/242)),
  exactly Fabric's 5-REPL cap, and acquires 2..5 are packed into the application the first created.
  That much is real. It is **not** a conf-delivery problem: the canary reads `alive` on the worker
  as well as the master. Do not resurrect this explanation.
- **[dbt-fabricspark#243](https://github.com/microsoft/dbt-fabricspark/issues/243) was closed on a
  false positive, by us.** It concluded `spark.sql.parquet.vorder.default: "true"` "seems to be
  working", on the strength of the small-write VORDER tags. The key reads `false` in-session on
  every REPL. Treat that issue's resolution as retracted — but note the correct reason is
  resource-profile precedence, not the adapter, and #257 has been retitled accordingly.
- **The V-Order key that is deprecated is spelled differently from the one in use.** Three
  near-identical spellings, one dead: `spark.sql.parquet.vorder.enable` was **removed in runtime
  1.3+**; `spark.sql.parquet.vorder.default` is the live session conf and is what `profiles.yml`
  sets; `delta.parquet.vorder.enabled` is a `TBLPROPERTIES` key and not a session conf at all.
  Community claims that "V-Order config is deprecated" trace back to the first spelling. Check
  which one a source is quoting before acting on it.
- **`stats.py`'s `vorder` column cannot see spark's V-Order, and never could.** It comes from
  duckrun's `get_stats()`, which reads the **table property** `delta.parquet.vorder.enabled` off
  `dt.metadata().configuration` (`dbt/adapters/duckrun/engine.py:909-913`). Nothing in this repo
  or in Fabric's writer sets that property — spark records V-Order as a **per-file `add.tags`
  entry**, and duckrun's own comment there notes `get_add_actions` does not surface tags. So that
  column reads `·` for spark whatever the files contain, and it is not evidence either way. The
  honest check is the Delta log: read `_delta_log/*.json` and look for `"VORDER": "true"` in the
  `add` actions. Two independent sources also warn the property and the file metadata are
  unrelated — either can be set without the other. Fixing the column means setting
  `delta.parquet.vorder.enabled` as a real table property (dbt-fabricspark honours
  `tblproperties`, but only through `create_table_as`, so existing tables need one
  `ALTER TABLE … SET TBLPROPERTIES`) or teaching duckrun's reader to read tags.
- **V-Order only affects files written after it, so an incremental leg flips over slowly.** There
  is no model-level equivalent and no way to retrofit it in place; `OPTIMIZE … VORDER` or a
  rewrite is what moves parquet already on disk. `benchmark/README.md`'s snapshot table predates
  all of this. A `·` on the other three engines is correct rather than a regression, but for two
  different reasons: delta-rs and DuckDB have no V-Order encoder at all, whereas Fabric Warehouse
  does — it is off by default on new warehouses and toggled at the warehouse level
  (`ALTER DATABASE`), not from anything in this repo.
- **A Fabric Environment was built for the V-Order problem and reverted. Nothing here uses one.**
  Not because it failed — it published fine and its `readHeavyForPBI` profile is the *documented*
  answer — but because **attaching one gives up the starter pool**. Microsoft's Livy docs say so in
  the line that carries the conf ("remove this line to use starter pools instead of an
  environment"), which means a cold on-demand cluster start on every run, the same penalty already
  recorded above for `session_idle_timeout`. A per-run startup cost to fix a write layout was the
  wrong trade for this repo. What the attempt established, so it does not have to be paid for
  twice:
  - **`spark.fabric.environmentDetails` is the reference key, NOT the adapter's `environmentId:`
    field.** `environmentId:` is a real dbt-fabricspark credential — documented in its README *and*
    CHANGELOG, with unit tests — that emits `spark.fabric.environment.id`, a conf key appearing
    **nowhere in Microsoft's Fabric documentation**. All three Livy API docs, and the adapter's own
    maintainer in [dbt-fabricspark#243](https://github.com/microsoft/dbt-fabricspark/issues/243),
    use `spark.fabric.environmentDetails: '{"id": "<guid>"}'` — a JSON string, not a bare guid.
    This repo spent a commit on `environmentId:` believing the adapter's docs. Treat it as a no-op
    that reads like working configuration: an unattached environment raises nothing, it just
    silently leaves `writeHeavy` in force.
  - A `sparkProperties` PATCH is a **merge, not a replace** — a key omitted from the body survives,
    so dropping one means sending it explicitly as `null`.
  - `runtimeVersion: "2.0"` (Spark 4.x, Delta Lake 4.x) **is accepted** by the environment API and
    publishes fine, so "cannot" is not the objection. The objection is that Microsoft advises
    against Delta 4.x table features on tables other workloads read, and `dbt_spark`'s tables are
    read by two — `stats.py` through delta-rs, `benchmark/` through Direct Lake. A protocol bump
    would break both and neither failure would name the runtime.
  - The Native Execution Engine (`spark.native.enabled`) was never enabled: an execution-side
    change with documented divergences (`round()`, `DECIMAL`→`FLOAT`) and no bearing on layout.
  - **A resource profile can also be set workspace-wide** (Workspace settings → Spark settings →
    "Optimize for your use case", workspace Admin only), or by making an environment the workspace
    default. Either would give the spark leg V-Order with **nothing at all in `profiles.yml`** —
    at the cost of changing behaviour for every notebook and job in the workspace, and of the
    setting living somewhere this repo cannot see or version.
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
- **The parity dashboard is not part of this workflow any more** — it is the dispatch-only
  `Table layout` workflow, so a green `dbt` run reports nothing about cross-engine agreement, and
  nobody sees drift until someone dispatches it. It used to be the `summary` job here, with no
  `if: always()`, so that a partial run could not publish a table with holes in it that reads as drift
  that isn't there. The same hazard now takes a different shape: dispatched by hand it can read four
  lakehouses mid-build. It shares `onelake-<ref>` so it queues behind a build rather than racing one,
  which covers the GitHub-side case but not a Fabric notebook still finishing after its job went
  green.
- Every leg is `dbt build` — the engine tests its own output, in the same DAG walk that wrote it.
  This replaced a separate test job that graded all four items with one neutral duckrun reader.
  What was bought: a failure stops at the node that broke, and four jobs disappeared. What was
  paid: the singular tests had to be written three times, once per dialect, and a *green* leg is
  still only a self-consistency statement. Read a green duckrun as "duckrun is self-consistent",
  never as "all four agree" — the mart parity table in `summary` is the only thing that compares
  engines to each other. (This bullet used to say dwh and spark ran `unique`/`not_null` only. They
  ran nothing at all; see the folder-key gating note at the top.)
- **The suite is two singular tests and four generic ones, on purpose — and all six now run on
  every engine.** `fct_summary` is asserted for grain uniqueness and nothing else;
  `dim_duid`/`dim_calendar` keep `unique` + `not_null` on their keys, plus the whitespace check.
  Everything that read an upstream table or encoded an expectation about the source was deleted.
  A red CI leg now means a duplicate key, a null key or a padded DUID — nothing else. Anything
  subtler surfaces as a ⚠️ in the parity table or not at all. Adding a test means adding it to all
  three dialect folders; one dialect only is a silent hole, because nothing reports which engine
  skipped what.
- **The grain check is now a full GROUP BY over `fct_summary` on four engines, not two.** That is
  the real cost of this coverage — one more scan of a 143M-row table per Fabric leg, per run, on
  paid capacity. It is worth it on dwh (the only engine that can genuinely duplicate) and cheap
  insurance on spark. If a leg's timing becomes the problem, the lever is the leg, not a date
  window on the test: a window would encode an assumption about *where* duplicates live, which is
  exactly the source knowledge this test is built to be free of.
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
- **The parity dashboard is its OWN workflow now (`Table layout`, `.github/workflows/table-layout.yml`),
  dispatch-only — because it costs ~10 minutes and reports something that barely moves.** It was the
  `summary` job of `ci.yml`. The iceberg item alone reads at 12m+ (386 files, 1,175 row groups over
  OneLake), which is why its timeout is 40 minutes, while what it reports — files, row groups, size,
  v-order — only changes when the tables are **rewritten**, and the facts are append-only
  incrementals. Every `dbt` dispatch was buying a ten-minute re-read of numbers that had not moved
  since the previous one. Two things fell out of the split: it can be asked *without* spending four
  Fabric legs first, and its result became **reusable** — `stats.py` also writes `STATS_JSON`, the
  workflow uploads it as the `stats` artifact, and `cu/` downloads it to print the layout beside the CU.
  A cached reading is sound *because* the layout is near-static; that is the same property that made
  running it per build wasteful. Hold onto the costs: no test compares one engine to another, so
  **nothing notices drift until someone dispatches this**; and being manual it can be fired mid-build,
  reading half-written tables and reporting drift that is a build in flight — which is why it shares
  `onelake-<ref>` with `ci.yml` so a dispatch queues rather than races. The JSON is a data contract with
  `cu/`: renaming a `DETAIL_KEYS` entry makes the layout table disappear over there with a note, not an
  error, so change both together.
- It runs `stats.py` and nothing else, over **every shared table** in pipeline order —
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
  layout is `stats.py`'s job in the *Table layout* workflow, and re-deriving it here would be a second, slower reader of
  the same Delta logs. The only endpoints touched are the Fabric control plane and XMLA. Keep it that
  way — the moment this writes a table into a lakehouse, `stats.py`'s unscoped `get_stats()` starts
  counting it and the parity dashboard reads it as drift.
- **The paid work is a matrix, one job per engine, `max-parallel: 1` — and the reason is the token,
  not the parallelism.** A Fabric/XMLA token lives about an hour; one job over four models, 21
  queries and two 600s gaps runs past that and the expiry lands mid-measurement on the last engine.
  Each job mints its own and retires it with the job. Consequences to hold onto: nothing computes a
  ratio during the measurement any more — each job uploads a report **fragment** and the free
  `report` job merges (`merge_reports.py`, **basename order**, meta fragment named to sort first so a
  per-engine fragment cannot overwrite the shared `run` block) and renders; and each job resolves the
  hot-only ladder's DUID itself, which is recorded per model and warned about on disagreement rather
  than assumed. Do not collapse it back into one job to "save runner minutes" — the runner is free and
  the capacity is not. **`xmla_compare.py` now refuses more than one engine outright.** It used to
  fall back to an in-process walk of every model, for running this from a laptop; that path is deleted
  — the laptop is not a supported way to spend this capacity, and a second orchestration shape kept
  alive to serve it meant two implementations answering the same question. `dbt`-style scouting is
  still a dispatch, just with `engines=duckrun,spark runs=1 cold=false`.
- **It shares `ci.yml`'s concurrency group (`onelake-<ref>`) deliberately.** Not for correctness, but
  because a concurrent dbt build contends for the same capacity, and capacity contention is the one
  thing a wall-clock benchmark cannot absorb. So a benchmark dispatch queues behind a `dbt` dispatch
  rather than racing it. Do not give it its own group to make it start sooner.
- **The test is: identical DAX, identical semantic models, four dbt adapters.** The adapter that
  wrote the parquet is the only variable, so everything above it is held constant — ONE `.bim`, ONE
  storage mode, one query suite. `deploy()` takes exactly one per-engine argument,
  `lakehouse=`/`warehouse=` (from `engines.KIND`); `mode=` is `engines.DEPLOY_MODE`, a single constant
  `"direct_lake"`, and **there is deliberately no per-engine `MODE` dict** — `test_templates.py`
  asserts one has not crept back. Requires duckrun ≥ 0.4.36 (the benchmark job pins that floor, above
  the repo's dbt floor of 0.4.35). Direct Lake is what makes a timing an answer about layout, and
  `directLakeOnly` means a query it cannot serve fails instead of falling back to the SQL endpoint and
  logging a pushdown time.
  **This reverses what used to be written here** ("`dwh` is DirectQuery and that asymmetry is
  load-bearing"). The asymmetry was never about storage — a warehouse's `Tables` are Delta in OneLake,
  which is how `stats.py` has always read them — and the labelling that was supposed to make it safe
  did not work. Measured, from the last DirectQuery run: cold ÷ hot was 15.9× / 47.1× / 17.4× on the
  three Direct Lake engines and **0.96× on dwh**, because a DirectQuery model has nothing to evict, so
  its dehydrate is a **no-op that SUCCEEDS** rather than the failure the hot-only degradation watches
  for. Fifteen bogus "cold" samples were recorded, dwh entered the COLD totals, and the summary printed
  it the **cold winner** — 27,622 ms against duckrun's 63,437 — for never doing the work being measured.
  Two kinds of number in one table will find a way into one comparison; the fix is to not produce both.
  So: don't reintroduce a DirectQuery leg, and don't re-add a per-engine mode to make one possible. If
  a pushdown-vs-Direct-Lake question ever needs answering, it is a different experiment and belongs in
  its own run, not as a fourth column beside three layouts.
  If anyone hand-authors a DirectQuery bim anyway rather than passing `mode=`, the old trap is still
  live: such a file must contain neither the camelCase Direct-Lake mode token nor a `onelake.dfs` URL
  **anywhere, prose included** — `_is_directlake_bim()` greps the raw bytes, so a `description` string
  naming the mode flips the model and makes deploy attempt a reframe it cannot serve. That mistake was
  made, and caught by a test, once already.
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
  still measured and reported per query; it just no longer decides who won. One thing that survives
  and still surprises: the **rank follows the summed totals, not the win count**, so "spark fastest
  (5 query wins)" beside "duckrun 1.02× (14 query wins)" is possible and correct — duckrun won most
  queries and lost the expensive one. Both numbers are printed and neither is corrected against the
  other.
- **There is no reference engine and no baseline, and do not reintroduce one.** Upstream had a real
  one — it built a candidate layout and compared it against the existing one — and this repo inherited
  the shape: `BENCH_ENGINES[0]` was the reference and every ratio read `base ÷ challenger`. Here the
  four engines are **peers**, so a baseline made every number in the report depend on the order the
  dispatch happened to list the engines in, and made "iceberg 1.30× faster" unreadable without
  remembering which engine the reference was. Engines are now **ranked**, with `× fastest` stated
  against the fastest total of the metric — a property of the measurement, not of the input list.
  Consequences: `engines.reference()` and `BENCH_REFERENCE` are gone (a test asserts neither comes
  back); side-by-side column order is **alphabetical**, which is the only order that is both neutral
  between peers and stable enough to read two runs side by side; a failed engine is now just a missing
  column, named in the findings, instead of a run-invalidating event when it happened to be the
  reference; and `render_summary.verify_ranking` replaced `verify_verdicts` — a ratio orientation
  inversion is no longer expressible, so what it guards is that the printed ranking agrees with the
  totals it came from (ordered by total, rank 1 lowest, `× fastest` ≥ 1). Still fatal, same reason: a
  table naming the slower engine the winner is worse than no table. `BENCH_ENGINES` order now decides
  only the order the jobs RUN in — index 0 is simply the one that skips the idle gap.
- **`benchmark/`'s pytest suite is the only CI check in this repo that touches no Fabric.** It is a
  `needs:` gate on the paid job. Run it before pushing anything under `benchmark/`:
  `python -m pytest benchmark/ -q`. Everything `test_templates.py` asserts would otherwise fail at
  *deploy* time, after ADOMD.NET is installed and the workspace resolved. The render layer is pure
  JSON → markdown, so a past run's `run-report` artifact re-renders offline with
  `RUN_REPORT=<file> python benchmark/render_report.py` — no credentials.
- Scout with `engines=duckrun,spark runs=1 cold=false cold_repeats=1 gap_seconds=0` before spending a
  full run: it exercises deploy → XMLA → render end to end in minutes rather than an hour of capacity.

## `cu/` is a third workflow, and it shares nothing with the other two

`cu/` + `.github/workflows/cu.yml` ("Capacity CU") answer what the querying *cost*: CU per semantic
model, read from the Fabric Capacity Metrics app's own semantic model by DAX over the Power BI
`executeQueries` REST endpoint. Fabric exposes **no per-operation CU REST API** — that model is the
only authoritative source, which is why this exists at all. [cu/README.md](cu/README.md) has the
detail.

- **The isolation is the design, not an accident.** No imports from `benchmark/`, no
  `run_report.json`, no artifact, no `needs:`, no shared concurrency group, no ADOMD, no .NET, no
  duckrun — `requests` is the whole dependency list. It is speculative tooling, so it is built to be
  deleted by removing one directory and one workflow file. Do not "DRY it up" against
  `benchmark/xmla_compare.py`; the duplication is what keeps that deletion free.
- **It correlates nothing with a GitHub run, and that is still true after the per-run split.** It
  cannot say which *query* produced a number. It can now say which *run* and which *engine*, and
  neither needed coupling to `benchmark/`: an engine has its own semantic model, so it is already its
  own row, and a run is inferred as a **cluster of active hours** split on more than
  `CU_RUN_GAP_HOURS` (default 2) idle hours, identified by its own time window. `benchmark/` still
  records durations but no absolute timestamps, and adding them is the coupling this avoids.
- **There is no chart, and that was tried.** A per-hour bar chart of the same rows was built and removed: at the hour bucket a real run is two or three bars per engine, which reads as noise next to the numbers it was drawn from. The app's own chart is drawn at 30 seconds, and that resolution lives only in `'Timepoint Interactive Detail'` — one request per bucket, **120 per hour per capacity**, rows carrying no timestamp column (the MPARAMETER *is* the timestamp) so a batch could not be attributed even if the parameter took a list. Real capacity to redraw numbers already in hand, so it stays tables. If it comes up again: sum `Timepoint CU (s)`, never `Total CU (s)` — the smoothing-duplication trap below applies to that table too.
- **The per-run split costs zero extra requests, and the hour bucket is its hard floor.** The hour
  column was always projected — it has to be, or `since` cannot be verified to bind — and was simply
  discarded after that check; the split is post-processing of rows already in hand, so it is still one
  request per capacity. What it cannot do is separate two dispatches inside the same hour:
  `Metrics By Item Operation And Hour` is bucketed hourly, and the finer instrument is the timepoint
  detail table this deliberately does not use. Do not reach for that table to sharpen the split — the
  dedup trap below is why, and the benchmark's own inter-engine gaps already create the idle hours the
  clustering keys off. When everything lands in one cluster the report says so rather than printing a
  one-row "runs" table that repeats the aggregate.
- **Deduplication by operation id is load-bearing.** `'Timepoint Interactive Detail'` is gated by a
  single 30-second `TimePoint` MPARAMETER, so the window is walked one bucket at a time — but an
  interactive operation is smoothed across 10–128 buckets and **reappears in every one carrying its
  full `Total CU`**. Summing the rows multiplies each operation by the buckets it spans. A five-minute
  window over one operation reports 140 CU deduplicated and 1,540 summed. Anyone replacing
  `collect()`'s keyed dict with a `SUM` produces numbers wrong by one to two orders of magnitude that
  still look plausible.
- **Column names are version-pinned; nothing hardcodes them.** Microsoft's own fabric-toolbox
  accelerator ships four DAX variants (v53/v47/v40/v37) because the schema moves between app
  versions. `discover_columns()` reads the real schema with `INFO.VIEW.COLUMNS()` first and resolves
  each role from candidate lists — `REQUIRED` roles fail with the actual column list printed,
  `OPTIONAL` ones degrade to "not filtering on it". This caught a real miss on the first dispatch:
  the candidates said `Item Name`, the app says `Item`.
- **A deploy mints a NEW item GUID, and `'Items'` is a lagging snapshot — this made the whole report
  read empty.** The metrics tables hold item GUIDs; a semantic model that was just created (or deleted
  and recreated — `overwrite=True` keeps its id, a recreate does not) has a GUID `'Items'` has not seen
  yet, so it resolves to no name, fails the `CU_MODELS` name filter, and its CU vanishes while the
  report prints "No semantic model activity" — indistinguishable from an idle capacity. Names are now
  resolved **live** from `GET /groups/{ws}/datasets` first (one request, same host and token as
  `executeQueries`, so no new dependency), `'Items'` only as fallback. Do not "simplify" that back to
  the Items join alone. Related and equally load-bearing: an empty result now prints its own
  diagnosis — rows returned after the floor, which filter dropped them and how many, any item whose
  name matched but whose workspace did not (with the real id), and the top spenders it did see. A bare
  GUID in that last table IS this trap.
- **`'Timepoint Interactive Detail'[Item]` is a GUID, and the table has no name or kind column** —
  the real columns are `Item`, `Operation`, `Operation Id`, `Total CU (s)`, `Workspace Id`, `User`,
  `Billing type`, `Status`, `Duration (s)`, `Timepoint CU (s)`. So `discover_items()` joins to
  `'Items'` (`Item Id`, `Item name`, `Item kind`) and that join is load-bearing twice over: without
  it the report is GUIDs against CU, and it is the only route to a semantic-model filter. An id
  missing from `'Items'` is kept under its raw GUID — dropping it would lose CU silently.
- **One capacity per query.** The detail table is DirectQuery and resolves one data location per
  query, so `CapacitiesList` takes exactly one capacity; several fails with an opaque
  `Internal Error: Error obtaining data location` naming neither cause nor capacity. This tenant has
  two, so `usable_capacities()` probes each before spending ~60 requests on it. Casing was *not* the
  cause (both work uppercase), but the probe tries other spellings anyway — one request, and the
  parameter is undocumented.
- **The service principal works against that model.** The community consensus says it does not, and
  this was built expecting a 401 — measured otherwise on run 30536137179, which read it on the OIDC
  SP. The `PBI_TOKEN` secret path stays as a free fallback (the workflow prefers a secret when one
  is set); a user token expires in ~1 hour, so per-investigation, not a standing secret.
- **`workflow_dispatch` only, same standing rule as the benchmark.** No `schedule`, no `push`, no
  `workflow_run`, no `needs:` from another workflow. Interactive reads against shared capacity are
  what a capacity admin notices. Its **own** concurrency group though, not `onelake-<ref>` — it
  measures nothing timing-sensitive, and queueing it behind a five-hour benchmark would push it
  toward the app's 14-day retention edge for no benefit.
- 14-day retention, ~6 minute lag, 5–64 minute smoothing: dispatch it ~10 minutes *after* whatever
  you want to measure. And timepoints are stamped in the offset configured **in the app**, not
  yours — a wrong `utc_offset_hours` reads as "no activity" rather than an error.
