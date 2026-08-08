# Working in this repo

One dbt project, four engines (`duckrun`, `iceberg`, `dwh`, `spark`), one landed copy of the
data. The thesis is *the engine doesn't matter, the output does* — so the models are written
per dialect (`models/duckdb`, `models/dwh`, `models/spark`, gated by `+enabled` in
`dbt_project.yml`) and every leg runs `dbt build`, so each engine writes and tests its own output
in one DAG walk. `stats.py` reads all four items through Delta on OneLake and puts every shared table
side by side — it is the only cross-engine check there is, and it is **no longer part of the build**:
it is the dispatch-only `layout` job, because it costs ~10 minutes to report something that
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
**dwh and spark ran zero tests** for as long as it stood — while this file and `dbt.yml` both said
the generic ones still applied. `dbt build --target dwh` was `dbt run` wearing a hat. Check a
gating change with `dbt parse --target <name>` and read the manifest's `disabled` block; the
adapters all install locally and parse needs no credentials, so it costs seconds and no capacity.

A cross-engine reader still exists if a determinism question comes up that the in-leg tests cannot
answer — point `duckrun.connect(<abfss Tables path>, read_only=True)` at any of the four items from
a laptop. It is no longer the only way to grade dwh and spark.

The traps below have all been hit for real. Each one cost a CI run or worse.
[LEARNINGS.md](LEARNINGS.md) records the longer investigations behind some of them — measured
numbers, and the routes that were tried and did not work. **[TODO.md](TODO.md) is open work** —
things that need a decision or a dispatch, with the cost of each stated. Read it before proposing
one; the answer may already be there, along with why it has not been done.

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
gh run download <run-id> -R djouallah/fabric-dbt-benchmark -n dbt-target-dwh -D /tmp/t
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

**`month_key` is gone from ALL FOUR ENGINES now, not just the duckdb tree.** dwh kept it for a
while after the duckdb facts dropped it, and that was an oversight rather than a decision — Fabric
Warehouse has no partitioning to attach it to, so on dwh it was a plain computed column that
**nothing read**: no model, test, macro, `stats.py` or `model.bim`. It survived only because
**duckrun forces this cleanup and dbt-fabric does not** — duckrun refuses a batch missing a column
the target has, dbt-fabric's merge does not check, so nothing broke and it sat there costing two
extra `TRY_CAST` + `YEAR`/`MONTH` per row on 369M-row `fct_scada` and a stored column in every
parquet file, on one engine, in a benchmark comparing write cost. Dropping it needed
`on_schema_change='sync_all_columns'` on the two dwh facts: with dbt's default `ignore`,
`dest_columns` comes from the **existing** relation, so the merge would still select `[month_key]`
from a temp relation that no longer has it (*Invalid column name*). Do not reintroduce the column
on any engine — the paragraphs below are why it existed, not an argument for it.

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

- **All three trees end with `ORDER BY date`, and that is a FAIRNESS invariant, not a layout
  claim.** The sort reaches no stored table — this SQL is a merge *source* on every engine — so
  its only real effect is cost. It was on duckdb and spark and missing from dwh, i.e. two legs
  paying for something the third did not, in a benchmark that compares their cost. Parity could
  have been reached by deleting two lines instead of adding one; adding it was the call. Either
  way the rule is **all three or none** — dropping it from one tree is a fairness regression
  wearing the costume of a cleanup. On dwh it lands in the outer SELECT of a Fabric CTAS
  (dbt-fabric builds `CREATE TABLE <temp> AS <model sql>` and merges from that relation; it does
  **not** wrap the model in `MERGE … USING (<sql>)`), so the derived-table ORDER BY restriction
  does not apply and this is legal there.
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
  work against that catalog when issued directly. `fabric_build.py` used to fire the rebuild step
  for duckrun **and** iceberg from one flag, so `REBUILD_SUMMARY=1` broke the iceberg leg and left
  a `fct_summary__dbt_backup` behind — which is why the `rebuild_summary` workflow input and both
  CI rebuild steps were removed. `REBUILD_SUMMARY` / `--vars 'rebuild_summary: true'` survives
  only as a by-hand lever in the dwh model; CI's clean-table lever is the `teardown` — every
  dispatch starts from nothing because the previous one deleted its own output item.
  See [LEARNINGS.md](LEARNINGS.md).
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

The notebook's NAME is load-bearing and is not duckrun's default: `dbt-<engine>-<random>`. Fabric
bills this leg's compute against the notebook item, so the engine has to be in the name — and it
has to be in the *prefix*, because a deleted item's display name stays reserved for minutes and
`_execute_notebook` creates the item with no retry. See the `cu/` section.

**The notebook GUID comes from duckrun, not from a lookup here.** `ScriptResult.item_id`
(duckrun ≥ 0.4.38) names the throwaway notebook whether or not it still exists, and a run that died
before the payload ran carries the same id on the raised exception — so both outcomes are
attributable and `fabric_run.py` records it on either path. It used to pass `keep_notebook=True`,
re-list the workspace, match the display name and delete the item itself: two extra control-plane
calls reimplementing duckrun's own teardown, and a name lookup that fails silently. Do not
reintroduce that. One consequence of letting duckrun delete it: duckrun's teardown only *warns* on
a failed delete, so the record deliberately carries **no `deleted` timestamp** and the item is left
to `provision.py teardown`, which polls for a 404 and goes red if it is still listed.

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
- **`cores: 4` cannot build `fct_summary`, and that is a DuckDB limit rather than a setting.**
  Measured twice (runs 30867258967 and 30876056186), instrumented the second time. Everything is configured
  correctly — `temp_directory` on a 142 GiB disk with 135 GiB free, `max_temp_directory_size` ~121 GiB
  (five times the memory limit), `preserve_insertion_order` false — and spilling **does** engage:
  4.6 GiB written across 9-10 files. It still dies, because DuckDB spills REACTIVELY: `rss` goes
  0 → 25.6 GiB in thirty seconds and eviction only starts at the wall, with `mem_avail` down to
  1.4 GiB on a 31.4 GiB node, and only ~4.6 GiB of the 24.6 GiB working set is evictable at all —
  the rest is a 143M-group hash table that is resident by nature. Out-of-core execution is not
  robust for every plan shape and this is one of the shapes it is not. Do not hunt for a setting,
  do not blame duckrun's 85% memory pin (the same pin is fine at `cores: 8`), and do not read table
  size as the predictor: `fct_scada` builds on the same node at **370M rows, 2.5× bigger**, because
  it streams CSV batch by batch and never exceeds 2.4 GiB. The evidence lives in
  [LEARNINGS.md](LEARNINGS.md); the instrumentation that produced it — node facts plus a 15s
  `rss`/`spill` sampler in `fabric_build.py`, and `macros/log_duckdb_settings.sql` reading the
  settings back from the live session — is permanent, free, and touches no DuckDB connection.
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
  the profile it made the two indistinguishable.
  **The profile is no longer pinned: it is the `spark_resource_profile` dispatch input, defaulting
  to `writeHeavy`.** So a default run writes the plain workspace layout and **no V-Order** — the
  measurements below still hold, they just describe what you get by dispatching with
  `readHeavyForPBI` rather than what every run does. Microsoft's
  [resource profile reference](https://learn.microsoft.com/en-us/fabric/data-engineering/configure-resource-profile-configurations)
  confirms the key is redundant — it publishes each profile's exact config set:

  | profile | configs |
  |---|---|
  | `writeHeavy` (workspace default) | `vorder.default: "false"`, `optimizeWrite.enabled: "null"`, `binSize: "128"`, `optimizeWrite.partitioned.enabled: "true"` |
  | `readHeavyForPBI` | **`vorder.default: "true"`**, `optimizeWrite.enabled: "true"`, **`binSize: "1g"`** |
  | `readHeavyForSpark` | `optimizeWrite.enabled: "true"`, `optimizeWrite.partitioned.enabled: "true"`, `binSize: "128"` — **does not set vorder at all** |

  Two things to carry from that table. `readHeavyForPBI` is the *only* profile that enables
  V-Order, so `readHeavyForSpark` would be the wrong choice here despite the name — and note the
  V-Order page contradicts this, saying "switch to `readHeavyforSpark` or `ReadHeavy` … which
  automatically enable V-Order". The profile reference and our own in-session measurement agree
  with each other and against that sentence; trust them. Second, the profile also flips
  `optimizeWrite` on with a **1 GB bin size** against `writeHeavy`'s 128 MB, so it rewrites file
  layout far beyond V-Order — `fct_summary` at 19 files / 1.2 GB should collapse to very few files.
  Expect the next build's layout table to move a lot, and read it as the profile working, not drift.
  Two dead hypotheses, recorded so they are not retried: the adapter is not dropping the conf, and
  REPL packing is not either (the canary reads `alive` on the worker, which is a packed acquire).
  Two earlier claims in this file were wrong and are retracted: that the conf was "inert / has
  never been in force" (delivery works; only the one key was losing), and that the conf was "the
  only thing switching V-Order on at all" (it switched nothing on until the profile changed).
  **Cost, and it is not free:** `readHeavyForPBI` changes write layout for the whole spark leg, not
  just V-Order. Judge it in the `layout` job of the `dbt` workflow before treating it as settled — and note
  this is the same profile the reverted Fabric Environment was built to get, now obtained with one
  line in `profiles.yml` and no environment, so no starter-pool penalty.
  To re-measure, re-add the probe: `git show df1e5ec -- macros/probe_spark_conf.sql`.
  **The large-write hole is CLOSED.** Confirmed by inspecting the parquet after a rebuild under
  `readHeavyForPBI`: V-Order is present throughout, including `mart/fct_summary`. The old
  observation — small writes tagged `add.tags {"VORDER": "true"}`, `fct_summary` `tags: {}` on all
  19 files — was read as "V-Order works but leaks on the large write, size cutoff unexplained".
  That reading assumed the session key was in force. It was not, so there was never a cutoff to
  explain; the whole thing was one setting that had never taken effect, and the tags on the small
  writes came from something else. Do not go looking for a size threshold — there isn't one.
  To re-verify after any layout change, read `layout.ordering.<engine>.vorder_files` in the run
  record — the `_delta_log` `add.tags` read, which the `layout` job now does every run and prints in
  its own step-summary section. `stats.py`'s `vorder` column cannot answer it — see the bullet below
  on why, and on the two parquet metrics recorded beside it that say whether the rows moved.
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
- **RETRACTED: "`stats.py`'s `vorder` column cannot see spark's V-Order, and never could." IT SEES
  IT, AND IT ALWAYS DID.** Checked against every spark record in `history/`: the column reads `True`
  for all seven `readHeavyForPBI` runs and `False` for all three `writeHeavy` runs and the one
  `readHeavyForSpark` — 12 for 12, no exceptions. The independent `add.tags` read added in
  `layout.ordering` agrees with it on both sides (12/12 files tagged under the PBI profile, 0/13
  under `writeHeavy`), so two sources with nothing in common now confirm each other.
  The retracted claim reasoned from duckrun's source — `get_stats()` reads the **table property**
  `delta.parquet.vorder.enabled` off `dt.metadata().configuration`
  (`dbt/adapters/duckrun/engine.py:909-913`), and nothing in THIS repo sets that property, so the
  column "must" be blind. The missing step is that **Fabric's own writer sets it** when the resource
  profile enables V-Order. Reading an adapter to predict what a column will say is not the same as
  reading the column, and this file spent months asserting a `·` that the records never contained.
  The two warnings that remain true and are worth keeping: the property and the per-file metadata
  are independent in principle — either can be set without the other — so they *can* disagree, and
  `vorder_files` is what to believe if they ever do. The by-hand recipe this file used to prescribe
  is gone: `layout.ordering` measures it every run.
  **Everything in this bullet is about SPARK. Do not carry "believe `vorder_files`" over to dwh** —
  there both signals are blind and the warehouse's own `sys.databases.is_vorder_enabled` is the only
  authority. See the retraction under *V-Order only affects files written after it*.
- **WHETHER THE ROWS WERE ACTUALLY REORDERED IS MEASURED NOW — `layout.ordering` in the run record,
  and one step-summary section in the `layout` job.** V-Order is documented as a row-reordering plus
  encoding pass and nothing here could say whether it happened; the record could state that a run
  asked for `readHeavyForPBI` and never that the parquet came out any different. Three signals over
  `fct_summary` only, per engine, each independently best-effort (`stats.py`: `rg_ordering`,
  `run_lengths`, `vorder_tags`, assembled by `ordering_for`):
  - **`columns[c].rg_overlap_pct`** — of consecutive row-group `[min,max]` ranges sorted by min, the
    percentage that INTERLEAVE. 0% = the row groups partition that column's range, which is what a
    global sort produces and what lets a reader skip segments. Free: it reads the `parquet_metadata`
    rows the encodings pass already fetched, so it costs no OneLake traffic.
    **The comparison is STRICT and that is load-bearing.** A row-group boundary almost never lands
    on a value boundary, so under a perfect sort one value straddles each boundary and the ranges
    TOUCH — counting a touch as an overlap scored every column of every case 100% on synthetic
    fct_summary-shaped data, sorted and shuffled alike. The metric saturated while looking like a
    finding. Pinned by `test_a_sorted_low_cardinality_column_reads_zero_not_a_hundred`.
  - **`columns[c].runs`** — adjacent equal-value runs in physical order over the first
    `ORDERING_SAMPLE_ROWS` (4M) rows of the largest file, ordered by `file_row_number` explicitly so
    the count cannot move with DuckDB's scan order or thread count. **This is the intra-file
    reordering V-Order claims, and the row-group ranges structurally cannot see it.**
  - **`vorder_files`** — live files whose Delta `add` action carries `tags.VORDER = "true"`, read
    from `_delta_log/*.json` with obstore. Last add per path wins and removes are NOT replayed: the
    live set comes from the file list the metadata fetch already holds, so this cannot disagree with
    the rest of the document about which files exist. A live file no JSON commit describes is
    `unknown` (checkpointed away), never silently counted untagged.
    **IT IS A SPARK-WRITER MARKER, so `ordering_for` SKIPS IT FOR dwh — see the retraction below.**
    An absent `vorder_files` on a warehouse is now the honest "this probe cannot see it"; a
    `tagged: 0` there was a successful read of a log that carries no such tag, which is a different
    statement from "the writer did not V-Order" and read identically on the page.
  **READ THE TWO PARQUET METRICS TOGETHER — neither alone says "sorted".** A secondary sort key
  repeats through the table, so its row-group ranges all span the domain (100% overlap) while its
  values stay perfectly grouped inside each row group (few runs); measured on a `date,time,DUID`
  sort, `date` reads 0% / 11 runs and `time` reads 100% / 3,106 runs. Sort by `DUID` instead and the
  two swap. A near-unique column (`mw`, `price`) is the built-in control: it cannot drop below ~100%
  unless the file really was reordered, so a run where every column falls together is measuring
  something real rather than low cardinality.
  **MEASURED ANSWER: V-ORDER DOES NOT REORDER THE ROWS. It is an encoding pass here, worth ~16% of
  the file size and nothing else.** Two spark dispatches an hour apart on identical data, differing
  only in resource profile — 31129088830 (`readHeavyForPBI`, 12 files, 1,059 MB, 12/12 files tagged
  `VORDER`) against 31131727297 (`writeHeavy`, 13 files, 1,260 MB, 0/13 tagged) — put the physical
  row order within measurement noise of each other on every column, with the un-V-Ordered run
  slightly MORE clustered on three of them:

  | column | `readHeavyForPBI` runs | `writeHeavy` runs |
  |---|---:|---:|
  | `date` | 88 (0.00%) | 77 (0.00%) |
  | `time` | 3,620,191 (90.50%) | 3,558,608 (88.97%) |
  | `price` | 3,614,952 (90.37%) | 3,546,547 (88.66%) |
  | `DUID` | 3,977,201 (99.43%) | 3,978,804 (99.47%) |
  | `mw` | 3,997,905 (99.95%) | 3,996,712 (99.92%) |

  So the V-Order documentation's "row reordering" is not observable in the parquet Fabric writes
  here, while the tag, the table property and a 16% size drop all say the feature genuinely engaged.
  Do not explain a V-Order CU result by row order — the rows are in the same order either way.
  What IS clustered is `date`, in BOTH runs equally: ~45,000-row runs against ~49,600 rows per date,
  i.e. each date contiguous. That is the model's own trailing `ORDER BY date` reaching the merge
  source, not the writer — which is exactly the confound this pair was run to separate, and the
  reason a single V-Order run could not have answered it. `time` and `price` sit ~9% below their
  random-adjacency baselines in both, which is the data (price repeats per region per interval), not
  the writer. `DUID` and `mw` are at baseline in both: nothing reordered them.
  One asymmetry worth not over-reading: `date` row-group overlap is 100% under the PBI profile and
  75% under `writeHeavy`, i.e. the PBI run's files each span the whole date domain while a quarter of
  the `writeHeavy` pairs are disjoint. That is `optimizeWrite`'s 1 GB bin packing rearranging which
  dates share a file, on 12 and 13 row groups respectively — a very small sample to draw a rule from.
  It lives under `layout.ordering`, a sibling of `stats`/`encodings` — **never in `layout.config`**,
  whose every key `variant()` walks into a dashboard column name: a MEASURED value there would split
  an engine's column and its layout bar every time the parquet moved. The dashboard does not read it
  at all yet, deliberately — `layoutKey`'s `sorted` element is still the DECLARED flag, and making
  it measured would re-band every historical run against a value none of them recorded.
- **V-Order only affects files written after it, so an incremental leg flips over slowly.** There
  is no model-level equivalent and no way to retrofit it in place; `OPTIMIZE … VORDER` or a
  rewrite is what moves parquet already on disk. `benchmark/README.md`'s snapshot table predates
  all of this. A `·` is correct for duckrun and iceberg — delta-rs and DuckDB have no V-Order encoder
  at all — and **was WRONG for dwh for every run ever measured; see the retraction below.**
- **RETRACTED: "Fabric Warehouse V-Order is off by default on new warehouses." IT IS ON BY DEFAULT,
  AND BOTH OF THIS REPO'S PROBES ARE BLIND TO IT.** Microsoft's
  [performance guidelines](https://learn.microsoft.com/en-us/fabric/data-warehouse/guidelines-warehouse-performance)
  (updated 2026-06-24): *"By default, V-Order is enabled on all warehouses."* Disabling is
  `ALTER DATABASE CURRENT SET VORDER = OFF`, **irreversible** once done, and the state is read with
  `SELECT [name], [is_vorder_enabled] FROM sys.databases` (`1` on, `0` off). Nothing in this repo runs
  that `ALTER`, and every dispatch creates its warehouse from nothing, so **every dwh run this repo
  has ever measured wrote V-Ordered parquet** — while the record said `false` and the page printed `·`
  and grouped dwh's layout bars as un-V-Ordered.
  Why it read false, and this is the part worth keeping: **both signals are SPARK-SHAPED.**
  `stats[dwh][*].vorder` is the Delta table property `delta.parquet.vorder.enabled`, a
  `TBLPROPERTIES` key; `ordering.dwh.vorder_files` was the Fabric **Spark** writer's per-file
  `add.tags.VORDER`. The warehouse engine sets neither. Measured on runs 31148571096 and
  31167379761 — freshly created warehouses — `mart.fct_summary` read **0 of 77** and **0 of 78** files
  tagged at `unknown: 0`, i.e. a completely successful read of a log with no such marker in it.
  **The trap is that the positive control is engine-specific.** `vorder_tags` demonstrably works — it
  returned `12/12` and `9/9` on spark runs — which proves it can see a *Spark* writer's tag and says
  nothing whatever about a warehouse's parquet. A probe that returns a plausible zero, with its own
  success indicator (`unknown: 0`) confirming the read, is the worst shape a measurement can have; do
  not read one as an answer without a control written by the *same* engine.
  Corroborating that the warehouse writer is a different beast, from our own records: dwh is the only
  engine reading `compression: UNCOMPRESSED`, and run 31148571096's dwh columns come back GUID-named
  (`col-198f7fa3-…`), i.e. parquet column mapping.
  **So the authoritative signal is recorded now**, by the dwh leg itself:
  `.github/scripts/dwh_vorder.py` queries `sys.databases` after the build and writes
  `layout.ordering.dwh.vorder_enabled`. It lives in `layout.ordering` and **never `layout.config`**,
  for the reason stated above that block. `ordering_for` no longer emits `vorder_files` for
  `kind == "warehouses"`, and `dashboard/app.js`'s `vorderOf()` prefers `vorder_enabled` over the
  property everywhere V-Order is grouped, captioned or printed. The six existing dwh records were
  **backfilled** to `vorder_enabled: true` on the documented default, the same way the sort keys were
  backfilled from the model at each run's SHA. The check is `typeof === "boolean"`, not truthiness: a
  real `false` (someone ran the `ALTER`) has to beat a `vorder: true`.
  One thing this does NOT claim: that dwh's V-Order changes its parquet measurably here. Its
  `rg_overlap_pct` is 100% on every column but `cutoff`, which is consistent with the measured spark
  finding that V-Order does not reorder rows. That is the same open question, not a new one.
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
    **Whether NEE writes V-Order is undocumented, checked 2026-07-31 and still unanswered.** The
    [NEE overview](https://learn.microsoft.com/en-us/fabric/data-engineering/native-execution-engine-overview)
    (updated 2026-06-05) does not mention V-Order anywhere — not as a supported feature, not in
    *Existing limitations*, not in *Other considerations*; its limitation list is entirely about
    query operators, file formats and semantic divergences, and the only layout items it names are
    Z-Order and Liquid Clustering, both read-side accelerations. The
    [V-Order page](https://learn.microsoft.com/en-us/fabric/data-engineering/delta-optimization-and-v-order)
    (same date) never mentions NEE. Neither references the other, so "V-Order is absent from the
    NEE limitations list" means *unstated*, not *supported* — do not cite that absence as evidence
    in either direction. It only becomes worth resolving if NEE is ever turned on here.
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

- **NEVER RUN TWO `Benchmark` RUNS AT ONCE. SERIAL, ALWAYS — this is an invariant, not a preference.**
  The first reason is the operational one: they share one Fabric capacity, and two builds plus two
  query suites competing for it get **THROTTLED**. Throttling does not fail loudly, it inflates every
  number in both runs, so the cost is two measurements that are quietly wrong rather than one that is
  late. A wall-clock benchmark cannot absorb capacity contention.
  The second reason is mechanical and is worse than a clash. Output item names are FIXED strings
  (`dbt_delta`, `dbt_dwh`), and `provision.py`'s `ensure()` REUSES an item it finds by that name
  rather than failing — `it = find(kind, name); if it: return it["id"]`. So two concurrent duckrun
  runs build into the same lakehouse and the same `mart.fct_summary`, interleaving writes, and the
  first teardown deletes the item the second is still using. Not an error you would see and retry: a
  corrupted table and a run that dies somewhere else.
  **The concurrency group only half-enforces this.** `onelake-${{ github.ref_name }}` is PER REF, so
  `gh workflow run Benchmark --ref <other-branch>` gets its own group and runs genuinely in parallel.
  That is the hole; do not use it. (`cancel-in-progress: false` also means at most ONE run can be
  pending — a third dispatch evicts the queued one rather than stacking, so the queue cannot be
  pre-loaded.)
  **To run several layouts, chain them: dispatch, wait for completion, dispatch the next.** See
  [TODO.md](TODO.md) for the loop. Nothing in the repo runs a batch for you, and nothing should
  without serialising it.
- Cancel superseded runs immediately (`gh run cancel <id>`) — spark and Fabric legs cost money.
- **The two DuckDB legs run on IDENTICAL DuckDB settings, and keeping them that way is the point
  of the pair.** `duckrun` and `iceberg` are the same DuckDB on the same notebook at the same
  `FABRIC_CORES`, which is what makes their two CU columns the sharpest comparison on the dashboard —
  so any `on-run-start` hook or `profiles.yml` setting given to one must be given to the other.
  This was violated for a long time: `dbt_project.yml` set `memory_limit = 4GB` for
  `target.name == 'duckrun'` alone while iceberg ran at DuckDB's default (~80% of node RAM), and
  the cap could not even be dispatched around because `DUCKDB_MEMORY_LIMIT` was never in
  `fabric_run.py`'s `_FORWARD` tuple. Its comment justified it as stopping an OOM on "the runner",
  which described the deleted GitHub-runner path, not the Fabric notebook this has run on since.
  The line is gone. Note the knock-on: duckrun's merge budget is a **0.3 share of the global
  limit** (`set_merge_memory_limit`), so the routed anti-join now gets 0.3 × default instead of
  0.3 × 4GB. Spill is unaffected — `temp_directory` is still set for both.
  **The `sorted` dispatch input is a KNOWING exception, and the only one.** With it on, duckrun
  writes `fct_summary` sorted and declares geometry, and **all three values are now dispatch inputs
  rather than literals in the model**: `sort_by` (default `date,time,price`), `row_group_size`
  (default `2000000`) and `file_size_mb` (default `1024`; choice of 1024/512/128).
  **BOTH OF THE FIRST TWO DEFAULTS HAVE MOVED, so a bare dispatch no longer reproduces the older
  history.** `sort_by` was `date,time` — the key the retired `'auto'` picker kept choosing, without
  its +19% profiling pass, with DUID's ~16% of size deliberately left on the table — and now carries
  `price` as well. `row_group_size` was `16000000`, spark `readHeavyForPBI`'s measured segment size
  (9×16.0M on this table) and VertiPaq's ceiling, the largest segment Direct Lake takes whole; 48M
  was declared first and was over it. 2M is the other end of that trade: ~72 row groups instead of
  ~9, so a query touching a narrow slice of the sort key scans far less, at the cost of more segments
  to open. Neither is a guess — a `date, time, price` run at 2.0M / 72 RG is already in `history/`.
  **The dashboard consequence is a NEW COLUMN AND A NEW GROUP, not drift — and nothing in `stats.py`
  had to change for that, BY DESIGN.** `_nonbaseline`'s baseline is pinned to `16000000`, the
  geometry `history/` was written under, and is deliberately NOT the live dispatch default: a 2M run
  therefore records `row_group_size: "2000000"` explicitly and `variant()` splits it into its own
  column, while the 16M history keeps the column it has. Had the baseline read the default, a 2M run
  would have recorded `None`, shared a column with the 16M history, and `columnsFor` — latest run per
  column — would have hidden nine runs of 9-RG history behind one 72-RG run, with the bars still
  separating so nothing looked broken. `layoutKey` bands the MEASURED row-group count and carries the
  sort column list, so 72 RG and 9 RG can never merge and neither can two sort keys. **Do not "tidy"
  the baseline to match this new default** — that is the trap, and `test_sort_key.py` pins it.
  Read a jump in the layout table as the defaults changing, and check the record's `inputs` block
  before calling it a regression.
  Three consequences worth holding. **`row_group_size` and `sort_by` are FREE TEXT**, so the `plan`
  job validates them — a positive integer, and comma-separated plain identifiers — because `plan` is
  free and runs before any leg spends capacity, whereas a typo reaching duckrun dies mid-write with
  the money already gone. A well-formed name that is not a column of the model still fails in the
  leg; catching that needs the manifest, which only exists in the notebook.
  **`stats.py`'s `declared_sort_key()` reads `DUCKDB_SORT_BY`, NOT the model** — it used to regex a
  literal list out of `fct_summary.sql`, and there is no literal left to match, so that regex would
  have returned `{}` and the page would have silently lost every sort caption.
  **The geometry is recorded only when it differs from the default**, exactly as `sorted` is recorded
  only when on: `variant()` skips null, so a default run keys to the same dashboard column as all the
  history, and a non-default one splits into its own — which `variantTag` then has to spell
  (`64c+sorted+4.0Mrg`), or two split columns would print one header.
  **needing duckrun ≥ 0.4.44**: 0.4.43's `_delta_core.sql` macro forwarded a fixed key list that
  carried `sort_by` and not the two geometry keys, so run 30955591822 wrote the estimator's 3f/19RG
  despite the config, silently. 0.4.44 forwards them (the notebook installs latest from PyPI) —
  and iceberg writes it unsorted, so the pair differs by more than the writer for that run. This is not a settings drift to be corrected: it
  cannot be corrected. `sort_by` and the geometry keys occur **zero times** in dbt-duckdb's adapter and its macro package,
  so iceberg has no way to express a sort or row-group size at all, and the trailing `ORDER BY date` in the model does
  not reach any engine's stored table (see the fairness invariant under `fct_summary`). Off — the
  default — the two are identical exactly as before, which is why the flag is a dispatch input rather
  than a config in the tree. `stats.py` records it under **duckrun only**: recording it under iceberg
  would split that engine's column between two runs whose parquet is byte-identical, and what the
  dispatch asked for is already in the record's `inputs` block.
- **Every engine takes 4 threads — duckrun, iceberg, dwh and spark alike.** It was
  8/8/4, so DAG-level concurrency was a hidden variable between the legs: a benchmark comparing
  engines should not also be comparing how many models each was allowed to build at once. duckrun
  was the exception for as long as its adapter pinned `config.threads = 1`; that pin is gone, so
  the duckrun/iceberg pair now differs only in the writer. **Spark's 4 is a hard cap, not the convention** — raise the
  shared value later and spark must stay behind: dbt-fabricspark opens one Spark REPL per thread
  and Fabric packs at most five per Livy session, so more means a second Spark application,
  separately billed, for one `dbt run`.
- **A default dispatch is REPRODUCIBLE, and it is now `teardown` + `skip_download` that make it
  so.** Teardown means no tables survive to build on, `skip_download` means the input archive does
  not move, so two dispatches of the same commit differ by nothing but what was deliberately
  changed. Each on its own is weaker: a from-scratch build over a grown archive still measures a
  different workload, and a frozen archive on top of yesterday's tables still measures yesterday's
  incremental state. What it costs is real and was accepted knowingly: **every dispatch is a
  from-scratch build of 370M rows for the selected engine**, not a top-up, and that is not optional:
  the teardown always runs. Turn `skip_download` off to extend the archive, which is a different
  question and deserves its own run. `land` still runs either way — it
  provisions the landing lakehouse; only the DOWNLOAD is skipped.
- **`engines` selects which leg runs, and everything is scoped to it.** The `plan` job computes both
  matrices with `fromJSON` and `stats.py` reads only `BUILD_ENGINES`. An unknown engine name is
  fatal in both places: a typo that silently builds nothing looks exactly like a build that worked.
  The `layout` job then records whatever was built, gated on nothing — comparing generations is the
  dashboard's job.
- **THE TEARDOWN DELETES EVERYTHING THE RUN CREATED, except `dbt_landing`, and it deletes BY GUID.**
  `reset_outputs` and the `reset` job are gone; `provision.py teardown <record>` runs LAST in
  `benchmark.yml` and removes every item this run's own record names whose `role` is not `landing` or
  `folder` — the output lakehouse or warehouse, `dbt_dwh_src`, the benchmark's semantic model. The
  throwaway notebook has already deleted itself in `fabric_run.py`'s `finally`.
  **Why, and it is not tidiness:** an item that outlives its run keeps drawing background CU —
  OneLake bills reads against an idle lakehouse, a Direct Lake model gets refreshed — and that CU
  lands in the NEXT dispatch's measurement window with nothing in the capacity data to say it came
  from an older generation. Run 30699626723 measured 1,578 CU for an iceberg item the dispatch had
  not even selected. Deleting per run is what makes an item GUID belong to exactly one run, which is
  the whole basis of GUID-keyed attribution.
  **It is UNCONDITIONAL — there is no `teardown` input.** A deleted item keeps its CU rows in the
  metrics model (verified by hand against the live model), so deleting costs nothing in measurement
  and there is no case for leaving one standing.
  **Items live in TWO FOLDERS and the split is the point.** `benchmark` holds everything a run
  creates and the teardown deletes; `landing` holds the one lakehouse that outlives every run. A
  workspace listing then shows at a glance what is disposable, and `benchmark` is EMPTY between
  dispatches — which is exactly the state a successful teardown leaves behind, so an item sitting
  there is a visible failure rather than one you have to go looking for.
  `benchmark/deploy_models.py` puts its semantic models in the same `benchmark` folder (`BENCH_FOLDER`),
  so one name covers every item either half of the workflow makes. Neither folder is ever deleted:
  they hold no data and cost nothing, and deleting the one landing lives in would be the same mistake
  as deleting landing. `folderId` is honoured only at CREATE, so `ensure()` also calls `reparent()`
  on an item it FOUND — otherwise anything provisioned before the split stays where it was, and for
  `dbt_landing` that is forever.
  **By GUID, not by name, is the safety property.** A name-driven teardown would delete a `dbt_spark`
  a concurrent dispatch had just created, and there is no undo. `dbt_landing` is refused twice over
  — by role, and by name in `drop_guid()` — because it holds the downloaded AEMO archive, the one
  thing here that cannot be rebuilt from the workspace; re-landing it means re-downloading years of
  files `download_limit` at a time. The `dbt` folder survives because landing lives in it.
  **A delete that does not take fails the job.** Fabric accepts the DELETE asynchronously (202) and
  an item still listed is still billable, so `drop_guid()` polls `GET /items/{guid}` for a 404 and
  the run goes red with `STILL BILLABLE` rather than reporting a clean teardown. Failures are
  collected, not raised one at a time — a warehouse that refuses must not leave the lakehouses
  standing behind it.
  **The display-name reservation moved to the good side of the run.** `reset` deleted immediately
  before the build, so Fabric was still holding the names when the legs created theirs — `409
  ItemDisplayNameNotAvailableYet`, which killed three of four legs on run 30639018466; the survivor
  survived only because its delete took 36s to propagate. Deleting at the END puts a whole idle
  period between a delete and the next dispatch's create. `ensure()`'s 409 poll (40 attempts at 15s
  ≈ 10 minutes) stays as the guard for back-to-back dispatches, and a leg waiting minutes there is
  that, not a permissions problem.
  Two costs, neither of which surfaces as an error: a recreated item has a **new GUID**, so anything
  bound to the old one (a Direct Lake semantic model, a shortcut) points at something gone —
  `benchmark/` survives because it deploys its models per dispatch — and the recreated warehouse
  comes back with a fresh `connectionString` and **no grants**.
  `python -m pytest .github/scripts/ -q` exercises the whole path against a stubbed Fabric in
  seconds, and the free `checks` job runs it before any leg spends capacity. Run it before
  touching this: the failure mode is deleting the wrong thing.
- **`full_load` in the record is DERIVED, not an input.** It is `true` when the run's own output
  item carries `created: true` — i.e. the previous run's teardown really did run. An input only ever
  stated an intention; 143M rows written from nothing and 3M rows appended are not the same run, and
  the record has to say which one a layout or a CU number describes.
- **`native_execution_engine` toggles Fabric's NEE on the spark leg, ON by default.** It sets
  `SPARK_NATIVE_ENABLED` → `spark.native.enabled` in the Livy conf, and that one key is the whole
  recipe: Microsoft's current session-level doc sets nothing else. Earlier preview guidance and
  most community posts pair it with
  `spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager` — that spelling is
  **absent from the current doc**, so this repo does not set it; check the doc, not a blog, before
  adding it. Read the NEE bullet above before drawing conclusions from a run: execution-side semantic
  divergences, silent JVM fallback for unsupported operators, and V-Order behaviour still unstated.
  **It was off by default and is now on.** That is a deliberate change of what a bare dispatch
  measures, not a finding: across seven spark runs NEE moves **nothing** — analytics CU 1,149 /
  1,306 / 1,480 with it on against 1,514 with it off under `readHeavyForPBI`, and the same file and
  row-group layout either way, which is why `LAYOUT_CONFIG` excludes it from the layout grouping in
  the first place. The default flipped because it is what a Fabric user gets by choosing the faster
  engine. Note the consequence for the page: `variantTag` omits a flag that is OFF, so spark columns
  now read `spark·readHeavyForPBI+NEE` by default and a deliberately-disabled run is the one that
  needs the explicit spelling — `columnsFor` already falls the whole engine back to explicit when two
  configs would collide, so nothing breaks, but the common column gains `+NEE`.
- **`spark_resource_profile` is a dispatch choice, default `writeHeavy`** (the workspace default,
  i.e. no V-Order). `readHeavyForPBI` is the only value that enables V-Order, and it also flips
  `optimizeWrite` to a 1 GB bin size, so ticking it rewrites file layout broadly — judge it in the
  `layout` job's table at the end of the run.
- **NOTHING THAT COMMITS OR SPENDS RUNS ON PUSH.** This replaces the older, blunter "nothing runs on
  push", and the narrowing is deliberate — read the reason before touching a trigger.
  The original: pushing to `main` used to trigger the four Fabric legs, so any code change — a
  script, a workflow file, a comment — spent paid capacity nobody asked for, and a batch of edits
  queued several such runs on the concurrency group. `paths-ignore` did not fix that: it is per-PUSH,
  not per-file, so a commit touching a doc *and* anything else still ran. It then carried a second
  load: **the workflows that commit back to the repo** — `Benchmark` a run record, `Capacity units`
  the CU ledger — are only safe while no workflow answers a push, or CI starts paying for its own
  commits.
  **`Dashboard` now answers a push, and it is safe for a reason that does not generalise.** It
  commits NOTHING — it deploys to Pages — and its filter is `paths: ['dashboard/**']`, which never
  matches the `history/` paths the other two write. So no commit can trigger a publish and no publish
  can make a commit: the loop is not reachable, and a publish costs a free runner minute rather than
  capacity. Two things must stay true, and they are the actual rule now:
  **`Benchmark` and `Capacity units` must never gain a `push:` trigger**, and **`history/` must never
  appear in that path filter** — that single edit builds the loop.
  The per-push mechanic still applies (a commit touching `dashboard/app.js` and something else fires
  it), and it is accepted here because the cost is one no-op deploy of an identical shell. The filter
  is the whole directory rather than the three files that reach the published bytes on purpose: a
  narrow filter that someone forgets to extend makes the page **silently** stop updating, while the
  broad one's worst case is a free deploy for a README edit.
  Start a build with `gh workflow run Benchmark` when you actually want one; that one is still
  dispatch-only and always will be.
- **THERE ARE THREE WORKFLOWS: `Benchmark`, `Capacity units` and `Dashboard`.** `all.yml`, `dbt.yml`
  and `cu.yml` are deleted. **They share nothing but the JSON in `history/`.**

  | workflow | file | does | triggered by |
  |---|---|---|---|
  | `Benchmark` | `benchmark.yml` | open the record, offline checks, plan, land, build, layout, resolve, bench, report, teardown, record | nightly `cron` · dispatch — it is the only one that spends capacity |
  | `Capacity units` | `capacity.yml` | `cu/measure.py` → commits `history/cu.json` | `workflow_run` after Benchmark · `17 10 * * *` · dispatch |
  | `Dashboard` | `dashboard.yml` | `dashboard/build.mjs` → deploys the page | `push` to `dashboard/**` · dispatch |

  In the normal case a human starts nothing but a `Benchmark`: the ledger tops itself up after every
  build, and the page publishes itself when its code changes. The one thing a human now has to
  remember is a SECOND `Capacity units` dispatch an hour later if a number matters — see the
  lower-bound bullet below.
  **The measurement was a job inside `Dashboard` and splitting it out is what bought all of this** —
  while one dispatch both measured and deployed, refreshing a number dragged a Pages deploy behind
  it, so "publish only when the page changes" was not expressible.
  The composition they replaced charged three taxes, and the third is what finally killed a run.
  Every input was declared twice, once as a `type: choice` on the dispatch form and once as a plain
  string across the call boundary (a `workflow_call` input cannot be a choice). The workspace secret
  had to be re-resolved in each file, because `jobs.<id>.with` is the one place the `secrets` context
  is unavailable. And **a called workflow can never hold MORE permission than its caller grants** —
  run 30735526504 died as a `startup_failure` with no jobs and no log because `dbt.yml` asked for
  `actions: read` after `all.yml` stopped granting it. One file has one permission block.
  One consequence worth holding: `layout`'s `if:` carries `inputs.build` alongside `!cancelled()`,
  and that half is load-bearing. A status function overrides GitHub's skip-when-a-dependency-skipped
  rule, so without it a benchmark-only dispatch would run `layout` and read `BUILD_ENGINES` off a
  `plan` job that never ran.
- Jobs no longer cancel the run when they fail, and no matrix is `fail-fast`. Every leg runs to
  its own conclusion, so `gh run view <id> --json jobs` reads straight: `failure` means that
  leg failed. Cancelling never saved the Fabric compute anyway — the notebook or Livy session
  keeps running workspace-side after the GitHub job dies — it only erased the evidence.
- **The `layout` job is part of the build and there is no standalone copy.** It costs ~10 minutes
  of OneLake reads per dispatch (the iceberg item alone 12m+) for numbers that only move when tables
  are REWRITTEN, which was once the argument for splitting it into its own dispatch-only workflow.
  That lost: nothing else compares the engines, and a dashboard nobody remembers to dispatch reports
  nothing. It must run BEFORE the teardown — it reads every table through the Delta log, and the
  teardown deletes them.
  **A FAILED LEG DOES NOT PAY FOR IT** — the `if:` also carries
  `needs.fabric.result != 'failure' && needs.server.result != 'failure'`. It used to run regardless,
  on the argument that a leg that failed late still wrote tables worth recording. It does write
  them; they are not worth recording. Every dispatch tears down its own items and the next one
  rebuilds from nothing, so a half-built table is a state that existed once and cannot recur, with
  nothing to compare it against — and the record cannot carry it anyway, because a failed build
  means no benchmark timings and `incomplete()` drops the whole record from the page. So it was
  10-15 minutes of paid OneLake reads producing a step-summary table for someone already reading the
  dbt log above it. Keep `!cancelled()` rather than `success()`: a skipped matrix (no engine of that
  kind selected) is not a failure and must still record.
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
- **There was a dispatch-only `Table layout` workflow. It is deleted — the `layout` job is the only
  copy, and reading the layout now means running a build.** Keep its numbers in mind: the iceberg
  item alone reads at 12m+ (386 files, 1,175 row groups over OneLake), which is why the timeout is
  40 minutes and not 15, while what it reports — files, row groups, size, v-order — only changes
  when the tables are **rewritten**, and the facts are append-only incrementals. That is the cost
  every green dispatch now pays, and it was the argument for splitting it out; it lost to the fact
  that nothing else compares the engines, so a dashboard nobody remembers to dispatch reports
  nothing. What the split bought and is now gone: asking the question *without* spending Fabric
  legs. `stats.py` writes the same document to two sinks — `STATS_JSON`, uploaded as the `stats`
  artifact, and the RUN RECORD, which is what outlives the 90-day retention and what the page reads.
  Renaming a `DETAIL_KEYS` entry makes the layout table disappear on the page with a note, not an
  error, so change both together.
  It also reports **whether the rows were physically reordered** — `layout.ordering`, see the
  V-Order bullet in *Facts that are easy to get wrong*. That rides on the footer read the encodings
  pass already makes, plus one bounded 4M-row scan of a single file per engine and a few small JSON
  commits, so it adds no second reader of the Delta logs.
  It also reports the INPUT side: `landing` — files and bytes under `dbt_landing/Files`, in total
  and per folder. Everything else in that document describes what came out, so without it a record
  can say a run wrote 143,980,961 rows and not say from how much. Read by LISTING (`obstore.list`,
  already a duckrun dependency), because DuckDB's `glob()` returns paths and no sizes and the archive
  is uncompressed CSV whose bytes are the point. Best-effort: a failure leaves the key ABSENT, never
  `{}`, because an empty dict reads as "an empty archive" rather than "not measured".
- It runs `stats.py` and nothing else, over **every shared table** in pipeline order —
  the staging view, the four facts, then `dim_calendar`/`dim_duid`/`fct_summary`. It was briefly
  cut to the three mart tables on the argument that the facts are inputs whose rows are implied by
  the summary's; that was wrong in the one situation the dashboard exists for. When `fct_summary`
  disagrees across engines, the fact counts on the rows above it are what separate "an input
  differs" from "the summary logic differs", and a mart-only table shows the symptom while hiding
  the cause. Totals are unscoped again, so they cover anything an item holds beyond this list.
  `summary.py` (the four-engine test dashboard) is deleted — its input was the `rr-<engine>.json`
  artifacts the test matrix uploaded, and there is no test matrix.

## The run record: one JSON per dispatch, and the item GUID is the point

`benchmark.yml` commits `history/runs/<UTC ts>-<run id>.json`. Every stage writes a JSON *fragment* naming
the Fabric items it touched **under their GUIDs**; the final `record` job downloads them all and
merges them into one document (`.github/scripts/record.py`, `finish`). Raw facts, no analysis.

**Why it exists.** Nothing recorded which items a run created. The GUIDs all existed in-process and
were thrown away — `stats.py` resolved all four and dropped them a line later, `provision.py` wrote
them to stderr, `fabric_run.py` never saw the notebook's at all. So CU could only be attributed by
matching item **display names**, which is why `cu/` carries a substring matcher, a `shared` column
for anything ambiguous, and a lagging `'Items'` snapshot join. A run that writes down what it created
turns that into a dictionary lookup.

Things that are load-bearing rather than stylistic:

- **`items` is a dict keyed by GUID, never a list.** The merge is a recursive dict union, which
  unions dicts and *replaces* lists — so with a list, the teardown fragment's `{deleted: …}` would
  overwrite the provision fragment's `{role, kind, name, created}` entirely. Pinned by a test.
- **Fragments are sorted by BASENAME, not by path.** `download-artifact` nests each artifact in its
  own directory, so full paths sort by artifact name and the `record-00-run` / `-10-land` /
  `-20-build` / `-30-layout` / `-40-bench` ordering would be lost. Same rule, same reason, as
  `benchmark/merge_reports.py`.
- **`RUN_RECORD` unset is a NO-OP**, deliberately. `provision.py` and `stats.py` must stay runnable
  by hand to reproduce a CI failure, and neither should need a record path to do it. One consequence:
  a job that forgot its `RUN_RECORD` env fails *silently*, producing a record missing those items —
  which is why every job's fragment is uploaded with `if-no-files-found: ignore` and the merge logs
  the item table it assembled.
- **`role` is the closed vocabulary** — `landing` | `output` | `dwh_src` | `folder` | `compute` |
  `semantic_model` — and it is what replaces name matching downstream. Adding an item kind means
  adding a role, not teaching a matcher another substring.
- **`benchmark/` does not import `.github/scripts/record.py`.** `deploy_models.py` writes its item
  ids through `report.merge(obj, path=os.environ["RUN_RECORD"])`, reusing the deep-merge it already
  had. Twenty duplicated lines are cheaper than making `benchmark/` non-deletable, which is a stated
  property of that directory.
- `python -m pytest .github/scripts/ -q` is the offline gate, and the free `checks` job runs it
  **before any leg spends capacity**. Everything it covers fails silently: a
  fragment that never lands, an entry a later stage overwrote, a merge order that dropped a deletion
  timestamp — each produces a record that looks fine and attributes CU to the wrong run.

## The query benchmark is a second workflow, and it only reads

`benchmark/` — the second half of the `Benchmark` workflow — asks the question the build half
does not: the parity table says the four engines hold the *same rows*, this measures how long Power BI
takes to **query** them. Ported from `djouallah/duckrun`'s `parquet_layout.yml`.
[benchmark/README.md](benchmark/README.md) has the detail; what matters when touching this repo:

- **THERE IS A NIGHTLY NOW: `cron: "17 7 * * *"` plus `workflow_dispatch`. `push`, `workflow_run`
  and `repository_dispatch` are still forbidden.** This REVERSES a rule that read "a human starts
  every run — not a nightly, not behind an `if:`", and the reasoning that rule carried is unchanged
  and now simply accepted: the benchmark's query passes are **interactive CU** on shared Fabric
  capacity, the class of usage a capacity admin sees and asks about. One run a day of that is the
  deliberate cost, and the cron is one line to remove if it stops being worth it.
  **`push` is forbidden for a different reason and that one has not moved**: this workflow COMMITS
  the run record, so a `push:` trigger would let its own commit start the next paid build. A clock
  is not a commit, which is why a nightly does not reopen that loop.
  07:17 UTC is 02:17 EST / 03:17 EDT — US Eastern asleep either side of the DST boundary — and 17:17
  in the metrics model's own +10 clock, so a night's results are there to read in the afternoon.
  ⚠️ **On a `schedule` event the `inputs` context is EMPTY and `workflow_dispatch` defaults do NOT
  apply**, so every input in that file carries its own scheduled value spelled
  `github.event_name == 'schedule' && '<value>' || inputs.<name>`. Never `inputs.x || 'default'`:
  that cannot tell an absent input from a deliberate one, so it would override `build: false` and
  turn the scouting recipe's `gap_seconds: 0` back into 600. The failures are silent and expensive —
  blank `engines` is fatal in `plan`, blank `build`/`benchmark` are falsy so a nightly would spend a
  runner and build nothing, and blank `sort_by` means NO SORT, i.e. a nightly quietly measuring a
  different layout than the form describes. **Every scheduled value is the form default, `cores`
  included — 8, not the 64 a hand dispatch usually passes**: 64 is for a run somebody is waiting on,
  and nobody waits on a nightly. So the nightly opens its own `·8c` column rather than joining the
  64c history, which is correct — `vcores` is part of `variant()`, and the CU rate (`cores / 2`) says
  they are different machines.
- **It measures a USER SESSION, and nothing is ever cleared. The pass number is the tier.**
  `deploy_models.py` **deletes and recreates** each semantic model, so it starts with an empty
  VertiPaq store; `xmla_compare.py` then walks the whole 25-query suite `runs` times — pass 1 **cold**,
  pass 2 **warm**, passes 3+ **hot** (median + spread), with `think_seconds` of idle between
  queries. Defaults `runs=6`, `think_seconds=4`, `gap_seconds=600`. Things worth not rediscovering:
  - **The per-query dehydrate is gone and must not come back.** It ran `clearValues` + `full` before
    *every* cold-tier query — 21 forced transcodes per engine per run. No user is ever in that state,
    and `clearValues` clears the **data cache**, not the data (TMSL: "Clear values in this object and
    all its dependents"), so it was never a statement about transcoding cost anyway.
  - **A new dataset is the only way to guarantee a cold VertiPaq store.** All the alternatives were
    checked: TMSL **`clearCache` clears query caches, not resident columns** (DAX Studio's Clear Cache
    button issues it and Direct Lake queries stay fast after — a hot→warm lever); **reframing is
    incremental** and retains dictionaries, so a redeploy-in-place is semiwarm at best; memory
    pressure and node reassignment do produce cold but are not commandable. `overwrite=True` keeps
    the item id, which is why the delete exists.
  - **Accepted cost:** one extra item GUID per dispatch in the Capacity Metrics item list. `cu/`
    survives it because it resolves names live from the REST API, and the display name never changes.
  - **`clearCache` between passes is deliberately NOT used**, even though it would make "warm" match
    Microsoft's strict definition (resident data, empty VertiScan caches). This reproduces user
    behaviour, not engine states; a user's second visit is simply their second visit.
  - **Cold and warm are single samples** — one first visit per deployed model — so they carry no
    spread and the old >25%-cold-spread noise filter is gone. Raising `runs` only strengthens hot; a
    second dispatch is what tests whether a cold number repeats. `runs<3` yields no hot tier, which
    the render layer shows as a gap.
  - **Nothing may touch the model between readiness and pass 1.** The top-DUID resolve therefore runs
    *after* pass 1 (it transcodes `DUID` and `mw`, which `probe_duid`/`probe_mw` measure) and the
    ladder joins at pass 2 unless `top_duid` is pinned; and the readiness probe reads `dim_calendar`,
    because `COUNTROWS(fct_summary)` was byte-identical to `probe_rowcount`. A stub-connection test
    in `test_verdicts.py` pins the exact sequence — it is the one guard against a silently warm
    "cold" pass.
  - **`probe_rowcount` must stay LAST among the probes.** The marginal-column-cost table subtracts it,
    and that only means "one more column" because every other probe is the first query to touch its
    column while rowcount runs once they are all resident. Pinned by a test.
- **Deploy models, run queries, report timings — that is the whole scope.** Upstream had to *build*
  the layouts it compared; here the four engines' own `mart.fct_summary` already are four layouts, at
  row-count parity. So there is no build phase, and deliberately **no stats phase either**: physical
  layout is `stats.py`'s job in the `layout` job of the `dbt` workflow, and re-deriving it here would be a second, slower reader of
  the same Delta logs. The only endpoints touched are the Fabric control plane and XMLA. Keep it that
  way — the moment this writes a table into a lakehouse, `stats.py`'s unscoped `get_stats()` starts
  counting it and the parity dashboard reads it as drift.
- **The paid work is a matrix, one job per engine, `max-parallel: 1` — and the reason is the token,
  not the parallelism.** A Fabric/XMLA token lives about an hour; one job over four models, 25
  queries x `runs` passes and two 600s gaps runs past that and the expiry lands mid-measurement on
  the last engine.
  Each job mints its own and retires it with the job. Consequences to hold onto: nothing computes a
  ratio during the measurement any more — each job uploads a report **fragment** and the free
  `report` job merges (`merge_reports.py`, **basename order**, meta fragment named to sort first so a
  per-engine fragment cannot overwrite the shared `run` block) and renders; and each job resolves the
  selectivity ladder's DUID itself after its cold pass, which is recorded per model and warned about
  on disagreement rather than assumed. Do not collapse it back into one job to "save runner minutes" — the runner is free and
  the capacity is not. **`xmla_compare.py` now refuses more than one engine outright.** It used to
  fall back to an in-process walk of every model, for running this from a laptop; that path is deleted
  — the laptop is not a supported way to spend this capacity, and a second orchestration shape kept
  alive to serve it meant two implementations answering the same question. `dbt`-style scouting is
  still a dispatch, just with `engines=duckrun,spark runs=3 think_seconds=0 gap_seconds=0`.
- **It runs after the build in the same workflow, on the same `onelake-<ref>` group.** Not for
  correctness — nothing here writes — but because a concurrent dbt build contends for the same
  capacity, and capacity contention is the one thing a wall-clock benchmark cannot absorb. `resolve`
  therefore `needs: layout`. Do not parallelise it to make the run shorter.
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
  the dehydrate of the day was a **no-op that SUCCEEDED** rather than the failure the hot-only
  degradation watched for. Fifteen bogus "cold" samples were recorded, dwh entered the COLD totals, and the summary printed
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
- Scout with `engines=duckrun,spark runs=3 think_seconds=0 gap_seconds=0` before spending a full
  run: it exercises
  deploy → XMLA → render end to end in minutes rather than an hour of capacity. Read two things in
  its log — the deploy printed a **different item id** than last time (`replaced <guid>`, so pass 1
  really was cold) and pass 1 > pass 2 > pass 3.

## `cu/` and `dashboard/` are the OTHER TWO workflows, and they join the run records on the item GUID

`cu/` + `capacity.yml` ("Capacity units") and `dashboard/` + `dashboard.yml` ("Dashboard") answer
what the workspace *cost*: CU per Fabric item, read from the Capacity Metrics app's own semantic
model by DAX over the Power BI `executeQueries` endpoint. Fabric exposes **no per-operation CU REST
API** — that model is the only authoritative source, which is why this exists.
[cu/README.md](cu/README.md) and [dashboard/README.md](dashboard/README.md) have the detail.

**Three workflows, one job each in spirit.** `Benchmark` builds, measures query time, deletes what it
created and commits `history/runs/<ts>-<run id>.json`; `Capacity units` reads capacity for the GUIDs
in those records and commits `history/cu.json`; `Dashboard` builds and deploys the page and touches
no data at all. `all.yml`, `dbt.yml` and `cu.yml` are gone.

- **MEASURING AND PUBLISHING ARE SEPARATE WORKFLOWS, AND THAT IS WHAT MAKES THE PAGE'S DYNAMISM
  REAL.** They used to be two jobs of `Dashboard`, so one dispatch did both and refreshing a number
  dragged a Pages deploy behind it — the page could be dynamic while the *wiring* stayed coupled.
  Now: `Capacity units` commits the ledger and publishes nothing; `Dashboard` publishes and measures
  nothing. Three consequences worth holding:
  - **`Capacity units` fires on `workflow_run` after `Benchmark`, and on dispatch. The daily
    `schedule` is GONE.** The `workflow_run` trigger is a scoped reversal of the "dispatch only" rule
    and it is earned: the rule's stated reason was that *publishing is a decision*, and an automatic
    measurement now publishes nothing. `Benchmark` has a nightly of its own, so a CU read fires after
    it automatically — that read is a LOWER BOUND, and the 10:17 cron is what raises it.
  - **`workflow_run`, never `workflow_call`** — see the three taxes below. It also means
    `benchmark.yml` needs **no edit**: the CU read is a separate run that starts after Benchmark
    completes, so Benchmark's duration, status and job graph are untouched. No conclusion filter: a
    failed Benchmark still spent capacity and still commits a record (`record` is `if: always()`).
  - **⚠️ `workflow_run` REQUIRES AN EXPLICIT CHECKOUT `ref:`.** A default checkout takes the
    TRIGGERING run's head SHA, which is from *before* Benchmark's `record` job pushed the run record
    — so the measurement would read `history/runs/` without the very run that triggered it and
    `floor_for()` would compute a floor excluding it. Silently incomplete, nothing says so.
    `capacity.yml` uses `${{ github.event.workflow_run.head_branch || github.ref_name }}` for the
    checkout **and** for the `pull --rebase` / `push` targets; `github.ref_name` alone is the default
    branch on that event regardless of where the build ran.
  - **`Capacity units` is NOT `continue-on-error`.** It was, while it gated a page deploy in the same
    run: a throttled metrics model had to cost a stale number rather than a stale page. Unattended on
    a schedule that inverts — a failed read would report green and the ledger would quietly stop being
    topped up. Red, so the scheduled-failure mail arrives; nothing downstream breaks, because the page
    keeps serving the last good ledger.
- **TWO AUTOMATIC READS, DOING DIFFERENT JOBS — and the second is what makes a number true.** A CU
  hour keeps growing for up to ~70 minutes (~6 min ingestion lag, 5–64 min smoothing), and
  `measure.py` has **no settle logic** — every read re-reads the whole window from the floor and
  merges with `max(old, new)`, so a re-read is idempotent and monotonic and two reads of one window
  can only RAISE a number, never lower one. That is what makes a pair of reads safe and a
  badly-timed one merely useless rather than wrong. The `workflow_run` read fires within a minute of
  a `Benchmark` finishing and is a deliberate LOWER BOUND, there so a fresh run's column is populated
  immediately; `cron: "17 10 * * *"` is the settling read.
  **ITS TIME IS ARITHMETIC, NOT A ROUND NUMBER.** `Benchmark`'s nightly starts 07:17; its run is a
  measured median of 31 minutes and max of 84 across 47 duckrun records, so it finishes ~07:48–~08:41,
  and plus the ~70 minute settle that is ~09:51 worst case. 10:17 clears it. An "hour after the
  nightly" would land mid-smoothing on a typical run and add almost nothing.
  **It is aimed at the NIGHTLY, not at every run** — that was the old daily `17 21 * * *`, which is not
  coming back — so **a run you start by hand still needs a dispatch to settle it.** The page's `may
  still rise` caveat is derived from the clock and expires after two hours, so past that a
  hand-started run's low number reads as settled whether or not it is. Note GitHub disables a
  schedule after 60 days of repo inactivity, silently; if that bites, the `workflow_run` read still
  populates the column, it just never gets raised.
  One run is about **two DAX queries** (`discover_columns()` plus one `read_cu()` per capacity, and
  `CU_CAPACITY_ID` is pinned to one), so the cadence is nearly free.
  **Do NOT add a high-water mark to narrow the floor.** It is one query either way — the aggregation
  is server-side, so a 14-day window costs no more requests than a one-day one — `max(old, new)`
  *depends* on re-reading the window, and narrowing the floor is exactly what produced the recorded
  "duckrun read 14.8 CU" moment.

- **TWO DIRECTORIES, AND THE SPLIT IS THE JOB EACH DOES.** `cu/` **EXPORTS** — `measure.py` reads the
  metrics model and writes `history/cu.json`, and that is all it does: Python, `requests`, no
  rendering. `dashboard/` **PRESENTS** — `app.js`, the `index.html` shell it lives in, `build.mjs`,
  and `app.test.mjs`: JavaScript, no third-party package of any kind, no bundler, no CDN. Neither
  imports the other and they share no file; what passes between them is `history/`, on disk and in
  git. Either can be deleted without touching the other, which is the same property `cu/` has always
  had against `benchmark/`.
- **THE PAGE IS DYNAMIC AND READS `history/` AT VIEW TIME, so PUBLISHING IS FOR THE VISUALISATION,
  NOT FOR DATA.** `site/index.html` is a SHELL — the stylesheet and `dashboard/app.js`, no numbers in
  it at all. `app.js` fetches `history/runs/*.json` and `history/cu.json` from
  `raw.githubusercontent.com` in the reader's browser on every load and does the whole join there. So
  a `Benchmark` run that commits a record, or a `measure` job that commits the ledger, shows up on the
  published page **with no deploy** — `gh workflow run Dashboard -f publish=false` is a complete
  top-up. Three things this pins down:
  - **It must be `raw.githubusercontent.com`, never the Pages origin.** Raw serves the repo's own
    files with `Access-Control-Allow-Origin: *` and a ~5 minute CDN TTL. Copying `history/` into
    `site/` would put the data back inside the published artifact and make every commit a republish
    again — the exact thing this removed. The repo is public and `history/` has always been
    committed, so this is not a new disclosure.
  - **The directory LISTING comes from the GitHub contents API**, because raw serves files and not
    indexes. That is 60 requests/hour/IP unauthenticated, one per page load. When it refuses, the
    page says so and names the limit — an empty page and a rate-limited API look identical to a
    reader, and only one of them means "nothing has ever been measured".
  - **DuckDB-WASM was considered and rejected.** ~300 KB of JSON, already in the shape the page
    wants; ~30 MB of wasm from a CDN to query it is a cost with no matching benefit. An ad-hoc SQL
    explorer over the records would be a different page, not this one.
- **`cu/dashboard.py` and `cu/report_html.py` ARE DELETED, and keeping one would have been the
  mistake.** Two implementations of the GUID join — one rendering markdown in Python, one rendering
  HTML in the browser — is exactly the drift the rest of this repo is built against, so the browser's
  is the only one. There is no `dashboard.md` and no job summary of the page any more. The whole
  Python suite moved with it: `node --test dashboard/app.test.mjs` is now what pins the join, the
  labelling rules and the chart, and `python -m pytest cu/ -q` pins the ledger alone.
  The port was verified row-for-row against the last Python render — 73 table rows and both charts
  identical on the real `history/`, with **one** difference: rounding TIES move by one in the last
  digit, because Python rounds half-to-even and JavaScript half-up (`1,378.5` printed `1,378`, now
  prints `1,379`). Display only.
- **`build.mjs` emits TWO files from the same module, and the second is why the offline copy still
  works.** `--out site/index.html` is the live shell; `--out dashboard.html --snapshot` inlines
  `history/` into a `<script id="snapshot">` for the per-run artifact, which has to open off a local
  disk with no network years later. `app.js` prefers an inlined snapshot over the network, so a frozen
  copy and the live page cannot disagree about a number — same module, same render path. The build
  parses the snapshot back out of the finished document (a truncated one renders as "no run records",
  indistinguishable from a repo nobody has measured) and CI checks the LIVE shell carries no snapshot
  (a page that ships its own data goes stale silently).
- **The page's knobs are QUERY PARAMS now, not dispatch inputs.** `?record=30776174056` renders one
  run alone, `?ref=`/`?repo=` read another branch or fork, `?table=` picks the layout table. A link to
  one run's page is a link. The `record` workflow input and `CU_RECORD` are gone.

- **EVERYTHING IS KEYED ON THE ITEM GUID, and that is the whole design.** The old reader matched item
  DISPLAY NAMES: `engine_of()` substring matching against `CU_ENGINES` in order, a `shared` column for
  anything ambiguous, a join to the app's lagging `'Items'` snapshot for names and kinds, and
  `sessionize()` guessing run boundaries from idle-hour gaps and repeated model names. All of it is
  deleted. Every item except `dbt_landing` is created and destroyed inside one run, so a GUID belongs
  to exactly one run; the class (`etl` vs `analytics`) comes from the `role` **we** recorded, not from
  an item kind read out of a snapshot. There is no `shared` bucket any more, because nothing is left
  that cannot be attributed.
- **THE REFRESH IS GONE, and the earlier claim that it was load-bearing is RETRACTED.** This file used
  to say a new item's CU "does not show up at all" without a pre-read refresh. That was the
  name-resolution failure misdiagnosed: an item resolving to no name failed the kind filter and its CU
  vanished from the report. The fact table is DirectQuery and carries `Item` and `Workspace Id` as
  columns of its own, so the workspace filter binds with no join and the GUID needs no resolving. What
  the refresh actually cost was real: Power BI throttles the REST API **per identity**, the service
  principal spent its budget, and on runs 30685959678 and 30691130030 every attempt drew 429 while a
  human refreshing by hand went straight through — leaving 41,887 CU of DuckDB compute in `shared`.
  Do not reintroduce it.
- **NO REFRESH IS NEEDED, and this is MEASURED** (2026-08-02, DAX against the live model). Two item
  GUIDs carried CU in `Metrics By Item Operation And Hour` — 7,654.8 and 33.2 — while being absent
  from the `'Items'` dimension **entirely**, both active AFTER the model's last refresh. The fact
  table is DirectQuery and reads live; `'Items'` is import-mode and only moves on refresh; this
  reader never joins it. A **deleted** item also keeps its rows: run 30743411308's `dbt_spark` was
  created 10:16 UTC and deleted 10:34, and reads 30,940.3 CU — matching the app's own Items view to
  the decimal. `measure.py` run against the live model found **6 of 6** recorded items across two
  records, deleted ones included.
  `coverage()` keeps checking it every read (`<record>: 2/2 recorded item(s) found`, `unfound` in the
  ledger's `reads` entry) — not because it is in doubt, but because it costs nothing and would notice
  if a future version of the app changed it.
- **The column names are MEASURED too, and `REQUIRED` leads with the real ones.** They are `Item Id`
  and `Datetime` — not `Item` and `Date Hour`, which is what the candidate lists tried first, so the
  reader worked only by falling through. Watch `Datetime` in particular: the table also has a `Date`
  column that is DATE-ONLY, and resolving `when` to it would compare the floor against midnight and
  silently widen every window. `Metrics By Item` also exists — one row per item, no time dimension,
  which is closer to what this wants — but with no date column there is nothing to floor and nothing
  to verify a floor against, and the hourly table summed per item gives identical totals.
- **THE LEDGER IS ONE NUMBER PER ITEM PER OPERATION, TWICE.** `history/cu.json` is
  `{"items": {GUID: {operation: CU}}, "seconds": {GUID: {operation: s}}}` and nothing else. The
  operation is in the grain for one reason only — it is the ONLY thing that separates compute from
  storage, which share an item. Three facts make everything else unnecessary: a
  DELETED item keeps its CU rows (verified by hand against the live model, which is why the teardown
  is unconditional); every item is deleted when its run ends, so a total can only ever be INCOMPLETE,
  never wrong; and a run's items belong to that run alone, so a total per item already is a total per
  run per engine. There is no hour grain, no per-run window allocation and no
  settle-and-freeze bookkeeping. There used to be all three, and removing them removed most of the
  file.
- **DURATION RIDES THE SAME READ, FOR FREE, AND ITS COLUMN IS OPTIONAL ON PURPOSE.** `Duration (s)`
  sits in the same Capacity Metrics row as `CU (s)`, so it is one more `SUM` in a `SUMMARIZECOLUMNS`
  that runs anyway — no extra request, no round trip, no capacity. It is the **only free source**:
  dbt's `run_results.json` never reaches the run record and the Fabric notebook cannot write one, so
  the alternative was plumbing per-leg timings back through `fabric_run.py`. It lives in `OPTIONAL`,
  not `REQUIRED`, and that distinction is load-bearing: `REQUIRED` is fatal by design, and a guessed
  column name in there would kill the CU read that works today to gain a number the page can live
  without. A miss logs what the table actually has and costs the two time sections only. `seconds` is
  a SIBLING of `items` rather than a nesting inside it, so both leaves stay plain floats and one
  `max(old, new)` rule serves both — same kind of quantity, same floor, so it can only grow.
- **Three rules, none of which needs any state, and each fails as a plausible number.** Only items
  the read RETURNED are touched, so one that has aged past retention keeps its last value —
  "upsert only, never remove", for free. `max(old, new)`, never a blind overwrite and never `+`: CU
  per item over a fixed window start only ever grows, so the larger value is the more complete one,
  which makes a re-read idempotent, an undercounted first read self-correcting, and an older item
  safe from truncation when the floor walks forward. Adding would multiply an item's cost by the
  number of reads. And the floor is the earliest recorded run start CLAMPED to `now - 14 days`, so
  one query covers everything still learnable and never more.
- **A record must be built and benchmarked to reach the page**, and `incomplete()` skips anything
  else by name. Run 30743411308 is the live example: its `bench` job was skipped by the `needs` bug,
  so it has an ETL half and no analytics, which on a chart reads as "querying spark was free". It
  lives in `history/runs/legacy/`.
- **A run that was never TORN DOWN renders WITH A CAVEAT, not rejected.** The creep is small and a
  missing column costs more than a caveated one, so `drifting()` marks such a run **still billing —
  N item(s) never deleted** in the sources table, the loudest of the three states because it is the
  only one that does not resolve by waiting. Run 30733912205 was the live example and was moved to
  `legacy/` for that, then moved back out; it is in `legacy/` again now for an unrelated reason —
  it built at `threads: 1`, which `variant()` cannot see, so it would have keyed to the same column
  as a `threads: 4` duckrun run.
  **Moving a record in or out of `legacy/` MOVES THE FLOOR**, which is easy to miss: `measure.py`
  derives it from the earliest remaining run start, so parking a record narrows the window and the
  items of any run outside it stop being read. That is what made duckrun read 14.8 CU for a moment —
  the ledger had only the trailing background hours. Re-dispatch `Dashboard` after moving one.
  `measure.py` deliberately does NOT filter on `incomplete()` — those items really did cost capacity
  and the ledger is the ledger; it is the PAGE that must only compare like with like.
- **`compute` against `storage` comes from the OPERATION, and it can only come from there.** They
  share an ITEM, which is measured, not assumed: `dbt_spark` [Lakehouse] bills 188,636 CU of `High
  Concurrency Session Livy Run` AND 20,268 of `OneLake Write via Redirect` against one GUID;
  `dbt_dwh` [Warehouse] bills 129,177 of `Warehouse Query` beside its own OneLake writes. An earlier
  version bucketed by the item's ROLE and was simply wrong for that reason. The rule is **every
  `OneLake …` operation is storage, everything else is compute**, checked against every operation
  name on the capacity. A dash means no operation of that kind was billed there — an iceberg
  lakehouse is 40,832 CU of pure OneLake, because its compute is the notebook, a different item.
  `analytics` is one bold row: a class is decomposed only when some column holds more than one
  bucket.
- **EVERY LAKEHOUSE HAS A PAIRED SQL ANALYTICS ENDPOINT, and it is a separate billable item.** Kind
  `Warehouse`, same display name, different GUID: `dbt_spark` 306.3 CU, `dbt_iceberg` 245.7,
  `dbt_delta` 278.9, `dbt_dwh_src` 54.5, all of it `SQL Endpoint Query`. It was invisible to the run
  record and therefore to the ledger's join until `provision.py` started reading
  `properties.sqlEndpointProperties.id` and recording it under the role `sql_endpoint`. It is in
  `TEARDOWN_KEEP` for a different reason from landing and the folder: it is not ours to delete —
  Fabric removes it with its parent lakehouse, so a DELETE would fail or race.
  **`dbt_landing` HAS ONE TOO, and it is the one door landing CU got onto the page through.** The
  role is `sql_endpoint`, not `landing`, so `NON_ENGINE_ROLES` never saw it: the same item
  (`A8CF6202-…`, `created: false`) is in EVERY run record and charged EVERY engine 130.4 CU it did
  not spend. `dashboard/app.js`'s `landingGuids()` catches it by NAME against the record's own `landing`
  items — nothing hardcoded, and an engine's own endpoint is untouched. It distorted more than a
  total: that endpoint bills 130.4 CU over 83.2 s, a rate of **1.6**, against a 64-vCore notebook's
  **32.0**, so blending them made duckrun and iceberg — the same DuckDB in the same notebook at the
  same vCores — read 28.5 and 26.4. Excluded, both read 32.0.
- **A fresh run is a LOWER BOUND and the page says so per column.** Dispatch `Dashboard` twice: the
  second read returns bigger numbers and `max()` takes them. "May still rise" on the page is DERIVED
  from `run.finished` being under two hours old — a property of the clock, not a flag written into a
  file that then has to be kept in step.
- **THE TWO CU BAR CHARTS ARE DELETED. There is ONE chart on the page** — the line chart under
  *Cost and speed by parquet layout*, one line per layout, warm ms to cold ms on a shared log axis
  at the height of its CU. `chartSvg`, `barPath`, `groupRows` and the `.bar` rules went with them.
  They were `Capacity units per parquet layout` and `Capacity units per engine build`, stacked,
  analytics first. **The reason is NOT that the build half stopped mattering** — it still carries
  the sharpest operational result here, **duckrun costs 1.8× at 64 cores for the same wall time**
  (705s/13,083 CU at 32 against 692s/23,992 at 64), which the analytics keying structurally CANNOT
  show because both wrote identical parquet. It is that **both drew a figure the page already prints
  as a figure one block away**: the analytics bar was the `CU` column of *Cost and speed by parquet
  layout*, the build bar the `etl` row of *Cost by engine*. A bar length is a worse way to read a
  number you can be told. **That no-loss claim is what to re-check before restoring one** — a test
  pins it, and a chart brought back for a number no table carries is a different argument from the
  one that removed these. `spreadFor` survives; the noise floor uses it.
  **The GROUPING survives too, and now surfaces as table rows and as the chart's lines.** Read the
  rest of this bullet as describing a layout GROUP, not a bar.
  Power BI never sees the engine: it opens parquet
  through Direct Lake and transcodes row groups, so what a query costs belongs to what was WRITTEN
  and the writer is metadata. The group is **named for its writer and described by the shape** —
  `spark readHeavyForPBI` over `V-Order · 10–11 RG`: the grouping is the layout, but a file
  count is a poor NAME even when it is the real subject, so the shape sits underneath where it
  explains why two writers would ever share a group. **Only ROW GROUPS are printed** — segments
  are what drive Direct Lake's transcode/scan cost and the file count was a second number saying
  less; the file BAND still separates groups, it just is not printed. **A sorted group names
  its sort columns** (`date, time · 9 RG`), **and that key comes off the RECORD — never a constant
  here.** THE KEY IS A PROPERTY OF THE COMMIT: the model declared `['date','time','DUID']` for a
  while and `['date','time']` since, so a constant in `app.js` is right only for today's model, and
  briefly was not — it captioned run 30955591822, a DUID sort, `by date, time`, which is the exact
  class of quiet lie this page is built against. Two spellings, both read, neither preferred:
  `dbt.<engine>.sort_by` is what the run DECLARED (`stats.py`'s `declared_sort_key()`, a literal-list
  regex over the model in its own checkout — the notebook cannot write to the record, so the
  manifest never reaches here), `dbt.<engine>.sort_by_auto` is what duckrun's picker RESOLVED
  (`fabric_run.py`'s log scrape, and the only witness for an `'auto'` run, whose declaration names no
  columns). **A sorted run with no key recorded reads `true`** — it shares a group with neither an
  unsorted run nor any named sort, and it adds no columns to say, because the label already says
  `sorted`. The five records predating `declared_sort_key()` were **backfilled** from the model at
  the SHA each ran; the two `'auto'` ones were deliberately left alone, their scrape being the better
  source. **Build CU stays keyed per COLUMN** — `Cost by engine` — because there the writer and the
  compute it was given are the entire subject.
  What forced the layout keying: duckrun at 64 cores and at 32 wrote 4 files and 27 row groups either
  way, so two entries 50% apart was not a comparison — it was one layout measured twice, presented as
  two results.
  **A GROUP'S NUMBER IS THE MEDIAN OF ITS RUNS, NOT THE MEAN — `groupMid`, and everything that
  summarises several runs goes through that one function** (`Cost and speed by layout`, the line
  chart and the mart rows are one measurement shown three times; deriving the middle separately is
  how a page plots 1,582 above a row reading 1,781). Measured: run 30966983384 read **2,629.3** analytics CU
  against 1,331.5, 1,577.1 and 1,586.7 for **byte-identical parquet** — 1 file, 9 RG, same sort —
  because its XMLA read billed 49s against ~33s and its model refresh took **28.4s against ~8s**.
  Nothing about the parquet makes a refresh 3.5× longer; the capacity was busy. Under a mean that one
  dispatch lifted its figure 11% and dwh's 16%, i.e. the page reported Fabric's weather as a property
  of the layout. **What the median does NOT fix, and must not be sold as fixing: at n=1 and n=2 it IS
  the mean**, and four of nine groups are that thin — it dampens an outlier once there are three
  samples, and only more dispatches make one trustworthy. min/max are still measured and stated in
  `Every run`'s per-run rows, so the median is what the page claims and the spread stays checkable.
  **Grouping is MEASURED, labelling is DECLARED — with ONE stated exception.** The key is
  `(V-Order, band(row groups), sort columns, ENGINE)`, the first two from the parquet as `stats.py`
  read it. **Two engines never share a bar** — a reversal of the older "Power BI never sees the
  engine" reading, which holds only if the key captures everything Power BI can tell apart, and it
  does not: there is no SIZE element, so duckrun at 777–1,006 MB merged with `spark
  readHeavyForSpark` at 1,235 MB under the label `duckrun, spark readHeavyForSpark`; the caption comes
  from `LAYOUT_CONFIG` so it does not re-word itself whenever a record lands. The sort element is
  the resolved COLUMN LIST, not a boolean, so two sorts on different keys never share a bar — the
  `['date','time','DUID']` and `['date','time']` runs split by file band today only by luck.
  **IT GROUPS RUNS, NOT COLUMNS, and that was got wrong for one release.** A column is
  `(engine, config)` and the layout is measured per RUN, so two runs of one column can write different
  parquet — which is not hypothetical: `duckrun·64c+sorted` wrote **3 files / 26 RG** under an explicit
  `sort_by=['date','time','DUID']` (run 30805417412) and **4 files / 25 RG** under the `sort_by='auto'`
  the picker resolved to `['date','time']` (run 30809945203). `layoutGroups` keyed the COLUMNS and
  `spreadFor` then poured every run of each into one entry, so those two landed together valued at
  their mean — **2,041.8, a number neither run measured** — described as `4 files · 25 RG` from the
  newer record alone, because `layoutLabel` ranged over columns too. Per run they are two entries
  sharing the writer `duckrun sorted`, at 2,454.1 and 1,629.5, and the shape is what tells them apart.
  Two rows with one writer is the correct output, not a defect to tidy: the writer answers who wrote
  it and the key answers what. A run whose record carries no file count falls back to its COLUMN
  rather than to a group of its own — the "two unmeasured layouts are not one layout" rule below is about two
  different columns and still holds, but one column's own runs must not split into a bar each with no
  caption able to say why.
  **`sorted` is the exception and it is DECLARED**, because nothing in `stats.py` measures sort
  ORDER — there is no column for it, so the config the run recorded is the only witness that the
  parquet differs. It earns its place the same way V-Order does (a write-time reordering of what
  Power BI transcodes; V-Order is already element `[0]`), and leaving it out is not neutral: the
  `sort_by='auto'` duckrun run wrote **4 files either way** and moved 27 → 25 row groups, i.e. the same
  bands, so it would have shared a bar with the unsorted run and had its cold/warm/hot means averaged in —
  destroying the comparison the flag exists to make. A record with no `sorted` key groups WITH an
  unsorted run rather than opening its own bar: all 13 pre-input records demonstrably wrote unsorted
  parquet, so absence here is not the "unmeasured" case below. If this ever needs to become
  measured, per-file `date` min/max from the Delta log says whether files cover disjoint date ranges,
  which is what a date sort actually produces.
  **Banded to powers of two, never exact** — exact
  equality splits dwh's own two runs from each other (78 files and 80, same writer, incremental
  drift) and splits duckrun on 1.1 MB of size. Accepted cost: 15 row groups and 17 land in different
  bands. A record with no file count keys to `None` and keeps its own bar — two UNMEASURED layouts
  are not one identical layout, and merging them would claim Power BI cannot distinguish two things
  nobody looked at.
  It surfaced two things the old chart hid: V-Order on and off sit in the SAME file band and differ
  2.8× (1,332 against 3,769), which is the sharpest experiment on the page; and NEE on and off
  produce the same layout, so the gap between them was never an NEE effect.
- **A LAYOUT ROW IS A WRITER, and `producer()` decides what that means.** `spark V-Order`,
  `spark default`, `duckrun` — not `spark·V-Order+NEE`, not `duckrun·64c`. `LAYOUT_CONFIG` is
  `("resource_profile", "sorted")` and the exclusions are **measured, not tidiness**: duckrun wrote 4
  files and 27 row groups at 64 cores and at 32, spark wrote the same layout with NEE on and off, so
  neither reaches the parquet and neither belongs on a chart about parquet. `sorted` is the reverse
  case and needed no measuring to admit — it is *nothing but* a physical ordering of the rows, so it
  reaches the parquet by definition. Its caption comes from `CONFIG_LABEL`, keyed `<key>=<value>`
  rather than by value: `PROFILE_LABEL` can be value-keyed because a profile NAME says which knob it
  is, and a bare `true` does not. The LABEL is just **`sorted`**, not the column list —
  `duckrun sorted by date, time, DUID` spent a wide label on a detail beside `spark V-Order`, which
  does not spell out what V-Order does either. The columns live in the CAPTION instead
  (`by date, time · 9 RG`), where the shape already sits. `PROFILE_LABEL` names a profile
  by its EFFECT (`readHeavyForPBI` → `V-Order`, `writeHeavy` → `default`) because that is the only
  thing a reader of this page wants from it; an unmapped profile keeps its own name rather than being
  guessed at — `readHeavyForSpark` reads like it enables V-Order and sets no vorder at all.
  `ENGINE_LABEL` does the same job for an engine whose TARGET name misleads: `iceberg` is written
  **`duckdb iceberg`**, because it reads as a format beside three engines when the writer is the same
  DuckDB duckrun uses, pointed at an Iceberg REST catalog instead of delta-rs — and on a page about
  what got written, that is the entire reason the pair exists. It names the **COLUMN** as well
  (`duckdb iceberg·64c`), so the page calls that engine one thing throughout instead of `iceberg` in
  every header and `duckdb iceberg` in every layout row. That is only safe because **`baseEngine`
  reverses it** — `STACK`, the adapter caption and the (engine, variant) join to a record stay keyed
  on `iceberg`, and without the reversal each would silently MISS rather than raise: a blank caption,
  a chart row quietly gone.
  `variantTag()` still names columns everywhere the ENGINE is the subject — `Cost by engine`, the CU
  table, the sources table — but it now **shares `PROFILE_LABEL`**, so a profile reads the same in a
  header as in a caption, and it **omits a flag that is OFF**: `spark·V-Order+NEE` against
  `spark·V-Order`, never `spark·readHeavyForPBI+noNEE`. That header appears in every table and both
  charts, so its width is a real cost, and `+noNEE` was spending it to say nothing happened.
  Absence-means-off is only unambiguous while every config of the engine RECORDS the flag —
  a record predating the dispatch input has no key at all and would collide with an explicit `false` —
  so `columnsFor` checks for a collision and falls the whole engine back to the explicit spelling.
  Two identical column headers is the failure it prevents, and it is silent about why.
  **The MART block's rows ARE the chart's lines** — same grouping, same members, same median — and every
  other block stays one row per DECLARED writer. That split replaced "two directions onto the same
  rows", which held only while no writer produced two layouts: the mart block is the only one carrying
  CU and the query tiers, so it is the only one where a row spanning two shapes prints a number
  belonging to neither, and `duckrun sorted` did exactly that. Its other blocks are physical layout
  alone, describing tables the mart's shape says nothing about, so splitting them the same way would
  print one row twice for a difference that is not in it. The mart's CU and its cold/warm/hot are both
  the group's own runs' mean — a page printing 1,916 in a bar and 1,960 in the row under it is asking
  the reader which one it meant.
  There is **no `writer` column**: the row label is the writer, so it printed `duckdb (iceberg)`
  beside `duckdb iceberg` and `spark` beside `spark V-Order`. `STACK`'s third entry is now unread.
  **Row counts live in the block HEADING**, not a column: identical on every row by design — the
  parity statement the project rests on — so repeating 143,980,961 down a table is a wide column
  carrying one fact. When the engines disagree the heading says so and the column returns.
  **For the MART that branch is now unreachable, and the signal MOVED** — see the generation filter
  below. It still fires for every other table, where nothing filters on the count.
- **THE PAGE SHOWS ONE SOURCE GENERATION, DEFINED BY THE NEWEST RUN.** `sameGeneration()` takes the
  mart's `total_rows` from the latest record and DROPS every run that disagrees. The columns are
  different dispatches days apart and nothing else made them comparable: if the AEMO archive changes,
  an engine nobody has rebuilt keeps its column and its numbers sit beside engines built from
  different data, in the tables and inside the chart's own groups.
  **Newest wins, NOT the most common value**, and that is the point rather than a shortcut — right
  after a genuine source change the old count is still the majority, which is exactly the case this
  handles; a mode would keep the stale generation and drop the new run.
  **It runs BEFORE `columnsFor`, and the order is load-bearing twice.** `columnsFor` takes the latest
  run per (engine, config), so filtering after it would let a stale run hold a column; and
  `spreadFor` walks the whole `runs` array for the groups and ranges, so filtering the array is
  what stops a mean blending two generations. Both come free from filtering at that one point.
  **The exclusion MUST stay loud, and that is what pays for the heading it silenced.** `renderSources`
  names every dropped run — engine, run id, its own count and the delta against current — plus the
  reference, and the footer says `(+N excluded)`. A silent drop would trade the `row counts DISAGREE`
  shout for nothing; named, it is strictly sharper ("duckrun wrote 143,980,960 against the current
  143,980,961"). Do not quiet this down.
  Two behaviours that are deliberate: a run recording **no** count is KEPT (unmeasured is a different
  claim from different — the same distinction `layoutKey` makes by keying `null` to its own bar), and
  with no reference anywhere **nothing** is filtered rather than everything vanishing. `?record=`
  bypasses it entirely, because pinning a run means asking for that run.
  **The failure mode, stated on the page as well as here:** newest-wins cannot tell "the source
  changed" from "the newest run is broken", so a bad newest run excludes all the good history. That is
  survivable only because it is loud — the note calls out `N of M runs were excluded` and says the
  newest is then the likelier anomaly — and because the next good run reverses it on its own.
- **THE SECONDS GET NO CHART. Do not add one back** — and this is exactly why they are a table ROW
  and nothing more. The page carries ONE chart and its subject is capacity units against query time,
  the measures it leads with and can defend. A second, drawn from billed operation seconds that SUM
  across concurrent operations and are not wall clock, invites precisely the cross-engine ranking
  that the caveat withdraws. A number that needs a caveat belongs where the caveat can sit beside it
  — in the row label, as `compute seconds` does — not in a mark, where length alone reads as a
  ranking and there is nowhere to put the qualification.
- **THE PAGE CARRIES TWO NON-CU MEASURES NOW, and each states where its own number bends.**
  *cold / warm / hot* appear **TWICE, and the two placements answer different questions.** They
  come from the RUN RECORDS, not the ledger: every record already holds
  `benchmark.timings.<model>.<query>`, and `benchmark/render_report.py` renders it per dispatch — but
  a dispatch builds ONE engine, so that report always has a single column and its ranking is
  degenerate. The composed page is the only place the three tiers can be read ACROSS engines.
  **Per LAYOUT** in the `Cost and speed by layout` table, beside the analytics CU — a group's median
  over its runs, cheapest first, with a title and no commentary. **Per RUN** in the sources table,
  which is what actually measured them: one dispatch, against one semantic model it had just
  deployed. They used to be columns of the mart's LAYOUT block and are not any more: that made one
  table answer both what the parquet looks like and what querying it cost, and it forced a group mean
  onto a row about parquet — a number no single run recorded. `Table layout` is now physical layout
  alone, sorted fewest-files-first, because ordering by a CU column that is no longer printed is a
  ranking a reader cannot check.
  Three things easy to get wrong. **cold/warm/hot are PASS POSITIONS**, not the record's own
  `tier` field, which is the query CATEGORY (`probe`/`composite`/`raw`/`hot_only`) and names four
  different things. **Each tier is summed over the queries every run carries AT THAT TIER**, and
  the sets genuinely differ — the selectivity-ladder queries have no `cold_ms` at all, the top DUID
  being resolved after pass 1 — so cold is two queries short and the note counts each tier
  rather than leaving a small total to be misread. And it is **reimplemented, never imported**:
  `render_report._totals`/`rank` take exactly this shape, and `cu/` importing `benchmark/` would end
  the isolation that makes this directory deletable by removing one folder and one workflow file.
- **`compute seconds` is ONE ROW, ON `etl` ONLY, and it is a reinstatement.** It was removed once, on
  the grounds that billed operation seconds SUM across concurrent operations and so needed more
  hedging than they were worth. That objection is true and unchanged; what was re-decided is that
  "how long did the build take" deserves an answer, with the hedge carried IN THE ROW LABEL
  (`compute seconds` <sub>billed, not wall clock</sub>) rather than in a note four rows below where
  it is attached to nothing. Read it as billed time: a duckrun leg is one long notebook run so its
  seconds land close to the clock, while spark's five Livy REPLs under one session sum to more than
  the wall time anyone waited. Comparable freely between two runs of the SAME engine; across engines
  only knowing that.
  **`analytics` gets no such row, deliberately** — the query half already reports latency properly as
  the `cold`/`warm`/`hot` milliseconds beside the layout that produced them, and those are time a
  user actually waited. A second, differently-defined duration beside them would invite the two to be
  compared.
  **COMPUTE seconds, never total**, for the same reason the rate is: storage bills real CU over
  essentially no time, so storage durations are noise tracking OneLake traffic. It also makes the
  column RECONCILE — `compute` CU ÷ `compute seconds` is exactly the rate printed underneath, so a
  reader can check it against itself (duckrun·64c: 20,665.6 ÷ 646 = 32.0). Absent when the ledger has
  no `seconds`, and a DASH per column the ledger has not read; never `0`, which would say the build
  was instant.
- *`compute CU per second`* is a **ROW OF THE ENGINE TABLE, not a section** — it comes off the SAME
  Capacity Metrics row as the CU above it, so a table of its own restated the whole GUID→role→bucket
  join. A class the ledger has not read yet is a
  DASH, never `0.0`: a zero there says the engine did that work for free. **The RATE
  is the sturdiest number here** — the concurrency is in the numerator and the denominator
  alike, so it cancels; a high rate is a WIDE engine, not a slow one. **It is COMPUTE ÷ COMPUTE, and
  that is not a refinement — a total-over-total rate is simply wrong.** A storage operation bills real
  CU over a duration of essentially nothing (one `OneLake Write via Redirect`: 383.25 CU in
  **0.049 s**, a rate of ~7,800), so including storage does not dilute the rate, it detonates it, by
  an amount tracking only how much OneLake traffic the engine happened to make. `CU (s)` is literally
  capacity-units × seconds, so `CU ÷ duration` is capacity units DRAWN — for a single-node Python
  notebook that is **`cores` ÷ 2**, fixed for a given core count and NOT a constant: 32.0 at the 64
  vCores the dispatch defaults to, 16.0 at 32, and `cores` is a dispatch input that can be anything.
  So the check when this reads oddly is **two DuckDB legs at the SAME `cores` reading the SAME
  number** — never that they read 32. (The page cannot blend two core counts into one column anyway:
  `vcores` is part of `variant()`, so a 32-core duckrun and a 64-core one are separate columns, and
  the column tag prints the count on the chart — `duckrun·32c` against `duckrun·64c`; a bare
  single-config column gets it as a caption instead.) Both render **nothing** when their
  input is absent — a record with no tier timings adds no columns, a ledger with no `seconds` has no
  time section — which is the correct
  output: an absent section says "not measured", a table of zeros would say "free" or "instant".
  This is also why `### About these numbers` no longer says "seconds would need the caveat; CU is the
  bill" — it now has to say what the caveat IS, since the page prints seconds and milliseconds too.
- **`landing` CU IS NOT ON THE PAGE, and neither is anything else that is not an engine.** The
  page compares engines; `dbt_landing` is the ingestion staging area that no run deletes and every
  run reads, so its CU is one cumulative figure belonging to none of them. It briefly had a row of
  its own and the same number appeared under every column, which reads as "each of them spent this".
  `NON_ENGINE_ROLES` skips it and the `folder` outright. The archive's SIZE is still reported from
  `stats.py`'s listing — input volume is a different question from what ingesting it cost. With this
  gone there is no inexact attribution left anywhere on the page.
- **The measurement can fail; the page cannot — and they can no longer fail together, because they
  are separate workflows.** A throttled metrics model turns `Capacity units` red and leaves the page
  serving the last good ledger; it cannot make a publish stale, because publishing does not wait on
  it. `Dashboard`'s `page` job installs **no Python at all** — not even `requests` — which is what
  proves by running that the render path reaches no network of its own beyond the two documents the
  browser fetches.
- **The `page` job checks out `ref: ${{ github.ref_name }}`, not the default SHA — and the reason
  narrowed twice.** A default checkout takes the triggering commit, so the OFFLINE snapshot would be
  frozen from the branch as it stood there rather than from its head, missing any ledger commit in
  between. The LIVE page was already immune (it reads the branch head at view time — the whole point
  of the arrangement), and now the measurement does not even run in this workflow. The snapshot is
  still not immune, and that is all the `ref:` protects.
- **`Dashboard` is `push` to `dashboard/**` plus dispatch; `Capacity units` is `workflow_run` after
  Benchmark plus a 10:17 `cron` plus dispatch; `Benchmark` is a 07:17 nightly plus dispatch.** This replaced a blanket
  "`workflow_dispatch` only" that applied when one workflow both measured and published. What each
  reason protects now: for `Benchmark`, capacity — unchanged and absolute. For the page, that
  publishing is a decision — still true, and satisfied because pushing to `dashboard/` IS that
  decision; it still never republishes because a number moved. For the ledger commit, that no
  workflow which commits answers a push — still true, and the `dashboard/**` filter is what keeps it
  true. See the push bullet in *CI etiquette* for the full reasoning; do not weaken it from here.
- **Every real GUID is a SECRET.** `FABRIC_WORKSPACE_ID`, `CU_CAPACITY_ID`, `CU_METRICS_WORKSPACE_ID`,
  `CU_METRICS_MODEL_ID`. No tracked file outside `history/` holds one — the `model.bim`'s Direct Lake
  URL carries a zeros placeholder, and `deploy()` rewrites both ids anyway. Keep that placeholder's
  SHAPE though: `_ONELAKE_REF` matches 36 chars of hex and dashes and **raises** when nothing matches.
  An input's `default:` takes no context, so the secret fallback lives in job-level `env`;
  `measure.py` keeps no default of its own, because a hardcoded fallback would put the value back in
  the repo and outvote the secret whenever the env var arrived empty.
- **The `since` filter is verified, not trusted.** `CALCULATETABLE` with a plain boolean predicate,
  never `FILTER(VALUES(...))` inside `SUMMARIZECOLUMNS` — the latter is ACCEPTED and silently changes
  nothing, and three different windows once returned byte-identical totals before anyone noticed. The
  hour is projected and the returned range is checked against the floor; a mismatch dies rather than
  writing a ledger that includes excluded time.
- **One capacity per query.** These tables are DirectQuery and resolve one data location per query;
  passing several fails with an opaque `Internal Error: Error obtaining data location` naming neither
  the cause nor the capacity. Pinning `CU_CAPACITY_ID` also halves the request count on this tenant's
  two.
- **Column names are version-pinned; nothing hardcodes them.** Microsoft's own accelerator ships four
  DAX variants because the schema moves between app versions. `discover_columns()` reads the real
  schema with `INFO.VIEW.COLUMNS()` and fails naming what was actually present. This caught a real
  miss: the candidates said `Item Name`, the app says `Item`.
- **`CU_MODEL_OFFSET_HOURS` is the app's own offset (+10), not UTC.** It is `measure.py`'s alone now
  — it turns a run's UTC start into a floor in the model's clock. The page never sees it: it used to
  apply the same offset to turn a run window into ledger hours for the landing allocation, and that
  allocation is gone with landing CU. A wrong value reads as "no activity" rather than as an error.
- **THERE ARE TWO OFFLINE GATES, one per directory, and each job runs its own.** `python -m pytest
  cu/ -q` pins the ledger — the three rules, the settle conditions, that an absent item keeps its
  value — and runs in `measure` before the login. `node --test dashboard/app.test.mjs` pins the page
  — the GUID join, the compute/storage split, the layout banding, the chart, and that a variant tag
  never contains the column separator (`baseEngine` splits on it, so a tag carrying one would make a
  column id unparseable back to its engine and every `STACK` lookup would silently miss) — and runs in
  `page`, which installs no Python. Both are offline, tokenless and about a second.
- **`history/legacy/` holds five records from the name-matching era.** Nothing reads them. They carry
  no item GUIDs so they cannot be joined to a ledger, and their numbers were measured under an
  attribution that put whole notebooks in `shared`. Kept for a human, not for the code.
- **The isolation is the design, not an accident.** No imports from `benchmark/`, no `run_report.json`,
  no shared concurrency group, no ADOMD, no .NET, no duckrun — `requests` is the whole runtime
  dependency of the measurement and the render layer has none at all. It is built to be deleted by
  removing one directory and one workflow file. Do not "DRY it up" against `benchmark/xmla_compare.py`;
  the duplication is what keeps that deletion free.
- **There is no chart of CU over time, and that was tried.** A per-hour bar chart reads as noise at the
  hour bucket. The app's own chart is drawn at 30 seconds, and that resolution lives only in
  `'Timepoint Interactive Detail'` — one request per 30-second bucket, 120 per hour per capacity, rows
  carrying no timestamp column. Real capacity spent to redraw numbers already in hand. If it comes up
  again: that table smooths one operation across 10-128 buckets and **repeats its full CU in every
  one**, so summing it multiplies by one to two orders of magnitude while still looking plausible —
  which is why the aggregate table is the one being read.
