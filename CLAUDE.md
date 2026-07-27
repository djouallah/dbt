# Working in this repo

One dbt project, four engines (`duckrun`, `iceberg`, `dwh`, `spark`), one landed copy of the
data. The thesis is *the engine doesn't matter, the output does* — so the models are written
per dialect (`models/duckdb`, `models/dwh`, `models/spark`, gated by `+enabled` in
`dbt_project.yml`) and one neutral DuckDB suite grades all four outputs by reading them through
Delta on OneLake.

The traps below have all been hit for real. Each one cost a CI run or worse.

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
is a branch you didn't test.

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

| target | strategy | why not something else |
|---|---|---|
| `duckrun` | `merge` | `delete+insert` in this adapter is a fenced **full-table overwrite** — it materializes every surviving target row plus the batch into a DuckDB temp table, then overwrites. On a 143M-row table that is a full rewrite *every run*. `merge` goes through delta-rs, which prunes target files from the source's own stats. |
| `iceberg` | `merge` + `when_matched: do_nothing` | The OneLake Iceberg REST catalog rejects a matched-UPDATE branch: `BadRequest 400`, one add-snapshot update per commit. Insert-only is sufficient because every input is append-only. |
| `spark` | `merge` | — |
| `dwh` | `delete+insert` | Real T-SQL DELETE+INSERT, cheap. Never `--full-refresh` here: on dbt-fabric that DROPs and recreates, which deadlocks Fabric's background stats maintenance, loses grants, and rebinds Direct Lake. Use `REBUILD_SUMMARY=1` instead. |

Before changing a strategy, read the adapter's own source rather than assuming the name means
what it does elsewhere. duckrun's lives in `dbt/adapters/duckrun/delta_plugin.py`.

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
- Repair lever: `dbt run --full-refresh -s fct_summary` on the merge engines,
  `REBUILD_SUMMARY=1` on dwh.

## Where the DuckDB fold runs

Decided per engine by `.github/scripts/pending_files.py`: archive-log rows whose
`csv_filename` isn't yet in the consuming table's `[file]` column, compared against
`LOCAL_FOLD_MAX_FILES`. Small fold → GitHub runner; big fold → Fabric notebook, data-local to
OneLake.

Do **not** go back to "did a new daily file land this run". That describes the download, not
the backlog: a from-scratch lakehouse has ~3000 files outstanding with nothing new landed, and
that heuristic puts it on a 7GB runner.

Fail-safe direction is always Fabric — it handles a fold of any size, the runner doesn't. If
the count can't be measured, report a huge number.

## Facts that are easy to get wrong

- **XTable *does* convert Iceberg positional deletes** into Delta deletion vectors. Emitting
  deletes is not what forces `iceberg` to stay insert-only; the REST catalog's 400 on
  matched-UPDATE is.
- **Livy compute is workspace-side.** `spark_config` in `profiles.yml` cannot size the session;
  change the workspace Spark pool.
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
