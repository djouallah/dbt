# benchmark — how fast is each engine's output to *query*?

`ci.yml` proves the four engines produce the **same rows**, and its `summary` job already reports how
each one physically wrote them. This adds the missing half: how long Power BI takes to answer the same
DAX against each engine's own copy of `mart.fct_summary`.

Three steps, and nothing else: **deploy a semantic model per engine, run the queries, report the
timings.** No table is built, no Delta log is read, no layout statistic is re-derived —
[`stats.py`](../.github/scripts/stats.py) owns that, and duplicating it here would just be a second,
slower reader of the same files. The only endpoints this touches are the Fabric control plane (to
deploy) and XMLA (to query).

**Manual only, non-gating.** `workflow_dispatch` on *Direct Lake benchmark*
([benchmark.yml](../.github/workflows/benchmark.yml)). It is never triggered by a push, and no other
workflow depends on it.

Ported from `djouallah/duckrun`'s `tests/parquet_layout/aemo/` (workflow `parquet_layout.yml`), which
in turn says it came from the AEMO project's own `benchmark/` — so this is where it started.

## Why there is no build step

Upstream had to **manufacture** the layouts it compared: it built a duckrun `SORTED BY AUTO` copy and
a Fabric Spark V-Order copy of one pristine fact, then benchmarked two semantic models over them.

Here they already exist. Four engines write the same table, and it *is* the same table — same rows,
four genuinely different physical shapes. Measured once against the live workspace, to establish the
premise (the live version of this is `ci.yml`'s `summary` job, not this run):

| engine | item | files | row groups | avg RG rows | size MB | vorder |
|---|---|---:|---:|---:|---:|---|
| duckrun | `dbt_delta` | 7 | 94 | 1,530,257 | 1035 | false |
| iceberg | `dbt_iceberg` | 386 | 1,175 | 122,420 | 1107 | false |
| spark | `dbt_spark` | 20 | 20 | 7,192,208 | 1217 | false |
| dwh | `dbt_dwh` | 79 | 79 | 1,820,812 | 1567 | false |

All four: **143,844,166 rows**. `dim_calendar` 3,197 and `dim_duid` 689 everywhere. Column names and
types are byte-identical across engines, *including case* — which is what lets one `.bim` template
serve all of them.

Two consequences of having nothing to build: `ci.yml`'s parity dashboard is untouched (no new table
can appear in its unscoped `get_stats()`, so it cannot read a benchmark run as drift), and re-running
is cheap in everything except capacity.

Note the `vorder` column, and that it is now **out of date**: that snapshot was taken when nothing
set `spark.sql.parquet.vorder.default` on the dbt-fabricspark session, so all four legs were
non-V-Order writers. `profiles.yml` now sets it in the spark target's `spark_config.conf`, which
makes `spark` the V-Order reference the upstream benchmark had to manufacture — for the *files it
writes from then on*. V-Order is a write-time layout: the numbers above describe parquet already on
disk and will only move as `fct_summary` is rewritten. Read `ci.yml`'s `summary` job, not this
table, for the current state — its `vorder` column is the live answer.

## What is compared

One semantic model per engine, named `aemo_<engine>`, over **every shared table that engine emits** —
the same eight `stats.py` reports on, in the schemas dbt writes them to:

| schema | tables |
|---|---|
| `mart` | `fct_summary`, `dim_duid`, `dim_calendar` |
| `landing` | `fct_scada`, `fct_price`, `fct_scada_today`, `fct_price_today`, `stg_csv_archive_log` |

The wide raw facts are a **column subset**: `fct_price` has ~130 columns and `fct_scada` ~55, nearly
all AEMO FCAS fields nothing here queries. Keys, timestamps, and the measure-bearing numerics are
carried; a Direct Lake column costs nothing until a query transcodes it, but it does cost anyone
reading the model. There is **one** `.bim`, deployed to all four engines, so one DAX suite runs
against four identical semantic surfaces by construction rather than by assertion.

Relationships wire each fact to `dim_duid` / `dim_calendar`, but **only `fct_summary`'s two set
`relyOnReferentialIntegrity`**. That flag lets the engine use an inner join, which silently drops rows
whose key is missing from the dimension. `fct_summary` is built with an INNER JOIN to `dim_duid`, so
its RI holds by construction; the raw facts carry retired units absent from the current AEMO
registration list — which is precisely what `stats.py`'s `duid_probe` exists to diagnose. Asserting RI
there would make the benchmark quietly measure fewer rows on the tables it is comparing.

### What is actually under test

**Identical DAX, identical semantic models, four dbt adapters.** The adapter that wrote the parquet
is the only variable, and everything above it is held constant on purpose: one `.bim`, one storage
mode, one query suite. `deploy()` therefore takes exactly one per-engine argument:

| knob | source | varies? |
|---|---|---|
| `lakehouse=` / `warehouse=` | `engines.KIND` | yes — which Fabric item holds the tables |
| `mode=` | `engines.DEPLOY_MODE` | **no** — one constant, `direct_lake`, for every engine |

Direct Lake is what makes the timing an answer about layout: a Delta→memory transcode (cold) and an
in-memory scan (hot), both shaped by how the files were written. `mode="direct_lake"` also sets
`directLakeBehavior: directLakeOnly`, so a query Direct Lake cannot serve **fails** rather than
falling back to the SQL endpoint and logging a pushdown time that would read as a slow layout.

**Why the mode is a premise and not a knob.** `dwh` was DirectQuery until duckrun 0.4.36, because
before `deploy(mode=)` a warehouse item could only be read that way. The intent was to label those
timings so nobody read them as a layout. It did not work, and the last DirectQuery run shows why:

| engine | mode | cold total (ms) | hot total (ms) | cold ÷ hot |
|---|---|--:|--:|--:|
| duckrun | Direct Lake | 63,437 | 3,990 | 15.9× |
| iceberg | Direct Lake | 180,298 | 3,829 | 47.1× |
| spark | Direct Lake | 69,449 | 4,000 | 17.4× |
| dwh | DirectQuery | 27,622 | 28,696 | **0.96×** |

A DirectQuery model has no transcoded data to evict, so its dehydrate is a **no-op that succeeds** —
not the failure the hot-only degradation was watching for. Fifteen "cold" samples got recorded that
were really just more pushdown queries, `dwh` entered the COLD totals, and the summary named it the
**cold winner** — 27,622 against duckrun's 63,437 — for the sole reason that it had no cold tier to
pay for. The ✔ went to the engine that never did the work being measured.

A warehouse's `Tables` are Delta in OneLake like any other item's — that is how `stats.py` has always
read them — so the asymmetry was never about the storage, and it is gone. All four are Direct Lake,
the second hand-authored template is deleted, and there is no per-engine `MODE` left to set: a
pushdown timing and a transcode timing are not the same measurement, and the only reliable way to
keep them out of one table is to not produce both.

## One job per engine, because a token lasts an hour

Every Fabric/XMLA token is valid for roughly an hour. One job walking four models over 21 queries
with two 600s idle gaps in it runs well past that, and the expiry lands mid-measurement on whichever
engine happens to be last — a run lost for a reason that has nothing to do with what is being
measured. So the paid work is a **matrix, one job per engine**: each mints its own token minutes
before it uses it and retires it with the job. `max-parallel: 1` keeps them serialized, because a
wall-clock benchmark cannot absorb two models contending for the same capacity, and `fail-fast: false`
keeps one engine's failure from costing the others their measurement.

Every step in those jobs is named `<engine> — …`, so the run's step list reads as the experiment
rather than as four indistinguishable copies of the same pipeline.

What the split costs: no process holds all four engines' timings any more, so **nothing computes a
ratio during the measurement**. Each job writes a report **fragment** and the free `report` job merges
them and renders. That is where the comparisons always belonged — `render_report.py` recomputed all of
them from the JSON anyway.

Two consequences worth knowing:

- An engine can be **missing entirely** (its job failed). `render_summary` names it against the
  dispatch's `engines` input rather than silently reporting three columns as a four-engine result.
- Each job resolves the hot-only ladder's DUID itself. Same rows in every engine means the same
  answer, but that is an expectation — the value is recorded per model and a disagreement is reported
  as a warning. `top_duid` on the dispatch (or `BENCH_TOP_DUID`) pins it.

## Pipeline

| step | job | script | notes |
|---|---|---|---|
| 1 | `checks` | [`test_verdicts.py`](test_verdicts.py) / [`test_templates.py`](test_templates.py) | Free gate, no Fabric. `needs:` on everything paid. |
| 2 | `resolve` | [`resolve_env.py`](resolve_env.py) | `WS_ID` + [`engines.py`](engines.py) → each engine's item GUID. Emits `PBI_WORKSPACE` (the workspace **display name** — XMLA addresses by name), `BENCH_ITEMS`, and the **engine matrix** the bench jobs fan out over. Resolving all engines here is the cheap early failure: a renamed item raises before any capacity is spent. Writes the `run` block as `report-00-meta.json`. |
| 3 | `bench (<engine>)` | [`deploy_models.py`](deploy_models.py) | This engine's model only, from the one template via duckrun's `workspace.deploy()`: `lakehouse=`/`warehouse=` rewrites the baked-in GUIDs, `mode=` forces the storage mode. Direct Lake, so it reframes. |
| 4 | `bench (<engine>)` | [`xmla_compare.py`](xmla_compare.py) | The payload: tiered DAX over ADOMD.NET, dehydrating per query for true cold timing. Measures **one** engine and computes **no** ratios — it refuses more than one, so there is only ever one orchestration shape. |
| 5 | `report` | [`merge_reports.py`](merge_reports.py) | Deep-merges every fragment in **basename order**, which is why the meta fragment is named to sort first: a per-engine fragment must not overwrite the shared `run` block. |
| 6 | `report` | [`render_report.py`](render_report.py) | Job summary + the derived `analysis` block — every ratio in the run is computed here. |
| 7 | `report` | [`render_summary.py`](render_summary.py) | Specialist findings. **Exits 1 if the printed ranking disagrees with the totals it came from** — the only thing here that fails the job. |

Everything lands in one `run_report.json` (uploaded as the `run-report` artifact); every number in
both reports recomputes from it offline. The per-engine fragments are uploaded too
(`report-fragment-<engine>`), so one engine's numbers survive a failure anywhere downstream of it.

## The query suite

25 queries in four tiers, in [`xmla_compare.py`](xmla_compare.py):

- **`probe`** (6) — one `fct_summary` column, full scan, scalar result. Cold time ≈ that column's
  transcode cost plus fixed overhead; `probe_rowcount` is the ~zero-column control, so subtracting it
  gives the marginal per-column cost. Cold **and** hot.
- **`composite`** (9) — realistic multi-column mart workloads. Cold and hot.
- **`raw`** (6) — one query per raw landing table, so nothing in the model goes unmeasured. Cold and
  hot. `raw_scada_mw` is the heaviest measurement in the suite: `fct_scada` is the largest table in
  the project, so a cold sum over one of its columns is the biggest Delta→memory transcode any engine
  here performs, and where a layout difference has the most room to show.
- **`hot_only`** (4) — a selectivity ladder (1 year → 1 month → 1 DUID → both). Hot only: row-group
  elimination is only visible once resident, since cold is dominated by full-column transcode.

21 of the 25 are cold-measured, so each engine pays 21 × `cold_repeats` dehydrate→query cycles —
21 at the default of 1, 63 at the old default of 3. That is the bulk of the run's cost, which is
why the default was cut to 1: this job runs on shared, billed capacity and the dehydrate cycles are
what an interactive-CU spike looks like to a capacity admin. Scout first, and raise `cold_repeats`
only for a run whose result has to survive argument.

Cold is forced per query by **dehydrating** first — a TMSL `clearValues` evicts all transcoded
column data, then a `full` reframes (metadata only on Direct Lake) — so the next query pays the full
cold cost. Per query, not once, because the queries share the big fact columns. `COLD_REPEATS` cycles
give a median and a spread instead of an n=1 point.

Rankings use **medians, never means** (one capacity spike among 110ms runs blows up a mean and
fabricates a winner), and hot runs 1 and 2 are dropped as the warm transition.

**The fastest engine wins a row, by any margin — there is no tie band.** There used to be one: a
per-query gap smaller than the larger of the two spreads was called a tie. It was removed because
of what it did to the side-by-side table. `best` was computed as best-vs-*second*-best, so on a
four-engine run iceberg beating spark by 2ms printed `tie` on a row where dwh was 4× slower than
either — and **every** row came out `tie`, which reads as "all four engines are equal". The exact
times are right there in the row; a reader can judge whether 2ms matters far better than a rule
that erases the winner and says nothing about the engine that lost by 300ms. Spread is still
measured and still reported per query (§2 of the specialist findings), it just no longer decides
who won.

Note the **rank follows the summed totals**, not the per-query win count, so the two can disagree —
"spark fastest (5 query wins)" beside "duckrun 1.02× (14 query wins)" means duckrun won most queries
and lost the one expensive one. Both are printed and neither is corrected against the other.

## No baseline

There is **no reference engine**. Upstream had a real one — it built a candidate layout and compared
it against the existing one — and this repo inherited the shape: `BENCH_ENGINES[0]` was the reference
and every ratio read `base ÷ challenger`. But these four engines are *peers*. A baseline made every
number in the report depend on the order the dispatch happened to list them in, and made
"iceberg 1.30× faster" unreadable without remembering which engine the reference had been.

So the engines are **ranked**, and every ratio is stated as `× fastest` — against the fastest total
of that metric, which is a property of the measurement rather than of the input list. Follow-on
effects worth knowing:

- side-by-side column order is **alphabetical**: the only order that is both neutral between peers
  and stable enough to read two runs against each other (ordering by result moves the columns
  whenever the winner changes);
- an engine whose job failed is just a **missing column**, named in the findings — it used to be a
  run-invalidating event when it happened to be the reference;
- the fatal guard is `render_summary.verify_ranking`. A ratio *orientation* inversion is no longer
  expressible, so what it checks is that the printed ranking agrees with the totals it was derived
  from: ordered by total, rank 1 the lowest, `× fastest` ≥ 1. Still fatal, for the original reason —
  a table naming the slower engine the winner is worse than no table.

**The defaults trade statistical strength for capacity cost, deliberately.** At `cold_repeats=1`
and `runs=3` there is exactly one measured sample per query per tier, so "median" is that sample
and both spreads are 0 — which means `render_summary`'s >25%-cold-spread noise filter flags
nothing. Read a default-inputs run as a *smoke test with timings*, not as a defensible ranking.
`cold_repeats=3 runs=5` (the previous defaults) is what a result worth quoting costs.

## Running it

**CI, and only CI.** Dispatch *Direct Lake benchmark*. Inputs: `workspace`, `engines` (order is the
order they are **measured** in — index 0 is simply the job that skips the idle gap; no number in the
report depends on it), `runs`, `cold`, `cold_repeats`, `gap_seconds` (applied *before* each engine
after the first), `top_duid` (optional pin for the hot-only ladder).

There is no supported way to run the paid part from a laptop: `xmla_compare.py` measures one engine
per process and the workflow is what fans it out. A cheap scouting **dispatch** — end to end in
minutes instead of an hour of capacity:

```
engines=duckrun,spark  runs=1  cold=false  cold_repeats=1  gap_seconds=0
```

**Free, locally, before pushing** (no credentials, no Fabric — this is the CI gate, and it runs as
a `needs:` on the paid job):

```bash
python -m pytest benchmark/ -q                                     # ranking + template checks
RUN_REPORT=some_run_report.json python benchmark/render_report.py   # re-render any past artifact
```

[`test_verdicts.py`](test_verdicts.py) pins the ranking layer: rank direction (ordered by total,
rank 1 lowest, `× fastest` ≥ 1), fastest-wins (a 1ms win is a win, and the `best` column names an
engine rather than `tie`), that no result depends on the engine order given and that no `reference()`
helper comes back, hot-only scoping (an engine whose dehydrate could not run), and comparable
totals.
[`test_templates.py`](test_templates.py) checks the `.bim` against duckrun's *own* repoint regexes and
pins the deploy wiring: that `DEPLOY_MODE` is one constant duckrun's own `_normalize_mode` accepts and
that no per-engine `MODE` has crept back, that `deploy_kwargs` pairs `warehouse=` with the warehouse
item and `lakehouse=` with a lakehouse, and that exactly one template exists. Everything it asserts
would otherwise fail at deploy time, after ADOMD.NET is installed and the workspace resolved — partway
through a run that has already spent capacity on the engines before it.

One trap worth keeping in mind if anyone reintroduces a hand-authored DirectQuery `.bim` instead of
using `mode=`: `_is_directlake_bim()` greps the raw bytes for the camelCase Direct-Lake token, so
**a description string naming the mode is enough** to flip it and make deploy attempt a reframe the
model cannot serve. Prose counts. (That one was caught for real, by that test, in the template that
has since been deleted.)

**Checking the premise still holds** — the tables are at parity and each engine wrote them
differently — is `ci.yml`'s `summary` job, or the same read from a laptop
(CLAUDE.md: *"Query the lakehouses directly before instrumenting CI"*):

```python
import duckrun
duckrun.connect("abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item-guid>/Tables",
                read_only=True).get_stats("mart.*")
```

On Windows leave `CURL_CA_INFO` unset — ci.yml's Linux CA path makes the parquet footer read fail
with an SSL error that looks like a credentials problem.

## Prerequisites

- The repo's federated identity (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID`, the same two secrets
  `ci.yml` uses) needs access to the workspace, and enough rights to create semantic models and run
  a TMSL refresh (cold timing needs write; without it the run silently falls back to hot-only).
- `mart.fct_summary`, `mart.dim_duid` and `mart.dim_calendar` must already exist in each engine's
  item — built by `ci.yml`. This reads them and never writes.
- XMLA read/write must be enabled on the capacity.
