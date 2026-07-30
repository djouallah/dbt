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
reading the model. Both `.bim` files are generated from one spec so their semantic surfaces are
identical by construction, and [`test_templates.py`](test_templates.py) asserts they stay that way —
that identity is what lets one DAX suite run against a Direct Lake and a DirectQuery model alike.

Relationships wire each fact to `dim_duid` / `dim_calendar`, but **only `fct_summary`'s two set
`relyOnReferentialIntegrity`**. That flag lets the engine use an inner join, which silently drops rows
whose key is missing from the dimension. `fct_summary` is built with an INNER JOIN to `dim_duid`, so
its RI holds by construction; the raw facts carry retired units absent from the current AEMO
registration list — which is precisely what `stats.py`'s `duid_probe` exists to diagnose. Asserting RI
there would make the benchmark quietly measure fewer rows on the tables it is comparing.

| mode | engines | template | what the timing means |
|---|---|---|---|
| **Direct Lake** | `duckrun`, `iceberg`, `spark` | [fct_summary.SemanticModel](fct_summary.SemanticModel/) | Delta→memory transcode (cold) and in-memory scan (hot). The physical layout *is* what is being measured. |
| **DirectQuery** | `dwh` | [fct_summary_dq.SemanticModel](fct_summary_dq.SemanticModel/) | SQL endpoint pushdown. A different engine, not a different layout. |

`dwh` being DirectQuery is a deliberate choice for now, not a fallback. It has three consequences the
code handles explicitly rather than hiding:

- **Hot only.** There is no transcoded column data to evict, so `dehydrate_model` fails and
  `bench_model` degrades it to hot timing — the expected path, and the log says so.
- **No refresh at deploy.** duckrun refreshes a Direct Lake model (a reframe) and deliberately does
  not refresh a DirectQuery one, because it queries live. That is not a partial deploy.
- **Labelled everywhere it appears.** Its timings are real measured query times, but they are not the
  same *kind* of number — so every verdict carries its `mode` and the reports tag it, rather than
  pooling it with the Direct Lake three and letting the reader conclude "dwh has a slow layout".

Direct Lake over a *warehouse* item is untried. The warehouse's `Tables` do surface Delta over OneLake
(that is how `stats.py` reads it), so it remains available later.

## Pipeline

| step | script | notes |
|---|---|---|
| 1 | [`resolve_env.py`](resolve_env.py) | `WS_ID` + [`engines.py`](engines.py) → each engine's item GUID. Emits `PBI_WORKSPACE` (the workspace **display name** — XMLA addresses by name) and `BENCH_ITEMS`. |
| 2 | [`deploy_models.py`](deploy_models.py) | One model per engine via duckrun's `workspace.deploy()`, which rewrites the GUIDs / SQL endpoint baked into the template. Narrows `BENCH_ENGINES` to what deployed. |
| 3 | [`xmla_compare.py`](xmla_compare.py) | The payload: tiered DAX over ADOMD.NET, dehydrating per query for true cold timing. |
| 4 | [`render_report.py`](render_report.py) | Job summary + the derived `analysis` block, merged back into the one JSON. |
| 5 | [`render_summary.py`](render_summary.py) | Specialist findings. **Exits 1 on a verdict-direction inversion** — the only thing here that fails the job. |

Everything lands in one `run_report.json` (uploaded as the `run-report` artifact); every number in
both reports recomputes from it offline.

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

21 of the 25 are cold-measured, so at `cold_repeats=3` each engine pays 63 dehydrate→query cycles.
That is the bulk of the run's cost — scout first.

Cold is forced per query by **dehydrating** first — a TMSL `clearValues` evicts all transcoded
column data, then a `full` reframes (metadata only on Direct Lake) — so the next query pays the full
cold cost. Per query, not once, because the queries share the big fact columns. `COLD_REPEATS` cycles
give a median and a spread instead of an n=1 point.

Verdicts use **medians, never means** (one capacity spike among 110ms runs blows up a mean and
fabricates a winner), hot runs 1 and 2 are dropped as the warm transition, and a per-query result
inside the larger of the two spreads is a **tie**, not a win.

## Running it

**CI:** dispatch *Direct Lake benchmark*. Inputs: `workspace`, `engines` (**order matters** — the
first is the reference every ratio is taken against), `runs`, `cold`, `cold_repeats`, `gap_seconds`.

A cheap scouting run — end to end in minutes instead of an hour of capacity:

```
engines=duckrun,spark  runs=1  cold=false  cold_repeats=1  gap_seconds=0
```

**Free, locally, before pushing** (no credentials, no Fabric — this is the CI gate, and it runs as
a `needs:` on the paid job):

```bash
python -m pytest benchmark/ -q                                     # verdict + template checks
RUN_REPORT=some_run_report.json python benchmark/render_report.py   # re-render any past artifact
```

[`test_verdicts.py`](test_verdicts.py) pins the verdict layer: ratio orientation, the tie rule,
explicit reference selection, DirectQuery scoping, and comparable totals.
[`test_templates.py`](test_templates.py) checks the two `.bim` files against duckrun's *own* repoint
regexes — everything it asserts would otherwise fail at deploy time, after ADOMD.NET is installed and
the workspace resolved. It also pins the sharpest trap here: `_is_directlake_bim()` greps the raw
bytes for the camelCase Direct-Lake token, so **a description string naming the mode is enough** to
flip the DirectQuery template and make deploy attempt a reframe it cannot serve. Prose counts. (That
one was caught for real, in this template, by that test.)

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
