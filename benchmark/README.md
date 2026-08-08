# benchmark — how fast is each engine's output to *query*?

`dbt.yml` proves the four engines produce the **same rows**, and its final `layout` job reports how
each one physically wrote them. This adds the missing half: how long Power BI takes to answer the same
DAX against each engine's own copy of `mart.fct_summary`.

Three steps, and nothing else: **deploy a semantic model per engine, run the queries, report the
timings.** No table is built, no Delta log is read, no layout statistic is re-derived —
[`stats.py`](../.github/scripts/stats.py) owns that, and duplicating it here would just be a second,
slower reader of the same files. The only endpoints this touches are the Fabric control plane (to
deploy) and XMLA (to query).

**A nightly plus `workflow_dispatch`, on *Benchmark*
([benchmark.yml](../.github/workflows/benchmark.yml)) — the same workflow that builds the tables.**
`cron: "17 7 * * *"` is 02:17 EST / 03:17 EDT, US Eastern asleep either side of the DST boundary,
which is the point: the query passes are interactive CU on shared capacity and should not land while
anyone is using it. This reverses a rule that said a human starts every run; what that rule protected
is unchanged and now accepted rather than avoided, and the cron is one line to remove.
**`push`, `workflow_run` and `repository_dispatch` are still never used** — that workflow commits the
run record, so a push trigger would let its own commit start the next paid build.
⚠️ A scheduled event supplies NO inputs (dispatch defaults do not apply to `schedule`), so every
input there carries its own scheduled value; see the header of `benchmark.yml` before adding one.

**The timings are not what the published page reports.** `cu/` measures what the querying *cost* in
capacity units, and reads none of this directory's output — the engines are all fast, so the CU is
the interesting number. This still has to RUN for that report to have an analytics side at all: the
Direct Lake passes are what create the CU being measured.

Ported from `djouallah/duckrun`'s `tests/parquet_layout/aemo/` (workflow `parquet_layout.yml`), which
in turn says it came from the AEMO project's own `benchmark/` — so this is where it started.

## Why there is no build step

Upstream had to **manufacture** the layouts it compared: it built a duckrun `SORTED BY AUTO` copy and
a Fabric Spark V-Order copy of one pristine fact, then benchmarked two semantic models over them.

Here they already exist. Four engines write the same table, and it *is* the same table — same rows,
four genuinely different physical shapes. Measured once against the live workspace, to establish the
premise (the live version of this is the `layout` job of the `dbt` workflow, not this run):

| engine | item | files | row groups | avg RG rows | size MB | vorder |
|---|---|---:|---:|---:|---:|---|
| duckrun | `dbt_delta` | 7 | 94 | 1,530,257 | 1035 | false |
| iceberg | `dbt_iceberg` | 386 | 1,175 | 122,420 | 1107 | false |
| spark | `dbt_spark` | 20 | 20 | 7,192,208 | 1217 | false |
| dwh | `dbt_dwh` | 79 | 79 | 1,820,812 | 1567 | false |

All four: **143,844,166 rows**. `dim_calendar` 3,197 and `dim_duid` 689 everywhere. Column names and
types are byte-identical across engines, *including case* — which is what lets one `.bim` template
serve all of them.

Two consequences of having nothing to build: `dbt.yml`'s parity dashboard is untouched (no new table
can appear in its unscoped `get_stats()`, so it cannot read a benchmark run as drift), and re-running
is cheap in everything except capacity.

Note the `vorder` column, and that it is now **out of date**: that snapshot was taken when nothing
set `spark.sql.parquet.vorder.default` on the dbt-fabricspark session, so all four legs were
non-V-Order writers. `profiles.yml` now sets it in the spark target's `spark_config.conf`, which
makes `spark` the V-Order reference the upstream benchmark had to manufacture — for the *files it
writes from then on*. V-Order is a write-time layout: the numbers above describe parquet already on
disk and will only move as `fct_summary` is rewritten. Read the `layout` job of the `dbt` workflow, not this
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

Direct Lake is what makes the timing an answer about layout: a Delta→memory transcode (the cold pass)
and an in-memory scan (warm, then hot), all shaped by how the files were written.
`mode="direct_lake"` also sets
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

A DirectQuery model has no transcoded data to evict, so the dehydrate of the day was a **no-op that
succeeded** — not the failure the hot-only degradation was watching for. Fifteen "cold" samples got
recorded that were really just more pushdown queries, `dwh` entered the COLD totals, and the summary
named it the **cold winner** — 27,622 against duckrun's 63,437 — for the sole reason that it had no
cold tier to pay for. The ✔ went to the engine that never did the work being measured. (That
dehydrate is gone — see *The session* below — but the lesson is about mixing two kinds of number in
one table, and it stands.)

A warehouse's `Tables` are Delta in OneLake like any other item's — that is how `stats.py` has always
read them — so the asymmetry was never about the storage, and it is gone. All four are Direct Lake,
the second hand-authored template is deleted, and there is no per-engine `MODE` left to set: a
pushdown timing and a transcode timing are not the same measurement, and the only reliable way to
keep them out of one table is to not produce both.

## One job per engine, because a token lasts an hour

Every Fabric/XMLA token is valid for roughly an hour. One job walking four models over 25 queries
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
- Each job resolves the selectivity ladder's DUID itself, after its cold pass. Same rows in every
  engine means the same answer, but that is an expectation — the value is recorded per model and a
  disagreement is reported as a warning. `top_duid` on the dispatch (or `BENCH_TOP_DUID`) pins it,
  which also lets the ladder run from pass 1.

## Pipeline

| step | job | script | notes |
|---|---|---|---|
| 1 | `checks` | [`test_verdicts.py`](test_verdicts.py) / [`test_templates.py`](test_templates.py) | Free gate, no Fabric. `needs:` on everything paid. |
| 2 | `resolve` | [`resolve_env.py`](resolve_env.py) | `WS_ID` + [`engines.py`](engines.py) → each engine's item GUID. Emits `PBI_WORKSPACE` (the workspace **display name** — XMLA addresses by name), `BENCH_ITEMS`, and the **engine matrix** the bench jobs fan out over. Resolving all engines here is the cheap early failure: a renamed item raises before any capacity is spent. Writes the `run` block as `report-00-meta.json`. |
| 3 | `bench (<engine>)` | [`deploy_models.py`](deploy_models.py) | This engine's model only, from the one template via duckrun's `workspace.deploy()`: `lakehouse=`/`warehouse=` rewrites the baked-in GUIDs, `mode=` forces the storage mode. Direct Lake, so it reframes. |
| 4 | `bench (<engine>)` | [`xmla_compare.py`](xmla_compare.py) | The payload: one user session over ADOMD.NET — `runs` passes over the whole suite, pass 1 cold, 2 warm, 3+ hot, nothing cleared between them. Measures **one** engine and computes **no** ratios — it refuses more than one, so there is only ever one orchestration shape. |
| 5 | `report` | [`merge_reports.py`](merge_reports.py) | Deep-merges every fragment in **basename order**, which is why the meta fragment is named to sort first: a per-engine fragment must not overwrite the shared `run` block. |
| 6 | `report` | [`render_report.py`](render_report.py) | Job summary + the derived `analysis` block — every ratio in the run is computed here. |
| 7 | `report` | [`render_summary.py`](render_summary.py) | Specialist findings. **Exits 1 if the printed ranking disagrees with the totals it came from** — the only thing here that fails the job. |

Everything lands in one `run_report.json` (uploaded as the `run-report` artifact); every number in
both reports recomputes from it offline. The per-engine fragments are uploaded too
(`report-fragment-<engine>`), so one engine's numbers survive a failure anywhere downstream of it.

## The query suite

25 queries in four tiers, in [`xmla_compare.py`](xmla_compare.py):

- **`probe`** (6) — one `fct_summary` column, full scan, scalar result. In the cold pass each probe is
  the first query to touch its column, so its time ≈ that column's transcode cost plus fixed
  overhead; `probe_rowcount` runs **last** among the probes, by which point everything is resident,
  so it is the ~zero-column control and subtracting it gives the marginal per-column cost. That
  ordering is load-bearing and `test_verdicts.py` pins it.
- **`composite`** (9) — realistic multi-column mart workloads.
- **`raw`** (6) — one query per raw landing table, so nothing in the model goes unmeasured.
  `raw_scada_mw` is the heaviest measurement in the suite: `fct_scada` is the largest table in
  the project, so a cold sum over one of its columns is the biggest Delta→memory transcode any engine
  here performs, and where a layout difference has the most room to show.
- **`hot_only`** (4) — a selectivity ladder (1 year → 1 month → 1 DUID → both).

Every query runs in every pass. The tier is descriptive — it used to gate a per-query dehydrate, and
that is gone.

## The session

**This measures a user session, not the engine.** `deploy_models.py` **deletes and recreates** the
semantic model, so it starts with an empty VertiPaq store; this script then walks the whole suite
`runs` times and the **pass number is the tier**:

| pass | tier | what it is |
|---|---|---|
| 1 | **cold** | first visit — pays the whole Delta→memory transcode, once |
| 2 | **warm** | second visit |
| 3…N | **hot** | settled — median + spread over N−2 samples |

**With think time between queries** (`think_seconds`, default 4). A person reads a visual before
clicking the next one; firing 25 queries back-to-back was the last thing left in here that no user
does. The pause sits **outside every timed region** — `run_query` starts its clock after it — so it
changes what is reproduced, not what is measured, and it applies between every consecutive pair
including across a pass boundary (the user does not know where that is). Two costs worth knowing: it
adds `think_seconds × (25 × runs − 1)` of idle per engine — ~10 minutes at the defaults — and that
idle is **inside the token's ~1 hour life**, which is precisely the headroom the one-job-per-engine
split exists to protect. Raising it much further is how a run dies to an expiry mid-measurement.
It is not `gap_seconds`: that one is between **engines**, for capacity-chart separation, and it
elapses before the token is minted.

**Nothing is ever cleared.** What this replaced was a per-query dehydrate: `clearValues` + `full`
before *every* cold-tier query, 21 forced transcodes per engine per run. No user is ever in that
state, and `clearValues` clears the data cache — TMSL defines it as no more than *"Clear values in
this object and all its dependents"* — which is not a statement about transcoding cost. The session
shape is both more realistic and cheaper: one transcode instead of 21.

**A new dataset is the cold guarantee, and there is no non-destructive alternative.** All three were
checked, so none needs retrying:

| lever | why not |
|---|---|
| TMSL `clearCache` | clears query caches, **not** resident columns — DAX Studio's Clear Cache button issues it and Direct Lake queries stay fast afterwards. A hot→warm lever, not a cold one. |
| reframing / redeploy in place | framing is *incremental*: it drops only segments whose row groups changed and **retains dictionaries**. Semiwarm at best. |
| memory pressure, node reassignment | genuinely do produce cold state, but neither is commandable. |

The accepted cost is one extra item GUID per dispatch in the Capacity Metrics app's item list. It
does not break `cu/` — that tool resolves names live from the REST API *precisely because* a recreate
mints a new GUID — and the display name never changes, so CU stays attributed. The other trade: if
the delete succeeds and the deploy then fails, that engine has no model at all, where an overwrite
failure used to leave the previous one standing.

**The labels are session positions, not engine states.** Microsoft uses the same words more narrowly
([Understand Direct Lake query performance](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-understand-storage):
*warm* = data resident, VertiScan caches empty; *hot* = resident **and** caches populated), and by
that definition pass 2 is arguably already hot, because pass 1 populated the caches too. A
`clearCache` between passes would manufacture the strict warm state. **It is deliberately not used** —
this reproduces user behaviour, and a user's second visit is simply their second visit. Do not add it
to make the label technically precise.

Cold here also means cold **VertiPaq**, not cold storage: OneLake and the capacity's local caching sit
underneath a new dataset untouched. Nothing available would give a colder number.

**Nothing touches the model between readiness and pass 1**, and two details exist only to keep that
true. The top-DUID resolve runs `TOPN` over `DUID` and `Total MWh` — the very columns `probe_duid`
and `probe_mw` measure — so it happens **after** the cold pass, and the ladder's two DUID queries
join the session at pass 2 with no cold number (pin `top_duid` and they run from pass 1 like
everything else). And the readiness probe reads `dim_calendar`, not `COUNTROWS(fct_summary)` as it
once did — that was byte-identical to `probe_rowcount`, so the readiness check was pre-warming the
control it would later be measured against.

Rankings use **medians, never means** for hot (one capacity spike among 110ms runs blows up a mean
and fabricates a winner). Cold and warm are single samples by construction — one first visit, one
second visit per deployed model — so neither carries a spread, and neither can be noise-filtered.
More cold samples means more dispatches, not a bigger `runs`.

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

**What the defaults buy.** `runs=6` gives one cold sample, one warm, and **four** hot — so the hot
median is a real median and the hot spread in §3 is a real spread. Drop to `runs=3` and there is a
single hot pass, every spread reads 0, and the run is a smoke test with timings rather than a
defensible ranking. Below 3 there is no hot tier at all, which the report shows as a gap rather than
a zero. Cold and warm cannot be strengthened by raising `runs` — a session has one first visit — so
the way to test whether a cold number is repeatable is a second dispatch.

## Running it

**CI, and only CI.** Dispatch *Direct Lake benchmark*. Inputs: `workspace`, `engines` (order is the
order they are **measured** in — index 0 is simply the job that skips the idle gap; no number in the
report depends on it), `runs` (**passes** over the suite — the pass number is the tier),
`think_seconds` (idle *between queries*, default 4), `gap_seconds` (idle *before each engine* after
the first — a different thing), `top_duid` (optional pin for the selectivity ladder; pinning it lets
the ladder run from pass 1).

There is no supported way to run the paid part from a laptop: `xmla_compare.py` measures one engine
per process and the workflow is what fans it out. A cheap scouting **dispatch** — end to end in
minutes instead of an hour of capacity:

```
engines=duckrun,spark  runs=3  think_seconds=0  gap_seconds=0
```

Two things to read in a scout's logs: the deploy printed a **different item id** than last time
(`replaced <guid>` — the delete took, so the model really was new and pass 1 really was cold), and
pass 1 > pass 2 > pass 3.

**Free, locally, before pushing** (no credentials, no Fabric — this is the CI gate, and it runs as
a `needs:` on the paid job):

```bash
python -m pytest benchmark/ -q                                     # ranking + template checks
RUN_REPORT=some_run_report.json python benchmark/render_report.py   # re-render any past artifact
```

[`test_verdicts.py`](test_verdicts.py) pins the ranking layer: rank direction (ordered by total,
rank 1 lowest, `× fastest` ≥ 1), fastest-wins (a 1ms win is a win, and the `best` column names an
engine rather than `tie`), that no result depends on the engine order given and that no `reference()`
helper comes back, per-metric scoping (a model with no cold/warm numbers is kept out of those totals
rather than emptying them), and comparable totals. It also pins **the session's shape**, which is
where the wrong-but-silent failures live: that `probe_rowcount` runs last among the probes, that the
pass number is the tier, that cold and warm carry no spread, that a query joining at pass 2 gets no
cold number, that nothing dehydrates any more — and, replaying a whole session against a stub
connection, that the only things touching the model between readiness and the end of pass 1 are the
readiness probe and the suite itself, in order.
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
differently — is the `layout` job of the `dbt` workflow, or the same read from a laptop
(CLAUDE.md: *"Query the lakehouses directly before instrumenting CI"*):

```python
import duckrun
duckrun.connect("abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item-guid>/Tables",
                read_only=True).get_stats("mart.*")
```

On Windows leave `CURL_CA_INFO` unset — dbt.yml's Linux CA path makes the parquet footer read fail
with an SSL error that looks like a credentials problem.

## Prerequisites

- The repo's federated identity (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID`, the same two secrets
  `dbt.yml` uses) needs access to the workspace, and enough rights to create semantic models and run
  a TMSL refresh (cold timing needs write; without it the run silently falls back to hot-only).
- `mart.fct_summary`, `mart.dim_duid` and `mart.dim_calendar` must already exist in each engine's
  item — built by `dbt.yml`. This reads them and never writes.
- XMLA read/write must be enabled on the capacity.
