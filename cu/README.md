# `cu/` — CU per engine, ETL against analytics

One question: **what has each engine cost in capacity units — to build its tables, and to query
them?**

Fabric has no per-operation CU REST API. The Capacity Metrics app's own semantic model is the only
authoritative source, so this reads it by DAX and prints it back engine-major: four columns, `etl`
(what *writes* the tables — lakehouses, warehouse, notebooks, Livy) against `analytics` (what
*queries* them — the semantic models) down the side, broken out by operation, then the same per run.

```
## Capacity units — since 2026-07-31 16:00 (model clock), as of 2026-08-01 14:10Z

|                     | landing | duckrun | iceberg | spark |     dwh |   total |
|:--------------------|--------:|--------:|--------:|------:|--------:|--------:|
| **etl**             |   300.0 |   215.0 |   178.0 | 210.0 |   188.0 | 1,091.0 |
| OneLake Write       |   240.0 |   120.0 |    90.0 |   0.0 |     0.0 |   450.0 |
| Spark Job           |     0.0 |     0.0 |     0.0 | 210.0 |     0.0 |   210.0 |
| Warehouse Query     |     0.0 |     0.0 |     0.0 |   0.0 |   188.0 |   188.0 |
| Notebook Run        |     0.0 |    95.0 |    88.0 |   0.0 |     0.0 |   183.0 |
| OneLake Read        |    60.0 |     0.0 |     0.0 |   0.0 |     0.0 |    60.0 |
| **analytics**       |     0.0 |   320.0 |     0.0 |   0.0 | 3,499.0 | 3,819.0 |
| XMLA Read Operation |     0.0 |   320.0 |     0.0 |   0.0 | 3,499.0 | 3,819.0 |
| **total**           |   300.0 |   535.0 |   178.0 | 210.0 | 3,687.0 | 4,910.0 |
```

**`landing` is a STAGE, not an engine**, and the table says so under itself. `dbt_landing` holds the
downloaded AEMO archive: `download_aemo.py` writes it and all four legs read it. It has its own
column because "the download cost X" is a real answer, where folding it into `shared` was a shrug —
but it is a *shared input cost*, so do not add it to an engine's column, and it **cannot** be split
between them: the metrics rows carry no consumer dimension, the legs read it concurrently, and they
read it as the same service principal. Any allocation key would be invented.

**Engine-major, because that is the repo's thesis** — same data, four engines, side by side — and it
is the only orientation in which "what did iceberg cost to build *and* to query" is one column read
top to bottom. It is also what makes the width manageable: **operations are rows**, so a lakehouse's
dozen OneLake operation types is a dozen rows, which markdown handles fine. An item-major table with
those as *columns* was the shape that got this width reverted the first time.

**Attribution is by item NAME**, because the metrics model carries no item-to-engine relationship
and nothing else in the row could supply one — `dbt_delta` and `aemo_duckrun` and
`dbt-duckrun-<random>` are duckrun's (`delta` is the alias that matters), `dbt_spark` is spark's, and
so on. Anything ambiguous goes to **`shared`** rather than to a guess, named in a footnote: a wrong
column is worse than an honest one. What ends up there is the legacy `duckrun-py-*` notebooks (both
DuckDB legs used that name, so they cannot be told apart) and any item nothing could name — a bare
GUID in `shared` is the snapshot-lag trap below, not an item called that.

Every column is printed even at 0.0 — one that disappears is indistinguishable from one that spent
nothing. `CU_ENGINES` sets the list and the order.

`etl: false` on the dispatch narrows it back to the semantic models exactly as before — worth having
when comparing against an older dispatch's numbers. `CU_ITEM_DETAIL=1` prints the item-major table
underneath, for when a column looks wrong and you need to see which item fed it.

**An unrecognised item kind is kept, never dropped.** It lands in a third `other` class and its kind
is named on stderr (`kind X: 1,234.0 CU -> other`), which is how a kind gets added to
`CLASS_BY_KIND`.

**An ETL number does not mean the same thing for every engine, and this is the thing to hold onto
when reading the table.** Three different attribution shapes:

| leg | where its compute is billed | so its `etl` column is |
|:--|:--|:--|
| `spark` | **the lakehouse** — Fabric bills Livy against `dbt_spark`, there is no Spark item of any kind | OneLake operations *and* the whole leg's compute, separable only by the operation rows |
| `duckrun`, `iceberg` | the throwaway **notebook** `duckrun.run_python` creates | `Notebook Run` against the leg's own column, because `fabric_run.py` names it `dbt-<engine>-<random>` |
| `dwh` | the **warehouse** | warehouse queries against `dbt_dwh` |

The per-leg notebook attribution exists because `fabric_run.py` names its notebook `dbt-<engine>-<random>`
rather than taking duckrun's default `duckrun-py-<runid>`, which was identical for both DuckDB legs
and made their compute one undivided row. **The random suffix has to stay**: the notebook is deleted
after every run, Fabric keeps a deleted item's display name reserved for minutes afterwards, and
`_execute_notebook` creates the item with no retry around it — so a fixed name would 409 the next
build to tidy up a report. The engine lives in the prefix, which is what `CU_GROUP_PREFIXES`
collapses on.

Read the rows as *what this item spent*, not as a like-for-like engine comparison — that is what
`benchmark/` is for.

**Time is a pinned floor, not a rolling window.** A window ("last 3h") moves with every dispatch
and can slice one benchmark in half, making an engine look cheap for no reason but where the
boundary fell. `since` stays put, everything after it accumulates, and two dispatches a day apart
are comparable. Its specific purpose: the app's ~14 days of retention spans more than one version of
what is being measured — the run where dwh was **DirectQuery** rather than Direct Lake, then the
switch from a per-query dehydrate to a user-session walk with think time (`8c037c8`/`debef3a`).
Those are different experiments and their CU must not be summed (see `benchmark/README.md`).

The default floor is **`2026-08-01T10:00:00`** (model clock) — the from-scratch `dbt` run
[30676635835](https://github.com/djouallah/dbt/actions/runs/30676635835), which started 00:53:23Z.
That is a harder boundary than a methodology change: it ran with `reset_outputs`, so all four output
items were **deleted and recreated**. Rows before it belong to items that no longer exist — same
display names, new GUIDs — so summing across the floor adds two generations of `dbt_delta` into one
number describing neither. It is also the first build whose notebooks are named per engine, i.e. the
first whose ETL is attributable at all. Bump it again the next time the outputs are reset or the
suite changes what it measures; blank means everything retained.

## Runs are separated, and it costs nothing extra

The floor means the aggregate above answers *"what has all our benchmarking cost since then"*, which
is not the question you usually have — you want **what one pass cost**. So the report also splits per
run:

```
### Runs detected: 2

|               | run 1<br>08-01 10:00→10:00 | run 2<br>08-01 20:00→20:00 |      total |
|:--------------|---------------------------:|---------------------------:|-----------:|
| **etl**       |                **1,091.0** |                    **0.0** |    1,091.0 |
| landing       |                      300.0 |                        0.0 |      300.0 |
| duckrun       |                      215.0 |                        0.0 |      215.0 |
| iceberg       |                      178.0 |                        0.0 |      178.0 |
| spark         |                      210.0 |                        0.0 |      210.0 |
| dwh           |                      188.0 |                        0.0 |      188.0 |
| **analytics** |                    **0.0** |                **3,819.0** |    3,819.0 |
| duckrun       |                        0.0 |                      320.0 |      320.0 |
| dwh           |                        0.0 |                    3,499.0 |    3,499.0 |
| **total**     |                **1,091.0** |                **3,819.0** |**4,910.0** |
```

Same orientation as the aggregate above — class subtotal in bold, engines under it — so the two read
the same way rather than making the eye re-learn the layout halfway down. Run 1 there is a **dbt
build** (all ETL, no model activity at all) and run 2 a benchmark dispatch, and neither had to be
told the other happened.

**A column is a whole run, never an hour.** A pass spread over 12:00→15:00 is one column carrying all
four hours' CU. The per-run hour *count* is in the footnote rather than the table, so nothing invites
reading the columns as hourly.

Runs are columns, so the table grows sideways. Past `CU_RUN_COLS` (default 8) the oldest fold into a
single `earlier` column — named in the header, restated in the footnote, and logged to stderr, never
silently. `CU_RUN_COLS=0` gives every run its own column. It only binds on a widened `since`, which
is why it is an env var and not a dispatch input.

**No extra requests.** Both signals the split uses — the item GUID and the hour — were always in
every row (the hour has to be, or `since` cannot bind) and were simply discarded after the name
lookup and the floor check. The split is pure post-processing of rows already in hand, so the request
count is unchanged: one per capacity.

**How a run is decided: one DEPLOYMENT GENERATION.** `deploy_models.py` deletes and recreates each
semantic model, so every dispatch mints a fresh item GUID per engine — and a model cannot be deployed
twice inside one dispatch. So walking the GUIDs oldest-first, a repeated model *name* is the boundary
between two runs. That rule uses no clock at all, which is why it survives a dispatch with different
`engines`, `runs` or `gap_seconds`, and why two dispatches **ten minutes apart** are two columns.

`run_gap_hours` (default 2) is the second rule and applies to one GUID's own hours: a model that was
*not* redeployed but is queried again days later splits there rather than dragging the later CU into
the earlier column.

**The rule extends to the throwaway notebooks, and to nothing else.** An item that is created fresh
for every run dates that run exactly the way a redeployed semantic model does — and a *collapsed*
name is precisely the signal that an item is throwaway, so `dbt-duckrun-*` repeating among the GUIDs
is the next dbt build. That is what gives a build with no benchmark beside it an exact boundary
rather than one inferred from idle time.

**A long-lived item cannot carry that rule, so it is allocated by HOUR.** A lakehouse or a warehouse
keeps one GUID for years, so no name ever repeats. Runs are *formed* from the generational items —
semantic models and throwaway notebooks — and every other item's
hours are then allocated to those windows — containment first, then adjacency within
`run_gap_hours`. Hours belonging to no model's window cluster into a run of their own, which is what
gives a dbt build with no benchmark beside it its own column (run 1 above). The cost, stated plainly:
**ETL allocation is only as sharp as the hour bucket**, so an ETL hour shared by two overlapping runs
is attributed to one of them rather than split. Analytics allocation stays exact, because it is still
by GUID. `cu/test_capacity_cu.py` pins both rules, including that every (item, hour) pair lands in
exactly one run — a duplicated pair double-counts CU and a dropped one makes the columns silently
total less than the aggregate above them.

**The hour bucket is no longer the floor on separating dispatches.** `Metrics By Item Operation And
Hour` is still bucketed hourly, and this used to mean two dispatches inside one hour were one
unsplittable column. Keying on the GUID removes that limit; the bucket now only bounds the gap rule
above, and it is why two adjacent columns can **overlap by an hour**. That costs nothing in accuracy:
CU is assigned to a column by item GUID, not by hour, and the metrics rows are per item. The
timepoint detail table has finer resolution and this deliberately does not use it (see below) — this
is the change that keeps it unnecessary.

**It is still not correlated with a GitHub run.** A run here is identified by its own GUIDs and time
window.
`benchmark/` records durations but no absolute timestamps, and adding them is the coupling this
directory exists without. If the split shows one cluster where you expected two, the report says so
rather than printing a one-column "runs" table that repeats the aggregate.

That is the whole output and the whole scope.

## The layout beside the CU

CU alone says which engine cost more, not why — and the answer is nearly always the physical layout.
So the report ends with one table putting them together:

```
### Layout of `fct_summary` — what the CU was spent scanning

| engine  | writer             |       CU |        rows | files | row groups | avg RG rows | size MB | vorder |
|:--------|:-------------------|---------:|------------:|------:|-----------:|------------:|--------:|:-------|
| duckrun | `delta-rs`         | 18,000.0 | 143,844,166 |     7 |         94 |   1,530,257 | 1,035.0 | no     |
| iceberg | `duckdb (iceberg)` | 37,227.3 | 143,844,166 |   386 |      1,175 |     122,420 | 1,107.0 | no     |
| spark   | `spark`            |  9,000.0 | 143,844,166 |    20 |         20 |   7,192,208 | 1,217.0 | yes    |
| dwh     | `warehouse`        | 12,000.0 | 143,844,166 |    79 |         79 |   1,820,812 | 1,567.0 | no     |
```

**No Delta log is read here, and it would be wasteful to.** Reading four Delta logs over OneLake takes
~10 minutes (the iceberg item alone 12m+), and the layout only changes when the tables are REWRITTEN —
which is why the dashboard is its own dispatch-only workflow rather than a job in every build. So the
numbers come from the `stats` artifact of the latest successful **dbt** run (its `layout` job — `stats.py` writing `STATS_JSON`), which the
workflow downloads with `gh run download`. That keeps this directory's one hard property: `requests` is
still the whole dependency list, there is no duckrun, no storage token, no OneLake read, and
`rm -rf cu/ .github/workflows/cu.yml` still removes every trace. The coupling is a JSON file produced
by a workflow that exists anyway, not code.

**The two halves come from different runs**, so the table prints which dashboard run the layout is from
and when it was written. A cached reading is sound precisely because the layout is near-static — but
dispatch `dbt` again after anything that rewrites the tables (`REBUILD_SUMMARY=1`, a
`--full-refresh`, an `OPTIMIZE`), or the CU will sit beside a layout that no longer exists.

**Failure is silent by design**: no dashboard run, an expired artifact (90 days), a renamed
`DETAIL_KEYS` entry — any of them drop the layout table and log why. A CU report is useful without it;
it is not useful if a missing artifact fails the job. The flip side is that a `stats.py` rename shows up
here as a *missing table*, so change both together.

`layout=false` skips the download. `layout_table` picks the table (default `fct_summary` — the mart the
benchmark queries; `dim_duid` at a few hundred rows explains nothing about a 143M-row scan).

## What it deliberately is not

**It shares nothing with `benchmark/`.** No imports, no `run_report.json`, no `needs:`, no
concurrency group, no ADOMD, no .NET, no duckrun. `requests` is the only runtime dependency (`pytest`
for the offline suite, which imports nothing but this script). It does read ONE
artifact — `stats` from the `layout` job of the `dbt` workflow, for the layout table above — and that is a
JSON file, not code: nothing is imported, and losing it costs one table.
Deleting `cu/` and `.github/workflows/cu.yml` removes it completely and nothing else in the repo
notices — which is the point, because this may not turn out to be useful. The four model names are
spelled out here rather than imported from `benchmark/engines.py` for the same reason.

**It correlates nothing.** CU per item over a wall-clock window. It cannot tell you which query, which
model, or which GitHub run produced a number — only which *run* in its own sense, and which item.

## Running it

`gh workflow run "Capacity units"` (or the Actions tab), `workflow_dispatch` only. **Wait ~10 minutes
after the activity you want to measure** — see the lag note below.

Every dispatch publishes the report to **<https://djouallah.github.io/dbt/>**. Pages is set to build
from Actions, so there is no `gh-pages` branch and nothing is committed to the repo. That page is the
**latest** report only — each dispatch overwrites it — and the per-run copy is that run's `cu-report`
artifact, carrying both the markdown and the HTML. The `publish` job is separate from the read
because `deploy-pages` needs the `github-pages` environment, and an environment on the job holding
the Fabric tokens would gate the capacity read rather than the publish; it runs only on success, so a
failed dispatch cannot overwrite a good page with a half-written one.

**The repo is public, so the page is public.** It carries capacity-unit totals per engine and item
names — no tokens, no ids, no data — but that is the trade, made deliberately.

`cu/report_html.py` does the markdown → HTML, over the report's own markdown subset and nothing
wider. One self-contained file: inline CSS, no script, no font, no image, no external URL at all, so
the artifact copy opens off a local disk with no network. Re-render any past report offline with
`python cu/report_html.py cu-report.md > page.html`.

| input | default | notes |
|---|---|---|
| `since` | `2026-08-01T10:00:00` | floor, **in the model's clock** (see below) — the from-scratch dbt run that reset every output item. Blank = everything retained |
| `etl` | true | report every item in the workspace, classified into `etl`/`analytics`. Off = semantic models only, the old scope exactly |
| `models` | the four `aemo_*` | comma-separated, leading the analytics rows and printed even at 0.0. With `etl` **on** this only orders; with `etl` off it also filters |
| `workspace` | `ea575278-…` | the workspace dbt.yml and benchmark.yml deploy to. With `etl` on this is the only filter left. Blank = all |
| `metrics_workspace_id` | `7f7f5d92-…` | where the Capacity Metrics app is installed |
| `metrics_model_id` | `0fdedd3b-…` | the app's semantic model |
| `capacity_id` | all | blank = every capacity the metrics model can see |
| `run_gap_hours` | `2` | idle hours that split a model that was *not* redeployed. Runs themselves separate on the item GUID. 0 = aggregate only |
| `run_ops` | false | per-run breakdown by operation type as well |
| `layout` | true | fetch the layout from the latest *dbt* run |
| `layout_table` | `fct_summary` | which table's layout to show |
| `refresh` | true | refresh the metrics semantic model and wait, **before** any DAX. A dispatch creates items the app has not seen; an unrefreshed model cannot report CU against them. Off = re-read a settled window without the wait |
| `debug` | false | dumps every table's columns to stderr |

Locally, with `PBI_TOKEN` set:

```bash
export PBI_TOKEN=$(az account get-access-token \
  --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
export CU_METRICS_WORKSPACE_ID=7f7f5d92-1603-4a02-a46a-0d90fe1ed119
export CU_METRICS_MODEL_ID=0fdedd3b-1451-4499-9ed4-aa3658100ec1
CU_SINCE=2026-08-01T10:00:00 CU_DEBUG=1 python cu/capacity_cu.py
```

Several knobs are env-only, because each only matters in a local investigation and the dispatch form
is long enough:

| env | default | what it does |
|:--|:--|:--|
| `CU_ENGINES` | `landing,duckrun,iceberg,spark,dwh` | the columns, and their order. Drop `landing` and `dbt_landing` falls back into `shared` |
| `CU_ITEM_DETAIL` | off | also print the item-major table, for when a column looks wrong |
| `CU_RUN_COLS` | `8` (`0` = unlimited) | runs with their own column before the oldest fold into `earlier` |
| `CU_GROUP_PREFIXES` | `dbt-duckrun-,dbt-iceberg-,duckrun-py-` | item name prefixes that collapse to one item |
| `CU_OP_COLS` | `6` (`0` = unlimited) | operation *columns* in the item-detail table only — the engine table has no column fold, since operations are rows there |

`FABRIC_TOKEN` is optional and is for **naming items only**. The datasets endpoint `PBI_TOKEN`
reaches lists semantic models and nothing else, so without a Fabric-audience token every lakehouse,
warehouse and notebook depends on the metrics app's lagging `'Items'` snapshot — and an item it has
not catalogued shows as a bare GUID in the `other` class. The workflow mints it from the same OIDC
login, `continue-on-error`; locally:

```bash
export FABRIC_TOKEN=$(az account get-access-token \
  --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
```

The report layer has an offline test suite — `python -m pytest cu/ -q`, no token, no network, ~2s —
and `cu.yml` runs it before the Azure login. It covers the run split, the class rollup, the operation
fold and the empty-report diagnosis, all of which fail the same way when they are wrong: a plausible
number, printed with confidence, off.

## The things that will bite

**It reads `Metrics By Item Operation And Hour`, not `Timepoint Interactive Detail`.** Mind the
spelling — the model also carries `Metrics By Item And Operation` (no time axis) and `Metrics By
Item And Hour` (no operation split). This is the one with both; the hour axis exists only to
support `since`, and nothing here reports by hour. The detail table was
tried first and is the wrong instrument. It is bucketed at 30 seconds and gated by a
single-timepoint `MPARAMETER`, so even a 3-hour window costs 360 requests per capacity; and because an
interactive operation is smoothed across 10–128 buckets it reappears in every one carrying its full
`Total CU`, so the rows have to be deduplicated by operation id or the total comes out one to two
orders of magnitude high. The aggregate answers the same question in **one request per
capacity**, already summed, with no double-counting to guard against. Summing *is* correct on this
table precisely because it is already an aggregate. The detail table remains the right tool for
drilling into one timepoint's individual operations — which is not what this does.

**Every deploy mints a NEW item GUID, and `'Items'` is a lagging snapshot.** This is what an empty
report turned out to be, not an idle capacity: the metrics tables hold item **GUIDs**, the join to
`'Items'` is the only route to a name, and a semantic model that was just created — or deleted and
recreated — has a GUID that snapshot has not seen. It resolves to no name, fails the `models` filter,
and its CU disappears while the report says "no activity". So names are resolved **live**, with
`'Items'` as the fallback: `GET /v1/workspaces/{ws}/items` (every kind, needs `FABRIC_TOKEN`) first,
then `GET /groups/{workspace}/datasets` (semantic models only, same host and token as
`executeQueries`). The second is not redundant — it is what still works with no Fabric-audience
token. If either call is refused the run still works, logs why, and the diagnostic below names the
unresolved GUIDs.

**Neither live call can name the throwaway notebooks**, and nothing is wrong when they don't:
`run_python` deletes its notebook on the way out, so the item is already gone from the workspace by
the time this reads. `'Items'` is the only route to those names — and it does carry them, which is
how the earlier attempt at this width ended up with a row per `duckrun-py-*` notebook in the first
place. What the live calls fix is the long-lived items: a lakehouse or warehouse provisioned during
the run being measured, and the semantic models.

**An empty report explains itself.** "No item activity" and "1,202 rows came back and every
one failed a filter" are opposite conclusions that used to print the same sentence. Now an empty
result prints how many rows the table returned after the floor, which filter rejected them and how
many, any item whose **name matched but workspace did not** (with its real workspace id), and the top
CU spenders it did see — so one dispatch says which knob is wrong instead of the next three guessing.
A bare GUID in that last table is the snapshot-lag trap above.

**With `etl` on, `workspace` is the only filter — and that is the right one to be left with.** The
name filter existed because display names are not unique across a tenant: a stale `aemo_spark` in
some other workspace would otherwise be added to this one's CU. The workspace test alone still stops
that, and everything inside `ea575278-…` is this repo's. `models` is then an ordering: the four are
printed even with no activity, because a `0.0` row distinguishes "deployed and never queried" from
"vanished", which a missing row would not. With `etl` **off** both filters stack as they always did.

**`since` is in the model's clock, NOT UTC.** The metrics tables stamp everything in the offset
configured inside the Capacity Metrics app — +10 here, so a benchmark that ran at 05:15Z sits under
hour 15:00 — and that is also what the app's UI shows you, so there is nothing to convert. Every
run logs `hours returned: … .. …`; read that once and you know what to set.

Detecting the offset automatically was tried twice and abandoned, which is worth not repeating:
`Timepoints` is a generated calendar running ~9 days into the **future** (it reported +227.5h),
`MAX()` over activity lags by however long the capacity has been idle, and no table in the model
carries the offset as a value.

**The `since` filter is verified, not trusted.** `FILTER(VALUES(...))` as a `SUMMARIZECOLUMNS`
argument was accepted without error and silently changed nothing: a 3-hour window, no filter, and a
12:00Z floor all returned byte-identical totals before anyone noticed. It now uses `CALCULATETABLE`
with a plain boolean predicate, projects the hour column, and **dies** if any returned row predates
the floor. A filter that fails loudly is fine; one that returns a plausible wrong number is the
worst thing this tool can do.

**One capacity per query.** These tables are DirectQuery and resolve one data location per query,
so `CapacitiesList` must carry exactly one capacity. Passing several fails with an opaque
`Internal Error: Error obtaining data location` that names neither the cause nor the capacity. This
tenant has two, so each is queried separately and the results merged.

**Item columns hold GUIDs, not names.** `Metrics By Item Operation And Hour[Item Id]` is an id, so
`items_for()` joins to `'Items'` (`Item Id`, `Item name`, `Item kind`) to resolve it. An id missing
from `'Items'` is kept under its raw GUID rather than dropped — losing CU silently is worse than an
ugly row.

**The metrics model is refreshed first, and the wait is the point.** `'Items'` is a lagging
snapshot: `deploy_models.py` deletes and recreates each semantic model, so every benchmark dispatch
mints four item GUIDs the app has never seen, and CU attached to an item the model does not know
about cannot be reported at all. So the run POSTs a refresh, polls until it completes
(`CU_REFRESH_TIMEOUT`, default 900s), and only then queries. It is **non-fatal by construction** —
the app's dataset is not ours, the service principal may hold no refresh rights on that workspace,
and a scheduled refresh may already be running (that one is waited on instead). Any of those logs a
line and reads the model as it stands, which is exactly what this tool did before the refresh
existed. Live name resolution from `GET /groups/{ws}/datasets` still covers the *names* either way;
what the refresh adds is the rows behind them.

**The service principal works — measured, against the expectation.** The community consensus is
that the Capacity Metrics semantic model rejects service principals, and this was built assuming
that would be the first thing to fail. It isn't — so the workflow mints `PBI_TOKEN` from the SP
and there is **no repo secret involved at all**. A `secrets.PBI_TOKEN` branch that took precedence
when set was removed once it was clear it would never fire; it only produced editor warnings about
a secret that deliberately did not exist. If the SP is ever refused, export a user token as
`PBI_TOKEN` for that investigation — it expires in about an hour.

**Column names move between app versions.** Microsoft's own fabric-toolbox accelerator ships four
DAX variants (v53/v47/v40/v37) for this reason, and it has already bitten here — the first
candidate list said `Item Name`, the app says `Item name`. Nothing hardcodes a name:
`discover_columns()` reads the real schema via `INFO.VIEW.COLUMNS()` and resolves each role from a
candidate list, failing with the actual column list printed. `debug: true` dumps every table's
columns — when a name moves, the replacement is usually next door.

**~14-day retention, ~6 minute lag, 5–64 minute smoothing.** A dispatch immediately after a
benchmark does not yet include it. Retention is the outer boundary and `since` the inner one:
CU from a benchmark older than ~14 days ages out regardless of `since`, so a total can fall between
two dispatches without anything being wrong. `CU (s)` is a smoothed attribution, not an instantaneous measurement.

## Never auto-trigger it

`workflow_dispatch` only. No `schedule`, no `push`, no `workflow_run`, no `needs:` from another
workflow — not even a nightly, not even behind an `if:`. Same standing rule
`benchmark/README.md` carries, for the same reason: these are reads against shared Fabric capacity,
and a run nobody chose to start is the one a capacity admin asks about.
