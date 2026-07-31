# `cu/` — CU per semantic model

One question: **what have the benchmark's four semantic models cost in capacity units?**

Fabric has no per-operation CU REST API. The Capacity Metrics app's own semantic model is the only
authoritative source, so this reads it by DAX and prints one table.

```
## Semantic model CU — since 2026-07-30 22:00 (model clock), as of 2026-07-30 14:10Z

| semantic model | XMLA Read Operation | Semantic model refresh | Query |    total |
|----------------|--------------------:|-----------------------:|------:|---------:|
| aemo_duckrun   |            12,000.0 |                  500.0 |   0.0 | 12,500.0 |
| aemo_iceberg   |                 0.0 |                    0.0 |   0.0 |      0.0 |
| aemo_spark     |             2,000.0 |                    0.0 |  75.0 |  2,075.0 |
| aemo_dwh       |                 0.0 |                    0.0 |   0.0 |      0.0 |
| **total**      |            14,000.0 |                  500.0 |  75.0 | 14,575.0 |
```

**Time is a pinned floor, not a rolling window.** A window ("last 3h") moves with every dispatch
and can slice one benchmark in half, making an engine look cheap for no reason but where the
boundary fell. `since` stays put, everything after it accumulates, and two dispatches a day apart
are comparable. Its specific purpose: the app's ~14 days of retention spans more than one version of
the benchmark — the run where dwh was **DirectQuery** rather than Direct Lake, and then the switch
from a per-query dehydrate to a user-session walk with think time (`8c037c8`/`debef3a`, 06:11Z on
2026-07-31). Those are different experiments and their CU must not be summed (see
`benchmark/README.md`), so the default floor sits at the first dispatch measured the current way,
`2026-07-31T16:00:00` in the model's clock. Bump it again the next time the suite changes; blank
means everything retained. Operation columns are discovered from the data and ordered by total CU,
so the expensive one reads first.

## Runs are separated, and it costs nothing extra

The floor means the aggregate above answers *"what has all our benchmarking cost since then"*, which
is not the question you usually have — you want **what one pass cost**. So the report also splits per
run:

```
### Runs detected: 3

| semantic model | run 1<br>07-30 12:00→15:00 | run 2<br>07-31 09:00→12:00 | run 3<br>07-31 20:00→20:00 |     total |
|:---------------|---------------------------:|---------------------------:|---------------------------:|----------:|
| aemo_duckrun   |                      160.0 |                      158.0 |                        0.0 |     318.0 |
| aemo_iceberg   |                      170.0 |                      171.0 |                        0.0 |     341.0 |
| aemo_spark     |                      180.0 |                      179.0 |                        0.0 |     359.0 |
| aemo_dwh       |                      300.0 |                      295.0 |                      275.0 |     870.0 |
| **total**      |                  **810.0** |                  **803.0** |                  **275.0** |**1,888.0**|
```

One row per model, one column per run — so "what did iceberg cost yesterday against today" is a
single row read left to right. It is the same shape as the aggregate table above it, deliberately.
Column 3 is a dwh-only dispatch, and it separates itself: nothing had to be told that it happened.

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
concurrency group, no ADOMD, no .NET, no duckrun. `requests` is the only dependency. It does read ONE
artifact — `stats` from the `layout` job of the `dbt` workflow, for the layout table above — and that is a
JSON file, not code: nothing is imported, and losing it costs one table.
Deleting `cu/` and `.github/workflows/cu.yml` removes it completely and nothing else in the repo
notices — which is the point, because this may not turn out to be useful. The four model names are
spelled out here rather than imported from `benchmark/engines.py` for the same reason.

**It correlates nothing.** CU per model over a wall-clock window. It cannot tell you which query or
which benchmark run produced a number.

## Running it

`gh workflow run "Capacity CU"` (or the Actions tab), `workflow_dispatch` only. **Wait ~10 minutes
after the activity you want to measure** — see the lag note below.

| input | default | notes |
|---|---|---|
| `since` | `2026-07-31T16:00:00` | floor, **in the model's clock** (see below) — the first dispatch measured the current way. Blank = everything retained |
| `models` | the four `aemo_*` | comma-separated, in report order. Blank = every semantic model |
| `workspace` | `ea575278-…` | the workspace dbt.yml and benchmark.yml deploy to. Blank = all |
| `metrics_workspace_id` | `7f7f5d92-…` | where the Capacity Metrics app is installed |
| `metrics_model_id` | `0fdedd3b-…` | the app's semantic model |
| `capacity_id` | all | blank = every capacity the metrics model can see |
| `run_gap_hours` | `2` | idle hours that split a model that was *not* redeployed. Runs themselves separate on the item GUID. 0 = aggregate only |
| `run_ops` | false | per-run breakdown by operation type as well |
| `layout` | true | fetch the layout from the latest *dbt* run |
| `layout_table` | `fct_summary` | which table's layout to show |
| `debug` | false | dumps every table's columns to stderr |

Locally, with `PBI_TOKEN` set:

```bash
export PBI_TOKEN=$(az account get-access-token \
  --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
export CU_METRICS_WORKSPACE_ID=7f7f5d92-1603-4a02-a46a-0d90fe1ed119
export CU_METRICS_MODEL_ID=0fdedd3b-1451-4499-9ed4-aa3658100ec1
CU_SINCE=2026-07-31T16:00:00 CU_DEBUG=1 python cu/capacity_cu.py
```

`CU_RUN_COLS` (default 8, `0` = unlimited) is env-only — how many runs get their own column before
the oldest fold into one. It only matters against a widened `since`, which is a local thing.

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
and its CU disappears while the report says "no activity". So names are resolved **live** from
`GET /groups/{workspace}/datasets` first (one request, same host and token as `executeQueries`, no new
dependency), with `'Items'` as the fallback for everything outside the workspace. If that call is
refused the run still works, logs why, and the diagnostic below names the unresolved GUIDs.

**An empty report explains itself.** "No semantic model activity" and "1,202 rows came back and every
one failed a filter" are opposite conclusions that used to print the same sentence. Now an empty
result prints how many rows the table returned after the floor, which filter rejected them and how
many, any item whose **name matched but workspace did not** (with its real workspace id), and the top
CU spenders it did see — so one dispatch says which knob is wrong instead of the next three guessing.
A bare GUID in that last table is the snapshot-lag trap above.

**Both filters are needed, and they stack.** Display names are not unique across a tenant, so a
stale `aemo_spark` in some other workspace would otherwise be silently added to this one's CU.
`models` selects by name, `workspace` restricts to the workspace the benchmark deploys to, and both
apply. Every requested model is printed even with no activity — a `0.0` row distinguishes "ran and
cost nothing" from "vanished", which a missing row would not.

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
