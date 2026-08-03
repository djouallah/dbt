# `cu/` — what the work cost, by Fabric item GUID

Two programs and one published page. `measure.py` reads capacity units — and duration — from the
Fabric Capacity Metrics app's own semantic model and keeps a cumulative ledger; `dashboard.py` joins
that ledger to the run records and renders. Both run in the `Dashboard` workflow, one of only two
workflows in this repo.

Fabric exposes **no per-operation CU REST API**. The metrics app's semantic model is the only
authoritative source, which is why this exists at all.

## The two documents

| file | written by | shape |
|---|---|---|
| `history/runs/<ts>-<run id>.json` | the `Benchmark` workflow | every Fabric item GUID that run created, with its `role`, plus the layout, the input archive and the raw query timings |
| `history/cu.json` | `cu/measure.py` | `{item GUID: {operation: CU}}`, and a `seconds` sibling of the same shape |

They are joined on the **item GUID**, and that is the whole design.

Attribution used to be substring matching on item DISPLAY NAMES — `engine_of()`, a `shared` column
for everything ambiguous, a join to the app's lagging `'Items'` snapshot for kinds, and heuristics
(idle-hour gaps, repeated model names) to guess where one run ended and the next began. All of it is
gone. Every item except the landing lakehouse is created and destroyed inside one run, so a GUID
belongs to exactly one run, and the class comes from the `role` the run recorded rather than from an
item kind read out of a snapshot that had usually not catalogued a minutes-old item.

## No refresh, and why that is safe

The old reader refreshed the metrics model before every read so that items minutes old would be
catalogued. Power BI throttles the REST API **per identity**, and the service principal spent its
budget: on runs 30685959678 and 30691130030, half an hour apart, every attempt drew 429 — while a
human refreshing by hand went straight through. Nothing failed and nothing looked broken; 41,887 CU
of DuckDB-leg compute simply printed under `shared`, because two throwaway notebooks resolved to no
name.

None of it was needed. `Metrics By Item Operation And Hour` carries `Item` (a GUID) and
`Workspace Id` as columns of its own, so the workspace filter binds with no join and the GUID needs
no resolving.

**No refresh is needed, and that is MEASURED, not argued** (2026-08-02, against the live model):

- Two item GUIDs carried CU in `Metrics By Item Operation And Hour` — 7,654.8 and 33.2 — while being
  **absent from the `'Items'` dimension entirely**, both active *after* the model's last refresh. The
  fact table is DirectQuery and reads live; `'Items'` is import-mode and only moves on refresh. This
  reader never joins `'Items'`.
- A **deleted** item keeps its rows. Run 30743411308 created `dbt_spark` at 10:16 UTC and the
  teardown deleted it at 10:34; it reads 30,940.3 CU, matching the app's own Items view to the
  decimal.
- `measure.py` run against the live model found **6 of 6** recorded items across two run records,
  deleted ones included.

The check stays anyway, because it costs nothing and would notice if a future version of the app
changed that. Every read logs

    history/runs/2026-08-02T1034Z-30743411308.json: 2/2 recorded item(s) found

and stores `unfound` in the ledger's `reads` entry.

## One number per item per operation, twice

```json
{"schema": 2, "updated": "...",
 "reads":   [{"at": "...", "since": "...", "items": 6, "changed": 4, "unfound": 0, "timed": 6}],
 "items":   {"<ITEM GUID>": {"Warehouse Query": 34016.048}},
 "seconds": {"<ITEM GUID>": {"Warehouse Query":   925.655}}}
```

That is the whole file, and it is the same shape as the app's own **Items** visual —
`Operation name | CU (s) | Duration (s)`, one row per item per operation.

**The operation is in the grain because it is the ONLY thing that separates COMPUTE from STORAGE**,
which share an item — see the engine-major bullet under *The page*.

**`seconds` is a SIBLING of `items`, not a nesting inside it.** Both leaves stay plain floats, so one
merge rule serves both and no reader's expected type changed. It is read from `Duration (s)` in the
same Capacity Metrics row, in the same `SUMMARIZECOLUMNS` — one more `SUM` on a query that runs
anyway, so it costs no request, no round trip and no capacity. That is the only free source for it:
dbt's own `run_results.json` never reaches the run record, and the Fabric notebook cannot write one.

**Its column is OPTIONAL by design.** `REQUIRED` is fatal — a role that will not resolve means the
read cannot be trusted, so it dies naming what the table actually had. Duration sits in `OPTIONAL`
instead, because its name is not measured against this app version the way the other five are, and a
guessed name in `REQUIRED` could kill the CU read that works today to gain a number the page can live
without. A miss costs the two time sections, logs what the table does have, and nothing else.

Three facts make everything else unnecessary:

1. **A deleted item keeps its CU rows in the metrics model.** Verified by hand against the live
   model: every item is still there after the teardown removed it. So deleting is free of
   measurement cost, and the teardown is unconditional.
2. **Every item is deleted when its run finishes**, so a total can only ever be INCOMPLETE, never
   wrong. The first read after a run usually undercounts — an hour's CU keeps growing for up to ~70
   minutes (~6 min ingestion lag, 5–64 min smoothing) — and the next read returns a bigger number.
3. **A run's items belong to that run and nothing else**, so a total per item already IS a total per
   run per engine.

There is no hour grain, no operation grain, no per-run window allocation and no settle-and-freeze
bookkeeping. There used to be all four.

### Three rules, none of which needs any state

- **Only items the read RETURNED are touched.** One that has aged past retention is simply absent
  from the result and keeps its last value — "upsert only, never remove", for free.
- **`max(old, new)`, never a blind overwrite and never `+`.** CU per item over a fixed window start
  only ever grows, so the larger value is the more complete one. That makes a re-read idempotent,
  makes an undercounted first read self-correcting, and protects an older item from being truncated
  when the floor walks forward past part of its window. Adding would multiply an item's cost by the
  number of times it was read and still look entirely plausible. **Seconds are the same kind of
  quantity** — a server-side SUM over the same rows from the same floor — so the same rule serves
  them unchanged.
- **The floor is bounded by retention**: the earliest recorded run start, clamped to `now − 14 days`,
  in the model's clock. One query covers everything that can still be learned and never more.

**A run measured just now is a LOWER BOUND, and the page says so per column.** Dispatch `Dashboard`
again an hour or two later and the numbers rise to their final value. Nothing has to be reconciled.

## Running it

```
gh workflow run Dashboard                       # measure, then publish
gh workflow run Dashboard -f measure=false      # re-render from what is committed — free, offline
                                                #   `publish` needs a status function for this path:
                                                #   GitHub's skip propagates past render's always()
gh workflow run Dashboard -f record=30752070535 # render one run alone
```

Locally, with no credentials and nothing installed:

```
python cu/dashboard.py > dashboard.md
python cu/report_html.py dashboard.md "footer" > index.html
```

The published page is <https://djouallah.github.io/fabric-dbt-benchmark/>, built from Actions with no
`gh-pages` branch and nothing committed; the per-run copy is that run's `dashboard` artifact. It is
self-contained by construction — inline CSS, no script, font or image, nothing fetched to render —
because that artifact has to open off a local disk years later.

## The page

- **Bar charts first**, ETL and analytics, one bar per column, **cheapest first** because "lower is
  better" makes the ranking the finding. A **zero sorts to the bottom**, never the top: zero means
  the engine did no such work, and at the top under that caption it would read as the winner. Each
  bar carries the adapter and the compute (`dbt-duckdb · 64 vCores`), because `iceberg` beside
  `duckrun` reads as an engine difference when it is a *writer* difference — same DuckDB, same
  notebook, same size.
- **Engine-major table**, engines across, **`compute` and `storage`** down, class subtotals in bold.
  The split comes from the OPERATION, and it has to: compute and storage share an ITEM. Measured
  against the live model — `dbt_spark` [Lakehouse] bills 188,636 CU of `High Concurrency Session Livy
  Run` and 20,268 of `OneLake Write via Redirect` against one GUID; `dbt_dwh` [Warehouse] bills
  129,177 of `Warehouse Query` beside its own OneLake writes. **Every `OneLake …` operation is
  storage; everything else is compute.** A dash means no operation of that kind was billed there at
  all — an iceberg lakehouse is 40,832 CU of pure OneLake, because its compute is the notebook, a
  different item. A class is only decomposed when some column holds more than one bucket, so
  `analytics` stays a single bold row.
- **Every lakehouse has a paired SQL analytics endpoint**, a separate billable `Warehouse` item with
  the same display name: `dbt_spark` 306.3 CU, `dbt_iceberg` 245.7, `dbt_delta` 278.9, all of it
  `SQL Endpoint Query`. It was invisible to the ledger until `provision.py` started recording it —
  the GUID is not the lakehouse's. It is never deleted by the teardown: Fabric removes it with its
  parent. **`dbt_landing` has one too, and it is the one door landing CU got onto the page through:**
  its role is `sql_endpoint`, not `landing`, so the role filter never saw it, and the same item
  appeared in every run record charging every engine 130.4 CU it did not spend. `landing_guids()`
  catches it by NAME against the record's own `landing` items, leaving an engine's own endpoint
  alone.
- **Engine-major is what makes the width work**: item-major needs a column per operation type and a
  lakehouse alone brings a dozen. **No total column and no grand-total row** — both would sum ACROSS
  engines, which is the one sum on this page that answers nothing, since the engines are alternatives
  to each other.
- **`landing` CU is not on the page at all.** The page compares ENGINES. `dbt_landing` is the
  ingestion staging area — no run deletes it, every run reads it — so its CU is one cumulative figure
  belonging to no engine, and it answers no question this page asks. It was briefly given a row of
  its own; the same number repeated under every column read as "each of them spent this". The
  archive's SIZE is still reported, because input volume is a different question from what ingesting
  it cost.
- **Input archive**: files and bytes in the landing archive, from `stats.py`'s listing. Every other
  number on the page describes what came OUT.
- **Table layout**, every shared table, mart first, with the analytics CU beside the mart alone (it
  is one number per engine, not per table).
- **Query time — cold, warm, hot.** The one thing on the page that is not capacity units, and it
  comes from the run records, not the ledger: `benchmark.timings.<model>.<query>` is already there on
  every record. `benchmark/render_report.py` renders it per dispatch, but a dispatch builds ONE
  engine, so that report always has a single column and its ranking is degenerate — composed here
  from every engine's latest run, this is the only place the three tiers can be read ACROSS engines
  at all. **cold** is the first visit to a freshly deployed semantic model, **warm** the second,
  **hot** the median of the passes after that; the record's own `tier` field is something else
  entirely (the query CATEGORY — `probe`/`composite`/`raw`/`hot_only`) and must not be confused with
  them. Each tier is summed over the queries **every column carries at that tier**, and the count is
  printed because it genuinely differs: the selectivity-ladder queries have no `cold_ms` at all, the
  top DUID being resolved only after pass 1, so cold is two queries short of warm and hot. A `hot
  spread` row carries the median per-query spread — where two columns sit closer together than that,
  the gap between them means nothing. Fastest per row in bold. The **cold** tier gets the chart,
  because it is the one the table layout moves: a first visit transcodes columns out of parquet, so
  V-Order, file count and row-group size show up there and nowhere else.
  Deliberately **reimplemented rather than imported** — `render_report._totals`/`rank` take exactly
  this shape, and `cu/` importing `benchmark/` would end the isolation that makes this directory
  deletable by removing one folder and one workflow file.
- **Time — how long the work took, and how hard it drew.** The same GUID→role→bucket join as the CU
  table, read off the ledger's `seconds` dict, with a `compute CU per second` row under each class.
  **Seconds here are BILLED OPERATION seconds, not wall clock**, and the difference is not small on
  every engine: a duckrun leg is one long notebook run so the two nearly agree, while spark opens
  five concurrent Livy REPLs under one session whose durations sum to more than the clock ever
  showed. **The rate is the sturdiest number in the section** — the average capacity the node drew
  while it ran, and the concurrency that makes spark's seconds hard to read appears in the numerator
  and the denominator alike, so it cancels. A high rate is a WIDE engine, not a slow one.
  **It is COMPUTE ÷ COMPUTE, and that is not a refinement — a total-over-total rate is wrong.** A
  storage operation bills real CU over a duration of essentially nothing (one `OneLake Write via
  Redirect`: 383.25 CU in **0.049 s**), so including storage does not dilute the rate, it detonates
  it, by an amount tracking only how much OneLake traffic the engine made. `CU (s)` is literally
  capacity-units × seconds, so `CU ÷ duration` is capacity units DRAWN — for a single-node Python
  notebook that is **`cores` ÷ 2**, fixed for a given core count and not a constant: 32.0 at the 64
  vCores dispatched by default, 16.0 at 32. The check when this reads oddly is two DuckDB legs at the
  **same** `cores` reading the **same** number, never that they read 32; `vcores` is part of
  `variant()`, so two core counts are two columns and the caption names each. The section renders
  **nothing** when the ledger has no seconds, which is the correct output both for a ledger written
  before the duration read and for a model that does not expose the column: an absent section says
  "not measured", a table of zeros would say "instant".
- **A record has to be built and benchmarked to reach the page.** `dashboard.py`'s `incomplete()`
  skips anything else and names why — a run with no benchmark shows an empty analytics column, which
  reads as "querying this engine was free" rather than "nobody measured it".
- **A run that was never TORN DOWN still renders, with a caveat.** Its items are alive and Fabric
  keeps billing them, so its total creeps upward and is an upper bound on that run rather than a
  measurement of it. It was briefly rejected outright; the creep is small and a column that
  disappears costs more than one carrying a caveat, so `drifting()` marks it **still billing** in the
  sources table instead — the loudest of the three states, because it is the only one that does not
  resolve by waiting. Deleting the items settles it.
- **Columns are each engine's latest run, once per config.** One dispatch builds one engine, so
  rendering the newest record alone would give a comparison page with one column. spark under
  `readHeavyForPBI` and spark under `writeHeavy` are two columns, because one number cannot answer
  for both. The cost — columns are different dispatches, days apart — is stated in the sources table
  rather than smoothed over.

The chart travels through the markdown as an HTML comment (`<!--chart:{…}-->`) that `report_html.py`
turns into SVG. The same markdown goes to the job summary, which sanitises inline SVG, so a comment
is the one form that is invisible there and drawable here. Do not "simplify" it into raw SVG.

## The CU columns are comparable, and that is the point of the unit

The engines are handed different compute — a 64-vCore notebook, a Livy pool, a warehouse — and it
does not qualify the comparison. **A capacity unit already prices that in.** 64 vCores for ten
minutes costs more CU than 8 vCores for ten minutes, which is exactly why CU leads: it is the bill.

**The two time sections do not have that property, and the page says which is which.** Billed
operation seconds SUM across concurrent operations, so a spark leg totals more than the clock it ran
on; query milliseconds are one sample of a shared capacity rather than a bill. They are on the page
because they answer a question CU cannot — how long a person waits, and how hard the engine drew
while they did — and each section states where its own number bends. Do not flatten the three into
one ranking.

The chart captions still name the configuration (`dbt-duckrun · 64 vCores`) because it says which
setting produced the number — a run at a different core count is a different data point, not an
invalid one.

## Things that will bite

- **`CU_MODEL_OFFSET_HOURS` is the app's own offset, not UTC** (+10 here). A wrong value reads as
  "no activity" rather than as an error.
- **The `since` filter is verified, not trusted.** `CALCULATETABLE` with a plain boolean predicate,
  never `FILTER(VALUES(...))` inside `SUMMARIZECOLUMNS` — the latter is accepted and silently changes
  nothing, and three different windows once returned byte-identical totals before anyone noticed. The
  hour is projected and the range that came back is checked against the floor.
- **One capacity per query.** These tables are DirectQuery and resolve one data location per query;
  passing several fails with an opaque `Internal Error: Error obtaining data location` naming neither
  the cause nor the capacity. Pinning `CU_CAPACITY_ID` also halves the request count on a tenant with
  two.
- **Column names move between app versions.** Microsoft's own accelerator ships four DAX variants for
  this reason. Every role is resolved against the real schema with `INFO.VIEW.COLUMNS()`, and a miss
  fails naming what was actually there.
- **Every real GUID is a secret.** `FABRIC_WORKSPACE_ID`, `CU_CAPACITY_ID`,
  `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`. No tracked file holds one, an input's `default:`
  cannot take a context, and `measure.py` keeps no fallback — a hardcoded one would put the value
  back in the repo and outvote the secret whenever the env var arrived empty.
- **14-day retention, ~6 minute lag, 5–64 minute smoothing.** Which is why the floor is clamped to
  the retention horizon: reading further back returns nothing, and an unbounded floor would grow the
  query for the life of the repo.
- **The render job checks out `ref: <branch>`, not the triggering SHA.** The measure job commits the
  ledger; a default checkout would read the version from before that commit and every page would be
  one dispatch stale.

## Env

| var | default | |
|---|---|---|
| `PBI_TOKEN` | — | minted by the workflow from the OIDC login |
| `CU_METRICS_WORKSPACE_ID` / `CU_METRICS_MODEL_ID` | — | the metrics app; both required |
| `CU_CAPACITY_ID` | — | pin it; unpinned costs an extra query plus a full read per capacity |
| `CU_WORKSPACE_FILTER` | — | the only row filter, and a column of the fact table itself |
| `CU_SINCE` | computed | override the floor, in the model's clock |
| `CU_MODEL_OFFSET_HOURS` | `10` | the app's own UTC offset |
| `CU_RETENTION_DAYS` | `14` | how far back the floor is allowed to reach |
| `CU_RUNS_DIR` / `CU_LEDGER` | `history/runs` / `history/cu.json` | |
| `CU_RECORD` | — | render one run alone (dashboard only) |
| `CU_LAYOUT_TABLE` | `fct_summary` | which table leads the layout section |

## Tests

`python -m pytest cu/ -q` — offline, no token, ~1s, and both jobs of the workflow run it. Everything
it pins fails as a plausible number rather than as an error: the three ledger rules, the GUID join,
that an absent item keeps its value, that a smaller later read never lowers a total, and that a
variant tag never contains the column separator.

## Isolation

No imports from `benchmark/`, no `run_report.json`, no shared concurrency group, no ADOMD, no .NET,
no duckrun. `requests` is the entire runtime dependency of the measurement and the render layer has
none at all, plus `pytest` for the offline suite. It is built to be deleted by removing one directory
and one workflow file — do not "DRY it up" against `benchmark/`; the duplication is what keeps that
deletion free.

## `history/legacy/`

Five records from the name-matching era, kept and read by nothing. They carry no item GUIDs, so they
cannot be joined to a ledger, and their numbers were measured under an attribution that put whole
notebooks in `shared`. They are there to be read by a human, not by this code.
