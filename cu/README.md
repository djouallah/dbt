# `cu/` — what the work cost, by Fabric item GUID

Two programs and one published page. `measure.py` reads capacity units from the Fabric Capacity
Metrics app's own semantic model and keeps a cumulative ledger; `dashboard.py` joins that ledger to
the run records and renders. Both run in the `Dashboard` workflow, one of only two workflows in this
repo.

Fabric exposes **no per-operation CU REST API**. The metrics app's semantic model is the only
authoritative source, which is why this exists at all.

## The two documents

| file | written by | shape |
|---|---|---|
| `history/runs/<ts>-<run id>.json` | the `Benchmark` workflow | every Fabric item GUID that run created, with its `role`, plus the layout, the input archive and the raw query timings |
| `history/cu.json` | `cu/measure.py` | `{item GUID: CU}` — one number per Fabric item |

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

## One number per item

```json
{"schema": 1, "updated": "...",
 "reads": [{"at": "...", "since": "...", "items": 6, "changed": 4}],
 "items": {"<ITEM GUID>": 31080.4}}
```

That is the whole file, and it is the same shape as the app's own **Items** visual —
`Item kind | Item Id | Item name | CU (s)`, one row per item. Three facts make everything else
unnecessary:

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
  number of times it was read and still look entirely plausible.
- **The floor is bounded by retention**: the earliest recorded run start, clamped to `now − 14 days`,
  in the model's clock. One query covers everything that can still be learned and never more.

**A run measured just now is a LOWER BOUND, and the page says so per column.** Dispatch `Dashboard`
again an hour or two later and the numbers rise to their final value. Nothing has to be reconciled.

## Running it

```
gh workflow run Dashboard                       # measure, then publish
gh workflow run Dashboard -f measure=false      # re-render from what is committed — free, offline
gh workflow run Dashboard -f record=30733912205 # render one run alone
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
  parent. That orientation
  is what makes the width work: item-major needs a column per operation type and a lakehouse alone
  brings a dozen. **No total column and no grand-total row** — both would sum ACROSS engines, which
  is the one sum on this page that answers nothing, since the engines are alternatives to each other.
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
- **Only WHOLE generations reach the page.** A record has to be built, benchmarked and torn down;
  `dashboard.py`'s `incomplete()` skips anything else and names why. A run that was not torn down has
  items still alive and still accruing, so its CU is not that run's cost but the cost of everything
  since; a run with no benchmark shows an empty analytics column, which reads as "querying this
  engine was free" rather than "nobody measured it". `measure.py` still reads the skipped records —
  their items really did cost capacity — and `history/runs/legacy/` holds the ones already known to
  be partial.
- **Columns are each engine's latest run, once per config.** One dispatch builds one engine, so
  rendering the newest record alone would give a comparison page with one column. spark under
  `readHeavyForPBI` and spark under `writeHeavy` are two columns, because one number cannot answer
  for both. The cost — columns are different dispatches, days apart — is stated in the sources table
  rather than smoothed over.

The chart travels through the markdown as an HTML comment (`<!--chart:{…}-->`) that `report_html.py`
turns into SVG. The same markdown goes to the job summary, which sanitises inline SVG, so a comment
is the one form that is invisible there and drawable here. Do not "simplify" it into raw SVG.

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
