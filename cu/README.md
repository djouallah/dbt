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
| `history/cu.json` | `cu/measure.py` | `{item GUID: {operation: {hour: CU}}}`, cumulative, plus which items have settled |

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
no resolving. And if a first read cannot see an item at all, the settle rule below picks it up on the
next one.

## The three ledger rules

1. **Upsert only, never remove.** A key that stops being returned — retention rolling past it — keeps
   its last value. This is the whole reason the ledger exists: the app retains about **14 days**.
2. **Latest read wins per `(guid, operation, hour)`.** An hour's CU keeps growing for up to ~70
   minutes after the fact (~6 min ingestion lag, 5–64 min smoothing), so overwriting is correct.
   Summing repeated reads would multiply every hour by how many times it was read — and still look
   plausible.
3. **Settle, then freeze.** An item is settled when a read changed nothing about it **and** its
   newest hour is at least `CU_SETTLE_HOURS` (3) old. A settled item is never rewritten and its time
   is never re-read.

Rule 3 is what "an item's CU is done when no more CU is being attributed to it" means in practice.
It also makes re-dispatching cheap: `measure.py` only ever queries from the earliest hour belonging
to an item that has not settled, so once a generation is final it costs nothing to look again.

**A fresh run is a LOWER BOUND, and the page says so per column.** Dispatch `Dashboard` again an hour
or two later to turn it into a final number.

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
- **Engine-major table**, engines across, operations down, class subtotals in bold. That orientation
  is what makes the width work: item-major needs a column per operation type and a lakehouse alone
  brings a dozen. **No total column and no grand-total row** — both would sum ACROSS engines, which
  is the one sum on this page that answers nothing, since the engines are alternatives to each other.
- **`landing` is a stage, not an engine.** `dbt_landing` holds the downloaded AEMO archive and is the
  only item that outlives a run, so it is the only thing allocated by hour window rather than by
  GUID, and it is reported on its own row — never added to an engine's column.
- **Input archive**: files and bytes in the landing archive, from `stats.py`'s listing. Every other
  number on the page describes what came OUT.
- **Table layout**, every shared table, mart first, with the analytics CU beside the mart alone (it
  is one number per engine, not per table).
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
- **14-day retention, ~6 minute lag, 5–64 minute smoothing.** A run older than the retention is
  force-settled with the reason recorded, because no further read can improve it and leaving it open
  would re-query forgotten time forever.
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
| `CU_SETTLE_HOURS` | `3` | quiet period before an item can freeze |
| `CU_MODEL_OFFSET_HOURS` | `10` | the app's own UTC offset |
| `CU_RETENTION_DAYS` | `14` | past this, an item is frozen whatever its state |
| `CU_RUNS_DIR` / `CU_LEDGER` | `history/runs` / `history/cu.json` | |
| `CU_RECORD` | — | render one run alone (dashboard only) |
| `CU_LAYOUT_TABLE` | `fct_summary` | which table leads the layout section |

## Tests

`python -m pytest cu/ -q` — offline, no token, ~1s, and both jobs of the workflow run it. Everything
it pins fails as a plausible number rather than as an error: the three ledger rules, the settle
conditions, the GUID join, the landing window allocation, and that a variant tag never contains the
column separator.

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
