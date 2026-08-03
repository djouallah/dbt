# `cu/` — what the work cost, by Fabric item GUID

**One program, one output.** `measure.py` reads capacity units — and duration — from the Fabric
Capacity Metrics app's own semantic model and keeps a cumulative ledger at `history/cu.json`. That is
the whole of this directory: an exporter. It renders nothing, draws nothing and has no opinion about
what a page should look like.

The page that reads its output lives in [`dashboard/`](../dashboard/README.md) and is JavaScript
running in the reader's browser. **The two share no file and neither imports the other** — what
passes between them is `history/cu.json`, on disk and in git. Either can be deleted without touching
the other, which is the same property this directory has always had against `benchmark/`.

Both halves are driven by the `Dashboard` workflow, one of only two workflows in this repo.

Fabric exposes **no per-operation CU REST API**. The metrics app's semantic model is the only
authoritative source, which is why this exists at all.

## The two documents

| file | written by | shape |
|---|---|---|
| `history/runs/<ts>-<run id>.json` | the `Benchmark` workflow | every Fabric item GUID that run created, with its `role`, plus the layout, the input archive and the raw query timings |
| `history/cu.json` | `cu/measure.py` | `{item GUID: {operation: CU}}`, and a `seconds` sibling of the same shape |

They are joined on the **item GUID**, and that is the whole design. `measure.py` reads the run records
only to learn which GUIDs to ask about and how far back to reach; the join itself happens in the page.

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
which share an item: `dbt_spark` [Lakehouse] bills 188,636 CU of `High Concurrency Session Livy Run`
and 20,268 of `OneLake Write via Redirect` against one GUID. Every `OneLake …` operation is storage;
everything else is compute. Bucketing by the item's ROLE was tried and was wrong for exactly that
reason. The page depends on this grain being here — it cannot recover a split the ledger threw away.

**`seconds` is a SIBLING of `items`, not a nesting inside it.** Both leaves stay plain floats, so one
merge rule serves both and no reader's expected type changed. It is read from `Duration (s)` in the
same Capacity Metrics row, in the same `SUMMARIZECOLUMNS` — one more `SUM` on a query that runs
anyway, so it costs no request, no round trip and no capacity. That is the only free source for it:
dbt's own `run_results.json` never reaches the run record, and the Fabric notebook cannot write one.

**Its column is OPTIONAL by design.** `REQUIRED` is fatal — a role that will not resolve means the
read cannot be trusted, so it dies naming what the table actually had. Duration sits in `OPTIONAL`
instead, because its name is not measured against this app version the way the other five are, and a
guessed name in `REQUIRED` could kill the CU read that works today to gain a number the page can live
without. A miss costs the rate row on the page, logs what the table does have, and nothing else.

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

**Committing the ledger is how the numbers reach the page.** Nothing has to be rendered or deployed
afterwards: the published page fetches `history/cu.json` on every load. A pure top-up is therefore
`gh workflow run Dashboard -f publish=false`, and the page shows the new numbers to the next reader.

## Running it

```
gh workflow run Dashboard                        # measure, then deploy the page
gh workflow run Dashboard -f publish=false       # measure only — the live page picks it up anyway
gh workflow run Dashboard -f measure=false       # deploy only, when dashboard/ changed
gh workflow run Dashboard -f since='2026-08-01 00:00:00'   # re-read a window by hand
```

Locally, with a token in `PBI_TOKEN` and the four GUIDs in the environment:

```
python cu/measure.py
```

`RUN_RECORD` unset is deliberately a no-op elsewhere in the repo; here the run records are read from
`CU_RUNS_DIR` and the ledger written to `CU_LEDGER`, both overridable, so a by-hand read can be
pointed at a copy rather than at the committed file.

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
  fails naming what was actually there. This caught a real miss: the candidate list said `Item Name`,
  the app says `Item`. Watch `Datetime` in particular — the table also has a DATE-ONLY `Date` column,
  and resolving the floor to that would compare against midnight and silently widen every window.
- **Every real GUID is a secret.** `FABRIC_WORKSPACE_ID`, `CU_CAPACITY_ID`,
  `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`. No tracked file holds one, an input's `default:`
  cannot take a context, and `measure.py` keeps no fallback — a hardcoded one would put the value
  back in the repo and outvote the secret whenever the env var arrived empty.
- **14-day retention, ~6 minute lag, 5–64 minute smoothing.** Which is why the floor is clamped to
  the retention horizon: reading further back returns nothing, and an unbounded floor would grow the
  query for the life of the repo.
- **Moving a record in or out of `history/runs/legacy/` MOVES THE FLOOR.** It is derived from the
  earliest remaining run start, so parking a record narrows the window and the items of any run
  outside it stop being read. That is what made duckrun read 14.8 CU for a moment. Re-dispatch
  `Dashboard` after moving one.
- **`measure.py` deliberately does NOT skip incomplete records.** Those items really did cost
  capacity and the ledger is the ledger; it is the PAGE that must only compare like with like.

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

The page's own knobs are query parameters now, not environment variables — see
[`dashboard/`](../dashboard/README.md).

## Tests

`python -m pytest cu/ -q` — offline, no token, ~1s, and the measure job runs it before the login.
Everything it pins fails as a plausible number rather than as an error: the three ledger rules, the
settle conditions, that an absent item keeps its value, and that a smaller later read never lowers a
total.

## Isolation

No imports from `benchmark/` and none from `dashboard/`. No `run_report.json`, no shared concurrency
group, no ADOMD, no .NET, no duckrun. `requests` is the entire runtime dependency, plus `pytest` for
the offline suite. It is built to be deleted by removing one directory — do not "DRY it up" against
`benchmark/xmla_compare.py`; the duplication is what keeps that deletion free.

## `history/runs/legacy/`

Records that are not a whole generation, kept and read by nothing on the page. The oldest carry no
item GUIDs at all, so they cannot be joined to a ledger, and their numbers were measured under an
attribution that put whole notebooks in `shared`. They are there to be read by a human.
