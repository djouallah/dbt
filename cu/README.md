# `cu/` — CU per semantic model

One question: **what did the Power BI querying actually cost in capacity units?**

Fabric has no per-operation CU REST API. The Capacity Metrics app's own semantic model is the only
authoritative source, so this reads it by DAX and prints one table.

```
## Semantic model CU — 2026-07-30 13:00Z → 13:30Z

| semantic model | CU |
|---|---:|
| aemo_duckrun | 1,842.3 |
| aemo_iceberg | 1,190.4 |
| **total**    | **3,032.7** |
```

That is the whole output and the whole scope.

## What it deliberately is not

**It shares nothing with `benchmark/`.** No imports, no `run_report.json`, no artifact, no
`needs:`, no concurrency group, no ADOMD, no .NET, no duckrun. `requests` is the only dependency.
Deleting `cu/` and `.github/workflows/cu.yml` removes it completely and nothing else in the repo
notices — which is the point, because this may not turn out to be useful.

**It correlates nothing.** CU per model over a wall-clock window. It cannot tell you which query,
which benchmark run, or which engine produced a number. Attributing CU to individual queries needs
absolute per-query timestamps that `benchmark/` does not currently record, and wiring that up is
exactly the coupling this is avoiding.

## Running it

`gh workflow run cu` (or the Actions tab), `workflow_dispatch` only. **Wait ~10 minutes after the
activity you want to measure** — see the lag note below.

| input | default | notes |
|---|---|---|
| `window_minutes` | `30` | 2 API calls per minute of window; Power BI caps at 120/min |
| `end_utc` | now | ISO-8601. Max 14 days back |
| `metrics_workspace_id` | `7f7f5d92-…` | where the Capacity Metrics app is installed |
| `metrics_model_id` | `0fdedd3b-…` | the app's semantic model |
| `capacity_id` | all | blank = every capacity the metrics model can see |
| `utc_offset_hours` | `0` | the offset configured **in** the app, not yours |
| `debug` | false | logs the discovered schema and progress to stderr |

Locally, the same script runs against any of these with `PBI_TOKEN` set:

```bash
export PBI_TOKEN=$(az account get-access-token \
  --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
export CU_METRICS_WORKSPACE_ID=7f7f5d92-1603-4a02-a46a-0d90fe1ed119
export CU_METRICS_MODEL_ID=0fdedd3b-1451-4499-9ed4-aa3658100ec1
CU_WINDOW_MINUTES=10 CU_DEBUG=1 python cu/capacity_cu.py
```

## The five things that will bite

**Deduplication is load-bearing, not tidiness.** `'Timepoint Interactive Detail'` is gated by a
single 30-second `TimePoint` parameter, so the window is walked one bucket at a time. But an
interactive operation is *smoothed* across 10 to 128 buckets and **reappears in every one of them
carrying its full `Total CU`**. Summing the rows would multiply each operation by however many
buckets it happens to span. `collect()` keys on operation id instead. Anyone "simplifying" that
into a `SUM` produces numbers that are wrong by one to two orders of magnitude and look plausible.

**The service principal works — measured, against the expectation.** The community consensus is
that the Capacity Metrics semantic model rejects service principals, and this was built assuming
that would be the first thing to fail. It isn't: run 30536137179 read the model on the OIDC SP with
no 401/403. The `PBI_TOKEN` secret path is kept as the fallback anyway, because the workflow prefers
a secret over the SP when one is set and that costs nothing to leave in place. A user token expires
in about an hour, so it is a per-investigation thing, not a standing secret.

**There is no item-kind column, so "semantic model" is not enforced.** The installed app's
`'Timepoint Interactive Detail'` carries `Item`, `Operation`, `Operation Id`, `Total CU (s)`,
`Workspace Id`, `User`, `Billing type`, `Status`, `Duration (s)`, `Timepoint CU (s)` — and no item
kind of any spelling. So nothing can filter the table down to semantic models. Two things make that
tolerable: the table holds **interactive** operations only, which on this capacity are Power BI
model reads; and the script logs every operation name it counted to stderr, so a warehouse or
GraphQL row inflating the total is visible rather than hidden inside a number labelled "semantic
model CU". `workspace_filter` is the one narrowing actually available — pass the dbt workspace GUID
to exclude everything else on the capacity.

**Column names move between app versions.** Microsoft's own fabric-toolbox accelerator ships four
DAX variants (v53/v47/v40/v37) for this reason, and the miss above is exactly the failure mode —
the candidate list said `Item Name`, the app says `Item`. Nothing hardcodes a name:
`discover_columns()` reads the real schema via `INFO.VIEW.COLUMNS()` first and resolves each role
from a candidate list. Roles in `REQUIRED` fail loudly with the actual column list printed; roles in
`OPTIONAL` degrade to "not filtering on it". Fix a miss by adding the new spelling to the list.
With `debug: true` it prints every table's columns, not just this one — when a name moves, the
replacement is usually next door.

**14-day retention, ~6 minute lag, 5–64 minute smoothing.** A dispatch immediately after a
benchmark sees nothing. A window older than 14 days is rejected up front rather than returning an
empty table that reads as zero. And `Total CU` is a *smoothed attribution* for an operation, not an
instantaneous measurement — fine for comparing models to each other, misleading if read as "this
model used N CU at that moment".

**The timepoint offset is the model's, not yours.** Timepoints are stamped against the UTC offset
configured in the Capacity Metrics app. A default install is 0. If yours isn't, the window lands
somewhere else entirely and the symptom is "no activity" rather than an error — hence
`utc_offset_hours`.

## Never auto-trigger it

`workflow_dispatch` only. No `schedule`, no `push`, no `workflow_run`, no `needs:` from another
workflow — not even a nightly, not even behind an `if:`. This is the same standing rule
`benchmark/README.md` carries, for the same reason: these are interactive reads against shared
Fabric capacity, and a run nobody chose to start is the one a capacity admin asks about.
