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

**Service principals probably do not work.** The Capacity Metrics semantic model is widely reported
to reject them, and this workflow is CI-only, so that is the most likely way it fails. The script
reads `PBI_TOKEN` and nothing else, and the workflow prefers a `PBI_TOKEN` secret over the OIDC SP
when one exists — so the fallback is a user token pasted as a secret. Those expire in about an
hour, so treat it as a per-investigation thing rather than a standing secret. A 401/403 says all
this in the error message.

**Column names move between app versions.** Microsoft's own fabric-toolbox accelerator ships four
DAX variants (v53/v47/v40/v37) for this reason. Nothing here hardcodes a name: `discover_columns()`
reads the real schema via `INFO.VIEW.COLUMNS()` first and resolves each role from a candidate list
in `WANTED`. A version bump fails with the actual column list printed, not silently empty. Fix it
by adding the new spelling to `WANTED`.

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
