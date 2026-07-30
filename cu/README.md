# `cu/` — CU per semantic model

One question: **what did the benchmark's four semantic models cost in capacity units?**

Fabric has no per-operation CU REST API. The Capacity Metrics app's own semantic model is the only
authoritative source, so this reads it by DAX and prints one table.

```
## Semantic model CU — last 3h (2026-07-30 08:20Z -> 11:20Z)

| semantic model | CU   |
|----------------|-----:|
| aemo_duckrun   | 42.5 |
| aemo_iceberg   |  0.0 |
| aemo_spark     |  8.0 |
| aemo_dwh       |  0.0 |
| **total**      | 50.5 |
```

That is the whole output and the whole scope.

## What it deliberately is not

**It shares nothing with `benchmark/`.** No imports, no `run_report.json`, no artifact, no
`needs:`, no concurrency group, no ADOMD, no .NET, no duckrun. `requests` is the only dependency.
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
| `window_hours` | `3` | one query per capacity regardless of size; max 14 days |
| `models` | the four `aemo_*` | comma-separated, in report order. Blank = every semantic model |
| `workspace` | `ea575278-…` | the workspace ci.yml and benchmark.yml deploy to. Blank = all |
| `metrics_workspace_id` | `7f7f5d92-…` | where the Capacity Metrics app is installed |
| `metrics_model_id` | `0fdedd3b-…` | the app's semantic model |
| `capacity_id` | all | blank = every capacity the metrics model can see |
| `debug` | false | dumps every table's columns to stderr |

Locally, with `PBI_TOKEN` set:

```bash
export PBI_TOKEN=$(az account get-access-token \
  --resource https://analysis.windows.net/powerbi/api --query accessToken -o tsv)
export CU_METRICS_WORKSPACE_ID=7f7f5d92-1603-4a02-a46a-0d90fe1ed119
export CU_METRICS_MODEL_ID=0fdedd3b-1451-4499-9ed4-aa3658100ec1
CU_WINDOW_HOURS=3 CU_DEBUG=1 python cu/capacity_cu.py
```

## The things that will bite

**It reads `Metrics By Item And Hour`, not `Timepoint Interactive Detail`.** The detail table was
tried first and is the wrong instrument. It is bucketed at 30 seconds and gated by a
single-timepoint `MPARAMETER`, so a 3-hour window costs 360 requests per capacity; and because an
interactive operation is smoothed across 10–128 buckets it reappears in every one carrying its full
`Total CU`, so the rows have to be deduplicated by operation id or the total comes out one to two
orders of magnitude high. The hourly aggregate answers the same question in **one request per
capacity**, already summed, with no double-counting to guard against. Summing *is* correct on this
table precisely because it is already an aggregate. The detail table remains the right tool for
drilling into one timepoint's individual operations — which is not what this does.

**Both filters are needed, and they stack.** Display names are not unique across a tenant, so a
stale `aemo_spark` in some other workspace would otherwise be silently added to this one's CU.
`models` selects by name, `workspace` restricts to the workspace the benchmark deploys to, and both
apply. Every requested model is printed even with no activity — a `0.0` row distinguishes "ran and
cost nothing" from "vanished", which a missing row would not.

**One capacity per query.** These tables are DirectQuery and resolve one data location per query,
so `CapacitiesList` must carry exactly one capacity. Passing several fails with an opaque
`Internal Error: Error obtaining data location` that names neither the cause nor the capacity. This
tenant has two, so each is queried separately and the results merged.

**Item columns hold GUIDs, not names.** `Metrics By Item And Hour[Item Id]` is an id, so
`items_for()` joins to `'Items'` (`Item Id`, `Item name`, `Item kind`) to resolve it. An id missing
from `'Items'` is kept under its raw GUID rather than dropped — losing CU silently is worse than an
ugly row.

**The service principal works — measured, against the expectation.** The community consensus is
that the Capacity Metrics semantic model rejects service principals, and this was built assuming
that would be the first thing to fail. It isn't. The `PBI_TOKEN` secret path is kept as a fallback
anyway, since the workflow prefers a secret over the SP when one is set and that costs nothing to
leave in place. A user token expires in about an hour, so it is a per-investigation thing.

**Column names move between app versions.** Microsoft's own fabric-toolbox accelerator ships four
DAX variants (v53/v47/v40/v37) for this reason, and it has already bitten here — the first
candidate list said `Item Name`, the app says `Item name`. Nothing hardcodes a name:
`discover_columns()` reads the real schema via `INFO.VIEW.COLUMNS()` and resolves each role from a
candidate list, failing with the actual column list printed. `debug: true` dumps every table's
columns — when a name moves, the replacement is usually next door.

**14-day retention, ~6 minute lag, 5–64 minute smoothing.** A dispatch immediately after a
benchmark sees nothing. `CU (s)` is a smoothed attribution, not an instantaneous measurement — fine
for comparing models to each other, misleading if read as "this model used N CU at that moment".

## Never auto-trigger it

`workflow_dispatch` only. No `schedule`, no `push`, no `workflow_run`, no `needs:` from another
workflow — not even a nightly, not even behind an `if:`. Same standing rule
`benchmark/README.md` carries, for the same reason: these are reads against shared Fabric capacity,
and a run nobody chose to start is the one a capacity admin asks about.
