# Incomplete run records

Runs that are not a whole generation, kept for reference and read by nothing. The page (`dashboard/app.js`)
skips a record for any of the reasons in its `incomplete()`; this one is here so it cannot even be
offered to it, and so `measure.py`'s floor is not held back by a run nobody will render.

A run that was never TORN DOWN does **not** belong here. That used to send run 30733912205 (duckrun)
to this directory and it was moved back out; it is here now for a different reason — the config it
ran under, see the table.

| record | engine | why |
|---|---|---|
| `2026-08-02T1034Z-30743411308.json` | spark | **No benchmark.** The `bench` job was skipped by a `needs` bug (a job with no `if:` defaults to `success()` over the whole transitive graph, and the skipped `duckdb` matrix poisoned it), so only the ETL half exists. An empty analytics column reads as "querying this engine was free" rather than "nobody measured it". |
| `2026-08-02T0610Z-30733912205.json` | duckrun | **Built at `threads: 1`**, before duckrun 0.4.38 lifted its adapter's `config.threads = 1` pin. Every other engine has always run at 4, so this is a different workload, not a slower one — and `variant()` reads only `layout.config` (vcores, resource profile, NEE), so it would have keyed to the same column as a `threads: 4` duckrun run and been silently superseded rather than distinguished. Removing it also lets `measure.py`'s floor walk forward off a generation nobody will render. |

It is still a perfectly good raw record of what that run did. It is simply not comparable to a
complete one — an empty analytics column reads as "querying spark was free" — and the page's whole
job is comparison.
