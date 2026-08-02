# Incomplete run records

Runs that are not a whole generation, kept for reference and read by nothing. `cu/dashboard.py`
skips a record for any of the reasons in its `incomplete()`; this one is here so it cannot even be
offered to it, and so `measure.py`'s floor is not held back by a run nobody will render.

A run that was never TORN DOWN does **not** belong here — that used to send run 30733912205 (duckrun)
to this directory, and it has been moved back. Its items were left alive and Fabric keeps billing
them, so its total creeps upward; but the creep is small, and a column that disappears costs more
than one carrying a caveat. `drifting()` marks it **still billing** in the sources table instead.

| record | engine | why |
|---|---|---|
| `2026-08-02T1034Z-30743411308.json` | spark | **No benchmark.** The `bench` job was skipped by a `needs` bug (a job with no `if:` defaults to `success()` over the whole transitive graph, and the skipped `duckdb` matrix poisoned it), so only the ETL half exists. An empty analytics column reads as "querying this engine was free" rather than "nobody measured it". |

It is still a perfectly good raw record of what that run did. It is simply not comparable to a
complete one — an empty analytics column reads as "querying spark was free" — and the page's whole
job is comparison.
