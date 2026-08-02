# Incomplete run records

Runs that are not a whole generation, kept for reference and read by nothing. `cu/dashboard.py`
skips a record for any of the reasons in its `incomplete()`; these two are here so they cannot even
be offered to it, and so `measure.py`'s floor is not held back by a run nobody will render.

| record | engine | why |
|---|---|---|
| `2026-08-02T0610Z-30733912205.json` | duckrun | **Not torn down.** It predates the teardown job, so `dbt_delta` and `aemo_duckrun` were left alive and have been billing ever since — their CU is not this run's cost, it is the cost of everything since. It also predates the landing-size listing. |
| `2026-08-02T1034Z-30743411308.json` | spark | **No benchmark.** The `bench` job was skipped by a `needs` bug (a job with no `if:` defaults to `success()` over the whole transitive graph, and the skipped `duckdb` matrix poisoned it), so only the ETL half exists. An empty analytics column reads as "querying this engine was free" rather than "nobody measured it". |

Both are still perfectly good raw records of what those runs did. They are simply not comparable to a
complete one, and the page's whole job is comparison.
