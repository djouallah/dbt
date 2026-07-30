# AEMO on four engines — one dbt project, switch the profile

An **educational** dbt project that runs the *same* Australian electricity-market (AEMO NEM)
pipeline on **four execution engines**. You pick the engine by switching the dbt **target** —
the model DAG, the `ref()` graph, and the tests are identical no matter which one you run.

```bash
dbt build --target duckrun  # DuckDB executes, delta-rs writes Delta Lake   (default; runs offline)
dbt build --target iceberg  # DuckDB + Iceberg REST catalog on OneLake
dbt build --target dwh      # Fabric Warehouse, pure T-SQL
dbt build --target spark    # Fabric Spark (Livy), writes Delta
```

## The one idea

Two of the engines speak the **same SQL dialect** (DuckDB), so they share one copy of every
model — switching between `duckrun` and `iceberg` really is *just* a profile change. The other
two have their **own SQL dialects** (Fabric Warehouse T-SQL, Spark SQL), so they are honest
*ports* of the same logic. That spectrum — identical code → dialect port — is the whole lesson.

```
                       ┌── duckrun  ─┐
   models/duckdb/  ────┤             │  same DuckDB SQL, two engines
                       └── iceberg  ─┘
   models/dwh/     ────── dwh          T-SQL port (OPENROWSET, TRY_CAST, [brackets])
   models/spark/   ────── spark        Spark SQL port (read_files, MERGE)
```

## How one project serves four engines

- **One profile, four outputs** (`profiles.yml`). `--target` selects the engine.
- **Three dialect folders** under `models/`, gated in `dbt_project.yml` so **exactly one is
  enabled** per target (on `target.type`). Because the model *names* are identical across
  folders, `ref()`, downstream models, and tests don't care which engine is live.

  | target | `type` | enabled folder |
  |---|---|---|
  | `duckrun` | `duckrun` | `models/duckdb` |
  | `iceberg` | `duckdb` | `models/duckdb` |
  | `dwh` | `fabric` | `models/dwh` |
  | `spark` | `fabricspark` | `models/spark` |

  > `iceberg` and `duckrun` both belong to the DuckDB family but have **different** adapter
  > `type`s, and `iceberg`/`ducklake`-style engines report `type: duckdb`. Where the two DuckDB
  > engines differ at all, the code keys on `target.name`, not `target.type`.

- **One shared download step** (`download_aemo.py`) — the only Python in the repo. It lands
  the raw AEMO files, **uncompressed**, into a *separate landing lakehouse*, plus a watermark
  `csv_raw_archive_log.parquet`. Every engine then just **reads those files with SQL**. Landing
  plain CSV is the key enabler: Fabric Warehouse `OPENROWSET` can't read gzip, and DuckDB/Spark
  read plain fine — so one landed format feeds all four.

## The pipeline

`stg_csv_archive_log` (view over the landed log) → `dim_calendar`, `dim_duid` → the daily and
intraday facts `fct_price[_today]`, `fct_scada[_today]` → `fct_summary` (the Power BI-facing
`(date, time, DUID)` grain joining generation to price). Every engine emits this identical set of
tables.

Every fact model writes with a keyed, insert-only strategy — never `append`, which has no
write-time key check and lets two overlapping runs both insert the same file. Each engine spells
that differently because the adapters differ: duckrun `insert`, iceberg `merge` +
`when_matched: do_nothing` (its catalog rejects multi-snapshot commits), spark `merge` +
`skip_matched_step`, and dwh a plain `merge`, the one adapter that cannot drop the matched
branch. **De-duplication removed the copy-paste, not the real engine differences.**

## Run it

### Quick, offline (DuckDB → local Delta)

```bash
pip install duckrun                 # brings dbt-duckdb, duckdb, deltalake
export FILES_PATH=./landing         # where the script lands raw CSVs
export ONELAKE_TABLES_PATH=./warehouse   # where duckrun writes Delta tables
python download_aemo.py             # land the raw CSVs once, then:
dbt build --target duckrun          # models + tests, one DAG walk
```

### The other engines

Install the adapter and set the engine's env vars, then `dbt build --target <name>`:

| target | adapter | key env vars |
|---|---|---|
| `iceberg` | `dbt-duckdb` | `WAREHOUSE_PATH`, `ONELAKE_ENDPOINT`, `ONELAKE_TOKEN`, `FILES_PATH` |
| `dwh` | `dbt-fabric-samdebruyn` | `FABRIC_DWH_SERVER`, `FABRIC_DWH_NAME`, `FABRIC_AUTH`, `FILES_PATH` |
| `spark` | `dbt-fabricspark` | `FABRIC_WORKSPACE_ID`, `FABRIC_LAKEHOUSE_ID`, `FABRIC_LAKEHOUSE_NAME`, `FABRIC_AUTH`, `FILES_PATH` |

All four share `FILES_PATH` (the landing lakehouse) and `DBT_SCHEMA` (default `mart`).

## CI — all four engines on real OneLake

`.github/workflows/ci.yml` runs the pipeline on **all four engines against Microsoft Fabric /
OneLake**. It's a matrix job (one per engine) that, in the `testing` workspace:

1. provisions the engine's Fabric item(s) **if missing** — a lakehouse for
   duckrun/iceberg/spark, a lakehouse + warehouse for dwh (`.github/scripts/provision.py`);
2. lands the raw AEMO files into that lakehouse's `Files` with the shared notebook;
3. `dbt build` against OneLake for that target — each engine writes and tests its own output in
   one DAG walk. A final `summary` job then reads all four items back through Delta and puts
   every shared table side by side — the staging view, the four facts, then `dim_calendar` /
   `dim_duid` / `fct_summary`, so a disagreement in the summary can be traced to its inputs on
   the rows above; that parity table
   is the only thing comparing engines to each other. Note the singular tests in `tests/` are
   DuckDB SQL and enabled on the duckdb-family targets only, so `dwh` and `spark` run just the
   generic key tests on the two dimensions.

Auth is **OIDC only** (the `fabric-github-deploy` app is Admin in the workspace) — the repo
needs just `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` secrets and a federated credential trusting
`repo:djouallah/dbt:ref:refs/heads/main`.

### Where the DuckDB-family build actually runs

In a Fabric notebook, always. `duckrun` and `iceberg` are both just DuckDB in a Python process, so
that process *could* live on the GitHub runner — but CI no longer tries to decide. `fabric_run.py`
zips the project into a throwaway notebook via `duckrun.run_python` and runs `fabric_build.py`
there, data-local to OneLake, so a backlog drain never pulls the corpus over the public internet.
`dwh` and `spark` never had the choice: their compute *is* Fabric's server, and the runner only
ever holds the dbt client.

Two placement heuristics were tried and removed. The first asked whether `land` had downloaded a
new `PUBLIC_DAILY` file, which describes the download rather than the backlog — a from-scratch
lakehouse has the whole ~3000-file archive waiting with nothing new landed, and that reads as
"small". The second counted each engine's genuinely pending files, but had to read that count
through the very tables the build was about to write, so an unreadable table collapsed the
estimate to its fail-safe sentinel regardless.

The saving on offer was one Fabric session start-up on quiet intraday runs. The cost of being
wrong was a 7GB runner thrashing through a full archive fold. One path is worth more than the
saving.

### Verify offline (no warehouse)

Targets also compile without connecting — useful locally:

```bash
dbt parse   --target duckrun     # manifest builds, enabled gates leave one model per name
dbt compile --target duckrun     # renders the DuckDB SQL (writes local Delta if run)
```

## Tests

Six assertions, over three tables, and **no test reads more than the one table it is about**:

| test | table | what it catches |
|---|---|---|
| `assert_fct_summary_grain` | `fct_summary` | a duplicate `(date, time, DUID)` — full table, no window |
| `unique` + `not_null` on `DUID` | `dim_duid` | a duplicate or missing unit key |
| `assert_duid_has_no_whitespace` | `dim_duid` | a padded `DUID`, which T-SQL matches and DuckDB/Spark don't |
| `unique` + `not_null` on `date` | `dim_calendar` | a duplicate or missing calendar date |

`fct_summary` is asserted for **uniqueness and nothing else**. It makes no claim about intervals
per day, unit counts, which dates should exist, or whether `mw`/`price` agree with the facts they
came from — so it cannot go red because AEMO published a short day or a backlog drained halfway.
The flip side is that it is the *only* thing watching that table: drift from `f(inputs)`, craters,
NULL prices and duplicates in the raw facts are all unasserted by design. The fact models and the
staging view carry descriptions and no tests at all.

Cross-engine agreement is not tested either — every assertion compares a table to itself. The
row-count parity table in the `summary` job is the one place the four outputs are compared.

Generic column tests run on **all four** targets — dbt renders them per adapter dialect. The
singular tests in `tests/` are DuckDB SQL and so are enabled only on the DuckDB-family targets
(`dbt_project.yml`), which means `dwh` — the one engine whose writes can genuinely race — is not
covered by the grain check. Run it there by hand.
