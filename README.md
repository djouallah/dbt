# AEMO on four engines — one dbt project, switch the profile

An **educational** dbt project that runs the *same* Australian electricity-market (AEMO NEM)
pipeline on **four execution engines**. You pick the engine by switching the dbt **target** —
the model DAG, the `ref()` graph, and the tests are identical no matter which one you run.

```bash
dbt run --target duckrun    # DuckDB executes, delta-rs writes Delta Lake   (default; runs offline)
dbt run --target iceberg    # DuckDB + Iceberg REST catalog on OneLake
dbt run --target dwh        # Fabric Warehouse, pure T-SQL
dbt run --target spark      # Fabric Spark (Livy), writes Delta
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

Each engine keeps the incremental strategy that actually fits it — e.g. the DuckDB models use an
insert-only `merge` (the OneLake Iceberg catalog rejects multi-snapshot commits), the Spark
models use `append` with a file-level filter. **De-duplication removed the copy-paste, not the
real engine differences.**

## Run it

### Quick, offline (DuckDB → local Delta)

```bash
pip install duckrun                 # brings dbt-duckdb, duckdb, deltalake
export FILES_PATH=./landing         # where the script lands raw CSVs
export ONELAKE_TABLES_PATH=./warehouse   # where duckrun writes Delta tables
python download_aemo.py             # land the raw CSVs once, then:
dbt run  --target duckrun
dbt test --target duckrun
```

### The other engines

Install the adapter and set the engine's env vars, then `dbt run --target <name>`:

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
3. `dbt run` + `dbt test` against OneLake for that target.

Auth is **OIDC only** (the `fabric-github-deploy` app is Admin in the workspace) — the repo
needs just `AZURE_CLIENT_ID` / `AZURE_TENANT_ID` secrets and a federated credential trusting
`repo:djouallah/dbt:ref:refs/heads/main`.

### Where the DuckDB-family build actually runs

`duckrun` and `iceberg` are the two engines CI is free to place, because both are just DuckDB in
a Python process — the same process can live on the GitHub runner or in a Fabric notebook. The
`land` job already knows how heavy the fold is (it just downloaded the files), so it decides:

| new `PUBLIC_DAILY` files landed | where `dbt run` executes | why |
|---|---|---|
| `0` | the GitHub runner | only intraday snapshots to fold — a Fabric session start-up costs more than the build |
| one or more (or unknown) | a Fabric notebook | a whole day of dispatch per file; keep it data-local to OneLake instead of pulling it over the public internet |

Both branches run the identical driver (`fabric_build.py`), so the build is the same either way;
only the interpreter it runs in changes. `dwh` and `spark` have no such choice: their compute *is*
Fabric's server, and the runner only ever holds the dbt client.

The signal is this run's **download**, not the engines' true backlog. If a leg fails, the run is
cancelled with files landed but unfolded, and the next push lands `0` new daily files and picks the
runner for a fold that isn't actually small — the job timeout is the backstop. Measuring the real
backlog means reading every engine's output, which isn't worth the machinery for the case a re-run
fixes.

### Verify offline (no warehouse)

Targets also compile without connecting — useful locally:

```bash
dbt parse   --target duckrun     # manifest builds, enabled gates leave one model per name
dbt compile --target duckrun     # renders the DuckDB SQL (writes local Delta if run)
```

## Tests

Generic column tests (`not_null`, `unique`, `relationships`, `accepted_values` in
`models/_*.yml`) run on **all four** targets — dbt renders them per adapter dialect. The custom
singular assertions in `tests/` are written in DuckDB SQL and are the **reference suite**,
enabled only on the DuckDB-family targets (`dbt_project.yml`).
