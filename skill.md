# Lessons Learned: DuckLake Metadata Sync with dbt-duckdb

## DETACH breaks dbt-duckdb adapter state

**Problem:** Calling `DETACH <database>` inside a dbt macro (on-run-start hook) breaks dbt-duckdb's internal connection state. Even if you `ATTACH` the database back, the adapter loses track of it and models fail with `Catalog Error: Catalog with name <db> does not exist!`.

**Root cause:** dbt-duckdb maintains its own internal record of attached databases. Raw SQL `DETACH`/`ATTACH` bypasses that tracking. The adapter thinks the database is gone even though DuckDB has it reattached.

**Rule:** Never `DETACH` a database that dbt-duckdb manages (via profiles.yml `attach:`) during on-run-start hooks or model execution. DETACH is only safe in on-run-end (nothing runs after it).

## Can't DETACH the active database

**Problem:** `DETACH <db>` fails silently if `<db>` is the currently active database (set via `database:` in profiles.yml).

**Fix:** Always `USE memory` before `DETACH`:
```sql
USE memory;
DETACH ducklake;
```

## COPY FORMAT BLOB creates a single file (not a directory)

**Confirmed:** `COPY (SELECT content FROM read_blob('remote')) TO '/tmp/file.db' (FORMAT BLOB)` writes a single file at the specified path — NOT a directory structure. This is different from partitioned COPY which creates directories.

## SHOW TABLES only shows the default schema

**Problem:** `SHOW TABLES` after `USE ducklake` only lists tables in the default schema (usually `main`). Model tables in named schemas (e.g., `aemo`) are invisible.

**Fix:** Use `information_schema.tables` for full visibility:
```sql
SELECT table_catalog, table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'ducklake'
ORDER BY table_schema, table_name
```

## BLOB length requires octet_length, not length

**Problem:** `length(content)` on a BLOB column fails:
```
No function matches 'length(BLOB)'
```

**Fix:** Use `octet_length(content)` for BLOB size in bytes.

## Solution: Pre-download metadata outside dbt

Since DETACH is not safe inside dbt hooks, the metadata SQLite file must be downloaded **before** `dbt run` starts, so that profiles.yml `attach:` opens the pre-populated file directly.

**Pattern:**
1. Notebook cell (before dbt): downloads blob from abfss:// to `/tmp/ducklake_metadata.db`
2. profiles.yml: `attach: ducklake:sqlite:/tmp/ducklake_metadata.db`
3. import_metadata (on-run-start): verification only — lists tables, no DETACH
4. export_metadata (on-run-end): `USE memory` → `DETACH` → `COPY ... FORMAT BLOB` upload (safe here, run is over)

## DuckLake requires local filesystem for SQLite metadata

DuckLake's metadata catalog is a SQLite database. SQLite needs local filesystem (file locking, fsync, WAL). It cannot run directly on object storage (abfss://, s3://, etc.).

**Workaround:** Sync the SQLite file as a binary blob between local `/tmp` and remote storage using `COPY (SELECT content FROM read_blob(...)) TO ... (FORMAT BLOB)`.
