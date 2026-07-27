"""Runs INSIDE a throwaway Fabric Python notebook (shipped by fabric_run.py via
duckrun.run_python). It is the notebook's entry script — a fresh interpreter whose cwd is the
unpacked dbt project root, with duckrun / dbt-duckdb already pip-installed.

Runs `dbt run` for one engine (argv[1]: `duckrun` = Delta | `iceberg`) against the landed data,
writing to that engine's OneLake lakehouse on Fabric compute — data-local, no data over the
GitHub runner. Tests are NOT run here — a separate CI job tests every engine's output with one
neutral DuckDB/Iceberg reader. Config (FILES_PATH, the output path, schema, limits) arrives via env
from fabric_run.py; the OneLake token is NOT shipped — it is acquired here from the Fabric runtime.
"""
import os
import subprocess
import sys
import time

_MAX_ATTEMPTS = 3


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"

    # The iceberg target (type: duckdb) reads ONELAKE_TOKEN from the env for its Iceberg REST
    # catalog + azure secret; acquire it from the Fabric runtime (notebookutils). duckrun (Delta)
    # self-acquires its own token, so setting it is harmless there.
    from duckrun import auth
    os.environ.setdefault("ONELAKE_TOKEN", auth.get_onelake_token())

    # Spill DuckDB temp files to the notebook's big work disk (the harness points TMPDIR there),
    # not the cramped /tmp overlay — a large iceberg aggregation / delta merge would fill /tmp.
    scratch = os.environ.get("TMPDIR") or "/tmp"
    os.environ.setdefault("DUCKDB_TEMP_DIR", os.path.join(scratch, "duckdb_spill"))

    # `dbt run` only — no tests. Testing is a separate CI job: a neutral DuckDB/Iceberg reader runs
    # the reference suite against every engine's output (the engine must not grade its own homework).
    base = ["--target", engine, "--profiles-dir", "."]

    # Retry ladder: the OneLake Iceberg REST catalog intermittently rejects a commit with
    # 409 Conflict ("One or more requirements failed. The client may retry.") under optimistic
    # concurrency — the same transient the standalone iceberg pipeline retries.
    #
    # Each attempt is a FRESH dbt subprocess, NOT an in-process dbtRunner re-invoke:
    # dbt-duckdb caches the DuckDB connection at module level, so a second invoke re-runs the
    # on-run-start `SET GLOBAL temp_directory` on a session whose temp dir is already in use —
    # "Cannot switch temporary directory after the current one has been used" — and every retry
    # is dead on arrival. Retries use `dbt retry` (with --target, else it renders the profile's
    # default target) so only the failed models re-run, not the whole idempotent build.
    ok = False
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        if attempt == 1 or not os.path.exists("target/run_results.json"):
            cmd = ["dbt", "run", *base]
        else:
            cmd = ["dbt", "retry", *base]
        print(f"[fabric_build] $ {' '.join(cmd)}", flush=True)
        ok = subprocess.run(cmd).returncode == 0
        if ok:
            break
        if attempt < _MAX_ATTEMPTS:
            backoff = 15 * attempt
            print(f"[fabric_build] {engine} attempt {attempt}/{_MAX_ATTEMPTS} failed; "
                  f"retrying in {backoff}s (transient OneLake commit conflicts)", flush=True)
            time.sleep(backoff)

    print(f"[fabric_build] {engine} build success={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
