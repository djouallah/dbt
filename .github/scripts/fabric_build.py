"""Run `dbt run` for one DuckDB-family engine (argv[1]: `duckrun` = Delta | `iceberg`) against the
landed data, writing to that engine's OneLake lakehouse.

CI always runs this inside a throwaway Fabric Python notebook, as the entry script fabric_run.py
ships via duckrun.run_python — a fresh interpreter whose cwd is the unpacked project root, with
duckrun / dbt-duckdb already pip-installed. ci.yml used to invoke it on the GitHub runner too for
folds a pending-file count called small; that path is gone.

Nothing below is location-specific, and it is worth keeping that way — it is what makes this
runnable by hand when you need to reproduce a CI failure. duckrun.auth resolves the OneLake token
from whatever is there (the Fabric runtime in a notebook, GitHub OIDC on a runner), so the token
is never shipped, and config (FILES_PATH, the output path, schema, limits) always arrives via env.

Tests are NOT run here — a separate CI job tests every engine's output with one neutral
DuckDB/Iceberg reader.
"""
import os
import subprocess
import sys
import time

_MAX_ATTEMPTS = 3


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"

    # The iceberg target (type: duckdb) reads ONELAKE_TOKEN from the env for its Iceberg REST
    # catalog + azure secret. get_onelake_token() picks the right source for wherever this is
    # running — notebookutils in Fabric, GitHub OIDC on the runner. duckrun (Delta) self-acquires
    # its own token, so setting it is harmless there.
    from duckrun import auth
    os.environ.setdefault("ONELAKE_TOKEN", auth.get_onelake_token())

    # Spill DuckDB temp files to the notebook's big work disk (the harness points TMPDIR there),
    # not the cramped /tmp overlay — a large iceberg aggregation / delta merge would fill /tmp.
    # setdefault, not assignment: a caller that already picked a spill dir keeps it.
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

    # One-time drift reconciliation. fct_summary's incremental path is a MERGE whose source
    # is only the handful of dates that can still be stale, so the rebuild lever has to be
    # dbt's own --full-refresh (a streaming overwrite) — a var that made the incremental
    # branch emit all history would hand delta_rs a 143M-row merge source instead.
    if ok and os.environ.get("REBUILD_SUMMARY") == "1":
        cmd = ["dbt", "run", "--select", "fct_summary", "--full-refresh", *base]
        print(f"[fabric_build] $ {' '.join(cmd)}", flush=True)
        ok = subprocess.run(cmd).returncode == 0

    print(f"[fabric_build] {engine} build success={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
