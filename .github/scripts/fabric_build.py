"""Run `dbt build` for one DuckDB-family engine (argv[1]: `duckrun` = Delta | `iceberg`) against
the landed data, writing to that engine's OneLake lakehouse.

CI always runs this inside a throwaway Fabric Python notebook, as the entry script fabric_run.py
ships via duckrun.run_python — a fresh interpreter whose cwd is the unpacked project root, with
duckrun / dbt-duckdb already pip-installed. ci.yml used to invoke it on the GitHub runner too for
folds a pending-file count called small; that path is gone.

Nothing below is location-specific, and it is worth keeping that way — it is what makes this
runnable by hand when you need to reproduce a CI failure. duckrun.auth resolves the OneLake token
from whatever is there (the Fabric runtime in a notebook, GitHub OIDC on a runner), so the token
is never shipped, and config (FILES_PATH, the output path, schema, limits) always arrives via env.

Tests run HERE, in the same invocation: `dbt build` interleaves model and test, so a broken model
stops the leg at the node that broke. There is no separate neutral-reader test job any more.
"""
import json
import os
import subprocess
import sys
import time

_MAX_ATTEMPTS = 3


def _only_tests_failed() -> bool:
    """True when every failing node of the last invocation was a data test.

    The retry ladder below exists for transient OneLake commit conflicts, which are a property of
    the WRITE. A data assertion is deterministic — it reads back the table this same invocation
    just wrote — so replaying it buys a second Fabric-side scan and the identical verdict, and
    under `dbt build` the downstream nodes it skipped stay skipped. Unreadable or absent
    run_results means we learned nothing, so fall through to retrying (the old behaviour).
    """
    try:
        with open("target/run_results.json") as fh:
            results = json.load(fh)["results"]
    except Exception:
        return False
    bad = [r for r in results if r["status"] in ("error", "fail")]
    return bool(bad) and all(r["unique_id"].startswith("test.") for r in bad)


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

    # `dbt build`: models and their tests in one DAG walk. The singular tests in tests/ are gated to
    # the duckdb-family targets by `data_tests: +enabled`, so this is the only place they run.
    #
    # No `--exclude tag:heavy`: nothing carries the tag now that the suite is one grain check on
    # fct_summary plus the dimension keys. Do not re-add the flag without re-adding a heavy test —
    # a selector matching zero nodes just warns and misdescribes what ran.
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
    # default target) so only the failed nodes re-run, not the whole idempotent build. `base` is
    # safe to pass to retry only because it is now just --target/--profiles-dir; a selection flag
    # in there would break it, since retry replays the selection recorded in run_results and
    # rejects --select/--exclude outright.
    ok = False
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        if attempt == 1 or not os.path.exists("target/run_results.json"):
            cmd = ["dbt", "build", *base]
        else:
            cmd = ["dbt", "retry", *base]
        print(f"[fabric_build] $ {' '.join(cmd)}", flush=True)
        ok = subprocess.run(cmd).returncode == 0
        if ok:
            break
        if _only_tests_failed():
            print(f"[fabric_build] {engine} attempt {attempt}/{_MAX_ATTEMPTS} failed on data "
                  f"tests only — deterministic, not a transient commit conflict; not retrying",
                  flush=True)
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
    # `build`, not `run`: the invocation above already tested the PRE-rebuild fct_summary, so the
    # table the leg reports on is not the one that was graded unless the tests run again after.
    if ok and os.environ.get("REBUILD_SUMMARY") == "1":
        cmd = ["dbt", "build", "--select", "fct_summary", "--full-refresh", *base]
        print(f"[fabric_build] $ {' '.join(cmd)}", flush=True)
        ok = subprocess.run(cmd).returncode == 0

    print(f"[fabric_build] {engine} build success={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
