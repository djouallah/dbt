"""Ship this dbt project into a throwaway Fabric Python notebook and run `dbt run` there.

This is the only way the DuckDB-family engines build. There was once a second path — run
fabric_build.py directly on the GitHub runner when the fold looked small — chosen per run by a
pending-file count; it is gone, and with it the risk of guessing wrong. Here,
duckrun.run_python zips the project, uploads it to a temporary Fabric notebook,
pip-installs duckrun, and runs .github/scripts/fabric_build.py as a subprocess on Fabric compute —
data-local to OneLake, so a backlog drain never pulls the corpus across the public internet —
streaming the log back.

duckrun creates the notebook, runs it and deletes it; `ScriptResult.item_id` names it (duckrun
>= 0.4.38), which is the whole reason this file records anything. Fabric bills this leg's compute
against that item, so a GUID nobody wrote down is compute the CU ledger cannot attribute to an
engine. This used to be `keep_notebook=True` plus a list-the-workspace-and-match-the-display-name
resolve and a delete of our own — a reimplementation of duckrun's teardown, two extra control-plane
calls, and a silent miss whenever the name lookup failed. The id is reported whether or not the
notebook still exists, and a run that died before the payload ran carries it on the exception, so
both outcomes are attributable.

argv[1] is the engine (`duckrun` = Delta | `iceberg`). Its output lakehouse and the landing
Files path are provisioned on the runner first (provision.py) and forwarded as CONFIG env — never
tokens: the notebook self-acquires its OneLake token from the Fabric runtime. duckrun itself
self-acquires the Fabric control-plane + OneLake tokens on the runner via GitHub OIDC
(AZURE_CLIENT_ID / AZURE_TENANT_ID + id-token: write), so no token is minted here.
"""
import os
import sys
import uuid

import duckrun

import record

# Config the shipped project reads via env_var() — forwarded into the notebook if present.
# Deliberately excludes tokens and the runner-only OneLake curl transport (AZURE_TRANSPORT_*).
# REBUILD_SUMMARY was forwarded here; the input that set it is gone. SPARK_NATIVE_ENABLED does
# not belong here either — it is a Livy conf, and there is no Livy session on this path.
_FORWARD = ("FILES_PATH", "ONELAKE_TABLES_PATH", "WAREHOUSE_PATH", "ONELAKE_ENDPOINT",
            "DBT_SCHEMA", "DUCKDB_SORTED", "download_limit", "daily_download_limit")


def _record_notebook(item_id, engine, name):
    """Record the throwaway notebook's GUID. Best-effort by construction, twice over: a missed
    GUID costs one un-attributed row in the CU ledger, and this also runs on the failure path,
    where an exception raised here would REPLACE the build's own and lose the real cause.

    No `deleted` timestamp: duckrun's teardown is best-effort (it warns rather than raising), so
    the record leaves the item to `provision.py teardown`, which polls for a 404 and goes red if
    it is still listed. A 404 there counts as success, which is the normal case.
    """
    try:
        if not item_id:
            print(f"[fabric_run] no item id for {name} — its compute GUID goes unrecorded",
                  flush=True)
            return
        record.item(item_id, "compute", "Notebook", name, engine=engine, created=True,
                    at=record.now())
        print(f"[fabric_run] notebook {name} ({item_id}) recorded", flush=True)
    except Exception as ex:                             # noqa: BLE001 — never fail a green build
        print(f"[fabric_run] could not record the notebook GUID ({type(ex).__name__}: {ex})",
              flush=True)


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"
    ws = os.environ["WS_ID"]
    cores = int(os.environ.get("FABRIC_CORES", "8"))
    env = {k: os.environ[k] for k in _FORWARD if os.environ.get(k)}
    # Name the throwaway notebook after the ENGINE. Fabric bills this leg's compute against the
    # notebook item, and duckrun's default name is `duckrun-py-<runid>` — identical for both DuckDB
    # legs, so their CU arrived as one undivided row. The random suffix is NOT decoration and must
    # stay: the notebook is deleted after every run and Fabric keeps a deleted item's DISPLAY NAME
    # reserved for minutes afterwards (the 409 that killed three legs on run 30639018466), while
    # `_execute_notebook` creates the item with no retry around it.
    name = f"dbt-{engine}-{uuid.uuid4().hex[:8]}"
    print(f"[fabric_run] engine={engine} cores={cores} notebook={name} "
          f"forwarding: {', '.join(sorted(env))}", flush=True)

    # `run_python` RAISES when no attempt produced a result (a session-level failure, e.g. capacity
    # throttling). That item was created and did bill, so it is recorded before the failure
    # propagates — duckrun sets `item_id` on the exception for exactly this case.
    try:
        res = duckrun.workspace(ws).run_python(
            ".",                                # ship this whole dbt project (cwd = project root)
            entry=".github/scripts/fabric_build.py",
            args=[engine],
            name=name,
            lakehouse="dbt_landing",            # hosts the tiny result/log round-trip files
            env=env,
            cores=cores,
            pip=["duckrun", "pytz"],            # duckrun brings dbt-duckdb + duckdb + deltalake
        )
    except BaseException as ex:
        _record_notebook(getattr(ex, "item_id", None), engine, name)
        raise
    _record_notebook(res.item_id, engine, name)

    print(f"[fabric_run] {engine} success={res.success} returncode={res.returncode}", flush=True)
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
