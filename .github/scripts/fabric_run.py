"""Ship this dbt project into a throwaway Fabric Python notebook and run `dbt run` there.

This is the only way the DuckDB-family engines build. There was once a second path — run
fabric_build.py directly on the GitHub runner when the fold looked small — chosen per run by a
pending-file count; it is gone, and with it the risk of guessing wrong. Here,
duckrun.run_python zips the project, uploads it to a temporary Fabric notebook,
pip-installs duckrun, and runs .github/scripts/fabric_build.py as a subprocess on Fabric compute —
data-local to OneLake, so a backlog drain never pulls the corpus across the public internet —
streaming the log back.

The notebook is deleted HERE rather than by duckrun (`keep_notebook=True`), for one reason: Fabric
bills this leg's whole compute against that item, and duckrun's teardown never hands back its id.
An unrecorded GUID is compute that the CU ledger cannot attribute to an engine. So the run keeps it,
resolves the GUID by display name, records it, and deletes it — in a `finally`, since a session-level
failure raises.

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
import requests

import record

FAB = "https://api.fabric.microsoft.com/v1"

# Config the shipped project reads via env_var() — forwarded into the notebook if present.
# Deliberately excludes tokens and the runner-only OneLake curl transport (AZURE_TRANSPORT_*).
# REBUILD_SUMMARY was forwarded here; the input that set it is gone. SPARK_NATIVE_ENABLED does
# not belong here either — it is a Livy conf, and there is no Livy session on this path.
_FORWARD = ("FILES_PATH", "ONELAKE_TABLES_PATH", "WAREHOUSE_PATH", "ONELAKE_ENDPOINT",
            "DBT_SCHEMA", "download_limit", "daily_download_limit")


def _find_notebook(ws, token, name):
    """The GUID of the notebook we just ran, by display name. None if it cannot be found.

    Best-effort by construction: a missed GUID costs one un-attributed row in the CU ledger, and
    failing the leg over it would throw away a build that succeeded.
    """
    try:
        r = requests.get(f"{FAB}/workspaces/{ws}/items?type=Notebook",
                         headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if r.status_code != 200:
            print(f"[fabric_run] could not list notebooks ({r.status_code}) — "
                  f"the compute GUID goes unrecorded", flush=True)
            return None
        return next((i["id"] for i in r.json().get("value", [])
                     if i.get("displayName") == name), None)
    except Exception as ex:                             # noqa: BLE001 — never fail a green build
        print(f"[fabric_run] could not resolve the notebook GUID ({type(ex).__name__})", flush=True)
        return None


def _delete_notebook(ws, token, item_id):
    try:
        r = requests.delete(f"{FAB}/workspaces/{ws}/items/{item_id}",
                            headers={"Authorization": f"Bearer {token}"}, timeout=60)
        if r.status_code not in (200, 202, 204):
            print(f"[fabric_run] WARNING: notebook {item_id} not deleted ({r.status_code}) — "
                  f"delete it by hand, it is billable", flush=True)
            return False
        return True
    except Exception as ex:                             # noqa: BLE001
        print(f"[fabric_run] WARNING: notebook {item_id} not deleted ({type(ex).__name__})",
              flush=True)
        return False


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

    # keep_notebook, then delete it here — the ONE reason being that duckrun's own teardown never
    # tells us the item id, and a notebook GUID nobody wrote down is a notebook whose compute CU
    # cannot be attributed to an engine. Deleting it a couple of HTTP calls later than duckrun would
    # have changes nothing else: the name still carries a fresh random suffix, so nothing waits on a
    # display-name reservation.
    #
    # try/finally, because `run_python` RAISES when no attempt produced a result (a session-level
    # failure, e.g. capacity throttling). With keep_notebook the item would then survive the leg and
    # keep costing — the one outcome this change must not introduce. The cleanup swallows its own
    # errors for the same reason in reverse: an exception raised inside `finally` REPLACES the
    # build's, and losing the real failure to a bookkeeping one is not a trade worth making.
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
            keep_notebook=True,
        )
    finally:
        try:
            from duckrun.auth import get_fabric_token
            token = get_fabric_token()
            nb = _find_notebook(ws, token, name)
            if nb:
                deleted = _delete_notebook(ws, token, nb)
                record.item(nb, "compute", "Notebook", name, engine=engine, created=True,
                            at=record.now(), **({"deleted": record.now()} if deleted else {}))
                print(f"[fabric_run] notebook {name} ({nb}) "
                      + ("deleted" if deleted else "LEFT BEHIND"), flush=True)
            else:
                print(f"[fabric_run] notebook {name} not found — it may already be gone; "
                      f"its compute GUID goes unrecorded", flush=True)
        except Exception as ex:                         # noqa: BLE001 — see the comment above
            print(f"[fabric_run] notebook cleanup failed ({type(ex).__name__}: {ex}) — "
                  f"CHECK THE WORKSPACE for a leftover {name}", flush=True)

    print(f"[fabric_run] {engine} success={res.success} returncode={res.returncode}", flush=True)
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
