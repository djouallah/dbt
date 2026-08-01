"""Ship this dbt project into a throwaway Fabric Python notebook and run `dbt run` there.

This is the only way the DuckDB-family engines build. There was once a second path — run
fabric_build.py directly on the GitHub runner when the fold looked small — chosen per run by a
pending-file count; it is gone, and with it the risk of guessing wrong. Here,
duckrun.run_python zips the project, uploads it to a temporary Fabric notebook,
pip-installs duckrun, and runs .github/scripts/fabric_build.py as a subprocess on Fabric compute —
data-local to OneLake, so a backlog drain never pulls the corpus across the public internet —
streaming the log back, then deletes the notebook.

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

# Config the shipped project reads via env_var() — forwarded into the notebook if present.
# Deliberately excludes tokens and the runner-only OneLake curl transport (AZURE_TRANSPORT_*).
# REBUILD_SUMMARY was forwarded here; the input that set it is gone. SPARK_NATIVE_ENABLED does
# not belong here either — it is a Livy conf, and there is no Livy session on this path.
_FORWARD = ("FILES_PATH", "ONELAKE_TABLES_PATH", "WAREHOUSE_PATH", "ONELAKE_ENDPOINT",
            "DBT_SCHEMA", "download_limit", "daily_download_limit")


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"
    ws = os.environ["WS_ID"]
    cores = int(os.environ.get("FABRIC_CORES", "8"))
    env = {k: os.environ[k] for k in _FORWARD if os.environ.get(k)}
    print(f"[fabric_run] engine={engine} cores={cores} "
          f"forwarding: {', '.join(sorted(env))}", flush=True)

    res = duckrun.workspace(ws).run_python(
        ".",                                    # ship this whole dbt project (cwd = project root)
        entry=".github/scripts/fabric_build.py",
        args=[engine],
        # Name the throwaway notebook after the ENGINE, because Fabric bills this leg's compute
        # against the notebook item and `cu/` can only report what the name says. duckrun's default
        # is `duckrun-py-<runid>`, identical for both DuckDB legs, so their CU was one undivided
        # row. The random suffix is NOT decoration and must stay: the notebook is deleted after
        # every run and Fabric keeps a deleted item's DISPLAY NAME reserved for minutes afterwards
        # (the 409 that killed three legs on run 30639018466), and `_execute_notebook` creates the
        # item with no retry around it. So the engine goes in the PREFIX — which is also what
        # cu/'s CU_GROUP_PREFIXES collapses on, giving one row per engine rather than one per run.
        name=f"dbt-{engine}-{uuid.uuid4().hex[:8]}",
        lakehouse="dbt_landing",                # hosts the tiny result/log round-trip files
        env=env,
        cores=cores,
        pip=["duckrun", "pytz"],                # duckrun brings dbt-duckdb + duckdb + deltalake
    )

    print(f"[fabric_run] {engine} success={res.success} returncode={res.returncode}", flush=True)
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
