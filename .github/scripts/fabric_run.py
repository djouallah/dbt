"""Ship this dbt project into a throwaway Fabric Python notebook and run `dbt run` there.

Used when `land` downloaded a new PUBLIC_DAILY file — a whole day of dispatch per file. (An
intraday-only fold runs fabric_build.py on the GitHub runner instead; see ci.yml.) Here,
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

import duckrun

# Config the shipped project reads via env_var() — forwarded into the notebook if present.
# Deliberately excludes tokens and the runner-only OneLake curl transport (AZURE_TRANSPORT_*).
_FORWARD = ("FILES_PATH", "ONELAKE_TABLES_PATH", "WAREHOUSE_PATH", "ONELAKE_ENDPOINT",
            "DBT_SCHEMA", "download_limit", "daily_download_limit", "REBUILD_SUMMARY")


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
        lakehouse="dbt_landing",                # hosts the tiny result/log round-trip files
        env=env,
        cores=cores,
        pip=["duckrun", "pytz"],                # duckrun brings dbt-duckdb + duckdb + deltalake
    )

    print(f"[fabric_run] {engine} success={res.success} returncode={res.returncode}", flush=True)
    return 0 if res.success else 1


if __name__ == "__main__":
    sys.exit(main())
