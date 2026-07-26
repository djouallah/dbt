"""Provision Fabric items (idempotent: create if missing, keep if present) in $WS_ID and
print the env vars dbt/the notebook read to stdout (the workflow appends stdout to
$GITHUB_ENV). Diagnostics -> stderr.

Usage:
  python provision.py land                 # the ONE shared landing lakehouse (holds Files)
  python provision.py {duckrun|iceberg|dwh|spark}   # that engine's OUTPUT item

The download happens once (in the `land` job); every engine job reads the same FILES_PATH
and only provisions its own output item. Naming is prefixed `dbt_` so it never clashes with
the other AEMO repos sharing this workspace.
"""
import os, sys, time, subprocess, requests

mode = sys.argv[1]
ws = os.environ["WS_ID"]
FAB = "https://api.fabric.microsoft.com/v1"


def token(resource):
    """AAD token via the az CLI — the fallback for jobs without duckrun (spark/dwh, which need
    az for their adapters' CLI auth anyway)."""
    return subprocess.check_output(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"], text=True).strip()


def fabric_token():
    """Fabric control-plane token. Prefer duckrun's native GitHub-OIDC federation (no az login
    needed — it exchanges a fresh OIDC JWT via AZURE_CLIENT_ID / AZURE_TENANT_ID + id-token);
    fall back to the az CLI where duckrun isn't installed."""
    try:
        from duckrun.auth import get_fabric_token
        return get_fabric_token()
    except Exception:
        return token("https://api.fabric.microsoft.com")


H = {"Authorization": "Bearer " + fabric_token()}


def find(kind, name):
    r = requests.get(f"{FAB}/workspaces/{ws}/{kind}", headers=H)
    r.raise_for_status()
    return next((i for i in r.json().get("value", []) if i["displayName"] == name), None)


def ensure_folder(name):
    """Find-or-create a workspace folder; return its id (so all items group under it)."""
    it = find("folders", name)
    if it:
        return it["id"]
    r = requests.post(f"{FAB}/workspaces/{ws}/folders", headers=H, json={"displayName": name})
    if r.status_code in (200, 201):
        return r.json()["id"]
    sys.stderr.write(r.text + "\n")
    r.raise_for_status()


FOLDER_ID = ensure_folder("dbt")


def ensure(kind, name, payload=None):
    it = find(kind, name)
    if it:
        sys.stderr.write(f"  {kind}/{name} exists ({it['id']})\n")
        return it["id"]
    sys.stderr.write(f"  creating {kind}/{name} in folder dbt ...\n")
    body = {"displayName": name, "folderId": FOLDER_ID}
    if payload:
        body.update(payload)
    r = requests.post(f"{FAB}/workspaces/{ws}/{kind}", headers=H, json=body)
    if r.status_code not in (200, 201, 202):
        sys.stderr.write(r.text + "\n")
        r.raise_for_status()
    for _ in range(120):
        it = find(kind, name)
        if it:
            return it["id"]
        time.sleep(5)
    raise SystemExit(f"timed out waiting for {kind}/{name}")


def warehouse_conn(name):
    for _ in range(60):
        wh = find("warehouses", name)
        if wh and (wh.get("properties") or {}).get("connectionString"):
            return wh["properties"]["connectionString"]
        time.sleep(5)
    raise SystemExit(f"no connectionString for warehouse {name}")


def workspace_display_name():
    """The workspace's display name, resolved from its GUID (WS_ID) — Spark's
    workspace_name for schema-enabled lakehouse relations. Derived, never hardcoded."""
    r = requests.get(f"{FAB}/workspaces/{ws}", headers=H)
    r.raise_for_status()
    return r.json()["displayName"]


base = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com"
lh_payload = {"creationPayload": {"enableSchemas": True}}
out = []

if mode == "land":
    lh = ensure("lakehouses", "dbt_landing", lh_payload)
    n = os.environ.get("CI_DOWNLOAD_LIMIT", "1000")   # one knob: same cap for daily + intraday
    out += [f"FILES_PATH={base}/{lh}/Files",
            f"download_limit={n}",
            f"daily_download_limit={n}"]

elif mode == "duckrun":
    lh = ensure("lakehouses", "dbt_delta", lh_payload)
    out += [f"ONELAKE_TABLES_PATH={base}/{lh}/Tables"]

elif mode == "iceberg":
    lh = ensure("lakehouses", "dbt_iceberg", lh_payload)
    out += [f"WAREHOUSE_PATH={ws}/{lh}",
            "ONELAKE_ENDPOINT=https://onelake.table.fabric.microsoft.com/iceberg"]

elif mode == "spark":
    lh = ensure("lakehouses", "dbt_spark", lh_payload)
    out += [f"FABRIC_WORKSPACE_ID={ws}",
            f"FABRIC_WORKSPACE_NAME={workspace_display_name()}",
            f"FABRIC_LAKEHOUSE_ID={lh}",
            "FABRIC_LAKEHOUSE_NAME=dbt_spark"]

elif mode == "dwh":
    ensure("warehouses", "dbt_dwh")
    conn = warehouse_conn("dbt_dwh")
    out += [f"FABRIC_DWH_SERVER={conn}",
            "FABRIC_DWH_NAME=dbt_dwh",
            f"FABRIC_WORKSPACE_ID={ws}",
            "FABRIC_AUTH=CLI"]
else:
    raise SystemExit(f"unknown mode {mode}")

print("\n".join(out))
