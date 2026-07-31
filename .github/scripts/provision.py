"""Provision Fabric items (idempotent: create if missing, keep if present) in $WS_ID and
print the env vars dbt/the notebook read to stdout (the workflow appends stdout to
$GITHUB_ENV). Diagnostics -> stderr.

Usage:
  python provision.py reset                # DELETE all four OUTPUT items (never the landing one)
  python provision.py land                 # the ONE shared landing lakehouse (holds Files)
  python provision.py {duckrun|iceberg|dwh|spark}   # that engine's OUTPUT item

The download happens once (in the `land` job); every engine job reads the same FILES_PATH
and only provisions its own output item. Naming is prefixed `dbt_` so it never clashes with
the other AEMO repos sharing this workspace.

`reset` is the start-from-nothing lever (the workflow's `reset_outputs` input), and it is a
**separate mode run in its own job before `land`** rather than something each leg does to its
own item on the way in. That ordering is the whole point: Fabric keeps a deleted item's DISPLAY
NAME reserved for minutes after the item stops being listed, so a drop immediately followed by a
create draws `409 ItemDisplayNameNotAvailableYet` — which is exactly how the per-leg version
failed on run 30639018466. Deleting everything up front and then landing the data gives the
reservation the whole download to expire in.

It deletes the whole ITEM, not tables inside it — a `Tables/<schema>/<name>` folder removed by
hand leaves the catalog entry behind and dbt then emits DML against nothing. `dbt_landing` is
**excluded by name** and `drop()` refuses it outright: that lakehouse holds the downloaded AEMO
archive, the one thing here that cannot be rebuilt from anything else in the workspace. Costs,
none of them errors: the dwh warehouse comes back with a new connectionString and no grants, and
every item comes back with a new GUID, so anything bound to the old one (a Direct Lake semantic
model, a shortcut) is pointing at an item that no longer exists. `benchmark/` survives it because
it deletes and recreates its models per dispatch; nothing else in this repo holds a binding.
"""
import os, sys, time, subprocess, requests

mode = sys.argv[1]
LANDING = "dbt_landing"
# Every OUTPUT item, and the only list `reset` will touch. Kept beside the per-mode branches
# below, which must name the same items.
OUTPUTS = [("lakehouses", "dbt_delta"), ("lakehouses", "dbt_iceberg"),
           ("lakehouses", "dbt_spark"), ("warehouses", "dbt_dwh")]
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


def drop(kind, name):
    """Delete an output item. The recreate is a LATER JOB's business, not this call's.

    Deletes the ITEM, never a folder under it: a `Tables/<schema>/<name>` directory removed by
    hand leaves the catalog entry behind and dbt then emits DML against nothing (see CLAUDE.md).
    Waits for it to stop being listed, which is a weaker guarantee than it looks — Fabric holds
    the display NAME for minutes longer, which is why nothing recreates anything here."""
    if name == LANDING:
        raise SystemExit(f"refusing to drop {name}: it holds the raw landing data")
    it = find(kind, name)
    if not it:
        sys.stderr.write(f"  {kind}/{name} absent, nothing to drop\n")
        return
    sys.stderr.write(f"  DROPPING {kind}/{name} ({it['id']}) — reset requested\n")
    r = requests.delete(f"{FAB}/workspaces/{ws}/items/{it['id']}", headers=H)
    if r.status_code not in (200, 202, 204):
        sys.stderr.write(r.text + "\n")
        r.raise_for_status()
    for _ in range(120):
        if not find(kind, name):
            sys.stderr.write(f"  {kind}/{name} dropped\n")
            return
        time.sleep(5)
    raise SystemExit(f"timed out waiting for {kind}/{name} to disappear")


def ensure(kind, name, payload=None):
    it = find(kind, name)
    if it:
        sys.stderr.write(f"  {kind}/{name} exists ({it['id']})\n")
        return it["id"]
    sys.stderr.write(f"  creating {kind}/{name} in folder dbt ...\n")
    body = {"displayName": name, "folderId": FOLDER_ID}
    if payload:
        body.update(payload)
    # BACKSTOP, not the fix. A create too soon after a drop hits `ItemDisplayNameNotAvailableYet`
    # (409): Fabric frees the display NAME minutes after the item stops being listed. In CI that
    # cannot bite any more — the `reset` job drops everything before `land`, so the whole download
    # sits between the delete and the create — but a by-hand `provision.py reset` followed
    # straight away by a build would hit it, as run 30639018466 did when each leg dropped its own
    # item on the way in. Fabric marks the error `isRetriable`, so poll until it clears; anything
    # not retriable still raises on the first response.
    for attempt in range(40):                      # ~10 minutes at 15s
        r = requests.post(f"{FAB}/workspaces/{ws}/{kind}", headers=H, json=body)
        if r.status_code in (200, 201, 202):
            break
        try:
            err = r.json()
        except ValueError:
            err = {}
        if not (err.get("errorCode") == "ItemDisplayNameNotAvailableYet" or err.get("isRetriable")):
            sys.stderr.write(r.text + "\n")
            r.raise_for_status()
        if attempt == 0:
            sys.stderr.write(f"  name '{name}' still reserved from the drop, waiting for Fabric "
                             f"to release it ...\n")
        time.sleep(15)
    else:
        raise SystemExit(f"gave up waiting for the name '{name}' to be reusable: {r.text}")
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

if mode == "reset":
    # Deletes only. Nothing is recreated here and nothing is printed to stdout — the engine legs
    # provision their own item as usual, minutes later, by which time Fabric has released the
    # display names. See the module docstring for why that gap is the design.
    for kind, name in OUTPUTS:
        drop(kind, name)
    sys.stderr.write(f"  reset: {len(OUTPUTS)} output items dropped, {LANDING} untouched\n")

elif mode == "land":
    lh = ensure("lakehouses", LANDING, lh_payload)
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
