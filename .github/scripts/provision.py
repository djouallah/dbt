"""Provision Fabric items (idempotent: create if missing, keep if present) in $WS_ID and
print the env vars dbt/the notebook read to stdout (the workflow appends stdout to
$GITHUB_ENV). Diagnostics -> stderr.

Usage:
  python provision.py reset                # DELETE all four OUTPUT items (never the landing one)
  python provision.py reset spark,dwh      # DELETE only those engines' items
  python provision.py land                 # the ONE shared landing lakehouse (holds Files)
  python provision.py {duckrun|iceberg|dwh|spark}   # that engine's OUTPUT item

The download happens once (in the `land` job) and every engine job provisions only its own output
item. The legs no longer share one FILES_PATH: each reads the SAME landed bytes through a
`Files/landing` shortcut in its own lakehouse (dwh through `dbt_dwh_src`, having no `Files` of its
own), so the read CU is attributed per engine instead of arriving as one undivided `dbt_landing`
row. See DWH_SRC. Naming is prefixed `dbt_` so it never clashes with the other AEMO repos sharing
this workspace.

`reset` is the start-from-nothing lever (the workflow's `reset_outputs` input), and it is a
**separate mode run in its own job before `land`** rather than something each leg does to its
own item on the way in. That ordering is the whole point: Fabric keeps a deleted item's DISPLAY
NAME reserved for minutes after the item stops being listed, so a drop immediately followed by a
create draws `409 ItemDisplayNameNotAvailableYet` — which is exactly how the per-leg version
failed on run 30639018466. Deleting everything up front and then landing the data gave the
reservation the whole download to expire in — but `skip_download` is on by default now, so that gap
is usually gone and `ensure()`'s 409 poll is what actually carries it. Polling the create is the
only authoritative test of whether a name is free, so this is sound; it just costs minutes.

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
# Every OUTPUT item, keyed by the ENGINE that writes it — the same key the dbt target, the leg's
# provision mode and `cu/`'s columns all use, and the only map `reset` will touch. Kept beside the
# per-mode branches below, which must name the same items.
#
# Keyed rather than a flat list because a reset is now SCOPED to the engines actually being built:
# a dispatch rebuilding spark alone must not delete the other three lakehouses, because every item
# dropped is a from-scratch rebuild of 370M rows charged to whoever dispatches next.
OUTPUT_BY_ENGINE = {"duckrun": ("lakehouses", "dbt_delta"),
                    "iceberg": ("lakehouses", "dbt_iceberg"),
                    "spark": ("lakehouses", "dbt_spark"),
                    "dwh": ("warehouses", "dbt_dwh")}
OUTPUTS = list(OUTPUT_BY_ENGINE.values())

# Every leg reads the landed CSVs through a `Files/landing` SHORTCUT to dbt_landing sitting in its
# OWN lakehouse, and that is what splits the read CU: OneLake accounts a transaction against the
# REQUESTED PATH, so a read through a shortcut is booked to the item hosting the shortcut, not to
# the item holding the bytes. It is the only way the documented rule ("the transaction usage counts
# against the capacity tied to the workspace where the shortcut is created") can be implemented.
# Before this, all four legs read dbt_landing directly and `cu/` had one undivided 6,578.9 CU row
# it could not attribute to anyone.
#
# No new items for duckrun/iceberg/spark — the shortcut goes into the output lakehouse they already
# have, so a leg's landing reads land in the same `cu/` column as its writes. dwh is the ONE
# exception and not by preference: a Fabric Warehouse has no `Files` section and cannot host a
# shortcut at all, so it gets a lakehouse holding this shortcut and nothing else.
#
# THE NAME IS LOAD-BEARING, and `dbt_dwh_landing` is the wrong spelling. `cu/capacity_cu.py`'s
# `engine_of()` substring-matches a display name against CU_ENGINES **in order** — which starts
# `landing` — so `dbt_dwh_landing` would match `landing` first and put dwh's reads straight back
# into the column this change exists to empty. `_src` collides with no engine token.
#
# NOT in OUTPUTS, deliberately: `reset` must never drop it. It is an input, it holds no data, and a
# shortcut host that gets deleted and recreated is the `ItemDisplayNameNotAvailableYet` 409 that
# killed three legs on run 30639018466. The other three shortcuts DO go down with `reset`, since
# they live inside items it deletes — which is why the shortcut is ensured in each engine's own
# mode, right after that mode recreates its lakehouse, and not once in `land` (where those three
# lakehouses may not exist yet, `reset` having just run).
DWH_SRC = "dbt_dwh_src"
LANDING_SHORTCUT = "landing"
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


def ensure_landing_shortcut(item):
    """Find-or-create `Files/landing` -> dbt_landing/Files inside `item`, and return the FILES_PATH
    the leg should read through. Never deletes anything, so it is safe on every run.

    Takes an item id and knows nothing about engines: the caller passes whichever lakehouse this
    leg reads from — its own output lakehouse for duckrun/iceberg/spark, DWH_SRC for dwh.

    Verified against the live warehouse before this was written: parquet OPENROWSET, an explicit
    multi-file CSV `BULK (...)` list and the `*.CSV` + `filepath(1)` fallback all return byte-
    identical results through the shortcut and through the direct path. `[file]` is unaffected —
    `parse_filename` stores the stem, never the path, so no merge key moves.
    """
    land = find("lakehouses", LANDING)
    if not land:
        raise SystemExit(f"{LANDING} does not exist — run `provision.py land` first")
    r = requests.get(f"{FAB}/workspaces/{ws}/items/{item}/shortcuts/Files/{LANDING_SHORTCUT}",
                     headers=H)
    if r.status_code == 200:
        sys.stderr.write(f"  shortcut Files/{LANDING_SHORTCUT} exists in {item}\n")
    else:
        sys.stderr.write(f"  creating shortcut Files/{LANDING_SHORTCUT} -> {LANDING}/Files "
                         f"in {item} ...\n")
        r = requests.post(
            f"{FAB}/workspaces/{ws}/items/{item}/shortcuts", headers=H,
            json={"path": "Files", "name": LANDING_SHORTCUT,
                  "target": {"oneLake": {"workspaceId": ws, "itemId": land["id"],
                                         "path": "Files"}}})
        if r.status_code not in (200, 201):
            sys.stderr.write(r.text + "\n")
            r.raise_for_status()
    return f"{base}/{item}/Files/{LANDING_SHORTCUT}"

if mode == "reset":
    # Deletes only. Nothing is recreated here and nothing is printed to stdout — the engine legs
    # provision their own item as usual, minutes later, by which time Fabric has released the
    # display names. See the module docstring for why that gap is the design.
    #
    # SCOPED to the engines named in argv[2] (comma-separated), or all four when absent. Dropping
    # an item the dispatch is not rebuilding is not a tidy-up: it is a from-scratch rebuild of that
    # engine's 370M rows, deferred onto whoever dispatches next. An unknown name is fatal rather
    # than ignored — a typo that silently resets nothing looks exactly like a reset that worked.
    picked = [e.strip() for e in (sys.argv[2] if len(sys.argv) > 2 else "").split(",") if e.strip()]
    picked = picked or list(OUTPUT_BY_ENGINE)
    unknown = [e for e in picked if e not in OUTPUT_BY_ENGINE]
    if unknown:
        raise SystemExit(f"reset: unknown engine(s) {unknown}; known: {list(OUTPUT_BY_ENGINE)}")
    for engine in picked:
        drop(*OUTPUT_BY_ENGINE[engine])
    kept = [e for e in OUTPUT_BY_ENGINE if e not in picked]
    sys.stderr.write(f"  reset: dropped {', '.join(picked)}"
                     + (f"; kept {', '.join(kept)}" if kept else "")
                     + f"; {LANDING} untouched\n")

elif mode == "land":
    lh = ensure("lakehouses", LANDING, lh_payload)
    # The FILES_PATH printed here is the DIRECT path, and stays that way: download_aemo.py writes
    # the archive through it, and the download's write CU belongs to `dbt_landing`. Only the legs
    # read through a shortcut, and each ensures its own — see the engine modes below.
    n = os.environ.get("CI_DOWNLOAD_LIMIT", "1000")   # one knob: same cap for daily + intraday
    out += [f"FILES_PATH={base}/{lh}/Files",
            f"download_limit={n}",
            f"daily_download_limit={n}"]

elif mode == "duckrun":
    lh = ensure("lakehouses", "dbt_delta", lh_payload)
    out += [f"ONELAKE_TABLES_PATH={base}/{lh}/Tables",
            f"FILES_PATH={ensure_landing_shortcut(lh)}"]

elif mode == "iceberg":
    lh = ensure("lakehouses", "dbt_iceberg", lh_payload)
    out += [f"WAREHOUSE_PATH={ws}/{lh}",
            "ONELAKE_ENDPOINT=https://onelake.table.fabric.microsoft.com/iceberg",
            f"FILES_PATH={ensure_landing_shortcut(lh)}"]

elif mode == "spark":
    lh = ensure("lakehouses", "dbt_spark", lh_payload)
    out += [f"FABRIC_WORKSPACE_ID={ws}",
            f"FABRIC_WORKSPACE_NAME={workspace_display_name()}",
            f"FABRIC_LAKEHOUSE_ID={lh}",
            "FABRIC_LAKEHOUSE_NAME=dbt_spark",
            f"FILES_PATH={ensure_landing_shortcut(lh)}"]

elif mode == "dwh":
    ensure("warehouses", "dbt_dwh")
    conn = warehouse_conn("dbt_dwh")
    # The one extra item this repo creates: a warehouse has no `Files` and cannot host a shortcut,
    # so dwh reads through a lakehouse holding nothing but that shortcut. See DWH_SRC.
    out += [f"FABRIC_DWH_SERVER={conn}",
            "FABRIC_DWH_NAME=dbt_dwh",
            f"FABRIC_WORKSPACE_ID={ws}",
            "FABRIC_AUTH=CLI",
            f"FILES_PATH={ensure_landing_shortcut(ensure('lakehouses', DWH_SRC, lh_payload))}"]
else:
    raise SystemExit(f"unknown mode {mode}")

print("\n".join(out))
