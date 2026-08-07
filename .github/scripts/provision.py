"""Provision Fabric items (idempotent: create if missing, keep if present) in $WS_ID and
print the env vars dbt/the notebook read to stdout (the workflow appends stdout to
$GITHUB_ENV). Diagnostics -> stderr.

Usage:
  python provision.py land                 # the ONE shared landing lakehouse (holds Files)
  python provision.py {duckrun|iceberg|dwh|spark}   # that engine's OUTPUT item
  python provision.py teardown <record>    # DELETE every item that record names, except landing

The download happens once (in the `land` job) and every engine job provisions only its own output
item. The legs no longer share one FILES_PATH: each reads the SAME landed bytes through a
`Files/landing` shortcut in its own lakehouse (dwh through `dbt_dwh_src`, having no `Files` of its
own), so the read CU is attributed per engine instead of arriving as one undivided `dbt_landing`
row. See DWH_SRC. Naming is prefixed `dbt_` so it never clashes with the other AEMO repos sharing
this workspace.

**Every item this touches is written down under its GUID**, into the run-record fragment named by
`RUN_RECORD` (see record.py) — created, found, or deleted, with the timestamp. That is the input the
CU ledger joins on: a GUID belongs to exactly one run, so `cu/` no longer has to guess an item's
owner from its display name. `RUN_RECORD` unset is a no-op, so running this by hand still works.

`teardown` is the end of that story and the reason a GUID belongs to one run at all. It runs LAST,
takes the run's own merged record as its argument, and deletes **every item that record names**
except the two whose `role` is `landing` or `folder` — so it can only ever delete what this run
created, and it needs no list of names to do it. That is a real property: a name-driven teardown
would happily delete an item some other dispatch had just made, and there is no undo.

It deletes the whole ITEM, not tables inside it — a `Tables/<schema>/<name>` folder removed by
hand leaves the catalog entry behind and dbt then emits DML against nothing. `dbt_landing` is
excluded twice over, by role and by name, and `drop()` refuses it outright: that lakehouse holds the
downloaded AEMO archive, the one thing here that cannot be rebuilt from anything else in the
workspace. Both FOLDERS survive too — they hold no data and cost nothing.

Items live in TWO folders and the split is the point: `benchmark` holds everything a run creates and
the teardown deletes, `landing` holds the one lakehouse that outlives every run. A workspace listing
then shows at a glance what is disposable, and `benchmark` is empty between dispatches — which is
exactly the state a successful teardown leaves behind. `benchmark/deploy_models.py` puts its semantic
models in the same `benchmark` folder, so one name covers every item either half of the workflow
makes.

Costs, none of them errors: the dwh warehouse comes back with a new connectionString and no grants,
and every item comes back with a new GUID, so anything bound to the old one (a Direct Lake semantic
model, a shortcut) points at an item that no longer exists. `benchmark/` survives it because it
deploys its models per dispatch. And **every build is now a full load**, because nothing survives to
build on incrementally — which is what `reset_outputs` used to buy, minus the job that bought it.

The display-name reservation this used to fight is now on the other side of the run. `reset` deleted
immediately before the build, so Fabric was still holding the names when the legs tried to create
them (`409 ItemDisplayNameNotAvailableYet`, which killed three legs on run 30639018466). Deleting at
the END puts a whole idle period between the delete and the next dispatch's create, and `ensure()`'s
40x15s poll stays as the guard.
"""
import json, os, sys, time, subprocess, requests

import record

mode = sys.argv[1]
LANDING = "dbt_landing"
# The REST collection name this script POSTs to, mapped to the item TYPE Fabric reports it as —
# which is the spelling the metrics model and the run record use. Two vocabularies for one thing,
# so the mapping lives here rather than being spelled out at each call site.
KIND = {"lakehouses": "Lakehouse", "warehouses": "Warehouse", "folders": "Folder"}
# What `teardown` will NOT delete, by role. `landing` holds the downloaded AEMO archive — the one
# thing here that cannot be rebuilt from anything else in the workspace — and `folder` is a workspace
# folder, which holds no data and costs nothing. Everything else a run creates is disposable by
# construction, which is the point: an item that outlives its run keeps drawing background CU into
# the NEXT run's window, and there is nothing in a capacity reading that says it did.
#
# `sql_endpoint` is in here for a different reason from the other two: it is not ours to delete.
# Fabric creates a SQL analytics endpoint alongside every lakehouse and removes it with the
# lakehouse, so attempting a DELETE would either fail or race the parent's. It is recorded — its CU
# is real and belongs to its engine — and then left alone.
TEARDOWN_KEEP = {"landing", "folder", "sql_endpoint"}

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
# into the column this change exists to empty. `_src` collides with no engine token. (That matcher
# is on its way out now that CU is attributed by GUID, but the name costs nothing to keep right.)
#
# It goes down with the teardown like everything else — it is recreated on the way in, holds no
# data, and an item that survives a run keeps drawing background CU into the next one's window.
# The `Files/landing` shortcut inside it goes with it, which is why the shortcut is ensured in each
# ENGINE's own mode rather than once in `land`: at `land` time the lakehouse that hosts it may not
# exist yet.
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

# Statuses worth trying again. Fabric hands back a bare 500 often enough to matter: run
# 31144099879's TEARDOWN died on `GET /workspaces/*/folders`, and the CU read died on
# `executeQueries` four minutes later — one bad four-minute window, two red runs, no code fault in
# either. 429 is the documented throttle.
_RETRY_STATUS = (429, 500, 502, 503, 504)
# Resolved off the module rather than imported, so the offline tests can keep substituting a plain
# namespace of verb functions for `requests` without also having to fake its exception tree. Real
# `requests` gives the precise base class; a stub without one degrades to `Exception`, which is
# correct for a stub that never raises transport errors in the first place.
_TRANSPORT_ERROR = getattr(getattr(requests, "exceptions", None), "RequestException", Exception)


def _req(method, url, *, tries=3, **kw):
    """One Fabric REST call, retried on TRANSIENT failures only, then returned AS-IS.

    **5xx, 429 and connection errors, nothing else.** A 4xx is a real answer that a caller here
    already reads: `409 ItemDisplayNameNotAvailableYet` has its own 40x15s poll in `ensure()`, and a
    404 from a DELETE means the item is already gone, which `drop_guid()` counts as success.
    Retrying those would either fight a caller that handles it or paper over a genuine bug.

    Exhausting the retries returns the LAST RESPONSE rather than raising, so every `raise_for_status`
    and every `status_code in (...)` check downstream sees exactly what it saw before this existed —
    the blast radius of adding this is a delay, never a different control flow. Only a connection
    error, which has no response to hand back, re-raises.
    """
    kw.setdefault("headers", H)
    kw.setdefault("timeout", 60)
    call = getattr(requests, method.lower())
    resp = exc = None
    for attempt in range(1, tries + 1):
        if attempt > 1:
            time.sleep(2 ** (attempt - 1))
        try:
            resp, exc = call(url, **kw), None
        except _TRANSPORT_ERROR as e:
            resp, exc = None, e
            sys.stderr.write(f"  {method} …/{url.rsplit('/', 1)[-1]}: {type(e).__name__} "
                             f"(attempt {attempt}/{tries})\n")
            continue
        if resp.status_code not in _RETRY_STATUS:
            return resp
        sys.stderr.write(f"  {method} …/{url.rsplit('/', 1)[-1]}: {resp.status_code} transient "
                         f"(attempt {attempt}/{tries})\n")
    if resp is not None:
        return resp
    raise exc


def find(kind, name):
    r = _req("GET", f"{FAB}/workspaces/{ws}/{kind}")
    r.raise_for_status()
    return next((i for i in r.json().get("value", []) if i["displayName"] == name), None)


def ensure_folder(name):
    """Find-or-create a workspace folder; return its id (so all items group under it)."""
    it = find("folders", name)
    if it:
        record.item(it["id"], "folder", "Folder", name, created=False, at=record.now())
        return it["id"]
    r = _req("POST", f"{FAB}/workspaces/{ws}/folders", json={"displayName": name})
    if r.status_code in (200, 201):
        fid = r.json()["id"]
        record.item(fid, "folder", "Folder", name, created=True, at=record.now())
        return fid
    sys.stderr.write(r.text + "\n")
    r.raise_for_status()


# TWO folders, and the split is the point: `benchmark` holds everything a run creates and the
# teardown deletes, `landing` holds the one lakehouse that outlives every run. A workspace listing
# then shows at a glance what is disposable and what is not — and `benchmark` is empty between
# dispatches, which is exactly the state the teardown is supposed to leave behind.
#
# `benchmark` rather than `dbt` because `benchmark/deploy_models.py` already puts its semantic models
# there (`BENCH_FOLDER`), so one name now covers every item either half of the workflow creates.
RUN_FOLDER, LANDING_FOLDER = "benchmark", "landing"
_FOLDERS = {}


def folder_id(name):
    """The folder's id, resolved on FIRST USE and cached — never at import.

    **LAZY IS THE WHOLE POINT.** These two were module-level assignments, so every subcommand paid
    `GET /workspaces/<ws>/folders` twice before `mode` was even dispatched — including `teardown`,
    which reads neither id (only `ensure()` does). Run 31144099879 lost its teardown to a transient
    Fabric 500 on exactly that call: the one job in the workflow that must always run, killed by an
    API it does not use, leaving items standing and its record with no deletion stamps. Resolving on
    demand means a teardown makes only the calls a teardown needs.
    """
    if name not in _FOLDERS:
        _FOLDERS[name] = ensure_folder(name)
    return _FOLDERS[name]


def reparent(item_id, folder_id, name):
    """Move an existing item into `folder_id`. Best-effort — a misfiled item still works.

    Needed because `folderId` is only honoured at CREATE: an item provisioned before this split
    stays where it was, and for `dbt_landing` that is forever, since nothing ever recreates it.
    """
    if not folder_id:
        return
    try:
        r = _req("POST", f"{FAB}/workspaces/{ws}/items/{item_id}/move",
                          json={"targetFolderId": folder_id}, timeout=60)
        if r.status_code in (200, 201, 202):
            sys.stderr.write(f"  moved {name} into folder {folder_id}\n")
        elif r.status_code not in (400, 404):
            sys.stderr.write(f"  note: could not move {name} ({r.status_code})\n")
    except Exception as ex:                            # noqa: BLE001
        sys.stderr.write(f"  note: could not move {name} ({type(ex).__name__})\n")


def drop_guid(guid, name, kind, role):
    """Delete one item BY ITS GUID and record the deletion. Returns True if it is gone afterwards.

    By GUID, not by name, and that is the safety property: `teardown` can only ever delete items
    this run's own record names, so a concurrent dispatch's freshly created `dbt_spark` cannot be
    caught by a name match. There is no undo for a wrong delete.

    A 404 counts as success — the item was already gone (the notebook deletes itself, a re-run of
    the teardown, a by-hand cleanup) and the record should say so either way.
    """
    if name == LANDING:
        raise SystemExit(f"refusing to drop {name}: it holds the raw landing data")
    r = _req("DELETE", f"{FAB}/workspaces/{ws}/items/{guid}")
    if r.status_code == 404:
        sys.stderr.write(f"  {kind}/{name} ({guid}) already gone\n")
    elif r.status_code not in (200, 202, 204):
        sys.stderr.write(f"  FAILED to delete {kind}/{name} ({guid}): {r.status_code} {r.text}\n")
        return False
    else:
        sys.stderr.write(f"  DELETED {kind}/{name} ({guid})\n")
    # Confirm rather than assume: the DELETE is accepted asynchronously (202), and an item that is
    # still listed is still billable. Same 10-minute budget as `drop()`.
    #
    # `tries=1` DELIBERATELY: this loop IS the retry, and letting `_req` back off inside it would
    # multiply the documented 120x5s budget by the backoff whenever Fabric is unwell — precisely
    # when the poll most needs to keep its shape. A connection blip is caught and read as "not gone
    # yet" rather than as an answer, because the only status that ends this loop is a 404 and a
    # thrown exception is not evidence the item was deleted.
    for _ in range(120):
        try:
            g = _req("GET", f"{FAB}/workspaces/{ws}/items/{guid}", tries=1)
        except _TRANSPORT_ERROR:
            g = None
        if g is not None and g.status_code == 404:
            record.item(guid, role, kind, name, deleted=record.now())
            return True
        time.sleep(5)
    sys.stderr.write(f"  WARNING: {kind}/{name} ({guid}) still listed — it is STILL BILLABLE\n")
    return False


def teardown(src):
    """Delete every item the run record names, except the roles in TEARDOWN_KEEP.

    Failures are collected, not raised one at a time: a warehouse that refuses to delete must not
    leave three lakehouses standing behind it. The exit status is non-zero if anything survived, so
    the job goes red and someone looks — a leftover item costs capacity silently otherwise.
    """
    with open(src, encoding="utf-8") as f:
        items = (json.load(f).get("items") or {})
    left = []
    for guid, it in sorted(items.items(), key=lambda kv: (kv[1].get("role", ""),
                                                          kv[1].get("name", ""))):
        role, name = it.get("role") or "?", it.get("name") or "?"
        if role in TEARDOWN_KEEP:
            sys.stderr.write(f"  keeping {role}/{name} ({guid})\n")
            continue
        if it.get("deleted"):
            sys.stderr.write(f"  {role}/{name} already deleted at {it['deleted']}\n")
            continue
        if not drop_guid(guid, name, it.get("kind") or "Item", role):
            left.append(f"{role}/{name} ({guid})")
    if left:
        raise SystemExit("teardown incomplete — these items are STILL BILLABLE: "
                         + "; ".join(left))
    sys.stderr.write(f"  teardown complete; {LANDING} untouched\n")


def record_sql_endpoint(item_id, name):
    """Record the SQL analytics endpoint Fabric creates alongside every lakehouse.

    It is a SEPARATE BILLABLE ITEM, of kind `Warehouse`, carrying the same display name — and it was
    invisible to this repo until it was measured: `dbt_spark` 306.3 CU, `dbt_iceberg` 245.7,
    `dbt_delta` 278.9, all of it `SQL Endpoint Query` and none of it in any run record, so the CU
    ledger's join could never see it. Small, but it is the difference between a total and nearly a
    total.

    Best-effort: the endpoint is provisioned asynchronously after the lakehouse, so a fresh one may
    have no id yet. Missing it costs a couple of hundred CU of attribution, never the build.
    """
    try:
        r = _req("GET", f"{FAB}/workspaces/{ws}/lakehouses/{item_id}")
        if r.status_code != 200:
            return None
        ep = ((r.json().get("properties") or {}).get("sqlEndpointProperties") or {}).get("id")
        if not ep:
            sys.stderr.write(f"  {name}: SQL endpoint not provisioned yet — not recorded\n")
            return None
        sys.stderr.write(f"  {name}: SQL endpoint {ep}\n")
        record.item(ep, "sql_endpoint", "Warehouse", name, created=False, at=record.now())
        return ep
    except Exception as ex:                            # noqa: BLE001 — never fail a build for this
        sys.stderr.write(f"  {name}: could not read the SQL endpoint ({type(ex).__name__})\n")
        return None


def ensure(kind, name, payload=None, role="output", folder=None):
    folder = folder_id(RUN_FOLDER) if folder is None else folder
    it = find(kind, name)
    if it:
        sys.stderr.write(f"  {kind}/{name} exists ({it['id']})\n")
        record.item(it["id"], role, KIND.get(kind, kind), name, created=False, at=record.now())
        # `folderId` is honoured only at CREATE, so an item provisioned before this split stays
        # where it was — and for `dbt_landing` that is forever, since nothing recreates it.
        reparent(it["id"], folder, name)
        if kind == "lakehouses":
            record_sql_endpoint(it["id"], name)
        return it["id"]
    sys.stderr.write(f"  creating {kind}/{name} ...\n")
    body = {"displayName": name, "folderId": folder}
    if payload:
        body.update(payload)
    # THE guard, and the only authoritative one. A create too soon after a delete draws
    # `ItemDisplayNameNotAvailableYet` (409): Fabric frees the display NAME minutes after the item
    # stops being listed, and polling the create is the only way to learn that it has. This was how
    # run 30639018466 lost three legs, when each leg dropped its own item on the way in. The
    # teardown now runs at the END of a run instead, so a whole idle period sits between a delete
    # and the next dispatch's create and this rarely fires — but back-to-back dispatches will still
    # land in it, and a leg waiting minutes here is that, not a permissions problem. Fabric marks
    # the error `isRetriable`; anything not retriable still raises on the first response.
    for attempt in range(40):                      # ~10 minutes at 15s
        r = _req("POST", f"{FAB}/workspaces/{ws}/{kind}", json=body)
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
            record.item(it["id"], role, KIND.get(kind, kind), name, created=True, at=record.now())
            if kind == "lakehouses":
                record_sql_endpoint(it["id"], name)
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
    r = _req("GET", f"{FAB}/workspaces/{ws}")
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
    r = _req("GET", f"{FAB}/workspaces/{ws}/items/{item}/shortcuts/Files/{LANDING_SHORTCUT}")
    if r.status_code == 200:
        sys.stderr.write(f"  shortcut Files/{LANDING_SHORTCUT} exists in {item}\n")
    else:
        sys.stderr.write(f"  creating shortcut Files/{LANDING_SHORTCUT} -> {LANDING}/Files "
                         f"in {item} ...\n")
        r = _req(
            "POST", f"{FAB}/workspaces/{ws}/items/{item}/shortcuts",
            json={"path": "Files", "name": LANDING_SHORTCUT,
                  "target": {"oneLake": {"workspaceId": ws, "itemId": land["id"],
                                         "path": "Files"}}})
        if r.status_code not in (200, 201):
            sys.stderr.write(r.text + "\n")
            r.raise_for_status()
    return f"{base}/{item}/Files/{LANDING_SHORTCUT}"

if mode == "teardown":
    # Deletes only, and nothing is printed to stdout — there is no env for a later job to read
    # because there is no later job. See teardown() and the module docstring.
    teardown(sys.argv[2])

elif mode == "land":
    lh = ensure("lakehouses", LANDING, lh_payload, role="landing",
                folder=folder_id(LANDING_FOLDER))
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
            f"FILES_PATH="
            f"{ensure_landing_shortcut(ensure('lakehouses', DWH_SRC, lh_payload, role='dwh_src'))}"]
else:
    raise SystemExit(f"unknown mode {mode}")

print("\n".join(out))
