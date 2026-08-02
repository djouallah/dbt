"""Read capacity units and duration from the Fabric Capacity Metrics model, PER FABRIC ITEM.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's own semantic model is the
only authoritative source, and it is read here with DAX over the Power BI `executeQueries` endpoint.

**The output is item GUID -> operation -> CU (s), cumulative**, and beside it the same shape in
SECONDS. The duration is one extra `SUM` in a query that runs anyway: no extra request, no extra
round trip, no capacity spent, and it is the only free answer to "how long did the ETL take" —
dbt's `run_results.json` never reaches the run record and the Fabric notebook cannot write one. Its
column is OPTIONAL (see `OPTIONAL`), so a model that does not expose it costs the page two sections
and nothing else. The operation is in the grain for exactly
one reason: it is the ONLY thing that separates COMPUTE from STORAGE. Both live inside one item — a
spark lakehouse bills 188,636 CU of `High Concurrency Session Livy Run` and ~31,800 of OneLake
operations against the same GUID, and a warehouse bills `Warehouse Query` beside its OneLake ops. No
per-item total can tell them apart.
That is all this repo needs, because of three facts:

1. **A deleted item keeps its CU rows.** Measured against the live model: run 30743411308's
   `dbt_spark` was created at 10:16 UTC and deleted by the teardown at 10:34, and still reads
   30,940.3 CU — matching the app's own Items view to the decimal. Deleting costs nothing.
2. **Every item is deleted when its run finishes**, so an item's total can only ever be INCOMPLETE,
   never wrong. A first read may undercount (~6 min ingestion lag, then 5-64 min of smoothing); the
   next read returns a bigger number and replaces it. It fixes itself, with nothing to reconcile.
3. **A run's items belong to that run and nothing else**, so a total per item already IS a total per
   run per engine. Attribution is the dashboard's dictionary lookup, not this file's problem.

Which is why there is no hour grain, no operation grain, no per-run window allocation and no
settle-and-freeze bookkeeping in here. There used to be all four.

**No refresh and no name resolution either.** The old reader refreshed the metrics model before every
read so items minutes old would be catalogued, and matched item DISPLAY NAMES to work out whose CU
was whose. Power BI throttles the REST API per identity: the service principal spent its budget, and
on two consecutive runs every refresh attempt drew 429 while 41,887 CU of DuckDB compute printed
under `shared` because two throwaway notebooks resolved to no name.

None of it was needed, and that is MEASURED rather than argued. The fact table carries `Item Id` and
`Workspace Id` as columns of its own, and it is DIRECTQUERY — on 2026-08-02 two item GUIDs carried CU
in it (7,654.8 and 33.2) while being absent from the import-mode `'Items'` dimension entirely, both
active AFTER the model's last refresh. A brand-new GUID is therefore summable the moment its rows
land, with no refresh, because this reader never joins `'Items'`.

## The ledger — `history/cu.json`

    {"schema": 2, "updated": "...",
     "reads":   [{"at": ..., "since": ..., "items": N, "changed": M, "unfound": K, "timed": T}],
     "items":   {"<ITEM GUID>": {"<operation>": 31080.4}},
     "seconds": {"<ITEM GUID>": {"<operation>":  1204.5}}}

`seconds` is a SIBLING of `items`, not a nesting inside it, so both leaves stay plain floats and one
merge rule serves both. It is absent or empty whenever the duration column was not resolved — which
the page reads as "not measured" and renders as no section, never as zero.

It exists because the app retains about **14 days**. Everything measured is gone within a fortnight
unless it is written down; a run record's GUIDs stay meaningful for years.

Three rules keep re-reading safe, and none of them needs any state:

- **Only items the read RETURNED are touched.** One that has aged past retention is simply absent
  from the result, so it keeps its last value. That is "upsert only, never remove", for free.
- **`max(old, new)`, never blind overwrite.** CU per item over a fixed window start only ever grows,
  so the larger number is always the more complete one — and this is also what protects an older
  item when the floor walks forward and its window gets truncated. Seconds are the same kind of
  quantity, read the same way from the same floor, so the same rule serves them unchanged.
- **The floor is bounded by retention.** The earliest recorded run start, clamped to `now - 14 days`,
  so one query covers everything that can still be learned and never more.

Env in: `PBI_TOKEN` (minted from the OIDC login), `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`,
`CU_CAPACITY_ID`, `CU_WORKSPACE_FILTER`. Optional: `CU_SINCE` (override the floor),
`CU_MODEL_OFFSET_HOURS`, `CU_RETENTION_DAYS`, `CU_RUNS_DIR`, `CU_LEDGER`.

stdout is a one-line summary; diagnostics go to stderr.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:                                   # the dashboard's render path never needs it
    requests = None

PBI = "https://api.powerbi.com/v1.0/myorg"
TOKEN = os.environ.get("PBI_TOKEN", "").strip()
WS = os.environ.get("CU_METRICS_WORKSPACE_ID", "").strip()
MODEL = os.environ.get("CU_METRICS_MODEL_ID", "").strip()
CAPACITY = os.environ.get("CU_CAPACITY_ID", "").strip()
# The only row filter, and it is a column of the fact table itself — which is what makes a GUID-only
# read possible with no join and no name resolution.
WS_FILTER = os.environ.get("CU_WORKSPACE_FILTER", "").strip().upper()

RUNS_DIR = os.environ.get("CU_RUNS_DIR", "history/runs").strip()
LEDGER = os.environ.get("CU_LEDGER", "history/cu.json").strip()

# The metrics model stamps its timestamps in the offset configured IN THE APP, not in UTC. A wrong
# value here reads as "no activity" rather than as an error, which is why it is named and not
# inlined. +10 for this tenant.
MODEL_OFFSET = timedelta(hours=float(os.environ.get("CU_MODEL_OFFSET_HOURS", "10")))
# What the app keeps. The floor is clamped to it: reading further back cannot return anything, and
# an unbounded floor would grow the query for the life of the repo.
RETENTION_DAYS = float(os.environ.get("CU_RETENTION_DAYS", "14"))
# 2 added the `seconds` sibling. Nothing branches on this — a ledger is normalised on load either
# way — it is here so a committed file says which shape it was written in.
SCHEMA = 2

# The hourly fact table. `Metrics By Item` also exists and is one row per item with no time
# dimension at all — closer to what this file wants — but it carries no date column, so there is
# nothing to put a floor on and nothing to verify a floor against. The hourly table summed per item
# gives byte-identical totals (checked against the app's own Items view) and keeps the window ours.
TABLE = "Metrics By Item Operation And Hour"
# Column names move between versions of the app — Microsoft's own accelerator ships four DAX variants
# for exactly this reason — so every role is resolved against the real schema and a miss fails
# specifically, naming what was actually there.
#
# **The first name in each list is the one MEASURED against the live model (2026-08-02); the rest are
# fallbacks.** They were the other way round and worked only by falling through, which is luck rather
# than design. Note especially `Datetime`: the table also has a `Date` column that is date-only, and
# resolving `when` to that would compare a floor against midnight and silently widen every window.
REQUIRED = {
    "item_id": ["Item Id", "ItemId", "Item"],
    "workspace_id": ["Workspace Id", "WorkspaceId", "Workspace"],
    "cu": ["CU (s)", "CU(s)", "Total CU (s)", "CU"],
    "when": ["Datetime", "Date Hour", "DateHour", "Date/Time"],
    # The operation is what separates COMPUTE from STORAGE, and it is the only thing that can:
    # both live inside one item. `dbt_spark` bills 188,636 CU of `High Concurrency Session Livy Run`
    # and ~31,800 of OneLake operations against the SAME lakehouse; `dbt_dwh` bills 129,177 of
    # `Warehouse Query` and ~3,300 of OneLake against the same warehouse. Split by item and the two
    # are inseparable. Measured 2026-08-02.
    "operation": ["Operation name", "Operation", "Operation Name"],
}

# Resolved if present, SKIPPED if not — and that distinction is the whole point of a second dict.
# `REQUIRED` is fatal by design: a role that does not resolve means the read cannot be trusted, so it
# dies naming what was there. Duration is a BONUS riding an existing query, and its column name is
# not measured against this app version the way the five above are. Putting it in `REQUIRED` would
# mean a guessed name could kill the CU read that works today, to gain a number the page can live
# without. So a miss costs the two time sections and nothing else, and the log names what the table
# actually has — which is how the right name gets added.
OPTIONAL = {
    "duration": ["Duration (s)", "Duration", "Total Duration (s)", "Operation Duration (s)"],
}


def log(msg):
    sys.stderr.write(msg + "\n")


def die(msg):
    log("ERROR: " + msg)
    raise SystemExit(1)


# --------------------------------------------------------------------------------- the metrics model

def execute_dax(dax, tries=4, fatal=True):
    """POST one DAX query. Retries the rate limits, honouring `Retry-After` when it is given."""
    url = f"{PBI}/groups/{WS}/datasets/{MODEL}/executeQueries"
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    for i in range(tries):
        r = requests.post(url, headers={"Authorization": f"Bearer {TOKEN}"}, json=body, timeout=300)
        if r.status_code == 200:
            return r.json()["results"][0]["tables"][0].get("rows", [])
        if r.status_code in (429, 502, 503, 504) and i < tries - 1:
            wait = int(r.headers.get("Retry-After") or min(60, 5 * 2 ** i))
            log(f"  {r.status_code} from executeQueries; retrying in {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (401, 403):
            die(f"executeQueries returned {r.status_code}. The service principal needs read access "
                f"to the Capacity Metrics model. A user token works as a manual escape hatch: "
                f"`az account get-access-token --resource https://analysis.windows.net/powerbi/api "
                f"--query accessToken -o tsv` exported as PBI_TOKEN.")
        if fatal:
            die(f"executeQueries returned {r.status_code}: {r.text[:400]}")
        log(f"  executeQueries returned {r.status_code}: {r.text[:200]}")
        return None
    return None


def strip_prefix(rows):
    """executeQueries returns keys as `Table[Column]` or `[Alias]`. Reduce to the bare name."""
    return [{re.sub(r"^.*\[|\]$", "", k): v for k, v in row.items()} for row in (rows or [])]


def discover_columns():
    """Resolve every role against the model's real schema, so a version bump fails specifically.

    A `REQUIRED` role that misses is fatal. An `OPTIONAL` one that misses is `None` and is NAMED, so
    the job log says which column the model actually has — the only way the right duration name gets
    found without spending a read to guess.
    """
    rows = strip_prefix(execute_dax("EVALUATE INFO.VIEW.COLUMNS()"))
    cols = {r.get("Name") for r in rows if r.get("Table") == TABLE}
    if not cols:
        die(f"the model at {WS}/{MODEL} has no table named '{TABLE}'. Either that is not the Fabric "
            f"Capacity Metrics model, or this app version renamed it. Tables present: "
            f"{sorted({r.get('Table') for r in rows if r.get('Table')})}")
    got, missing = {}, []
    for role, candidates in REQUIRED.items():
        hit = next((c for c in candidates if c in cols), None)
        (got.__setitem__(role, hit) if hit else missing.append(f"{role} (tried {candidates})"))
    if missing:
        die(f"'{TABLE}' exists but these columns were not found: {'; '.join(missing)}. "
            f"Present: {sorted(cols)}. Add the actual name to REQUIRED in this file.")
    for role, candidates in OPTIONAL.items():
        got[role] = next((c for c in candidates if c in cols), None)
        if got[role]:
            log(f"  {role} -> '{got[role]}'")
        else:
            log(f"  no {role} column (tried {candidates}) — that measurement is skipped, the rest is "
                f"unaffected. '{TABLE}' has: {sorted(cols)}")
    return got


def capacities():
    if CAPACITY:
        return [CAPACITY]
    # Unpinned costs an extra query to enumerate, then a full read per capacity. Pinning it is the
    # normal case and halves the request count on a tenant with two.
    for col in ("Capacity Id", "capacity Id", "CapacityId"):
        rows = execute_dax(f"EVALUATE VALUES('Capacities'[{col}])", fatal=False)
        if rows:
            ids = [str(v) for r in strip_prefix(rows) for v in r.values() if v]
            if ids:
                return ids
    die("could not read any capacity id; set CU_CAPACITY_ID")


def read_cu(cap, since, c):
    """CU and DURATION per (item, operation) for one capacity, from `since` onward, summed
    server-side.

    The operation is in the grain because it is the ONLY thing that separates compute from storage —
    a spark lakehouse bills its Livy compute and its OneLake operations against one item, and so does
    a warehouse. The hour is not: nothing downstream wants it, and cumulative CU per item is the form
    that stays meaningful once the app has forgotten the window.

    Duration rides along in the same `SUMMARIZECOLUMNS` when the column resolved — one more `SUM` on
    a query that runs anyway, so it costs no request, no round trip and no capacity. It is what lets
    the page say how long the work took as well as what it cost, and there is no other free source:
    dbt's own `run_results.json` never reaches the run record and the notebook cannot write one.

    `FirstHour` is projected for one reason and it is not decoration: a DAX filter that is accepted
    and then silently ignored produces a plausible wrong number, which is the worst failure this tool
    can have. It happened once — `FILTER(VALUES(...))` inside `SUMMARIZECOLUMNS` was accepted and
    changed nothing, and three different windows returned byte-identical totals before anyone
    noticed. So the predicate is a plain boolean inside `CALCULATETABLE`, and the caller checks the
    earliest hour that actually came back against the floor it asked for.

    ONE CAPACITY PER QUERY, deliberately. These tables are DirectQuery and resolve one data location
    per query; passing several fails with an opaque `Internal Error: Error obtaining data location`
    naming neither the cause nor the capacity.
    """
    secs = (f',\n        "Seconds", SUM ( \'{TABLE}\'[{c["duration"]}] )' if c.get("duration") else "")
    inner = f"""SUMMARIZECOLUMNS (
        '{TABLE}'[{c['item_id']}],
        '{TABLE}'[{c['workspace_id']}],
        '{TABLE}'[{c['operation']}],
        "CU", SUM ( '{TABLE}'[{c['cu']}] ),
        "FirstHour", MIN ( '{TABLE}'[{c['when']}] ){secs}
    )"""
    if since:
        lit = (f"DATE({since.year}, {since.month}, {since.day}) + "
               f"TIME({since.hour}, {since.minute}, 0)")
        inner = f"CALCULATETABLE (\n        {inner},\n        '{TABLE}'[{c['when']}] >= {lit}\n    )"
    return strip_prefix(execute_dax(f"""
DEFINE
    MPARAMETER 'CapacitiesList' = {{ "{cap}" }}
EVALUATE
    {inner}
""".strip(), fatal=False))


# ------------------------------------------------------------------------------------- the ledger

def blank():
    return {"schema": SCHEMA, "updated": None, "reads": [], "items": {}, "seconds": {}}


def load_ledger(path=None):
    path = path or LEDGER
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return blank()
    doc.setdefault("items", {})
    # Absent on every ledger written before the duration read, and absent again on any read where
    # the model had no duration column. `{}` is the correct state for both: the page renders what it
    # HAS, so no seconds means no time section rather than a section full of zeros.
    doc.setdefault("seconds", {})
    doc.setdefault("reads", [])
    # An earlier ledger stored one NUMBER per item, before the operation was needed to separate
    # compute from storage. Such an entry cannot be bucketed — the number is both, mixed — so it is
    # dropped rather than filed under a guessed operation, and the next read repopulates it in full.
    # Safe because the floor reaches back to the earliest run, and nothing outside retention could
    # have been in there anyway.
    for key in ("items", "seconds"):
        legacy = [g for g, v in doc[key].items() if not isinstance(v, dict)]
        for g in legacy:
            del doc[key][g]
        if legacy:
            log(f"  dropped {len(legacy)} {key} entr(ies) stored without operations — this read "
                f"repopulates them")
    return doc


def save_ledger(doc, path=None):
    path = path or LEDGER
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        # sort_keys so a read that moves one number is a one-line diff in the commit; indent=1 so
        # the diff is readable at all.
        json.dump(doc, f, indent=1, sort_keys=True)
    return path


def load_runs(directory=None):
    """Every run record, oldest first. These are what say which window is worth reading."""
    directory = directory or RUNS_DIR
    out = []
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return out
    for n in names:
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(directory, n), encoding="utf-8") as f:
                rec = json.load(f)
        except Exception as ex:                       # noqa: BLE001
            log(f"  skipping {n}: unreadable ({type(ex).__name__})")
            continue
        rec["_file"] = n
        out.append(rec)
    return out


def fold(rows, cols):
    """`({guid: {op: CU}}, {guid: {op: seconds}}, [earliest hours seen])` for this workspace's rows.

    Two parallel dicts, and the leaf of each stays a plain FLOAT. Nesting `{"cu": …, "seconds": …}`
    under the operation would have been tidier to look at and would have changed the type every
    reader downstream expects — `item_cu`, `total`, `apply`, the dashboard's join and every test that
    touches them. Two flat dicts governed by one merge rule cost a parameter instead.

    The seconds dict is EMPTY when the model had no duration column, never zeros: "not measured" and
    "took no time" are different claims, and the page has to be able to tell them apart.

    The workspace test is the only filter, and it is applied here rather than in DAX because the
    query is one round trip either way and a rejected row is easier to explain from this side.
    """
    cu, secs, stamps = {}, {}, []
    for r in rows or []:
        guid = str(r.get(cols["item_id"]) or "").upper()
        wsid = str(r.get(cols["workspace_id"]) or "").upper()
        value = r.get("CU")
        if not guid or value is None:
            continue
        if WS_FILTER and wsid != WS_FILTER:
            continue
        first = str(r.get("FirstHour") or "")[:19]
        if first:
            stamps.append(first)
        op = str(r.get(cols["operation"]) or "(unnamed)").strip()
        per = cu.setdefault(guid, {})
        per[op] = round(per.get(op, 0.0) + float(value), 3)
        seconds = r.get("Seconds")
        if seconds is not None:
            per_s = secs.setdefault(guid, {})
            per_s[op] = round(per_s.get(op, 0.0) + float(seconds), 3)
    return cu, secs, stamps


def apply(ledger, read, key="items"):
    """Merge a read into one of the ledger's dicts, keeping the LARGER number. Returns how many
    (item, operation) pairs moved.

    `max`, not overwrite, and never `+`. A per-item sum over a window with a FIXED FLOOR only ever
    grows, so the larger value is always the more complete one — which makes a re-read idempotent,
    makes an undercounted first read self-correcting, and protects an older item from being truncated
    when the floor walks forward past part of its window. Adding would multiply an item's cost by the
    number of times it was read, and still look entirely plausible.

    **The rule holds identically for duration**, for exactly that reason and no other: it is the same
    kind of quantity — a server-side SUM over the same rows, from the same floor.
    """
    changed = 0
    for guid, ops in read.items():
        cur = ledger.setdefault(key, {}).setdefault(guid, {})
        for op, value in ops.items():
            if cur.get(op) is None or value > cur[op]:
                cur[op] = value
                changed += 1
    return changed


def total(ledger, guid, key="items"):
    """One item's CU — or seconds — across every operation it was billed for."""
    return sum(((ledger.get(key) or {}).get(guid) or {}).values())


def coverage(runs, read):
    """Which recorded items this read did and did not find. Returns `[(file, found, missing)]`.

    The standing check on the assumption that no metrics-model REFRESH is needed. That assumption is
    now MEASURED rather than argued (2026-08-02, against the live model): two item GUIDs carried CU
    in the fact table — 7,654.8 and 33.2 — while being absent from the `'Items'` dimension entirely,
    and both were active AFTER the model's last refresh. The fact table is DirectQuery and reads
    live; `'Items'` is import-mode and only moves on refresh. This reader never joins `'Items'`, so a
    brand-new GUID is summable the moment its rows land.

    Kept anyway, because it costs nothing and it is the thing that would notice if a future version
    of the app changed that. A run whose items are all found minutes after it finished confirms it; a
    run whose items are still missing hours later would disprove it, and the fix would be an opt-in
    refresh rather than a guess.

    A `folder` is excluded: a workspace folder never accrues a capacity unit — it is not even in the
    `'Items'` dimension — so its absence means nothing either way.
    """
    out = []
    for rec in runs:
        items = {g: it for g, it in (rec.get("items") or {}).items()
                 if (it.get("role") or "") != "folder"}
        if not items:
            continue
        missing = [f"{it.get('role', '?')}/{it.get('name') or g}"
                   for g, it in sorted(items.items(), key=lambda kv: kv[1].get("role", ""))
                   if g not in read]
        out.append((rec.get("_file", "?"), len(items) - len(missing), missing))
    return out


def floor_for(runs, now_model):
    """The earliest hour worth asking about: the first recorded run start, clamped to retention.

    Clamped because reading further back can return nothing — the app has forgotten it — while an
    unbounded floor would make the query grow for the life of the repo. Floored to the hour because
    the fact table is hourly and a finer floor would exclude a run's own first rows.
    """
    horizon = (now_model - timedelta(days=RETENTION_DAYS)).replace(minute=0, second=0, microsecond=0)
    starts = []
    for rec in runs:
        stamp = (rec.get("run") or {}).get("started")
        if not stamp:
            continue
        try:
            t = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
        except ValueError:
            continue
        if t.tzinfo:
            t = t.astimezone(timezone.utc).replace(tzinfo=None)
        starts.append((t + MODEL_OFFSET).replace(minute=0, second=0, microsecond=0))
    if not starts:
        return horizon
    return max(min(starts), horizon)


def main():
    if requests is None:
        die("`requests` is not installed, and reading the metrics model needs it. "
            "(Only the reader does — cu/dashboard.py renders on the standard library alone.)")
    if not TOKEN:
        die("PBI_TOKEN is empty — the workflow mints it from the OIDC login.")
    if not (WS and MODEL):
        die("CU_METRICS_WORKSPACE_ID and CU_METRICS_MODEL_ID must both be set.")

    now_model = datetime.now(timezone.utc).replace(tzinfo=None) + MODEL_OFFSET
    ledger = load_ledger()
    runs = load_runs()
    floor = floor_for(runs, now_model)

    override = os.environ.get("CU_SINCE", "").strip()
    if override:
        try:
            floor = datetime.fromisoformat(override.replace("Z", ""))
        except ValueError:
            die(f"CU_SINCE={override!r} is not ISO-8601. It is in the MODEL's clock, not UTC.")
        log(f"  CU_SINCE overrides the computed floor: {floor}")

    log(f"  {len(runs)} run record(s); reading from {floor} "
        f"(model clock, UTC+{MODEL_OFFSET.total_seconds() / 3600:g})")
    cols = discover_columns()
    read, timed, seen, rows_seen = {}, {}, [], 0
    for cap in capacities():
        rows = read_cu(cap, floor, cols)
        if rows is None:
            log(f"  capacity {cap}: refused the query — skipping it")
            continue
        rows_seen += len(rows)
        per_item, per_secs, stamps = fold(rows, cols)
        log(f"  capacity {cap}: {len(rows)} row(s), {len(per_item)} in this workspace")
        for src, dest in ((per_item, read), (per_secs, timed)):
            for guid, ops in src.items():
                per = dest.setdefault(guid, {})
                for op, value in ops.items():
                    per[op] = round(per.get(op, 0.0) + value, 3)
        seen += stamps

    # Verify the floor actually BOUND — see read_cu.
    if seen:
        lo = min(seen)
        log(f"  earliest hour returned: {lo}")
        if lo < floor.strftime("%Y-%m-%dT%H:%M:%S"):
            die(f"the `since` filter did NOT bind: asked for >= {floor}, got rows from {lo}. "
                f"Refusing to write a ledger that silently includes excluded time.")

    changed = apply(ledger, read)
    changed_s = apply(ledger, timed, "seconds")

    # Did the read find what the runs say exists? See coverage() — this is the standing check on the
    # one assumption the no-refresh design rests on.
    cover = coverage(runs, read)
    unfound = sum(len(m) for _f, _n, m in cover)
    for name, found, missing in cover:
        log(f"  {name}: {found}/{found + len(missing)} recorded item(s) found"
            + (f" — MISSING {', '.join(missing)}" if missing else ""))
    if unfound:
        log(f"  {unfound} recorded item(s) returned no rows. Expected for a run that finished in "
            f"the last ~10 minutes (ingestion lag) — dispatch Dashboard again. If they are still "
            f"missing hours later, the metrics model needs a refresh to surface a new item GUID "
            f"and this file's central assumption is wrong.")

    ledger["schema"] = SCHEMA
    ledger["updated"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    # `timed` is in the read entry so a ledger can say for itself whether the duration column was
    # there — otherwise a page with no time section looks like a render bug rather than a model that
    # does not expose the column.
    ledger["reads"].append({"at": ledger["updated"], "since": floor.isoformat(),
                            "items": len(read), "changed": changed, "unfound": unfound,
                            "timed": len(timed)})
    path = save_ledger(ledger)

    for guid, ops in sorted(read.items(), key=lambda kv: -sum(kv[1].values()))[:12]:
        log(f"    {guid}  {sum(ops.values()):>12,.1f} CU  "
            f"{sum((timed.get(guid) or {}).values()):>10,.1f} s  ({len(ops)} operation(s))")
    log(f"  {rows_seen} row(s) read, {len(read)} item(s) in this workspace, {changed} updated"
        + (f", {changed_s} duration(s) updated over {len(timed)} item(s)" if timed
           else "; no duration column, so no seconds were written"))
    print(f"{path}: {len(ledger['items'])} item GUID(s), "
          f"{sum(total(ledger, g) for g in ledger['items']):,.1f} CU total"
          + (f", {sum(total(ledger, g, 'seconds') for g in ledger['seconds']):,.0f} s" if
             ledger["seconds"] else "")
          + (f", {unfound} recorded item(s) not yet visible" if unfound else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
