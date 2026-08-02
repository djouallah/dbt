"""Read capacity units from the Fabric Capacity Metrics model BY ITEM GUID, into a cumulative ledger.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's own semantic model is the
only authoritative source, and it is read here with DAX over the Power BI `executeQueries` endpoint.

**Everything is keyed on the item GUID, and that is the whole design.** The previous reader matched
item DISPLAY NAMES — `engine_of()` substring matching, a `shared` column for anything ambiguous, a
join to the app's lagging `'Items'` snapshot for names and kinds, and a pre-read REFRESH of the
metrics model so a minutes-old item would be catalogued at all. That refresh is what Power BI
throttled per-identity: on runs 30685959678 and 30691130030 every attempt drew 429, and 41,887 CU of
DuckDB-leg compute printed under `shared` because two throwaway notebooks resolved to no name.

None of it is needed. The fact table carries `Item` (a GUID) and `Workspace Id` as columns of its
own, so the workspace filter binds without a name and the GUID needs no resolving — and the
`Benchmark` workflow now writes down every GUID it created in `history/runs/<ts>-<run id>.json`.
Attribution is a dictionary lookup, done by the DASHBOARD, not here. **No refresh, no `'Items'` join,
no name matching, no classification.** This file measures; it does not interpret.

## The ledger — `history/cu.json`

    {"schema": 1, "updated": "...",
     "reads": [{"at": ..., "since": ..., "rows": N, "changed": M, "settled": K}],

     # THE PERMANENT RECORD: cumulative CU per item, per operation.
     "items": {"<ITEM GUID>": {"cu": {"<operation>": total},
                               "settled": "<why and when>" | null,
                               "first_hour": ..., "last_hour": ...}},

     # Per-run rollup of the ONE item that outlives a run and so has no total of its own.
     "runs": {"<run id>": {"landing": {"<operation>": total}, "settled": bool}},

     # WORKING SET, not history: hour detail for items that can still move. Pruned on settle.
     "hours": {"<ITEM GUID>": {"<operation>": {"<hour>": CU}}}}

It exists because the app retains about **14 days**. Everything measured is gone within a fortnight
unless it is written down, and a run record's GUIDs stay meaningful for years.

**`items` is the shape that lasts, and `hours` is scaffolding.** The hour grain exists for exactly
one reason — to make a re-read idempotent while an item's numbers are still moving — and that reason
expires the moment the item settles. So a settled item collapses to `{operation: total}` and its
hours are dropped: the permanent record is cumulative CU per item, which is the only form still
meaningful once the app has forgotten the window it came from. A ledger that kept every hour forever
would grow without bound to preserve a resolution nothing can check any more.

Three rules make re-reading safe, and all three matter:

1. **Upsert only, never remove.** A key that stops being returned — retention rolling past it — keeps
   its last value. Deleting on absence would quietly erase the history this file exists to keep.
   (Collapsing an hour into a settled total is not removal: the CU stays, at a coarser grain.)
2. **Latest read wins per `(guid, operation, hour)`.** An hour's CU keeps growing for up to ~70
   minutes after the fact (~6 min ingestion lag, 5-64 min smoothing), so overwriting is correct.
   SUMMING repeated reads would multiply every hour by the number of times it was read — which is
   also why the collapse only happens once, at settle, and never to an open item.
3. **Settle, then freeze.** An item is settled when a read changed nothing about it AND its newest
   hour is older than `CU_SETTLE_HOURS`. A settled item is never rewritten and its time is never
   re-read. That is "an item's CU is done when no more CU is being attributed to it", and it is also
   what makes the missing refresh safe: an item the first read could not see is picked up by the
   next one, because nothing is final until two reads agree.

**`dbt_landing` is the exception and needs the `runs` rollup.** It is the one item that is never
deleted, so it never settles and its hours would accumulate forever. Instead its CU is rolled up per
RUN — summed over the hours inside that run's own window — and frozen when the rest of that run's
items settle, at which point the hours it covered are dropped.

Env in: `PBI_TOKEN` (minted from the OIDC login), `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`,
`CU_CAPACITY_ID`, `CU_WORKSPACE_FILTER`. Optional: `CU_SINCE` (override the computed floor),
`CU_SETTLE_HOURS`, `CU_MODEL_OFFSET_HOURS`, `CU_RETENTION_DAYS`, `CU_RUNS_DIR`, `CU_LEDGER`.

stdout is a short summary; diagnostics go to stderr.
"""
import json
import os
import re
import sys
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
# The only filter left. It is a column of the fact table itself, so it binds with no name resolution
# and no join — which is what makes a GUID-only read possible at all.
WS_FILTER = os.environ.get("CU_WORKSPACE_FILTER", "").strip().upper()

RUNS_DIR = os.environ.get("CU_RUNS_DIR", "history/runs").strip()
LEDGER = os.environ.get("CU_LEDGER", "history/cu.json").strip()

# The metrics model stamps its timestamps in the offset configured IN THE APP, not in UTC. A wrong
# value here reads as "no activity" rather than as an error, which is why it is named and not
# inlined. +10 for this tenant.
MODEL_OFFSET = timedelta(hours=float(os.environ.get("CU_MODEL_OFFSET_HOURS", "10")))
# How quiet an item has to be before its numbers are frozen. Must comfortably exceed the ingestion
# lag plus the smoothing window (~6 min + up to 64 min); 3 hours is that with room to spare.
SETTLE_HOURS = float(os.environ.get("CU_SETTLE_HOURS", "3"))
# After this, the app has forgotten the window and no further read can improve it. An item still
# unsettled then is frozen anyway, with the reason recorded — leaving it open forever would make
# every future read re-query time that can never change.
RETENTION_DAYS = float(os.environ.get("CU_RETENTION_DAYS", "14"))
SCHEMA = 1

# Roles whose items never accrue a capacity unit and so can never settle. `folder` is a workspace
# folder; `landing` is the archive lakehouse, which is never deleted and is rolled up per run
# instead. Waiting on either would hold every run open forever.
NEVER_SETTLES = {"folder", "landing"}

TABLE = "Metrics By Item Operation And Hour"
# Column names move between versions of the app — Microsoft's own accelerator ships four DAX variants
# for exactly this reason — so every role is resolved against the real schema and a miss fails
# specifically, naming what was actually there.
REQUIRED = {
    "item_id": ["Item", "Item Id", "ItemId"],
    "workspace_id": ["Workspace Id", "WorkspaceId", "Workspace"],
    "operation": ["Operation", "Operation Name", "OperationName"],
    "cu": ["CU (s)", "CU(s)", "Total CU (s)", "CU"],
    "when": ["Date Hour", "DateHour", "Datetime", "Date/Time", "Hour"],
}


def log(msg):
    sys.stderr.write(msg + "\n")


def die(msg):
    log("ERROR: " + msg)
    raise SystemExit(1)


# --------------------------------------------------------------------------------- the metrics model

def execute_dax(dax, tries=4, fatal=True):
    """POST one DAX query. Retries the rate limits, honouring `Retry-After` when it is given."""
    import time
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
    """Resolve every role against the model's real schema, so a version bump fails specifically."""
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
    """CU per (item, operation, hour) for one capacity, from `since` onward, summed server-side.

    Summing here is correct: these rows are already an aggregate per (item, operation, hour), so
    there is no smoothing duplication to deduplicate away — unlike `'Timepoint Interactive Detail'`,
    where one operation reappears in every bucket it spans carrying its full CU.

    ONE CAPACITY PER QUERY, deliberately. These tables are DirectQuery and resolve one data location
    per query; passing several fails with an opaque `Internal Error: Error obtaining data location`
    naming neither the cause nor the capacity.
    """
    inner = f"""SUMMARIZECOLUMNS (
        '{TABLE}'[{c['item_id']}],
        '{TABLE}'[{c['workspace_id']}],
        '{TABLE}'[{c['operation']}],
        '{TABLE}'[{c['when']}],
        "CU", SUM ( '{TABLE}'[{c['cu']}] )
    )"""
    # CALCULATETABLE with a plain boolean predicate, NOT a FILTER(VALUES(...)) inside
    # SUMMARIZECOLUMNS. The latter is ACCEPTED and silently changes nothing — three different windows
    # once returned byte-identical totals before anyone noticed — which is why the hour is also
    # projected and the caller verifies the range it actually got back.
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
    return {"schema": SCHEMA, "updated": None, "reads": [], "items": {}, "runs": {}, "hours": {}}


def load_ledger(path=None):
    path = path or LEDGER
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return blank()
    for key, default in (("items", {}), ("runs", {}), ("hours", {}), ("reads", [])):
        doc.setdefault(key, default)
    return doc


def is_settled(ledger, guid):
    return bool((ledger["items"].get(guid) or {}).get("settled"))


def roll_up(ledger, guid):
    """Recompute an OPEN item's cumulative totals from its hours, and its first/last hour.

    Called after every merge so `items` is the single place anything downstream reads, whether the
    item is still moving or frozen. A settled item is never passed here — its totals are the ones
    that were true when it stopped accruing, and recomputing from hours it no longer has would zero
    them.
    """
    hours = ledger["hours"].get(guid) or {}
    entry = ledger["items"].setdefault(guid, {"cu": {}, "settled": None})
    entry["cu"] = {op: round(sum(per.values()), 3) for op, per in sorted(hours.items())}
    stamps = [h for per in hours.values() for h in per]
    entry["first_hour"] = min(stamps) if stamps else None
    entry["last_hour"] = max(stamps) if stamps else None
    return entry


def save_ledger(doc, path=None):
    path = path or LEDGER
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        # sort_keys so a re-read that changes one hour produces a one-line diff in the commit rather
        # than a reshuffled file; indent=1 so the diff is readable at all.
        json.dump(doc, f, indent=1, sort_keys=True)
    return path


def load_runs(directory=None):
    """Every run record, oldest first. These name the GUIDs and the windows worth measuring."""
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


def run_floor(rec):
    """When this run's CU can first appear, in the METRICS MODEL's clock, floored to the hour.

    The record stamps UTC; the model stamps its own offset. Floored because the fact table is hourly
    and a finer floor would exclude the run's own first rows.
    """
    started = (rec.get("run") or {}).get("started")
    if not started:
        return None
    try:
        t = datetime.fromisoformat(started.replace("Z", "+00:00"))
    except ValueError:
        return None
    if t.tzinfo:
        t = t.astimezone(timezone.utc).replace(tzinfo=None)
    return (t + MODEL_OFFSET).replace(minute=0, second=0, microsecond=0)


def pending(runs, ledger, now_model):
    """`(floor, guids, reasons)` — the earliest hour still worth reading, and whose GUIDs are open.

    A run is DONE when every GUID it names is settled. A run older than the app's retention is done
    whatever its state, because no further read can improve it — and saying so is better than
    re-querying a window the model has forgotten.
    """
    horizon = now_model - timedelta(days=RETENTION_DAYS)
    floors, guids, reasons = [], set(), {}
    for rec in runs:
        floor = run_floor(rec)
        items = rec.get("items") or {}
        if not items:
            continue
        open_guids = [g for g in items if not is_settled(ledger, g)]
        if not open_guids:
            continue
        if floor is not None and floor < horizon:
            for g in open_guids:
                freeze(ledger, g, f"retention ({RETENTION_DAYS:g}d) passed with no further rows")
            reasons[rec["_file"]] = f"{len(open_guids)} item(s) aged out"
            continue
        guids.update(open_guids)
        if floor is not None:
            floors.append(floor)
        reasons[rec["_file"]] = f"{len(open_guids)}/{len(items)} item(s) open"
    return (min(floors) if floors else None), guids, reasons


def merge_rows(ledger, rows, cols, now_model):
    """Upsert this read into the working set. Returns `(kept, changed, skipped_settled, hours)`.

    Upsert-only and last-write-wins per `(guid, operation, hour)` — see the module docstring. A
    settled item is skipped entirely: its total is frozen, it has no hours left to write into, and
    re-reading a window the app has started to age out could only take value away.
    """
    kept = changed = skipped = 0
    stamps = []
    for r in rows or []:
        guid = str(r.get(cols["item_id"]) or "").upper()
        wsid = str(r.get(cols["workspace_id"]) or "").upper()
        value = r.get("CU")
        if not guid or value is None:
            continue
        if WS_FILTER and wsid != WS_FILTER:
            continue
        if is_settled(ledger, guid):
            skipped += 1
            continue
        stamp = str(r.get(cols["when"]) or "")[:19]
        if not stamp:
            continue
        stamps.append(stamp)
        op = str(r.get(cols["operation"]) or "(unnamed)").strip()
        slot = ledger["hours"].setdefault(guid, {}).setdefault(op, {})
        new = round(float(value), 3)
        if slot.get(stamp) != new:
            slot[stamp] = new
            changed += 1
        kept += 1
    for guid in ledger["hours"]:
        roll_up(ledger, guid)
    return kept, changed, skipped, stamps


def freeze(ledger, guid, why):
    """Collapse an item to its cumulative total and drop the hour detail.

    This is the point of the ledger. The hours were only ever there to make a re-read idempotent
    while the numbers moved; once the item is final they preserve a resolution nothing can check
    against any more — the app has a 14-day memory — while growing the file without bound.
    """
    entry = roll_up(ledger, guid) if guid in ledger["hours"] else \
        ledger["items"].setdefault(guid, {"cu": {}, "first_hour": None, "last_hour": None})
    entry["settled"] = why
    ledger["hours"].pop(guid, None)
    return entry


def settle(ledger, changed_guids, now_model, keep=()):
    """Freeze every open item this read left untouched that has been quiet long enough.

    Two conditions, both needed. UNCHANGED, because an item still accruing must not be frozen at a
    partial number — and requiring two agreeing reads is also what covers the missing refresh: an
    item the first read could not see yet is picked up by the second. QUIET, because "the read
    changed nothing" is trivially true of an hour that has not finished smoothing and has not been
    written to yet either.

    `keep` names items that must never freeze — `dbt_landing`, which is never deleted and so is never
    done. It is rolled up per RUN instead (see `roll_up_landing`).
    """
    frozen = []
    quiet_before = now_model - timedelta(hours=SETTLE_HOURS)
    for guid in sorted(ledger["hours"]):
        if guid in changed_guids or guid in keep:
            continue
        last = (ledger["items"].get(guid) or {}).get("last_hour")
        if not last:
            continue
        try:
            if datetime.fromisoformat(last) > quiet_before:
                continue
        except ValueError:
            continue
        freeze(ledger, guid, f"unchanged and quiet since {last}")
        frozen.append(guid)
    return frozen


def roll_up_landing(ledger, runs):
    """Attribute the landing lakehouse's CU to the run that was active, and prune what is finished.

    `dbt_landing` is the one item a run does not delete, so it never settles and its hours would
    accumulate for the life of the repo. Its CU is instead summed per RUN over the hours inside that
    run's own `started`/`finished` window, and frozen when the rest of that run's items settle — at
    which point those hours are dropped.

    An hour that falls in no run's window is left alone: it is real CU that nothing can attribute
    yet, and dropping it to keep the file small would be the one thing this file must never do.
    """
    landing = {g for rec in runs for g, it in (rec.get("items") or {}).items()
               if (it.get("role") or "") == "landing"}
    if not landing:
        return landing
    covered = set()
    for rec in runs:
        rid = str((rec.get("run") or {}).get("id") or rec.get("_file"))
        items = rec.get("items") or {}
        window = _window(rec)
        if not window[0]:
            continue
        per_op, hours_used = {}, []
        for guid in landing & set(items):
            for op, per in (ledger["hours"].get(guid) or {}).items():
                for hour, value in per.items():
                    if window[0] <= hour <= window[1]:
                        per_op[op] = round(per_op.get(op, 0.0) + value, 3)
                        hours_used.append((guid, op, hour))
        # Only the items that CAN settle count towards "this run is done". The `dbt` folder never
        # accrues a capacity unit, so it never settles — waiting on it would hold every run open
        # forever, and the landing rollup would never freeze. Same exclusion the page applies.
        watch = [g for g, it in items.items() if (it.get("role") or "") not in NEVER_SETTLES]
        done = bool(watch) and all(is_settled(ledger, g) for g in watch)
        if (ledger["runs"].get(rid) or {}).get("settled"):
            continue                                  # already frozen; never recomputed
        ledger["runs"][rid] = {"landing": per_op, "settled": done}
        if done:
            covered.update(hours_used)
    for guid, op, hour in covered:
        per = (ledger["hours"].get(guid) or {}).get(op) or {}
        per.pop(hour, None)
        if not per:
            (ledger["hours"].get(guid) or {}).pop(op, None)
        if not ledger["hours"].get(guid):
            ledger["hours"].pop(guid, None)
    # Landing gets NO entry in `items`, and that is deliberate rather than an omission. `items` means
    # "this is what the item finally cost"; landing is never deleted, so it has no final cost, and an
    # entry there would show only the part not yet attributed to a run — a half-total that reads like
    # a whole one. Its CU lives in exactly two places: `runs[*].landing` once attributed, and `hours`
    # while it is not.
    for guid in landing:
        ledger["items"].pop(guid, None)
    return landing


def _window(rec):
    """This run's `(started, finished)` as ledger hour strings, or `(None, None)`."""
    run = rec.get("run") or {}
    out = []
    for key in ("started", "finished"):
        raw = run.get(key)
        if not raw:
            return None, None
        try:
            t = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None, None
        if t.tzinfo:
            t = t.astimezone(timezone.utc).replace(tzinfo=None)
        out.append((t + MODEL_OFFSET).replace(minute=0, second=0, microsecond=0).isoformat())
    return out[0], out[1]


def total(ledger, guid):
    return sum((ledger["items"].get(guid) or {}).get("cu", {}).values())


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
    floor, guids, reasons = pending(runs, ledger, now_model)
    for name, why in sorted(reasons.items()):
        log(f"  {name}: {why}")

    override = os.environ.get("CU_SINCE", "").strip()
    if override:
        try:
            floor = datetime.fromisoformat(override.replace("Z", ""))
        except ValueError:
            die(f"CU_SINCE={override!r} is not ISO-8601. It is in the MODEL's clock, not UTC.")
        log(f"  CU_SINCE overrides the computed floor: {floor}")

    if not guids and not override:
        log("  every recorded item is settled — nothing left that a read could change")
        print(f"CU ledger unchanged: {len(ledger['items'])} item(s), all settled.")
        save_ledger(ledger)
        return 0
    if floor is None:
        die("no run record carries a start time, and CU_SINCE is unset — refusing to read the whole "
            f"retained window by accident. Set CU_SINCE, or check {RUNS_DIR}/.")

    log(f"  reading from {floor} (model clock, UTC+{MODEL_OFFSET.total_seconds() / 3600:g}) "
        f"for {len(guids)} open item GUID(s)")
    cols = discover_columns()
    kept = changed = skipped = 0
    seen_hours = []
    before = {g: json.dumps(ops, sort_keys=True) for g, ops in ledger["hours"].items()}
    for cap in capacities():
        rows = read_cu(cap, floor, cols)
        if rows is None:
            log(f"  capacity {cap}: refused the query — skipping it")
            continue
        log(f"  capacity {cap}: {len(rows)} row(s)")
        k, c, s, hrs = merge_rows(ledger, rows, cols, now_model)
        kept, changed, skipped = kept + k, changed + c, skipped + s
        seen_hours += hrs

    # Verify the floor actually BOUND. A DAX filter that is accepted and then ignored produces a
    # plausible wrong number, which is the worst failure this tool can have — it has happened once
    # already, with FILTER(VALUES(...)) inside SUMMARIZECOLUMNS.
    if seen_hours:
        lo, hi = min(seen_hours), max(seen_hours)
        log(f"  hours returned: {lo} .. {hi}")
        if lo < floor.strftime("%Y-%m-%dT%H:%M:%S"):
            die(f"the `since` filter did NOT bind: asked for >= {floor}, got rows from {lo}. "
                f"Refusing to write a ledger that silently includes excluded time.")

    after = {g: json.dumps(ops, sort_keys=True) for g, ops in ledger["hours"].items()}
    changed_guids = {g for g in after if before.get(g) != after[g]}
    # `dbt_landing` is never deleted and so is never done — it is rolled up per RUN instead, and
    # excluded from settling so it keeps a working set for the runs still open.
    landing = roll_up_landing(ledger, runs)
    frozen = settle(ledger, changed_guids, now_model, keep=landing)
    roll_up_landing(ledger, runs)          # again: this read's settles may have closed a run

    ledger["schema"] = SCHEMA
    ledger["updated"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    ledger["reads"].append({"at": ledger["updated"], "since": floor.isoformat() if floor else None,
                            "rows": kept, "changed": changed, "settled": len(frozen)})
    path = save_ledger(ledger)

    log(f"  {kept} row(s) kept, {changed} value(s) changed, {skipped} skipped as already settled, "
        f"{len(frozen)} item(s) frozen this read")
    for g in frozen:
        log(f"    settled {g}: {total(ledger, g):,.1f} CU")
    settled = sum(1 for g in ledger["items"] if is_settled(ledger, g))
    # Three places hold CU and the sum of them is what was measured: items (final, per item), runs
    # (landing, attributed to a run), and hours (read but not yet attributable to either).
    items_cu = sum(total(ledger, g) for g in ledger["items"])
    runs_cu = sum(v for r in ledger["runs"].values() for v in (r.get("landing") or {}).values())
    open_cu = sum(v for per in ledger["hours"].values() for op in per.values() for v in op.values())
    print(f"{path}: {len(ledger['items'])} item GUID(s), {settled} settled, "
          f"{len(ledger['hours'])} still carrying hour detail — "
          f"{items_cu:,.1f} CU on items + {runs_cu:,.1f} landing + {open_cu:,.1f} unattributed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
