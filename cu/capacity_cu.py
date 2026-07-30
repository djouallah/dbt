"""CU per semantic model, read from the Fabric Capacity Metrics app's semantic model.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's model is the only
authoritative source, so this queries it by DAX over the Power BI `executeQueries` endpoint and
prints one two-column table: semantic model name, CU consumed.

Standalone on purpose. It shares NOTHING with benchmark/ — no imports, no run_report.json, no
artifact, no ADOMD, no .NET. `requests` is the only dependency. Delete cu/ and .github/workflows/
cu.yml and the rest of the repo does not notice.

It also correlates nothing: this is CU per model over a wall-clock window. It cannot say which
query, run or engine produced it. That is the entire scope.

Stdout is the markdown table and nothing else (the workflow redirects it into
$GITHUB_STEP_SUMMARY); diagnostics go to stderr.
"""
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests

# Own the encoding rather than inherit it: this prints markdown that GitHub renders, and a Windows
# console is cp1252, which mangles an em dash and would hard-fail on a semantic model whose name
# carries anything non-ASCII.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8")
    except Exception:
        pass

PBI = "https://api.powerbi.com/v1.0/myorg"

TOKEN = os.environ.get("PBI_TOKEN", "").strip()
WS = os.environ.get("CU_METRICS_WORKSPACE_ID", "").strip()
MODEL = os.environ.get("CU_METRICS_MODEL_ID", "").strip()
CAPACITY = os.environ.get("CU_CAPACITY_ID", "").strip()

# The detail table is gated by a single 30-second TimePoint, so a window costs window/30s requests.
# Power BI allows 120 executeQueries per minute per user; 30 minutes = 60 calls is comfortable,
# 60 minutes = 120 sits exactly on the limit. Raise it knowingly.
WINDOW_MIN = int(os.environ.get("CU_WINDOW_MINUTES", "30") or 30)

# The model stores timepoints against ITS OWN UTC offset, not yours. A default app install is 0.
# If the app was configured with an offset, the window silently lands in the wrong place — which
# looks like "no activity" rather than an error.
OFFSET_H = float(os.environ.get("CU_UTC_OFFSET_HOURS", "0") or 0)

# Optional explicit end of the window, ISO-8601 UTC. Default: now. Handy for going back to a
# benchmark that finished a while ago (subject to the app's 14-day retention).
END_UTC = os.environ.get("CU_END_UTC", "").strip()

DEBUG = os.environ.get("CU_DEBUG", "").strip().lower() in ("1", "true", "yes")

TABLE = "Timepoint Interactive Detail"

# Column names move between Capacity Metrics app versions — Microsoft's own fabric-toolbox
# accelerator carries four DAX variants (v53/v47/v40/v37) for exactly this reason. So nothing here
# hardcodes a name: the schema is discovered first and each role resolved from candidates, in
# order of preference.
WANTED = {
    "operation_id": ["Operation Id", "OperationId", "Operation ID"],
    "item_name":    ["Item Name", "ItemName", "Item"],
    "item_kind":    ["Item Kind", "ItemKind", "Item Type", "ItemType"],
    "operation":    ["Operation", "Operation Name", "OperationName"],
    "total_cu":     ["Total CU", "Total CU (s)", "TotalCU", "CU (s)", "Total CUs"],
}

# What counts as a semantic model in the Items/detail schema. Compared case-insensitively.
MODEL_KINDS = {"semanticmodel", "dataset", "semantic model"}


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def die(msg, code=1):
    log(f"ERROR: {msg}")
    sys.exit(code)


def execute_dax(dax, tries=4, fatal=True):
    """POST one DAX query, return its rows as a list of dicts.

    Retries 429 (the per-user request cap) honouring Retry-After, and 5xx. Anything else is
    surfaced with the API's own message — an empty table must never be mistaken for zero CU.

    fatal=False returns None on a rejected query instead of exiting, for callers that are probing
    candidate spellings and expect misses. Auth failures stay fatal either way: retrying a 403
    against a different column name only buries the real reason.
    """
    url = f"{PBI}/groups/{WS}/datasets/{MODEL}/executeQueries"
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    for i in range(1, tries + 1):
        r = requests.post(url, json=body, headers=headers, timeout=180)
        if r.status_code == 200:
            return r.json()["results"][0]["tables"][0].get("rows", [])
        if r.status_code in (429, 502, 503, 504) and i < tries:
            wait = int(r.headers.get("Retry-After") or min(60, 5 * 2 ** i))
            log(f"  {r.status_code} from executeQueries; retrying in {wait}s "
                f"(attempt {i}/{tries})")
            time.sleep(wait)
            continue
        detail = r.text[:600].replace("\n", " ")
        if r.status_code in (401, 403):
            die(f"{r.status_code} from executeQueries. The Capacity Metrics semantic model is "
                f"widely reported NOT to accept service principals — if this ran on the OIDC SP, "
                f"supply a user token as the PBI_TOKEN secret instead. API said: {detail}")
        if not fatal:
            return None
        die(f"executeQueries returned {r.status_code}: {detail}")
    if not fatal:
        return None
    die("executeQueries exhausted its retries")


def strip_prefix(rows):
    """executeQueries returns keys as `Table[Column]` or `[Alias]`. Reduce to the bare name."""
    out = []
    for row in rows:
        out.append({re.sub(r"^.*\[|\]$", "", k): v for k, v in row.items()})
    return out


def discover_columns():
    """Read the detail table's real column names, so a version bump fails loudly and specifically.

    INFO.VIEW.COLUMNS() is a DAX INFO function, so it goes down the same executeQueries path as
    everything else — no DMV, no XMLA endpoint, no extra permission.
    """
    rows = strip_prefix(execute_dax(
        f'EVALUATE FILTER(INFO.VIEW.COLUMNS(), [Table] = "{TABLE}")'))
    cols = {r.get("Name") for r in rows if r.get("Name")}
    if not cols:
        die(f"the semantic model at {WS}/{MODEL} has no table named '{TABLE}'. Either that is not "
            f"the Fabric Capacity Metrics model, or this app version renamed it.")
    if DEBUG:
        log(f"  '{TABLE}' columns: {sorted(cols)}")

    resolved, missing = {}, []
    for role, candidates in WANTED.items():
        hit = next((c for c in candidates if c in cols), None)
        if hit:
            resolved[role] = hit
        else:
            missing.append(f"{role} (tried {candidates})")
    if missing:
        die(f"'{TABLE}' exists but these columns were not found: {'; '.join(missing)}. "
            f"Present: {sorted(cols)}. Add the actual name to WANTED in this file.")
    return resolved


def discover_capacities():
    """Every capacity the model can see. Used when CU_CAPACITY_ID is unset.

    Passing them all is correct for the single-capacity case and harmless otherwise — the query
    filters to semantic models by name, and a capacity with no activity contributes no rows.
    """
    for col in ("capacity Id", "Capacity Id", "CapacityId"):
        rows = execute_dax(f"EVALUATE VALUES('Capacities'[{col}])", fatal=False)
        if rows is None:
            continue
        ids = [str(v) for r in strip_prefix(rows) for v in r.values() if v]
        if ids:
            return ids
    die("could not read any capacity id from the model's 'Capacities' table; "
        "set CU_CAPACITY_ID explicitly")


def timepoints(end, minutes):
    """The 30-second boundaries covering [end - minutes, end], oldest first.

    Timepoints are stamped in the model's own UTC offset, so the window is shifted by OFFSET_H
    before being turned into DAX literals.
    """
    end = end + timedelta(hours=OFFSET_H)
    end = end.replace(second=(end.second // 30) * 30, microsecond=0)
    n = int(minutes * 2)
    return [end - timedelta(seconds=30 * i) for i in range(n, -1, -1)]


def detail_dax(tp, cap_ids, c):
    caps = ", ".join(f'"{x}"' for x in cap_ids)
    lit = (f"DATE({tp.year}, {tp.month}, {tp.day}) + "
           f"TIME({tp.hour}, {tp.minute}, {tp.second})")
    return f"""
DEFINE
    MPARAMETER 'CapacitiesList' = {{ {caps} }}
    MPARAMETER 'TimePoint' = {lit}
EVALUATE
    SELECTCOLUMNS (
        '{TABLE}',
        "OperationId", '{TABLE}'[{c['operation_id']}],
        "ItemName",    '{TABLE}'[{c['item_name']}],
        "ItemKind",    '{TABLE}'[{c['item_kind']}],
        "Operation",   '{TABLE}'[{c['operation']}],
        "TotalCU",     '{TABLE}'[{c['total_cu']}]
    )
""".strip()


def collect(tps, cap_ids, cols):
    """Query every timepoint and return {operation_id: (item_name, operation, total_cu)}.

    Keyed by operation id, NOT summed across timepoints. An interactive operation is smoothed over
    10 to 128 timepoints and reappears in each one carrying its FULL Total CU — so adding the rows
    up would multiply every operation by the number of buckets it happens to span. Deduplicating
    on operation id is the difference between a number and a fiction.
    """
    seen = {}

    def one(tp):
        return tp, strip_prefix(execute_dax(detail_dax(tp, cap_ids, cols)))

    # Four at a time: enough to make a 60-call window quick, far enough under the 120/min cap that
    # the 429 path stays exceptional rather than routine.
    with ThreadPoolExecutor(max_workers=4) as pool:
        for i, (tp, rows) in enumerate(pool.map(one, tps), 1):
            for r in rows:
                if str(r.get("ItemKind", "")).strip().lower() not in MODEL_KINDS:
                    continue
                oid = r.get("OperationId")
                cu = r.get("TotalCU")
                if oid is None or cu is None:
                    continue
                prev = seen.get(oid)
                cu = float(cu)
                # Max, not first: a partially-smoothed slice can appear before the final total.
                if prev is None or cu > prev[2]:
                    seen[oid] = (r.get("ItemName") or "(unnamed)", r.get("Operation") or "", cu)
            if DEBUG and i % 10 == 0:
                log(f"  {i}/{len(tps)} timepoints, {len(seen)} operations so far")
    return seen


def render(seen, start, end):
    per_model = {}
    for name, _op, cu in seen.values():
        per_model[name] = per_model.get(name, 0.0) + cu
    # Round before totalling, not after: a cost table whose rows visibly don't add up to its own
    # total is the fastest way to make someone stop believing the whole report.
    per_model = {k: round(v, 1) for k, v in per_model.items()}

    # ASCII arrow on purpose: this also runs from a laptop, and a Windows console is cp1252.
    span = f"{start:%Y-%m-%d %H:%MZ} -> {end:%H:%MZ}"
    print(f"## Semantic model CU — {span}\n")
    if not per_model:
        print(f"No semantic model activity in this window. Note the app holds **14 days**, "
              f"operations land **~6 minutes** after they run, and a non-zero "
              f"`CU_UTC_OFFSET_HOURS` shifts where the window lands.")
        return
    print("| semantic model | CU |")
    print("|---|---:|")
    for name, cu in sorted(per_model.items(), key=lambda kv: -kv[1]):
        print(f"| {name} | {cu:,.1f} |")
    print(f"| **total** | **{sum(per_model.values()):,.1f}** |")


def main():
    if not TOKEN:
        die("PBI_TOKEN is empty — the workflow mints it from the OIDC login, or you supply a "
            "user token as a secret. See cu/README.md.")
    if not (WS and MODEL):
        die("CU_METRICS_WORKSPACE_ID and CU_METRICS_MODEL_ID must both be set.")

    end = (datetime.fromisoformat(END_UTC.replace("Z", "+00:00")).astimezone(timezone.utc)
           if END_UTC else datetime.now(timezone.utc))
    if (datetime.now(timezone.utc) - end).days > 14:
        die("the requested window is older than the Capacity Metrics app's 14-day retention; "
            "it would return nothing, which reads as zero CU.")

    cols = discover_columns()
    cap_ids = [CAPACITY] if CAPACITY else discover_capacities()
    tps = timepoints(end, WINDOW_MIN)
    log(f"capacities={cap_ids} window={WINDOW_MIN}min timepoints={len(tps)} "
        f"offset={OFFSET_H}h")

    seen = collect(tps, cap_ids, cols)
    log(f"{len(seen)} distinct operations across {len(tps)} timepoints")
    render(seen, end - timedelta(minutes=WINDOW_MIN), end)


if __name__ == "__main__":
    main()
