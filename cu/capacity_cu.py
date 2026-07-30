"""CU per semantic model, read from the Fabric Capacity Metrics app's semantic model.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's model is the only
authoritative source, so this queries it by DAX over the Power BI `executeQueries` endpoint and
prints one table: the benchmark's semantic models against the operation types they spent CU on.

Time is a pinned FLOOR (`since`), not a rolling window. A window moves with every dispatch and can
slice one benchmark in half, making an engine look cheap for no reason but where the boundary fell;
a floor stays put and everything after it accumulates. Its purpose is to exclude the run in which
dwh was DirectQuery rather than Direct Lake — still inside the app's ~14-day retention, and not the
same experiment.

Deliberately scoped to the semantic models. Widening it to every item in the workspace was tried
and reverted: the lakehouses' OneLake read AND write operations bring a dozen operation types each,
plus a row per throwaway `duckrun-py-*` notebook, and the table stopped being readable. The
pipeline's own cost is a different question from what querying the four models costs, and mixing
them in one table served neither.

Standalone on purpose. It shares NOTHING with benchmark/ — no imports, no run_report.json, no
artifact, no ADOMD, no .NET. `requests` is the only dependency.

Reads 'Metrics By Item Operation And Hour', NOT 'Timepoint Interactive Detail'. The detail table was
tried first and is the wrong instrument: it is bucketed at 30 seconds and gated by a
single-timepoint MPARAMETER, so even a 3-hour window costs 360 requests per capacity, and because
an interactive operation is smoothed across 10-128 buckets it reappears in each one at full value
and has to be deduplicated by operation id. The aggregate answers the same question in one request
per capacity, already summed, with no double-counting to guard against. The detail table remains
the right tool for drilling into one timepoint's individual operations — not what this does.

Stdout is the markdown table and nothing else; diagnostics go to stderr.
"""
import os
import re
import sys
import time
from datetime import datetime, timezone

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

# The models to report, in this order. Defaults to the benchmark's four, because the capacity is
# shared and an unfiltered report is dominated by unrelated models nobody here cares about.
#
# Named rather than imported from benchmark/engines.py on purpose: this tool shares no code with
# benchmark/ so that deleting it stays free. If the benchmark's PREFIX or engine list changes, this
# list changes with it — a rename shows up as a row of 0.0, which is why every requested model is
# always printed even when it has no activity. Set empty for every semantic model on the capacity.
MODELS = [m.strip() for m in os.environ.get(
    "CU_MODELS", "aemo_duckrun,aemo_iceberg,aemo_spark,aemo_dwh").split(",") if m.strip()]

# The workspace the models live in — the same one ci.yml and benchmark.yml deploy to. Applied ON
# TOP of the name filter, not instead of it: display names are not unique across a tenant, so a
# stale `aemo_spark` in some other workspace would otherwise be silently added to this one's CU.
# Blank = every workspace on the capacity.
WS_FILTER = os.environ.get(
    "CU_WORKSPACE_FILTER", "ea575278-bd81-459c-9680-47829898c902").strip().upper()

DEBUG = os.environ.get("CU_DEBUG", "").strip().lower() in ("1", "true", "yes")

# A FLOOR, not a rolling window, and the difference matters. A window ("last 3h") moves with every
# dispatch and can slice one benchmark in half, making an engine look cheap for no reason but where
# the boundary fell. A pinned floor stays put: everything after it accumulates, and two dispatches a
# day apart are comparable.
#
# What it is for: the app retains ~14 days, which still contains the run where dwh was DirectQuery
# rather than Direct Lake. Those two are not the same experiment and their CU must not be summed —
# see benchmark/README.md on the DirectQuery leg. Set this past that run and the report starts
# clean. Bump it again whenever you want to start fresh; blank means everything retained.
SINCE = os.environ.get("CU_SINCE", "2026-07-30T12:00:00Z").strip()

# Item x operation x hour. The hour axis exists only to support SINCE; nothing here reports by hour.
# Mind the spelling: the model also has 'Metrics By Item And Operation' (no time axis) and 'Metrics
# By Item And Hour' (no operation split). This is the one with both.
TABLE = "Metrics By Item Operation And Hour"
ITEMS = "Items"

# Column names move between Capacity Metrics app versions — Microsoft's own fabric-toolbox
# accelerator carries four DAX variants (v53/v47/v40/v37) for exactly this reason. So nothing here
# hardcodes a name: the schema is discovered first and each role resolved from candidates.
#
# Verified against the installed app on 2026-07-30.
REQUIRED = {
    TABLE: {
        "item_id":      ["Item Id", "ItemId"],
        "workspace_id": ["Workspace Id", "WorkspaceId"],
        "operation":    ["Operation name", "Operation Name", "Operation"],
        "cu":           ["CU (s)", "CU(s)", "CU", "Total CU (s)"],
        "when":         ["Datetime", "DateTime", "Date"],
    },
    ITEMS: {
        "id":   ["Item Id", "ItemId"],
        "name": ["Item name", "Item Name", "ItemName"],
        "kind": ["Item kind", "Item Kind", "ItemKind"],
    },
}

# What counts as a semantic model in 'Items'[Item kind]. Compared case-insensitively.
MODEL_KINDS = {"semanticmodel", "dataset", "semantic model"}


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def die(msg, code=1):
    log(f"ERROR: {msg}")
    sys.exit(code)


def execute_dax(dax, tries=4, fatal=True):
    """POST one DAX query, return its rows as a list of dicts.

    Retries 429 (the per-user request cap) honouring Retry-After, and 5xx. fatal=False returns None
    on a rejected query instead of exiting, for callers probing candidate spellings. Auth failures
    stay fatal either way: retrying a 403 against a different name only buries the real reason.
    """
    url = f"{PBI}/groups/{WS}/datasets/{MODEL}/executeQueries"
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    for i in range(1, tries + 1):
        r = requests.post(url, json=body, headers=headers, timeout=300)
        if r.status_code == 200:
            return r.json()["results"][0]["tables"][0].get("rows", [])
        if r.status_code in (429, 502, 503, 504) and i < tries:
            wait = int(r.headers.get("Retry-After") or min(60, 5 * 2 ** i))
            log(f"  {r.status_code} from executeQueries; retrying in {wait}s ({i}/{tries})")
            time.sleep(wait)
            continue
        detail = r.text[:600].replace("\n", " ")
        if r.status_code in (401, 403):
            die(f"{r.status_code} from executeQueries. If this ran on the OIDC service principal "
                f"and the Capacity Metrics model refused it, supply a user token as the PBI_TOKEN "
                f"secret. API said: {detail}")
        if not fatal:
            return None
        die(f"executeQueries returned {r.status_code}: {detail}")
    return None if not fatal else die("executeQueries exhausted its retries")


def strip_prefix(rows):
    """executeQueries returns keys as `Table[Column]` or `[Alias]`. Reduce to the bare name."""
    return [{re.sub(r"^.*\[|\]$", "", k): v for k, v in row.items()} for row in (rows or [])]


def discover_columns():
    """Resolve every role against the model's real schema, so a version bump fails specifically."""
    rows = strip_prefix(execute_dax("EVALUATE INFO.VIEW.COLUMNS()"))
    by_table = {}
    for r in rows:
        by_table.setdefault(r.get("Table"), set()).add(r.get("Name"))
    if DEBUG:
        for t in sorted(x for x in by_table if x):
            log(f"  [{t}] {sorted(c for c in by_table[t] if c and not c.startswith('RowNumber-'))}")

    resolved = {}
    for table, roles in REQUIRED.items():
        cols = by_table.get(table, set())
        if not cols:
            die(f"the semantic model at {WS}/{MODEL} has no table named '{table}'. Either that is "
                f"not the Fabric Capacity Metrics model, or this app version renamed it. Tables "
                f"present: {sorted(t for t in by_table if t)}")
        got, missing = {}, []
        for role, candidates in roles.items():
            hit = next((c for c in candidates if c in cols), None)
            if hit:
                got[role] = hit
            else:
                missing.append(f"{role} (tried {candidates})")
        if missing:
            die(f"'{table}' exists but these columns were not found: {'; '.join(missing)}. "
                f"Present: {sorted(cols)}. Add the actual name to REQUIRED in this file.")
        resolved[table] = got
    return resolved


def discover_capacities():
    """Every capacity the model can see. Used when CU_CAPACITY_ID is unset."""
    for col in ("Capacity Id", "capacity Id", "CapacityId"):
        rows = execute_dax(f"EVALUATE VALUES('Capacities'[{col}])", fatal=False)
        if rows is None:
            continue
        ids = [str(v) for r in strip_prefix(rows) for v in r.values() if v]
        if ids:
            return ids
    die("could not read any capacity id from the model's 'Capacities' table; "
        "set CU_CAPACITY_ID explicitly")


def items_for(cap, c):
    """Map item GUID -> (name, kind).

    Necessary, not decorative: the metrics tables hold item **GUIDs**, so without this the report
    is a list of GUIDs against CU, which answers nothing. 'Items' is also where the item kind
    lives, so this is the only route to filtering down to semantic models.
    """
    rows = execute_dax(f"""
DEFINE
    MPARAMETER 'CapacitiesList' = {{ "{cap}" }}
EVALUATE
    SELECTCOLUMNS (
        '{ITEMS}',
        "Id",   '{ITEMS}'[{c['id']}],
        "Name", '{ITEMS}'[{c['name']}],
        "Kind", '{ITEMS}'[{c['kind']}]
    )
""".strip(), fatal=False)
    return {str(r["Id"]).upper(): (r.get("Name") or "", r.get("Kind") or "")
            for r in strip_prefix(rows) if r.get("Id")}


def cu_for(cap, since, c):
    """CU per (item, operation) for one capacity, summed server-side, from `since` onward.

    Summing IS correct here, unlike on the timepoint detail table: these rows are already an
    aggregate per (item, operation, hour), so there is no smoothing duplication to deduplicate away.

    One capacity per query, deliberately. These tables are DirectQuery and resolve one data
    location per query; passing several capacities fails with an opaque
    `Internal Error: Error obtaining data location` that names neither the cause nor the capacity.
    """
    when = c["when"]
    inner = f"""SUMMARIZECOLUMNS (
        '{TABLE}'[{c['item_id']}],
        '{TABLE}'[{c['workspace_id']}],
        '{TABLE}'[{c['operation']}],
        '{TABLE}'[{when}],
        "CU", SUM ( '{TABLE}'[{c['cu']}] )
    )"""
    # CALCULATETABLE with a plain boolean predicate, NOT a FILTER(VALUES(...)) argument inside
    # SUMMARIZECOLUMNS. The latter was accepted without error and silently changed nothing — every
    # window produced byte-identical totals, which is why the hour column is now projected and the
    # caller checks the range it actually got back. A filter that fails loudly is fine; one that
    # returns a plausible wrong number is not.
    if since:
        lit = (f"DATE({since.year}, {since.month}, {since.day}) + "
               f"TIME({since.hour}, {since.minute}, 0)")
        body = f"CALCULATETABLE (\n        {inner},\n        '{TABLE}'[{when}] >= {lit}\n    )"
    else:
        body = inner
    return strip_prefix(execute_dax(f"""
DEFINE
    MPARAMETER 'CapacitiesList' = {{ "{cap}" }}
EVALUATE
    {body}
""".strip(), fatal=False))


def render(cells, since, asof):
    """cells is {(model, operation): cu}. Rendered as models x operation types."""
    span = (f"since {since:%Y-%m-%d %H:%MZ}" if since else "everything retained")
    print(f"## Semantic model CU — {span}, as of {asof:%Y-%m-%d %H:%MZ}\n")
    if not cells:
        print(f"No semantic model activity {span}. Operations land **~6 minutes** after they run, "
              f"so a benchmark that just finished is not in here yet"
              + (" — and `since` may simply be in the future." if since and since > asof else "."))
        return

    # When specific models were asked for, print every one of them in the order given, including
    # the ones with no activity. A model that silently vanishes from the table is indistinguishable
    # from one that was never deployed; a 0.0 row says which.
    per_model = {}
    for (m, _op), cu in cells.items():
        per_model[m] = per_model.get(m, 0.0) + cu
    models = ([(m, per_model.get(m.lower(), 0.0)) for m in MODELS] if MODELS
              else sorted(per_model.items(), key=lambda kv: -kv[1]))

    # Operation columns ordered by total CU, so the expensive one is the first thing read.
    per_op = {}
    for (_m, op), cu in cells.items():
        per_op[op] = per_op.get(op, 0.0) + cu
    ops = [op for op, _ in sorted(per_op.items(), key=lambda kv: -kv[1])]

    print("| semantic model | " + " | ".join(ops) + " | total |")
    print("|---|" + "---:|" * (len(ops) + 1))
    for name, total in models:
        vals = [cells.get((name.lower(), op), 0.0) for op in ops]
        print(f"| {name} | " + " | ".join(f"{v:,.1f}" for v in vals) + f" | **{total:,.1f}** |")
    grand = sum(t for _, t in models)
    print("| **total** | "
          + " | ".join(f"**{per_op[op]:,.1f}**" for op in ops)
          + f" | **{grand:,.1f}** |")


def main():
    if not TOKEN:
        die("PBI_TOKEN is empty — the workflow mints it from the OIDC login, or you supply a "
            "user token as a secret. See cu/README.md.")
    if not (WS and MODEL):
        die("CU_METRICS_WORKSPACE_ID and CU_METRICS_MODEL_ID must both be set.")
    asof = datetime.now(timezone.utc)
    since = None
    if SINCE:
        try:
            since = datetime.fromisoformat(SINCE.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            die(f"CU_SINCE={SINCE!r} is not ISO-8601 (e.g. 2026-07-30T12:00:00Z)")
        if (asof - since).days > 14:
            log(f"note: since={SINCE} predates the app's ~14-day retention — the report starts "
                f"wherever the data actually does, not there")

    cols = discover_columns()
    caps = [CAPACITY] if CAPACITY else discover_capacities()
    log(f"capacities={caps} since={SINCE or '(everything retained)'} "
        f"workspace={WS_FILTER or '(all)'} models={MODELS or '(every semantic model)'}")

    wanted = {m.lower() for m in MODELS}
    cells, unknown, seen_hours = {}, 0, []
    for cap in caps:
        items = items_for(cap, cols[ITEMS])
        rows = cu_for(cap, since, cols[TABLE])
        if rows is None:
            log(f"  capacity {cap}: refused the query — skipping it")
            continue
        log(f"  capacity {cap}: {len(rows)} item-rows, {len(items)} items resolved")
        for r in rows:
            iid = str(r.get(cols[TABLE]["item_id"]) or "").upper()
            wsid = str(r.get(cols[TABLE]["workspace_id"]) or "").upper()
            cu = r.get("CU")
            if cu is None or not iid:
                continue
            if WS_FILTER and wsid != WS_FILTER:
                continue
            name, kind = items.get(iid, ("", ""))
            key = (name or iid).lower()
            if wanted:
                if key not in wanted:
                    continue
            elif kind and kind.strip().lower() not in MODEL_KINDS:
                continue
            if not name:
                # Keep it under its GUID rather than drop it: losing CU silently is worse than an
                # ugly row. Counted so the log can say how much of the table is opaque.
                unknown += 1
            op = str(r.get(cols[TABLE]["operation"]) or "(unnamed)").strip()
            stamp = str(r.get(cols[TABLE]["when"]) or "")
            if stamp:
                seen_hours.append(stamp)
            cells[(key, op)] = cells.get((key, op), 0.0) + float(cu)

    if unknown:
        log(f"  {unknown} item ids had no entry in '{ITEMS}' — shown as raw GUIDs")

    # Verify the floor actually bound. A DAX filter that is accepted and then ignored produces a
    # plausible wrong number, which is the worst failure this tool can have: it already happened
    # once, with FILTER(VALUES(...)) inside SUMMARIZECOLUMNS, and three different windows returned
    # byte-identical totals before anyone noticed. So check the hours that came back.
    if seen_hours:
        lo, hi = min(seen_hours), max(seen_hours)
        log(f"  hours returned: {lo} .. {hi}")
        if since:
            floor = since.strftime("%Y-%m-%dT%H:%M:%S")
            if lo[:19] < floor:
                die(f"the `since` filter did NOT bind: asked for >= {floor}, but rows came back "
                    f"from {lo}. Refusing to print a total that silently includes excluded time.")
    for (name, op), cu in sorted(cells.items(), key=lambda kv: -kv[1]):
        log(f"  {name} / {op}: {cu:,.1f} CU")

    render({k: round(v, 1) for k, v in cells.items()}, since, asof)


if __name__ == "__main__":
    main()
