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
#
# EXPRESSED IN THE MODEL'S OWN CLOCK, not UTC. The Capacity Metrics tables stamp everything in the
# offset configured inside the app — +10 here, so a benchmark that ran at 05:15Z sits under hour
# 15:00 — and that is also what the app's UI shows you. Taking `since` in the same clock means
# there is nothing to convert and nothing to get wrong.
#
# Detecting that offset was tried twice and abandoned. 'Timepoints' is a generated calendar running
# ~9 days into the FUTURE (it returned +227.5h), MAX() over activity lags by however long the
# capacity has been idle, and there is no offset column anywhere in the model. Every run logs the
# range of hours it actually saw, so one dispatch tells you what to set this to.
SINCE = os.environ.get("CU_SINCE", "2026-07-30T22:00:00").strip()

# Run separation. The hour axis is already in every row (it has to be, to make `since` bind), so
# splitting the report per RUN costs nothing extra: no more requests, no new query, pure
# post-processing of rows already in hand.
#
# A "run" is a maximal cluster of active hours: a new one starts when the gap since the previous hour
# that had any activity exceeds CU_RUN_GAP_HOURS. Sized from what the benchmark actually is — with one
# job per engine and a 600s inter-engine gap, a full pass is ~2h of essentially continuous activity,
# and two dispatches are hours apart. 0 disables segmentation and prints only the aggregate.
#
# THE RESOLUTION LIMIT IS HARD AND IT IS THE APP'S, not this code's: 'Metrics By Item Operation And
# Hour' is bucketed at ONE HOUR. So two dispatches inside the same hour are one run here and there is
# no way to tell them apart from this table — the timepoint detail table is the only instrument with
# finer resolution, and cu/README.md says why this does not use it. Two things make the hour bucket
# enough anyway: per-ENGINE separation does not depend on time at all (each engine has its own
# semantic model, so it is already its own row), and the benchmark's own gaps are what create the
# idle hours the split keys off.
#
# What this deliberately does NOT do is correlate a run with a GitHub run id. `benchmark/` records
# durations but no absolute timestamps, and adding them is the coupling cu/ exists without. A run
# here is identified by its own time window and nothing else.
RUN_GAP_HOURS = int(os.environ.get("CU_RUN_GAP_HOURS", "2") or 0)

# Per-run breakdown BY OPERATION as well as by model. Off by default: with several runs it is a wall
# of tables, and the aggregate operation table below already answers "what kind of work cost the CU".
RUN_OPS = os.environ.get("CU_RUN_OPS", "").strip().lower() in ("1", "true", "yes")

# A bar chart of the same numbers, one bar per hour per engine. On by default because it is FREE — the
# same rows the tables are built from, drawn instead of tabulated. A table says what each engine cost;
# the chart says where in the run it was spent, which is the thing you actually look at a capacity
# chart for.
#
# ONE BAR = ONE HOUR, and that is the app's bucket, not a choice. The Capacity Metrics app's own chart
# is drawn at 30 seconds, which lives only in 'Timepoint Interactive Detail' — one request per bucket,
# 120 per hour, and its rows carry no timestamp so a batch could not be attributed. That is a real
# capacity cost to draw a prettier picture of numbers already in hand, so it is not done. If a
# finer-grained shape is ever genuinely needed, read cu/README.md first: the request count, the
# `Total CU` smoothing-duplication trap, and the reason the aggregate is the default are all recorded
# there.
CHART = os.environ.get("CU_CHART", "true").strip().lower() not in ("0", "false", "no")

# Bars are scaled against the largest single (engine, hour) cell, so heights are comparable across
# engines AND across runs in one reading. 8 unicode block levels; "·" is a genuine zero.
BLOCKS = "▁▂▃▄▅▆▇█"

# Item x operation x hour. The hour axis supports SINCE and the per-run split below.
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


def datasets_in_workspace(ws):
    """{dataset GUID: name} straight from the Power BI REST API, for the workspace we deploy to.

    This exists because `'Items'` in the Capacity Metrics model is a LAGGING SNAPSHOT and the item it
    is most likely to be missing is the one we care about: **every deploy mints a new semantic model
    GUID**. `overwrite=True` updates a definition in place, but a model that was deleted and recreated
    — or deployed for the first time — is a new item id, and until the app's snapshot catches up that
    id resolves to no name, fails the name filter, and its CU vanishes from this report while the
    report says "no activity". That is not a hypothetical: it is what an empty run turned out to be.

    The REST API is authoritative and current, needs no new dependency (same host, same token as
    executeQueries), and costs one request. The 'Items' join stays as the fallback for everything
    outside our workspace.

    Best-effort: a 401/403 here (the SP lacking workspace read) leaves the old behaviour rather than
    failing the run, and says so.
    """
    if not ws:
        return {}
    try:
        r = requests.get(f"{PBI}/groups/{ws}/datasets",
                         headers={"Authorization": f"Bearer {TOKEN}"}, timeout=60)
    except Exception as ex:
        log(f"  could not list datasets in {ws} ({type(ex).__name__}) — falling back to '{ITEMS}'")
        return {}
    if r.status_code != 200:
        log(f"  GET /groups/{ws}/datasets returned {r.status_code} — falling back to '{ITEMS}' "
            f"(a new deploy's GUID may not be in its snapshot yet)")
        return {}
    out = {str(d["id"]).upper(): d.get("name") or "" for d in r.json().get("value", [])
           if d.get("id")}
    log(f"  {len(out)} datasets resolved live from workspace {ws}")
    return out


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


def _model_rows(cells):
    """[(display name, total cu)] in report order.

    When specific models were asked for, every one of them appears in the order given, including the
    ones with no activity: a model that silently vanishes from the table is indistinguishable from one
    that was never deployed, and a 0.0 row says which."""
    per_model = {}
    for (m, _op), cu in cells.items():
        per_model[m] = per_model.get(m, 0.0) + cu
    return ([(m, per_model.get(m.lower(), 0.0)) for m in MODELS] if MODELS
            else sorted(per_model.items(), key=lambda kv: -kv[1]))


def _op_table(cells):
    """models x operation types, printed. Operation columns ordered by total CU, so the expensive one
    is the first thing read."""
    per_op = {}
    for (_m, op), cu in cells.items():
        per_op[op] = per_op.get(op, 0.0) + cu
    ops = [op for op, _ in sorted(per_op.items(), key=lambda kv: -kv[1])]
    models = _model_rows(cells)

    print("| semantic model | " + " | ".join(ops) + " | total |")
    print("|---|" + "---:|" * (len(ops) + 1))
    for name, total in models:
        vals = [cells.get((name.lower(), op), 0.0) for op in ops]
        print(f"| {name} | " + " | ".join(f"{v:,.1f}" for v in vals) + f" | **{total:,.1f}** |")
    grand = sum(t for _, t in models)
    print("| **total** | "
          + " | ".join(f"**{per_op[op]:,.1f}**" for op in ops)
          + f" | **{grand:,.1f}** |")


def sessionize(hours, gap_hours=RUN_GAP_HOURS):
    """Cluster active hours into runs. `hours` is an iterable of datetimes; returns a list of sorted
    hour lists, oldest run first.

    A new run starts when the gap since the previous ACTIVE hour exceeds `gap_hours`. Nothing here
    assumes how long a run is or how many there should be — an idle gap is the only signal, which is
    why it survives a dispatch with different `engines`, `runs` or `gap_seconds` inputs.
    """
    uniq = sorted(set(hours))
    if not uniq or gap_hours <= 0:
        return [uniq] if uniq else []
    runs = [[uniq[0]]]
    for h in uniq[1:]:
        if (h - runs[-1][-1]).total_seconds() > gap_hours * 3600:
            runs.append([])
        runs[-1].append(h)
    return runs


def render_runs(hourly, runs):
    """One row per detected run: its window, and the CU each model spent inside it.

    This is the whole point of the per-run split — the aggregate table sums every dispatch since the
    floor, so a model's number there is "all the benchmarking we have done", not "what one pass
    costs". A row here is one pass.
    """
    if len(runs) < 2:
        # One cluster is not a separation, and printing a one-row "runs" table beside an identical
        # aggregate reads as two findings where there is one. Say why instead.
        span = "" if not runs else f" ({runs[0][0]:%Y-%m-%d %H:%M} .. {runs[0][-1]:%H:%M})"
        print(f"\n<sub>All activity sits in a single ≤{RUN_GAP_HOURS}h-contiguous cluster{span}, so "
              f"there is nothing to separate — the table above IS that run. Raise `since` or lower "
              f"`run_gap_hours` to split more finely; the app's hour bucket is the floor on how fine "
              f"that can get.</sub>")
        return
    names = [m for m, _ in _model_rows({(k[0], k[1]): v for k, v in hourly.items()})]
    print(f"\n### Runs detected: {len(runs)}\n")
    print("| run | window (model clock) | hours | " + " | ".join(names) + " | total |")
    print("|---|:--|--:|" + "---:|" * (len(names) + 1))
    for i, hrs in enumerate(runs, start=1):
        window = (f"{hrs[0]:%Y-%m-%d %H:%M} → {hrs[-1]:%H:%M}" if hrs[0].date() == hrs[-1].date()
                  else f"{hrs[0]:%Y-%m-%d %H:%M} → {hrs[-1]:%m-%d %H:%M}")
        inside = set(hrs)
        per = {}
        for (m, op, h), cu in hourly.items():
            if h in inside:
                per[m] = per.get(m, 0.0) + cu
        vals = [per.get(n.lower(), 0.0) for n in names]
        print(f"| {i} | {window} | {len(hrs)} | "
              + " | ".join(f"{v:,.1f}" for v in vals)
              + f" | **{sum(vals):,.1f}** |")
    print(f"\n<sub>A run is a cluster of active hours separated from the next by more than "
          f"{RUN_GAP_HOURS}h idle. The metrics table is bucketed at ONE HOUR, so two dispatches "
          f"inside the same hour are one row here and cannot be told apart from this table. Per-"
          f"ENGINE separation does not depend on time — each engine has its own semantic model, so it "
          f"is already its own column.</sub>")

    if RUN_OPS:
        for i, hrs in enumerate(runs, start=1):
            inside = set(hrs)
            cells = {}
            for (m, op, h), cu in hourly.items():
                if h in inside:
                    cells[(m, op)] = cells.get((m, op), 0.0) + cu
            print(f"\n#### Run {i} — {hrs[0]:%Y-%m-%d %H:%M} → {hrs[-1]:%H:%M}, by operation\n")
            _op_table(cells)


def render_chart(hourly, runs):
    """CU per hour per engine, as bars. Costs nothing — same rows as the tables.

    Two columns per hour so the hour labels line up under their own bars, and `┊` between runs so a
    gap in the capacity chart is visible as a gap here. Every engine asked for gets a line even if it
    is empty, for the same reason it gets a 0.0 table row: an absent line and an idle one are
    different findings.
    """
    hours = sorted({h for (_m, _o, h) in hourly})
    if not hours:
        return
    per = {}                                     # (engine, hour) -> cu
    for (m, _op, h), cu in hourly.items():
        per[(m, h)] = per.get((m, h), 0.0) + cu
    peak = max(per.values()) if per else 0.0
    if peak <= 0:
        return

    names = [m for m, _ in _model_rows({(k[0], k[1]): v for k, v in hourly.items()})]
    # Which run each hour belongs to, so a boundary can be drawn between them.
    run_of = {h: i for i, hrs in enumerate(runs) for h in hrs}
    width = max((len(n) for n in names), default=7)

    def cell(v):
        if v <= 0:
            return "··"
        lvl = BLOCKS[min(len(BLOCKS) - 1, int(v / peak * len(BLOCKS)) if v < peak else len(BLOCKS) - 1)]
        return lvl * 2

    print(f"\n### Shape — CU per hour\n")
    print("```")
    print(f"peak {peak:,.1f} CU  ·  1 bar = 1 hour ({len(hours)} active)  ·  "
          f"┊ = gap between runs  ·  ·· = idle")
    for n in names:
        row = []
        for i, h in enumerate(hours):
            if i and run_of.get(h) != run_of.get(hours[i - 1]):
                row.append("┊")
            row.append(cell(per.get((n.lower(), h), 0.0)))
        print(f"{n:<{width}}  {''.join(row)}")
    # Hour axis, two chars per hour to match; the date changes are called out underneath.
    axis = []
    for i, h in enumerate(hours):
        if i and run_of.get(h) != run_of.get(hours[i - 1]):
            axis.append("┊")
        axis.append(f"{h:%H}")
    print(f"{'hour':<{width}}  {''.join(axis)}")
    days = sorted({h.date() for h in hours})
    print(f"{'':<{width}}  {'· '.join(str(d) for d in days)} (model clock)")
    print("```")


def render_empty(span, seen, dropped, active, near):
    """Explain an empty report instead of asserting an idle capacity.

    The failure that produced this: the query succeeded, the floor bound, 1,202 rows came back, and
    every one of them failed the workspace/name filter — which printed as "No semantic model
    activity", i.e. exactly what an idle capacity prints. Two opposite conclusions, one sentence. So
    when nothing matches, say how many rows were seen, which filter rejected them, and what WAS
    spending the CU."""
    print(f"**No CU matched the filters** {span}.\n")
    if not seen:
        print("And no rows came back at all, so this is about the time filter, not the item filter: "
              "operations land **~6 minutes** after they run and are smoothed over 5–64 minutes, so a "
              "benchmark that just finished is not in here yet. `since` is in the **model's clock** "
              "(the app's own offset, not UTC) — a floor set in the future reads as silence.")
        return
    print(f"{seen:,} rows came back **after** the floor, so the time filter is not the problem — "
          f"every one of them was rejected by an item filter:\n")
    print("| filter | rows dropped |")
    print("|:--|--:|")
    print(f"| workspace id ≠ `{WS_FILTER or '(none)'}` | {dropped['workspace']:,}"
          + (f" (of which **{dropped['workspace_blank']:,} had a blank workspace id**)"
             if dropped["workspace_blank"] else "") + " |")
    print(f"| name not in `{','.join(MODELS) or '(any)'}` | {dropped['name']:,} |")
    if dropped["kind"]:
        print(f"| item kind not a semantic model | {dropped['kind']:,} |")
    if near:
        print(f"\n⚠ **{len(near)} item(s) matched the NAME but not the workspace** — almost certainly "
              f"the cause. Their actual workspace ids:\n")
        print("| item | kind | workspace id | CU |")
        print("|:--|:--|:--|--:|")
        for (nm, kd, ws), cu in sorted(near.items(), key=lambda kv: -kv[1])[:10]:
            print(f"| {nm} | {kd or '—'} | `{ws}` | {cu:,.1f} |")
        print(f"\nEither set `workspace` to that id, or blank it to stop filtering by workspace.")
    if active:
        print(f"\nWhat did spend CU {span} (top 10, any item):\n")
        print("| item | kind | workspace id | CU |")
        print("|:--|:--|:--|--:|")
        for (nm, kd, ws), cu in sorted(active.items(), key=lambda kv: -kv[1])[:10]:
            print(f"| {nm} | {kd or '—'} | `{ws}` | {cu:,.1f} |")


def render(cells, hourly, since, asof, seen=0, dropped=None, active=None, near=None):
    """cells is {(model, operation): cu}; hourly is {(model, operation, hour): cu}."""
    span = (f"since {since:%Y-%m-%d %H:%M} (model clock)" if since else "over everything retained")
    print(f"## Semantic model CU — {span}, as of {asof:%Y-%m-%d %H:%MZ}\n")
    if not cells:
        render_empty(span, seen, dropped or {"workspace": 0, "workspace_blank": 0, "name": 0,
                                            "kind": 0}, active or {}, near or {})
        return

    print(f"Every dispatch since the floor, summed:\n")
    _op_table(cells)
    runs = sessionize(h for (_m, _o, h) in hourly) if (RUN_GAP_HOURS > 0 and hourly) else []
    if runs:
        render_runs(hourly, runs)
    if CHART and hourly:
        render_chart(hourly, runs or [sorted({h for (_m, _o, h) in hourly})])


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
            since = datetime.fromisoformat(SINCE.replace("Z", "").strip())
        except ValueError:
            die(f"CU_SINCE={SINCE!r} is not ISO-8601 (e.g. 2026-07-30T22:00:00). It is in the "
                f"MODEL's clock, not UTC — a trailing Z is ignored rather than honoured.")

    cols = discover_columns()
    caps = [CAPACITY] if CAPACITY else discover_capacities()
    since_local = since
    log(f"capacities={caps} since={SINCE or '(everything retained)'} "
        f"workspace={WS_FILTER or '(all)'} models={MODELS or '(every semantic model)'}")

    # One request, before the per-capacity loop: the workspace is the same for every capacity.
    live_names = datasets_in_workspace(WS_FILTER)
    missing = [m for m in MODELS if m.lower() not in {n.lower() for n in live_names.values()}]
    if MODELS and live_names and missing:
        log(f"  note: {', '.join(missing)} do not exist as datasets in {WS_FILTER} right now — "
            f"expect 0.0 rows for them")

    wanted = {m.lower() for m in MODELS}
    cells, hourly, unknown, seen_hours = {}, {}, 0, []
    unparsed = 0
    # Why rows were DROPPED, and what was active instead. An empty report is the one outcome this
    # tool cannot explain from its own output otherwise: the query succeeded, the floor bound, rows
    # came back, and every one of them failed a filter — that reads identically to "the capacity was
    # idle", which is the opposite conclusion. Counted per reason, and the biggest spenders kept, so
    # one dispatch says which filter to fix instead of the next three guessing.
    dropped = {"workspace": 0, "workspace_blank": 0, "name": 0, "kind": 0}
    active = {}     # (name-or-guid, kind, workspace id) -> cu, over rows that failed a filter
    near = {}       # rows whose NAME matched but whose workspace did not -> the likeliest cause
    seen = 0        # rows the metrics table returned, before any local filter
    for cap in caps:
        items = items_for(cap, cols[ITEMS])
        rows = cu_for(cap, since_local, cols[TABLE])
        if rows is None:
            log(f"  capacity {cap}: refused the query — skipping it")
            continue
        log(f"  capacity {cap}: {len(rows)} item-rows, {len(items)} items resolved")
        seen += len(rows)
        for r in rows:
            iid = str(r.get(cols[TABLE]["item_id"]) or "").upper()
            wsid = str(r.get(cols[TABLE]["workspace_id"]) or "").upper()
            cu = r.get("CU")
            if cu is None or not iid:
                continue
            # Live REST names win over the metrics app's 'Items' snapshot: a just-deployed semantic
            # model has a GUID the snapshot has not seen yet, and resolving it from the snapshot alone
            # is what made a redeploy read as an idle capacity.
            live = live_names.get(iid)
            name, kind = items.get(iid, ("", ""))
            if live:
                name, kind = live, (kind or "SemanticModel")
            key = (name or iid).lower()
            # Resolve the name BEFORE the workspace test, purely so a rejection can be described.
            # The tests themselves are unchanged and still stack — see cu/README.md on why both are
            # needed — they just now say what they threw away.
            if WS_FILTER and wsid != WS_FILTER:
                dropped["workspace"] += 1
                if not wsid:
                    dropped["workspace_blank"] += 1
                bucket = near if (wanted and key in wanted) else active
                k = (name or iid, kind, wsid or "(blank)")
                bucket[k] = bucket.get(k, 0.0) + float(cu)
                continue
            if wanted:
                if key not in wanted:
                    dropped["name"] += 1
                    k = (name or iid, kind, wsid or "(blank)")
                    active[k] = active.get(k, 0.0) + float(cu)
                    continue
            elif kind and kind.strip().lower() not in MODEL_KINDS:
                dropped["kind"] += 1
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
            # Keep the hour too — the per-run split is pure post-processing of this. An unparseable
            # stamp costs only the segmentation, never the CU: it still lands in `cells` above.
            try:
                hour = datetime.fromisoformat(stamp[:19])
            except ValueError:
                unparsed += 1
                continue
            hkey = (key, op, hour)
            hourly[hkey] = hourly.get(hkey, 0.0) + float(cu)

    if unknown:
        log(f"  {unknown} item ids had no entry in '{ITEMS}' — shown as raw GUIDs")
    if unparsed:
        log(f"  {unparsed} rows had an unparseable timestamp — counted in the totals, excluded "
            f"from the per-run split")

    # Verify the floor actually bound. A DAX filter that is accepted and then ignored produces a
    # plausible wrong number, which is the worst failure this tool can have: it already happened
    # once, with FILTER(VALUES(...)) inside SUMMARIZECOLUMNS, and three different windows returned
    # byte-identical totals before anyone noticed. So check the hours that came back.
    if seen_hours:
        lo, hi = min(seen_hours), max(seen_hours)
        log(f"  hours returned: {lo} .. {hi}")
        if since_local:
            floor = since_local.strftime("%Y-%m-%dT%H:%M:%S")
            if lo[:19] < floor:
                die(f"the `since` filter did NOT bind: asked for >= {floor}, but rows came back "
                    f"from {lo}. Refusing to print a total that silently includes excluded time.")
    for (name, op), cu in sorted(cells.items(), key=lambda kv: -kv[1]):
        log(f"  {name} / {op}: {cu:,.1f} CU")

    runs = sessionize(h for (_m, _o, h) in hourly) if RUN_GAP_HOURS > 0 else []
    for i, hrs in enumerate(runs, start=1):
        log(f"  run {i}: {hrs[0]:%Y-%m-%d %H:%M} .. {hrs[-1]:%H:%M} ({len(hrs)} active hours)")

    if not cells and seen:
        log(f"  dropped: {dropped['workspace']} on workspace "
            f"({dropped['workspace_blank']} blank), {dropped['name']} on name, "
            f"{dropped['kind']} on kind")

    render({k: round(v, 1) for k, v in cells.items()},
           {k: round(v, 1) for k, v in hourly.items()}, since, asof,
           seen=seen, dropped=dropped, active=active, near=near)


if __name__ == "__main__":
    main()
