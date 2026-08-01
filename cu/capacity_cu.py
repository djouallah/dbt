"""CU per ENGINE, read from the Fabric Capacity Metrics app's semantic model.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's model is the only
authoritative source, so this queries it by DAX over the Power BI `executeQueries` endpoint and
prints one engine-major table: four columns, `etl` (what WRITES the tables — lakehouses, warehouse,
notebooks, Livy) against `analytics` (what QUERIES them — the semantic models) down the side, each
class broken out by operation type, then the same thing per run.

Engine-major because that is the repo's thesis — same data, four engines, side by side — and it is
the only orientation in which "what did iceberg cost to build and to query" is one column read top
to bottom. It is also what makes the width manageable: operations are ROWS, so a lakehouse's dozen
OneLake operation types is a dozen rows rather than a dozen columns.

Time is a pinned FLOOR (`since`), not a rolling window. A window moves with every dispatch and can
slice one benchmark in half, making an engine look cheap for no reason but where the boundary fell;
a floor stays put and everything after it accumulates. Its purpose is to exclude the run in which
dwh was DirectQuery rather than Direct Lake — still inside the app's ~14-day retention, and not the
same experiment.

Scoped to every item in ONE workspace, classified and then attributed to an engine by NAME
(`engine_of`) — the metrics model carries no item-to-engine relationship, so a name is all there is.
An ambiguous name goes to `shared` rather than to a guess: a wrong column is worse than an honest
one. Widening past the semantic models was tried once before and reverted because an item-major
table of a dozen operation columns is unreadable; the orientation is what fixed that, with
`CLASS_BY_KIND` rolling every item up to a class and `GROUP_PREFIXES` collapsing the throwaway
notebooks. `CU_ETL=0` restores the old semantic-models-only report exactly, and `CU_ITEM_DETAIL=1`
prints the item-major table underneath for debugging.

Reporting both is the point: what a query costs is only half the bill, and the other half is the
build that wrote the tables. They are still two questions, so they are two rows — never one number.

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
import json
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
# Only for naming items — see fabric_items(). A DIFFERENT token audience from PBI_TOKEN, which is
# why it is optional: without it the report still prints, it just names fewer items.
FABRIC = "https://api.fabric.microsoft.com/v1"

TOKEN = os.environ.get("PBI_TOKEN", "").strip()
FABRIC_TOKEN = os.environ.get("FABRIC_TOKEN", "").strip()
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

# Report EVERY item in the workspace, not only the semantic models. ON by default.
#
# What it changes, precisely: with it on, `CU_MODELS` stops being a FILTER and becomes an ORDERING —
# the named models still lead the analytics rows and still print a 0.0 when they had no activity,
# but a lakehouse, a warehouse, a notebook or a Livy session is no longer thrown away for failing
# to be in that list. `CU_WORKSPACE_FILTER` is then the only filter left, which is the right one to
# be left with: everything in that workspace is this repo's, and nothing else is.
#
# Off restores the previous report byte for byte (name filter, then kind filter, semantic models
# only) — worth having for a straight comparison against an older dispatch's numbers.
ETL = os.environ.get("CU_ETL", "1").strip().lower() not in ("0", "false", "no", "")

# Item kind -> class. Compared lower-cased with spaces removed, so "Semantic Model", "SemanticModel"
# and "semanticmodel" are one key.
#
# `analytics` is what READS the tables, `etl` is what WRITES them. Anything unrecognised lands in
# `other` and is printed rather than dropped — with its kind named in the stderr log, because that
# log is how a kind gets added here. `other` means "this file has not seen that kind yet", never
# "ignore it".
#
# **Livy bills against the LAKEHOUSE**, measured, not assumed — there is no Spark item of any kind in
# the report. So `dbt_spark`'s row is its OneLake operations AND the whole spark leg's compute added
# together, and the only thing that separates them is the operation column. `sparkjobdefinition` /
# `sparkapplication` below are kept for completeness and are expected to stay empty here.
CLASS_BY_KIND = {
    # reads
    "semanticmodel": "analytics", "dataset": "analytics", "report": "analytics",
    "paginatedreport": "analytics", "dashboard": "analytics", "datamart": "analytics",
    # writes
    "lakehouse": "etl", "warehouse": "etl", "sqlendpoint": "etl",
    "sqlanalyticsendpoint": "etl", "notebook": "etl", "sparkjobdefinition": "etl",
    "sparkapplication": "etl", "datapipeline": "etl", "dataflow": "etl", "dataflowgen2": "etl",
    "environment": "etl", "mirroreddatabase": "etl", "eventhouse": "etl", "kqldatabase": "etl",
}
CLASS_ORDER = ["etl", "analytics", "other"]

# The report is ENGINE-MAJOR: four columns, one per engine, because that is this repo's whole
# thesis — same data, four engines, side by side — and it is the only orientation in which "what did
# iceberg cost to build and to query" is one column read top to bottom. Operations are ROWS, which is
# also what makes the width problem go away: a dozen OneLake operation types is a dozen rows, and
# markdown handles that fine.
#
# Every engine is printed even at 0.0, for the same reason every named model is: a column that
# disappears is indistinguishable from an engine that spent nothing.
ENGINES = [e.strip() for e in os.environ.get(
    "CU_ENGINES", "duckrun,iceberg,spark,dwh").split(",") if e.strip()]

# How an item NAME says which engine it belongs to. Matched as a substring of the lower-cased
# display name, engines tried in CU_ENGINES order.
#
# `delta` is the alias that matters: duckrun's output lakehouse is `dbt_delta`, not `dbt_duckrun`.
# Everything else is the engine's own name — `aemo_iceberg`, `dbt_spark`, `dbt-duckrun-<random>`.
ENGINE_ALIASES = {"duckrun": ("duckrun", "delta")}

# Names that contain an engine token but must NOT be attributed to it. `duckrun-py-` is duckrun's
# DEFAULT notebook name, which both DuckDB legs used before `fabric_run.py` started naming its own —
# so those rows are genuinely ambiguous, and guessing them into the duckrun column would be a wrong
# number rather than a missing one. They land in `shared`, which is named in a footnote.
UNATTRIBUTABLE = ("duckrun-py-",)

# The old item x operation table, kept for debugging behind an env var. Off by default: the engine
# table above answers the question, and the whole reason the first attempt at this width was
# reverted is that item rows plus operation columns is too much table.
ITEM_DETAIL = os.environ.get("CU_ITEM_DETAIL", "").strip().lower() in ("1", "true", "yes")

# Items whose display name starts with one of these collapse into ONE row named `<prefix>*`.
#
# Throwaway notebooks are the reason this exists: `duckrun.run_python` creates one per call and
# deletes it afterwards, so a week of dispatches is a week of one-row-each notebooks that are the
# same thing wearing different GUIDs. Collapsing is display only — the run split still allocates by
# GUID, so nothing about WHICH run a notebook belongs to is lost.
#
# `fabric_run.py` names them `dbt-<engine>-<random>` precisely so this collapse can separate the two
# DuckDB legs; the random suffix is what keeps Fabric's display-name reservation from 409ing the
# next run, so the engine has to live in the prefix. `duckrun-py-` is duckrun's own default and
# stays for the older rows still inside the app's ~14-day retention.
#
# A collapsed name is also the signal that an item is THROWAWAY, which the run split uses: an item
# recreated per run carries the same exact generation rule as a semantic model. See main().
GROUP_PREFIXES = [p.strip() for p in os.environ.get(
    "CU_GROUP_PREFIXES", "dbt-duckrun-,dbt-iceberg-,duckrun-py-").split(",") if p.strip()]

# Operation columns to print before folding the rest into one `other` column. The semantic models
# spend on three or four operation types; a lakehouse alone brings a dozen OneLake ones, and that is
# what made the earlier attempt at this unreadable. Folded columns are counted in the header, never
# silently dropped. 0 prints every operation type.
MAX_OP_COLS = int(os.environ.get("CU_OP_COLS", "6") or 0)

# The workspace the models live in — the same one dbt.yml and benchmark.yml deploy to. Applied ON
# TOP of the name filter, not instead of it: display names are not unique across a tenant, so a
# stale `aemo_spark` in some other workspace would otherwise be silently added to this one's CU.
# Blank = every workspace on the capacity.
WS_FILTER = os.environ.get(
    "CU_WORKSPACE_FILTER", "ea575278-bd81-459c-9680-47829898c902").strip().upper()

DEBUG = os.environ.get("CU_DEBUG", "").strip().lower() in ("1", "true", "yes")

# Refresh the metrics model before querying it, and wait. ON by default: every benchmark dispatch
# creates four semantic models the app has never seen, and an unrefreshed model cannot report CU
# against an item that is not in its tables yet. Off is for a re-read of a window that has already
# settled, where the refresh is pure latency. See refresh_metrics_model() for why a failure here
# is logged rather than fatal.
REFRESH = os.environ.get("CU_REFRESH", "1").strip().lower() not in ("0", "false", "no", "")
REFRESH_TIMEOUT = int(os.environ.get("CU_REFRESH_TIMEOUT", "900"))

# A FLOOR, not a rolling window, and the difference matters. A window ("last 3h") moves with every
# dispatch and can slice one benchmark in half, making an engine look cheap for no reason but where
# the boundary fell. A pinned floor stays put: everything after it accumulates, and two dispatches a
# day apart are comparable.
#
# What it is for: the app retains ~14 days, and the benchmark's METHODOLOGY has moved inside that
# window more than once — the run where dwh was DirectQuery rather than Direct Lake, and then the
# per-query dehydrate being dropped for a user-session walk with think time (8c037c8 / debef3a,
# 06:11Z on 2026-07-31). None of those are the same experiment and their CU must not be summed.
# So the floor sits at the first dispatch measured the CURRENT way — 30609137059, which started
# 06:16Z, i.e. hour 16:00 in the model's clock. Bump it again the next time the suite changes;
# blank means everything retained, and a wider floor is a dispatch input away when the older runs
# are wanted for a one-off comparison.
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
SINCE = os.environ.get("CU_SINCE", "2026-07-31T16:00:00").strip()

# Run separation. Both signals it uses are already in every row — the item GUID and the hour — so
# splitting the report per RUN costs nothing extra: no more requests, no new query, pure
# post-processing of rows already in hand.
#
# A "run" is ONE DEPLOYMENT GENERATION, and the item GUID is what says so. `deploy_models.py` DELETES
# and recreates each semantic model, so every dispatch mints a fresh GUID for every engine it ran —
# the same fact recorded in CLAUDE.md as the trap that once made this report read empty. A model
# therefore cannot appear twice in one dispatch, so when a model NAME repeats among the GUIDs, the
# repeat is by definition the next run. That rule is time-free, which is what makes it exact.
#
# CU_RUN_GAP_HOURS is the SECOND rule, for activity with no redeploy behind it: one GUID queried
# again days later (a subset-`engines` dispatch, or someone opening the report in Power BI) splits
# when the gap since its own previous active hour exceeds this. 2h is sized from what the benchmark
# is — one job per engine and a 600s inter-engine gap, so a pass is ~2h of essentially continuous
# activity. 0 disables segmentation entirely and prints only the aggregate.
#
# The hour bucket of 'Metrics By Item Operation And Hour' is still this table's resolution, but it is
# NO LONGER the floor on separating dispatches — two dispatches ten minutes apart used to be one
# unsplittable column and now are two, because they are two GUIDs. It only bounds the gap rule above,
# and it is why two adjacent runs can share an hour: the CU is still allocated exactly, because the
# rows are per item. The timepoint detail table remains unnecessary — cu/README.md says why it is
# also unwise.
#
# What this deliberately does NOT do is correlate a run with a GitHub run id. `benchmark/` records
# durations but no absolute timestamps, and adding them is the coupling cu/ exists without. A run
# here is identified by its own item GUIDs and time window, and nothing else.
RUN_GAP_HOURS = int(os.environ.get("CU_RUN_GAP_HOURS", "2") or 0)

# Per-run breakdown BY OPERATION as well as by model. Off by default: with several runs it is a wall
# of tables, and the aggregate operation table below already answers "what kind of work cost the CU".
RUN_OPS = os.environ.get("CU_RUN_OPS", "").strip().lower() in ("1", "true", "yes")

# A run is a COLUMN, so the run table grows sideways with every dispatch inside the floor. At the
# default `since` that is one to three columns; against a blank `since` (~14 days retained) it can be
# a dozen, and a dozen-column markdown table is not readable — which is the whole point of the table.
# So the oldest fold into one `earlier` column and the most recent MAX_RUN_COLS keep their own.
#
# Never silently: the fold is named in the column header, restated in the footnote, and logged. 0
# disables it and prints every run as its own column. No workflow input, deliberately — it only binds
# on a widened `since`, which is a local-investigation case, and the dispatch form is long enough.
MAX_RUN_COLS = int(os.environ.get("CU_RUN_COLS", "8") or 0)

# Physical layout, printed beside the CU. CU on its own says which engine cost more; it does not say
# WHY, and the answer is almost always the layout — 37,227 CU next to "386 files, 122k avg row group"
# is a finding, while either number alone is trivia.
#
# The numbers are NOT read here. They come from `stats.py` in the `layout` job of the `dbt` workflow, which already
# reads all four Delta logs, via the `stats` artifact of the latest successful `dbt` run — the workflow
# downloads it and points STATS_JSON at it. That keeps this directory's one hard property intact:
# `requests` is still the whole dependency list, there is no duckrun, no storage token, no OneLake
# read, and `rm -rf cu/ .github/workflows/cu.yml` still removes every trace. The coupling is a JSON
# file produced by a job that runs anyway, not code.
#
# It is a JSON data contract, so it fails QUIETLY if stats.py renames a key: the layout table is
# skipped with a note and the CU report is unaffected. That is the right trade here (a CU report is
# still useful without layout) but it means a rename over there shows up as a missing table over here,
# not as an error. stats.py's docstring carries the same warning from its side.
STATS_JSON = os.environ.get("STATS_JSON", "").strip()

# Which table's layout to show. One table, not all eight: this is the mart the benchmark queries and
# the CU was spent on. `dim_duid`/`dim_calendar` are a few hundred rows and their layout explains
# nothing about a 143M-row scan.
LAYOUT_TABLE = os.environ.get("CU_LAYOUT_TABLE", "fct_summary").strip()

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


def refresh_metrics_model():
    """Refresh the Capacity Metrics semantic model and wait for it, BEFORE any DAX runs.

    Why: `benchmark/deploy_models.py` deletes and recreates each semantic model, so a dispatch
    mints four item GUIDs the metrics model has never seen — and **without this refresh their CU
    does not show up at all**. Do not talk yourself out of it by noting that
    `'Timepoint Interactive Detail'` is DirectQuery and therefore reads live: that is true and it
    is not sufficient, because the report only surfaces an item the model has catalogued. Resolving
    names live from the REST API fixes the LABEL, not this.

    NON-FATAL by construction, but not because rights are in doubt — the service principal IS a
    contributor on the metrics workspace, so the call is expected to succeed. It stays non-fatal
    because a scheduled refresh may already be running (that one is waited on instead) and because
    a CU report over an already-settled window is still worth printing if the refresh misbehaves.
    A `refresh NOT started (403 ...)` line means the SP's access has changed and is worth chasing,
    not shrugging at.
    """
    base = f"{PBI}/groups/{WS}/datasets/{MODEL}/refreshes"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    r = requests.post(base, json={"notifyOption": "NoNotification"}, headers=headers, timeout=60)
    if r.status_code in (200, 202):
        log("  refresh accepted, waiting for it to finish ...")
    elif r.status_code in (400, 409) and "already" in r.text.lower():
        # A scheduled refresh beat us to it. Waiting on it is exactly as good.
        log("  a refresh is already running; waiting for that one instead")
    else:
        log(f"  refresh NOT started ({r.status_code}: {r.text[:200].replace(chr(10), ' ')}) — "
            f"reading the model as it stands. Newly created items may be missing.")
        return False

    deadline = time.time() + REFRESH_TIMEOUT
    while time.time() < deadline:
        time.sleep(20)
        g = requests.get(f"{base}?$top=1", headers=headers, timeout=60)
        if g.status_code != 200:
            log(f"  cannot read refresh status ({g.status_code}) — continuing without waiting")
            return False
        rows = g.json().get("value") or []
        status = (rows[0].get("status") if rows else "") or ""
        if status == "Completed":
            log(f"  refresh completed in {int(time.time() - (deadline - REFRESH_TIMEOUT))}s")
            return True
        if status in ("Failed", "Disabled", "Cancelled"):
            err = (rows[0].get("serviceExceptionJson") or "")[:200]
            log(f"  refresh {status.lower()} ({err}) — reading the model as it stands")
            return False
    log(f"  refresh still running after {REFRESH_TIMEOUT}s — not waiting any longer. The numbers "
        f"below are whatever the model holds now; re-dispatch if a run looks missing.")
    return False


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
                f"and the Capacity Metrics model refused it, run the workflow with PBI_TOKEN set "
                f"to a user token (see cu/README.md). API said: {detail}")
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


def fabric_items(ws):
    """{item GUID: (display name, kind)} for EVERY item kind in the workspace, live.

    The same trap `datasets_in_workspace()` exists for, one class worse. `'Items'` in the metrics
    model is a lagging snapshot, and the items this report most wants to name are exactly the ones
    least likely to be in it: a `duckrun-py-*` notebook that the very run being measured created
    minutes ago. Unnamed means unclassified, which means it lands in `other` as a bare GUID — CU
    kept, but nothing said about it.

    The datasets endpoint cannot answer this: it lists semantic models only. The Fabric items API
    lists every kind, but on a DIFFERENT audience (`api.fabric.microsoft.com`), so it needs its own
    token — `cu.yml` mints one beside `PBI_TOKEN` from the same OIDC login. Optional by
    construction: no token, a 401/403, or an unreachable host all fall back to `'Items'` with a
    line saying so, because a report that names most items is worth far more than no report.
    """
    if not ws:
        return {}
    if not FABRIC_TOKEN:
        log("  FABRIC_TOKEN unset — item names come from the metrics app's 'Items' snapshot alone, "
            "so an item created by the run being measured may show as a raw GUID")
        return {}
    out, url = {}, f"{FABRIC}/workspaces/{ws}/items"
    try:
        while url:
            r = requests.get(url, headers={"Authorization": f"Bearer {FABRIC_TOKEN}"}, timeout=60)
            if r.status_code != 200:
                log(f"  GET /workspaces/{ws}/items returned {r.status_code} — naming falls back to "
                    f"'{ITEMS}' for whatever it did not return")
                break
            doc = r.json()
            for it in doc.get("value") or []:
                if it.get("id"):
                    out[str(it["id"]).upper()] = (it.get("displayName") or "", it.get("type") or "")
            url = doc.get("continuationUri") or ""
    except Exception as ex:
        log(f"  could not list items in {ws} ({type(ex).__name__}) — naming falls back to '{ITEMS}'")
    log(f"  {len(out)} items of every kind resolved live from workspace {ws}")
    return out


def classify(kind):
    """Item kind -> 'analytics' | 'etl' | 'other'. Unknown kinds are kept, never dropped."""
    return CLASS_BY_KIND.get((kind or "").strip().lower().replace(" ", ""), "other")


def display_label(name):
    """Collapse the throwaway-item families to one row. Display only — see GROUP_PREFIXES."""
    for p in GROUP_PREFIXES:
        if name.lower().startswith(p.lower()):
            return p + "*"
    return name


def engine_of(label):
    """Which engine's column an item belongs in, or None for `shared`.

    Name-based, because the metrics model carries no such relationship and nothing else in the row
    could supply it. That is a real limitation and it is why `UNATTRIBUTABLE` exists: an ambiguous
    name goes to `shared` rather than to a guess, since a wrong column is worse than an honest one.
    """
    low = (label or "").lower()
    if any(low.startswith(p.lower()) for p in UNATTRIBUTABLE):
        return None
    for e in ENGINES:
        if any(a in low for a in ENGINE_ALIASES.get(e, (e,))):
            return e
    return None


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


def _item_rows(cells, meta):
    """[(display label, class, total cu)] in report order.

    Every model named in CU_MODELS appears, in the order given, including the ones with no activity:
    a model that silently vanishes is indistinguishable from one that was never deployed, and a 0.0
    row says which. Everything else follows by CU descending. The CLASS grouping is applied by the
    printer, not here, so this order is only the order WITHIN a class.
    """
    per = {}
    for (k, _op), cu in cells.items():
        per[k] = per.get(k, 0.0) + cu
    rows, seen = [], set()
    for m in MODELS:
        k = m.lower()
        seen.add(k)
        rows.append((m, (meta.get(k) or {}).get("cls", "analytics"), per.get(k, 0.0)))
    for k, total in sorted(per.items(), key=lambda kv: -kv[1]):
        if k in seen:
            continue
        info = meta.get(k) or {}
        rows.append((info.get("label") or k, info.get("cls", "other"), total))
    return rows


def _ops_for(cells):
    """Operation columns, most expensive first, with the tail folded into one. Returns
    `(printed ops, folded ops)` — the fold is named in the header and counted, never silent."""
    per_op = {}
    for (_k, op), cu in cells.items():
        per_op[op] = per_op.get(op, 0.0) + cu
    ordered = [op for op, _ in sorted(per_op.items(), key=lambda kv: -kv[1])]
    if MAX_OP_COLS > 0 and len(ordered) > MAX_OP_COLS + 1:
        return ordered[:MAX_OP_COLS], ordered[MAX_OP_COLS:]
    return ordered, []


def _engine_cols(meta, keys):
    """`(columns, shared items)`. Every engine always gets a column; `shared` only appears when
    something actually landed there, and then it is named."""
    shared = sorted({(meta.get(k) or {}).get("label", k) for k in keys
                     if (meta.get(k) or {}).get("engine") is None})
    return list(ENGINES) + (["shared"] if shared else []), shared


# Why a given item cannot be given a column. Only the ones actually in `shared` are explained, so
# the footnote describes the report in front of you rather than every case that could arise.
SHARED_WHY = {
    "dbt_landing": "the downloaded AEMO archive, which every leg reads",
    "duckrun-py-*": "duckrun's DEFAULT notebook name, used by both DuckDB legs before "
                    "`fabric_run.py` named its own — genuinely ambiguous",
}


def _shared_note(shared):
    if not shared:
        return
    parts = [f"`{s}`" + (f" ({SHARED_WHY[s]})" if s in SHARED_WHY else "") for s in shared]
    print(f"\n<sub>`shared` is CU no engine can be given: " + ", ".join(parts)
          + ". Attribution is by item NAME — the metrics model carries no such relationship — so an "
            "ambiguous name lands here rather than in a guessed column.</sub>")


def _engine_table(cells, meta):
    """Engines across, operations down, grouped by class — the shape the whole repo reads in.

    The class row carries its own subtotal, so "what did the build cost against the querying" is two
    bold rows rather than a sum the reader has to do. Operations are ordered by total CU within
    their class, so the expensive one is the first thing under each heading.
    """
    cols, shared = _engine_cols(meta, {k for k, _op in cells})
    per, cls_total, op_total = {}, {}, {}
    for (k, op), cu in cells.items():
        info = meta.get(k) or {}
        cls, col = info.get("cls", "other"), info.get("engine") or "shared"
        per[(cls, op, col)] = per.get((cls, op, col), 0.0) + cu
        cls_total[(cls, col)] = cls_total.get((cls, col), 0.0) + cu
        op_total[(cls, op)] = op_total.get((cls, op), 0.0) + cu

    def row(label, vals, bold=False):
        f = (lambda v: f"**{v:,.1f}**") if bold else (lambda v: f"{v:,.1f}")
        print(f"| {'**' + label + '**' if bold else label} | " + " | ".join(f(v) for v in vals)
              + f" | {f(sum(vals))} |")

    print("| | " + " | ".join(cols) + " | total |")
    print("|:--|" + "---:|" * (len(cols) + 1))
    grand = [0.0] * len(cols)
    for cls in CLASS_ORDER:
        ops = sorted((op for (c, op) in op_total if c == cls),
                     key=lambda op: -op_total[(cls, op)])
        if not ops:
            continue
        sub = [cls_total.get((cls, c), 0.0) for c in cols]
        grand = [a + b for a, b in zip(grand, sub)]
        row(cls, sub, bold=True)
        for op in ops:
            row(op, [per.get((cls, op, c), 0.0) for c in cols])
    row("total", grand, bold=True)
    _shared_note(shared)


def _op_table(cells, meta):
    """items x operation types, grouped by class with a subtotal per class.

    The class subtotals are the answer to "what did the ETL cost against the analytics"; the item
    rows underneath are why. One table rather than two, because the two halves are only comparable
    if they are read in the same units on the same page.
    """
    ops, folded = _ops_for(cells)
    rows = _item_rows(cells, meta)
    by_class = {}
    for label, cls, total in rows:
        by_class.setdefault(cls, []).append((label, total))
    headers = ops + ([f"other ({len(folded)} ops)"] if folded else [])

    def cells_for(key):
        vals = [cells.get((key, op), 0.0) for op in ops]
        if folded:
            vals.append(sum(cells.get((key, op), 0.0) for op in folded))
        return vals

    print(f"| item | " + " | ".join(headers) + " | total |")
    print("|:--|" + "---:|" * (len(headers) + 1))
    grand = [0.0] * len(headers)
    for cls in CLASS_ORDER:
        group = by_class.get(cls)
        if not group:
            continue
        sub = [0.0] * len(headers)
        for label, total in group:
            vals = cells_for(label.lower())
            sub = [a + b for a, b in zip(sub, vals)]
            print(f"| {label} | " + " | ".join(f"{v:,.1f}" for v in vals)
                  + f" | **{total:,.1f}** |")
        grand = [a + b for a, b in zip(grand, sub)]
        if len(by_class) > 1:
            print(f"| **{cls}** | " + " | ".join(f"**{v:,.1f}**" for v in sub)
                  + f" | **{sum(sub):,.1f}** |")
    print("| **total** | " + " | ".join(f"**{v:,.1f}**" for v in grand)
          + f" | **{sum(grand):,.1f}** |")
    if folded:
        print(f"\n<sub>The {len(folded)} smallest operation types are folded into `other`: "
              + ", ".join(f"`{op}`" for op in folded)
              + f". Raise `CU_OP_COLS` (currently {MAX_OP_COLS}) to give them columns.</sub>")


def _cluster_hours(hours, gap_hours):
    """Cluster datetimes into maximal blocks separated by more than `gap_hours` of idle.

    Was the whole of `sessionize` when a run was nothing but a gap in the hour axis. It is now the
    inner rule only — applied per item GUID, never across items.
    """
    uniq = sorted(set(hours))
    if not uniq or gap_hours <= 0:
        return [uniq] if uniq else []
    blocks = [[uniq[0]]]
    for h in uniq[1:]:
        if (h - blocks[-1][-1]).total_seconds() > gap_hours * 3600:
            blocks.append([])
        blocks[-1].append(h)
    return blocks


def _run_for_hour(wins, hour, gap_hours):
    """Index of the run an item that was NOT redeployed belongs to for one hour, or None.

    Containment first — and when two run windows overlap (they can, by an hour), the one that
    started later, since that is the dispatch that hour actually belongs to. Then adjacency within
    the gap, nearest edge wins, ties to the earlier run. Then None, meaning "this hour is not part
    of any run formed so far" and the caller gives it a run of its own.
    """
    inside = [i for i, (lo, hi) in enumerate(wins) if lo <= hour <= hi]
    if inside:
        return max(inside, key=lambda i: wins[i][0])
    near = [((lo - hour).total_seconds() if hour < lo else (hour - hi).total_seconds(), i)
            for i, (lo, hi) in enumerate(wins)]
    near = [(d, i) for d, i in near if d <= gap_hours * 3600]
    return min(near)[1] if near else None


def sessionize(events, gap_hours=RUN_GAP_HOURS):
    """Group activity into runs. `events` is an iterable of `(item_id, label, hour, generational)`;
    returns `[{"items": {item_id, ...}, "labels": {name, ...}, "hours": [datetime, ...],
       "pairs": {(item_id, hour), ...}}, ...]`, oldest run first. `pairs` is what the caller
    allocates CU with; `items`/`hours`/`labels` are for the header, the footnote and the log.

    TWO KINDS OF ITEM, and only one of them can carry the exact rule. A semantic model is deleted
    and recreated on every dispatch (`generational=True`), so its GUID dates it. A lakehouse, a
    warehouse or a SQL endpoint lives for years and its GUID says nothing about when — while a
    `duckrun-py-*` notebook is generational in fact but not in name, since a fresh name each time
    means the repeat rule can never fire on it. So the runs are FORMED from the generational items
    alone, exactly as before, and everything else is then allocated to them BY HOUR against those
    fixed windows. Hours that fall in no window cluster into runs of their own by the gap rule —
    which is what gives a dbt build with no benchmark beside it its own column.

    The cost of that, stated plainly: ETL allocation is only as sharp as the hour bucket, so an ETL
    hour shared by two overlapping runs is attributed to one of them by the rule above rather than
    split. Analytics allocation stays exact, because it is still by GUID.

    A run is ONE DEPLOYMENT GENERATION. Every benchmark dispatch deletes and recreates each semantic
    model, so it mints a fresh GUID per engine, and a model cannot be deployed twice inside one
    dispatch — so a repeated model NAME among GUIDs ordered by time IS the boundary between two runs.
    That rule needs no clock at all, which is why it separates two dispatches ten minutes apart that
    the hour bucket cannot tell apart, and why it survives a dispatch with different `engines`,
    `runs` or `gap_seconds` inputs.

    `gap_hours` is the second rule, and it applies to a GUID's own hours: one model that was NOT
    redeployed but is queried again days later splits into two segments rather than dragging the
    later run's CU into the earlier column. `gap_hours <= 0` keeps its documented meaning — one
    cluster, i.e. aggregate only.
    """
    gen, stable = {}, {}
    for iid, label, hour, generational in events:
        (gen if generational else stable).setdefault((iid, label), set()).add(hour)
    if not gen and not stable:
        return []
    if gap_hours <= 0:
        both = {**gen, **stable}   # an item is one or the other, so the keys cannot collide
        return [{"items": {iid for iid, _l in both},
                 "labels": {l for _iid, l in both},
                 "hours": sorted({h for hs in both.values() for h in hs}),
                 "pairs": {(iid, h) for (iid, _l), hs in both.items() for h in hs}}]

    # (first hour, label, iid, hours) per contiguous block of one GUID's activity. `label` and `iid`
    # are in the sort key only to make ties deterministic — two engines of one dispatch start in the
    # same hour, and which of them is read first must not depend on dict ordering.
    segments = sorted((blk[0], label, iid, blk)
                      for (iid, label), hs in gen.items()
                      for blk in _cluster_hours(hs, gap_hours))

    runs = []
    for first, label, iid, blk in segments:
        cur = runs[-1] if runs else None
        if cur is None or label in cur["labels"] or (
                (first - cur["hours"][-1]).total_seconds() > gap_hours * 3600):
            runs.append({"items": set(), "labels": set(), "hours": [], "pairs": set()})
            cur = runs[-1]
        cur["items"].add(iid)
        cur["labels"].add(label)
        cur["hours"] = sorted(set(cur["hours"]) | set(blk))
        # (item, hour) is the allocation key, not the item alone: a GUID that was NOT redeployed but
        # queried again days later is split by the gap rule, so it belongs to two runs and only the
        # hour says which of its rows go where.
        cur["pairs"] |= {(iid, h) for h in blk}

    # The long-lived items, allocated by hour against the windows just formed. The windows are
    # FROZEN first and every hour is tested against the same set: allocating and widening in one
    # pass would make the answer depend on the order the items happened to be read in.
    wins = [(r["hours"][0], r["hours"][-1]) for r in runs]
    extra, leftover = {}, []
    for (iid, label), hs in sorted(stable.items()):
        for h in sorted(hs):
            ci = _run_for_hour(wins, h, gap_hours) if wins else None
            if ci is None:
                leftover.append((iid, label, h))
                continue
            runs[ci]["items"].add(iid)
            runs[ci]["labels"].add(label)
            runs[ci]["pairs"].add((iid, h))
            extra.setdefault(ci, []).append(h)

    # Activity belonging to no run so far is a run of its own — a dbt build with no benchmark beside
    # it, which is most of them. Clustered across items rather than per item, because the items of
    # one build are concurrent by construction.
    for blk in _cluster_hours([h for _i, _l, h in leftover], gap_hours):
        span = set(blk)
        r = {"items": set(), "labels": set(), "hours": sorted(span), "pairs": set()}
        for iid, label, h in leftover:
            if h in span:
                r["items"].add(iid)
                r["labels"].add(label)
                r["pairs"].add((iid, h))
        runs.append(r)

    for ci, hs in extra.items():
        runs[ci]["hours"] = sorted(set(runs[ci]["hours"]) | set(hs))
    runs.sort(key=lambda r: r["hours"][0])
    return runs


def _window(hrs, year=False):
    """`MM-DD HH:MM→HH:MM` over a run's first and last ACTIVE hour bucket, short form when same-day."""
    fmt = "%Y-%m-%d %H:%M" if year else "%m-%d %H:%M"
    return (f"{hrs[0]:{fmt}}→{hrs[-1]:%H:%M}" if hrs[0].date() == hrs[-1].date()
            else f"{hrs[0]:{fmt}}→{hrs[-1]:%m-%d %H:%M}")


def render_runs(hourly, runs, meta, cells=None):
    """One ROW per item, one COLUMN per detected run, with an ETL-against-analytics table above it.

    This is the whole point of the per-run split — the aggregate table sums every dispatch since the
    floor, so a model's number there is "all the benchmarking we have done", not "what one pass
    costs". A column here is one pass.

    Oriented model-down / run-across on purpose, and it used to be the transpose. The question this
    answers is "what did *iceberg* cost yesterday against today", which on the old shape meant
    reading down one column and across two rows; here it is one row read left to right. It also
    matches the aggregate table directly above it, so the two read the same way rather than making
    the eye re-learn the layout halfway down the report.

    A column is a whole run, NOT an hour: a pass spread over 12:00→15:00 is one column carrying all
    four hours' CU. The per-run hour COUNT is in the footnote rather than the table for the same
    reason — it is diagnostic, and in a column it invited reading the table as hourly.

    CU is assigned to a column by ITEM GUID for a redeployed item, and by HOUR for one that lives
    across runs (see sessionize). The first is exact even where two windows overlap; the second is
    as sharp as the hour bucket, which is the price of having the ETL items in the table at all.
    """
    if len(runs) < 2:
        # One run is not a separation, and printing a one-column "runs" table beside an identical
        # aggregate reads as two findings where there is one. Say why instead.
        span = "" if not runs else f" ({_window(runs[0]['hours'], year=True)})"
        print(f"\n<sub>All activity falls in a single run{span}, so there is "
              f"nothing to separate — the table above IS that run. Raise `since` to reach earlier "
              f"dispatches; each one redeployed the models, so each is its own column.</sub>")
        return
    keys = {k for (k, _o, _h, _i) in hourly}
    engine_cols, shared = _engine_cols(meta, keys)

    # Columns, oldest left. Run numbering always counts from the oldest run overall, so a folded
    # report still calls the newest run by the same number an unfolded one would.
    folded = max(0, len(runs) - MAX_RUN_COLS) if MAX_RUN_COLS > 0 else 0
    cols = []
    if folded:
        early = sorted(h for r in runs[:folded] for h in r["hours"])
        cols.append((f"earlier<br>{folded} runs, {_window(early)}",
                     {p for r in runs[:folded] for p in r["pairs"]}))
        log(f"  run table: folding the {folded} oldest runs into one column "
            f"(CU_RUN_COLS={MAX_RUN_COLS})")
    for i, r in enumerate(runs[folded:], start=folded + 1):
        cols.append((f"run {i}<br>{_window(r['hours'])}", set(r["pairs"])))

    # One pass over `hourly`, not one per run: map each (item, hour) to its column first.
    pair_col = {p: ci for ci, (_hdr, pairs) in enumerate(cols) for p in pairs}
    per, cls_tot = {}, {}
    for (k, _op, h, iid), cu in hourly.items():
        ci = pair_col.get((iid, h))
        if ci is None:
            continue
        info = meta.get(k) or {}
        cls, col = info.get("cls", "other"), info.get("engine") or "shared"
        per[(ci, cls, col)] = per.get((ci, cls, col), 0.0) + cu
        cls_tot[(ci, cls)] = cls_tot.get((ci, cls), 0.0) + cu

    print(f"\n### Runs detected: {len(runs)}\n")

    # Same orientation as the aggregate above — class subtotal in bold, engines under it — so the
    # two tables read the same way rather than making the eye re-learn the layout halfway down.
    print("| | " + " | ".join(h for h, _ in cols) + " | total |")
    print("|:--|" + "---:|" * (len(cols) + 1))
    col_tot = [0.0] * len(cols)
    for cls in CLASS_ORDER:
        vals = [cls_tot.get((ci, cls), 0.0) for ci in range(len(cols))]
        if not any(vals):
            continue
        col_tot = [a + b for a, b in zip(col_tot, vals)]
        print(f"| **{cls}** | " + " | ".join(f"**{v:,.1f}**" for v in vals)
              + f" | **{sum(vals):,.1f}** |")
        for e in engine_cols:
            ev = [per.get((ci, cls, e), 0.0) for ci in range(len(cols))]
            if any(ev):
                print(f"| {e} | " + " | ".join(f"{v:,.1f}" for v in ev)
                      + f" | **{sum(ev):,.1f}** |")
    grand = sum(col_tot)
    print("| **total** | " + " | ".join(f"**{v:,.1f}**" for v in col_tot)
          + f" | **{grand:,.1f}** |")
    # No shared note here: the aggregate table above this one has already printed it, and the same
    # paragraph twice on one page reads as two different caveats.

    shape = ", ".join(f"run {i}: {len(r['items'])} items over {len(r['hours'])}h"
                      for i, r in enumerate(runs, start=1))
    print(f"\n<sub>A run is formed from the items that are RECREATED each time — the semantic "
          f"models a benchmark dispatch redeploys, and the throwaway notebook each dbt leg runs in — "
          f"so one of those names repeating among the item GUIDs is the next run, which is why two "
          f"dispatches inside the same hour are still two columns here, and why adjacent windows "
          f"can overlap by an hour ({shape}; windows are the first and last active hour bucket, in "
          f"the model's clock). Their CU is allocated by item GUID, so an overlap costs nothing in "
          f"accuracy. **The lakehouses and the warehouse live across runs**, so theirs is allocated "
          f"by HOUR against those windows instead — as sharp as the hour bucket and no sharper — and "
          f"hours belonging to no window at all form a run of their own. A long-lived item splits "
          f"only on more than {RUN_GAP_HOURS}h idle."
          + (f" The {folded} oldest runs are folded into the `earlier` column; raise `CU_RUN_COLS` "
             f"(currently {MAX_RUN_COLS}) to give them their own." if folded else "") + "</sub>")

    # The run columns can total LESS than the aggregate table, and silently: a row whose hour stamp
    # would not parse lands in `cells` but never in `hourly`, so it is in no run. Small, but this file
    # refuses to print a plausible wrong number elsewhere and should not start here.
    if cells:
        agg = sum(cells.values())
        if agg - grand > 0.05:
            print(f"\n<sub>⚠️ These columns total {grand:,.1f} CU against the aggregate table's "
                  f"{agg:,.1f} — {agg - grand:,.1f} CU came back with an hour stamp that could not "
                  f"be parsed, so it is counted above and belongs to no run. stderr says how many "
                  f"rows.</sub>")
        elif grand - agg > 0.05:
            # Every hourly row is also a cells row, so the runs can only ever total LESS. More means
            # the two were built from different data and one of these tables is wrong — say that,
            # rather than blaming a timestamp that would not explain it.
            print(f"\n<sub>⚠️ These columns total {grand:,.1f} CU, MORE than the aggregate table's "
                  f"{agg:,.1f}. That should be impossible — every hour-stamped row is also in the "
                  f"aggregate — so one of the two tables is wrong. Do not quote either.</sub>")

    if RUN_OPS:
        for i, r in enumerate(runs, start=1):
            inside = r["pairs"]
            ops = {}
            for (m, op, h, iid), cu in hourly.items():
                if (iid, h) in inside:
                    ops[(m, op)] = ops.get((m, op), 0.0) + cu
            print(f"\n#### Run {i} — {_window(r['hours'], year=True)}, by operation\n")
            _op_table(ops, meta)


def load_layout(path=None):
    """`stats.py`'s JSON, or {} — never an exception. See STATS_JSON above for where it comes from."""
    path = path or STATS_JSON
    if not path:
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except Exception as ex:
        log(f"  no layout: {path} unreadable ({type(ex).__name__}: {ex})")
        return {}
    if not doc.get("stats"):
        log(f"  no layout: {path} carries no stats block")
        return {}
    return doc


def render_layout(doc, cu_by_engine, table=LAYOUT_TABLE):
    """The layout of `table` per engine, with that engine's ANALYTICS CU beside it.

    One table, so cost and shape are read together rather than in two browser tabs. The CU column is
    the engine's total from this run's report; the rest is `stats.py`'s reading of the Delta log. They
    come from different runs at different times — hence the provenance line, which is not decoration:
    quoting a layout from a dbt run three days older than the CU beside it is the way this misleads.
    """
    stats = doc.get("stats") or {}
    engines = [e for e in (doc.get("engines") or stats) if (stats.get(e) or {}).get(table)]
    if not engines:
        log(f"  no layout: no engine in the artifact carries '{table}'")
        return
    run = doc.get("run") or {}
    meta = doc.get("engines") or {}

    # Only the columns that say something about a scan. `schema` and `compression` are in the artifact
    # and deliberately not here — this is a cost table, not a duplicate of the parity dashboard.
    cols = [("total_rows", "rows", 0), ("num_files", "files", 0),
            ("num_row_groups", "row groups", 0), ("avg_row_group", "avg RG rows", 0),
            ("size_mb", "size MB", 1)]
    print(f"\n### Layout of `{table}` — what the CU was spent scanning\n")
    print("| engine | writer | CU | " + " | ".join(h for _k, h, _d in cols) + " | vorder |")
    print("|:--|:--|--:|" + "--:|" * len(cols) + ":--|")
    for e in engines:
        d = stats[e][table]
        cu = cu_by_engine.get(e)
        cells = []
        for key, _h, dp in cols:
            v = d.get(key)
            cells.append("—" if v is None else f"{float(v):,.{dp}f}")
        vo = d.get("vorder")
        writer = (meta.get(e) or {}).get("writer") or "—"
        print(f"| {e} | `{writer}` | {'—' if cu is None else f'{cu:,.1f}'} | "
              + " | ".join(cells) + f" | {'yes' if vo else 'no'} |")
    print(f"\n<sub>Layout from the **layout** job of `dbt` run `{run.get('id') or '?'}` "
          f"(sha `{(run.get('sha') or '?')[:7]}`), written {run.get('written') or '?'} — **a different "
          f"run from the CU above**, so read it as \"the layout as of that dispatch\", and dispatch "
          f"`dbt` again if the tables have been rewritten since. The CU column is this "
          f"report's own total per engine. Nothing here re-read a Delta log; the full eight-table "
          f"dashboard is that run's own summary.</sub>")


def render_empty(span, seen, dropped, active, near):
    """No row survived the filters — say WHICH filter ate them.

    This is the one outcome the report cannot explain from its own numbers: the query succeeded, the
    floor bound, rows came back, and every one of them was thrown away locally. On the page that is
    indistinguishable from an idle capacity, which is the opposite conclusion and the one that gets
    drawn. So the drops are counted per reason and the biggest spenders that were dropped are named
    — a bare GUID among them IS the lagging-'Items' trap, and a name in `near` is the workspace
    filter pointing at the wrong workspace.
    """
    print(f"No item activity {span}.\n")
    if not seen:
        print("The metrics model returned **no rows at all** for that window — the capacity really "
              "was idle, or `since` is ahead of the last hour it holds (stderr logs the range).")
        return
    print(f"The metrics model returned **{seen:,} rows**, and every one of them was dropped here:\n")
    print("| filter | rows dropped |")
    print("|:--|--:|")
    print(f"| workspace ≠ `{WS_FILTER}` | {dropped.get('workspace', 0):,}"
          + (f" (of which {dropped['workspace_blank']:,} had a blank workspace id)"
             if dropped.get("workspace_blank") else "") + " |")
    print(f"| name not in `CU_MODELS` | {dropped.get('name', 0):,} |")
    print(f"| kind is not a semantic model | {dropped.get('kind', 0):,} |")
    if near:
        print("\nThese matched a requested NAME but sat in another workspace — the likeliest cause, "
              "and the fix is `workspace`, not `models`:\n")
        print("| item | kind | workspace id | CU |")
        print("|:--|:--|:--|--:|")
        for (name, kind, wsid), cu in sorted(near.items(), key=lambda kv: -kv[1])[:10]:
            print(f"| {name} | {kind or '—'} | `{wsid}` | {cu:,.1f} |")
    if active:
        print("\nThe biggest spenders it did see and dropped. **A bare GUID here is an item the "
              "metrics app has not catalogued yet** — a just-deployed model or a throwaway "
              "notebook — not an unnamed item:\n")
        print("| item | kind | workspace id | CU |")
        print("|:--|:--|:--|--:|")
        for (name, kind, wsid), cu in sorted(active.items(), key=lambda kv: -kv[1])[:10]:
            print(f"| {name} | {kind or '—'} | `{wsid}` | {cu:,.1f} |")


def render(cells, hourly, meta, since, asof, seen=0, dropped=None, active=None, near=None):
    """cells is {(item, operation): cu}; hourly is {(item, operation, hour, item_id): cu};
    meta is {item: {"label", "cls", "kind", "gen"}}."""
    span = (f"since {since:%Y-%m-%d %H:%M} (model clock)" if since else "over everything retained")
    scope = "Capacity CU" if ETL else "Semantic model CU"
    print(f"## {scope} — {span}, as of {asof:%Y-%m-%d %H:%MZ}\n")
    if not cells:
        render_empty(span, seen, dropped or {"workspace": 0, "workspace_blank": 0, "name": 0,
                                            "kind": 0}, active or {}, near or {})
        return

    print(f"Everything since the floor, summed:\n")
    _engine_table(cells, meta)
    if ITEM_DETAIL:
        print("\n#### The same CU by item\n")
        _op_table(cells, meta)
    runs = (sessionize((iid, k, h, (meta.get(k) or {}).get("gen", False))
                       for (k, _o, h, iid) in hourly)
            if (RUN_GAP_HOURS > 0 and hourly) else [])
    if runs:
        render_runs(hourly, runs, meta, cells)
    doc = load_layout()
    if doc:
        # The layout table's CU column is what QUERYING that engine cost, not its build — it sits
        # beside file counts and row groups to explain a scan, and the ETL cost explains nothing
        # about one.
        analytics = {}
        for (k, _op), cu in cells.items():
            info = meta.get(k) or {}
            if info.get("cls") == "analytics" and info.get("engine"):
                analytics[info["engine"]] = analytics.get(info["engine"], 0.0) + cu
        render_layout(doc, analytics)


def main():
    if not TOKEN:
        die("PBI_TOKEN is empty — the workflow mints it from the OIDC login. "
            "See cu/README.md.")
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

    # Before ANY query: the model has to know about items this dispatch just created.
    if REFRESH:
        log("refreshing the Capacity Metrics semantic model ...")
        refresh_metrics_model()
    else:
        log("CU_REFRESH is off — reading the metrics model as it stands")

    cols = discover_columns()
    caps = [CAPACITY] if CAPACITY else discover_capacities()
    since_local = since
    log(f"capacities={caps} since={SINCE or '(everything retained)'} "
        f"workspace={WS_FILTER or '(all)'} models={MODELS or '(every semantic model)'}")

    # Two requests, before the per-capacity loop: the workspace is the same for every capacity.
    # The items call covers every kind and the datasets call covers semantic models; the second is
    # not redundant, it is what still works when there is no Fabric-audience token.
    live_items = fabric_items(WS_FILTER) if ETL else {}
    live_names = datasets_in_workspace(WS_FILTER)
    missing = [m for m in MODELS if m.lower() not in {n.lower() for n in live_names.values()}]
    if MODELS and live_names and missing:
        log(f"  note: {', '.join(missing)} do not exist as datasets in {WS_FILTER} right now — "
            f"expect 0.0 rows for them")

    wanted = {m.lower() for m in MODELS}
    cells, hourly, unknown, seen_hours = {}, {}, 0, []
    meta = {}       # display key -> {"label", "cls", "kind", "gen"}
    kinds = {}      # item kind -> CU, logged so an unrecognised one can be classified next time
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
            # is what made a redeploy read as an idle capacity. The Fabric items call is checked
            # first because it is the only one of the three that names a notebook or a lakehouse.
            name, kind = items.get(iid, ("", ""))
            if live_names.get(iid):
                name, kind = live_names[iid], (kind or "SemanticModel")
            if live_items.get(iid):
                name, kind = live_items[iid]
            cls = classify(kind)
            label = display_label(name) if name else iid
            key = label.lower()
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
            # With ETL on, the workspace test above is the ONLY filter: everything in that workspace
            # is this repo's, and the name list is demoted to an ordering. Off, the two old filters
            # stand unchanged, so a dispatch with CU_ETL=0 reproduces an older report exactly.
            if not ETL:
                if wanted:
                    if key not in wanted:
                        dropped["name"] += 1
                        k = (name or iid, kind, wsid or "(blank)")
                        active[k] = active.get(k, 0.0) + float(cu)
                        continue
                elif kind and kind.strip().lower() not in MODEL_KINDS:
                    dropped["kind"] += 1
                    continue
            kinds[kind or "(unknown)"] = kinds.get(kind or "(unknown)", 0.0) + float(cu)
            meta.setdefault(key, {"label": label or iid, "cls": cls, "kind": kind,
                                  "engine": engine_of(label),
                                  # sessionize's exact generation rule needs an item that is created
                                  # fresh for every run. Two things qualify: a semantic model (the
                                  # benchmark deletes and recreates them) and a COLLAPSED name (the
                                  # collapse exists because those items are throwaway — a new GUID
                                  # under a stable prefix every dbt build). Everything else lives
                                  # across runs and is allocated by hour instead.
                                  "gen": ((kind or "").strip().lower() in MODEL_KINDS
                                          or (bool(name) and label != name))})
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
            # The item GUID is kept at this grain and nowhere else: the aggregate is per model name,
            # but the per-run split needs to tell one deployment of a model from the next, and the
            # GUID is the only thing in the row that says which generation it was.
            hkey = (key, op, hour, iid)
            hourly[hkey] = hourly.get(hkey, 0.0) + float(cu)

    if unknown:
        log(f"  {unknown} item ids resolved to no name anywhere — shown as raw GUIDs")
    # What kinds were seen, and how they were classified. This log line is the route by which an
    # unrecognised kind gets into CLASS_BY_KIND — most usefully, whichever kind Fabric bills dbt's
    # Livy sessions against, which has never been read off a real dispatch.
    for kind, cu in sorted(kinds.items(), key=lambda kv: -kv[1]):
        log(f"  kind {kind}: {cu:,.1f} CU -> {classify(kind)}")
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

    runs = (sessionize((iid, k, h, (meta.get(k) or {}).get("gen", False))
                       for (k, _o, h, iid) in hourly) if RUN_GAP_HOURS > 0 else [])
    for i, r in enumerate(runs, start=1):
        hrs = r["hours"]
        # The items are named, not counted: a run holding three of the four dispatched engines is a
        # deploy that failed, and in the table it is a 0.0 cell indistinguishable from an engine that
        # was never asked for.
        log(f"  run {i}: {hrs[0]:%Y-%m-%d %H:%M} .. {hrs[-1]:%H:%M} ({len(hrs)} active hours, "
            f"{len(r['items'])} item GUIDs: {', '.join(sorted(r['labels']))})")

    if not cells and seen:
        log(f"  dropped: {dropped['workspace']} on workspace "
            f"({dropped['workspace_blank']} blank), {dropped['name']} on name, "
            f"{dropped['kind']} on kind")

    render({k: round(v, 1) for k, v in cells.items()},
           {k: round(v, 1) for k, v in hourly.items()}, meta, since, asof,
           seen=seen, dropped=dropped, active=active, near=near)


if __name__ == "__main__":
    main()
