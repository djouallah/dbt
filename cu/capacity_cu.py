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
    # MEASURED on run 30676341725, and it is the spelling the metrics app actually uses for a Fabric
    # notebook: 92,542 CU arrived as `SynapseNotebook` and only 10,615 as `Notebook`, for items of
    # the same family. Both spellings are live, so both are mapped.
    "synapsenotebook": "etl", "jupyternotebook": "etl",
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
# Every column is printed even at 0.0, for the same reason every named model is: a column that
# disappears is indistinguishable from one that spent nothing.
#
# `landing` leads and is NOT an engine — it is the stage every engine reads from: `dbt_landing` holds
# the downloaded AEMO archive and `download_aemo.py` writes it. It gets a column because "the
# download cost X" is a real answer, where folding it into `shared` was a shrug.
#
# It USED to hold the legs' reads too, as one undivided `OneLake Read` row nothing could attribute,
# and this file said flatly that it could not be split. It can, and now is: each leg reads the same
# bytes through a `Files/landing` shortcut in its own lakehouse (provision.py), and OneLake accounts
# a transaction against the REQUESTED PATH — so the read is booked to the item hosting the shortcut,
# which for three legs IS their output lakehouse and for dwh is `dbt_dwh_src` (a warehouse has no
# `Files`). What stays in this column is the download's write and the round-trip.
ENGINES = [e.strip() for e in os.environ.get(
    "CU_ENGINES", "landing,duckrun,iceberg,spark,dwh").split(",") if e.strip()]

# How an item NAME says which engine it belongs to. Matched as a substring of the lower-cased
# display name, engines tried in CU_ENGINES order.
#
# `delta` is the alias that matters: duckrun's output lakehouse is `dbt_delta`, not `dbt_duckrun`.
# Everything else matches its own name — `aemo_iceberg`, `dbt_spark`, `dbt-duckrun-<random>`, and
# `dbt_landing` for the `landing` column.
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
# POST attempts for the refresh, and it exists because of a 429. Only a 429 is retried — every
# other non-2xx is answered on the first response, since retrying a 403 only buries the reason.
# Two spare attempts at the advertised ~120s cover the per-user cap without pushing the job's own
# timeout (45 min) anywhere near the edge.
REFRESH_TRIES = int(os.environ.get("CU_REFRESH_TRIES", "3"))

# A FLOOR, not a rolling window, and the difference matters. A window ("last 3h") moves with every
# dispatch and can slice one benchmark in half, making an engine look cheap for no reason but where
# the boundary fell. A pinned floor stays put: everything after it accumulates, and two dispatches a
# day apart are comparable.
#
# What it is for: the app retains ~14 days, and what is being MEASURED has changed inside that
# window more than once — the run where dwh was DirectQuery rather than Direct Lake, then the
# per-query dehydrate being dropped for a user-session walk with think time (8c037c8 / debef3a).
# None of those are the same experiment and their CU must not be summed.
#
# The floor now sits at the `dbt` run of 2026-08-01 (30676635835, 00:53:23Z → hour 10:00 in the
# model's clock), and that one is a harder boundary than a methodology change: it ran with
# `reset_outputs`, so all four output items were DELETED and recreated. Every row before it belongs
# to items that no longer exist — same display NAMES, different GUIDs — so summing across the floor
# adds two generations of `dbt_delta` into one number that describes neither. It is also the first
# build whose notebooks are named per engine, i.e. the first whose ETL is attributable at all.
#
# Bump it again the next time the outputs are reset or the suite changes what it measures; blank
# means everything retained, and a wider floor is a dispatch input away for a one-off comparison.
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
SINCE = os.environ.get("CU_SINCE", "2026-08-01T10:00:00").strip()

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

# Where to write this generation's permanent record, or blank for none. Set only by the super
# workflow: a standalone `cu.yml` dispatch measures whatever window it was given, which is not a
# generation and must not be filed as one. See write_history().
HISTORY_JSON = os.environ.get("CU_HISTORY_JSON", "").strip()

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

    A 429 is RETRIED, unlike every other non-2xx, and measured rather than assumed: on run
    30685959678 this call drew `429 ... Retry in 120 seconds`, the refresh was skipped, and the two
    throwaway `dbt-<engine>-*` notebooks the build had just created and deleted resolved to no name
    anywhere — so 41,887 CU of DuckDB-leg compute landed in `shared`/`other` instead of in the
    duckrun and iceberg columns. The report degraded exactly as designed and was still wrong,
    because the one thing that makes a minutes-old item visible had not run. `execute_dax` already
    honours Retry-After for the same cap; this path did not, and gave up on the first response.

    The retry is worth having and is NOT a cure. Power BI throttles the REST API **per identity**:
    on that run and the next, half an hour apart, every attempt by the service principal was
    refused, while a human refreshing by hand in between went straight through. So a 429 usually
    means "this identity is out", which no amount of waiting inside one job fixes; skip the refresh
    (`refresh: false`) for a re-read of a settled window instead.

    Where this file's own requests go, counted rather than guessed, because an earlier version of
    this note said "~60 executeQueries per run" and that was the DELETED timepoint design: the DAX
    is **6 queries** — `INFO.VIEW.COLUMNS`, `VALUES('Capacities')`, then `items_for` + `cu_for` per
    capacity, twice for this tenant's two. The expensive caller is the loop below, which polls
    `GET …/refreshes?$top=1` every 20s for up to REFRESH_TIMEOUT — **up to 45 requests** — so the
    refresh path spends roughly seven times what the measurement does, and then gets refused. If
    throttling has to be attacked, attack that poll interval, not the query count.
    """
    base = f"{PBI}/groups/{WS}/datasets/{MODEL}/refreshes"
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    for attempt in range(1, REFRESH_TRIES + 1):
        r = requests.post(base, json={"notifyOption": "NoNotification"}, headers=headers,
                          timeout=60)
        if r.status_code != 429 or attempt == REFRESH_TRIES:
            break
        # The body carries the delay when the header does not — "Retry in 120 seconds".
        m = re.search(r"retry in (\d+)", r.text.lower())
        wait = int(r.headers.get("Retry-After") or (m.group(1) if m else 120))
        log(f"  429 from refresh (the per-user request cap); retrying in {wait}s "
            f"({attempt}/{REFRESH_TRIES - 1})")
        # Capped against a pathological Retry-After, NOT against REFRESH_TIMEOUT — that one bounds
        # how long we wait for a refresh to finish, which is a different question.
        time.sleep(min(wait, 300))
    if r.status_code in (200, 202):
        log("  refresh accepted, waiting for it to finish ...")
    elif r.status_code in (400, 409) and "already" in r.text.lower():
        # A scheduled refresh beat us to it. Waiting on it is exactly as good.
        log("  a refresh is already running; waiting for that one instead")
    else:
        log(f"  refresh NOT started ({r.status_code}: {r.text[:200].replace(chr(10), ' ')}) — "
            f"reading the model as it stands. Newly created items may be missing, and a throwaway "
            f"notebook that is missing takes a whole leg's compute into `shared`.")
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

    The same trap `datasets_in_workspace()` exists for, widened to every item kind: `'Items'` in the
    metrics model is a lagging snapshot, and an item it has not catalogued resolves to no name, which
    means no class and no engine — a bare GUID in `other`/`shared`, CU kept but nothing said about it.
    The datasets endpoint cannot cover this, because it lists semantic models only. The Fabric items
    API lists every kind, but on a DIFFERENT audience (`api.fabric.microsoft.com`), so it needs its
    own token — `cu.yml` mints one beside `PBI_TOKEN` from the same OIDC login.

    **It cannot name the throwaway notebooks, and do not expect it to.** `run_python` DELETES its
    notebook on the way out, so by the time this runs the item is gone from the workspace and no live
    listing can hold it — `'Items'` is the only route to those names, and empirically it does carry
    them (the earlier attempt at this width showed a row per `duckrun-py-*` notebook, which is where
    those names came from). What this call actually fixes is the long-lived items: a lakehouse,
    warehouse or SQL endpoint provisioned during the run being measured, and semantic models, which
    it also covers.

    Optional by construction: no token, a 401/403, or an unreachable host all fall back to `'Items'`
    with a line saying so, because a report that names most items is worth far more than no report.
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
    # Only reachable if CU_ENGINES is set without `landing`; by default it has its own column.
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


def _chart(title, subtitle, rows):
    """Emit a chart spec for the HTML renderer, as an HTML COMMENT.

    One data path, two outputs. The same markdown goes to the GitHub job summary and to the page,
    and GitHub sanitises inline SVG — so the chart cannot be drawn here. A comment is invisible in
    the summary (no stray code block, no junk) and `report_html.py` picks it up and draws the bars.
    The numbers below it in the tables are the same numbers, so the summary loses nothing but the
    picture.

    Sorted CHEAPEST FIRST, because "lower is better" makes the ranking the finding — the chart
    answers "who cost least" before the reader has compared any two bar lengths. The cost of that is
    real and worth knowing: an engine sits at a different height in the two charts, so the pair is
    read one at a time rather than scanned across. The tables keep the fixed column order, and they
    are the lookup.

    A ZERO sorts to the BOTTOM, not the top. Zero here means "this engine did no such work" — a
    benchmark that skipped it, say — and at the top, under a "lower is better" caption, that reads
    as the winner. It is the one value whose rank would lie.
    """
    rows = sorted(rows, key=lambda r: (r[1] == 0, r[1]))
    if not any(v for _l, v in rows):
        return
    print(f"\n<!--chart:{json.dumps({'title': title, 'subtitle': subtitle, 'rows': rows})}-->")


def _engine_table(cells, meta):
    """Engines across, operations down, grouped by class — the shape the whole repo reads in.

    The class row carries its own subtotal, so "what did the build cost against the querying" is two
    bold rows rather than a sum the reader has to do. Operations are ordered by total CU within
    their class, so the expensive one is the first thing under each heading.

    **No total column and no grand-total row**, deliberately. Both summed ACROSS engines, and that is
    the one sum on this page that means nothing: the engines are four alternatives to each other, so
    adding duckrun to dwh answers no question anyone has. The class subtotals stay because they sum
    DOWN a column, which is "what this engine spent building" — a real number.
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
        print(f"| {'**' + label + '**' if bold else label} | " + " | ".join(f(v) for v in vals) + " |")

    # The corner cell names the measure. Every number in the table is one, and a matrix whose values
    # carry no unit gets quoted as "26,128" with no idea what of.
    print("| CU (s) | " + " | ".join(cols) + " |")
    print("|:--|" + "---:|" * len(cols))
    landing_cu = 0.0
    for cls in CLASS_ORDER:
        ops = sorted((op for (c, op) in op_total if c == cls),
                     key=lambda op: -op_total[(cls, op)])
        if not ops:
            continue
        sub = [cls_total.get((cls, c), 0.0) for c in cols]
        if "landing" in cols:
            landing_cu += sub[cols.index("landing")]
        row(cls, sub, bold=True)
        for op in ops:
            row(op, [per.get((cls, op, c), 0.0) for c in cols])
    if landing_cu:
        print("\n<sub>`landing` is a STAGE, not an engine: one lakehouse holding the downloaded "
              "AEMO archive, written by `download_aemo.py`. What is left here is that WRITE plus "
              "the result/log round-trip `fabric_run.py` does — a shared input cost, so do not add "
              "it to an engine's column. The legs' READS of the same bytes are no longer here: "
              "each reads through a `Files/landing` shortcut in its own lakehouse, and OneLake "
              "books a transaction against the requested path, so those land in the engine "
              "columns.</sub>")
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
    # No total column and no grand-total row, for the same reason as up there: a run's columns are
    # separate passes and a row's cells are separate engines, so neither direction sums to anything
    # anyone asked for. `grand` is still computed — the conservation check below needs it — it is
    # just not printed.
    print("| CU (s) | " + " | ".join(h for h, _ in cols) + " |")
    print("|:--|" + "---:|" * len(cols))
    col_tot = [0.0] * len(cols)
    for cls in CLASS_ORDER:
        vals = [cls_tot.get((ci, cls), 0.0) for ci in range(len(cols))]
        if not any(vals):
            continue
        col_tot = [a + b for a, b in zip(col_tot, vals)]
        print(f"| **{cls}** | " + " | ".join(f"**{v:,.1f}**" for v in vals) + " |")
        for e in engine_cols:
            ev = [per.get((ci, cls, e), 0.0) for ci in range(len(cols))]
            if any(ev):
                print(f"| {e} | " + " | ".join(f"{v:,.1f}" for v in ev) + " |")
    grand = sum(col_tot)
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


def render_hardware(doc):
    """What the build ran ON, from `stats.py`'s `config` block — never from this file's assumptions.

    A CU number is not comparable without it: 26,000 CU at 64 vCores and at 8 are different findings,
    and the same is true of a Spark leg with a different resource profile. Anything the run did not
    record prints as "not recorded", because a default filled in here would read exactly like a
    measurement.

    **dwh has no row at all**, and that is a change from a version that gave it one reading
    "workspace default — Fabric Warehouse exposes no per-run compute knob". Fabric Warehouse
    exposes no knob, so that row said the same thing on every run ever printed and could never
    differ between two reports — a constant occupying a quarter of a table whose only job is to
    say what varied. The table is what the dispatch CHOSE; dwh chooses nothing.
    """
    cfg = (doc.get("config") or {})
    run = doc.get("run") or {}
    rows = []
    for e in ENGINES:
        if e in ("landing", "dwh"):
            continue
        c = cfg.get(e) or {}
        bits = []
        if e in ("duckrun", "iceberg"):
            v = c.get("vcores")
            bits.append(f"{v} vCores (Fabric Python notebook)" if v else None)
        if e == "spark":
            p, n = c.get("resource_profile"), c.get("native_execution_engine")
            bits.append(f"resource profile `{p}`" if p else None)
            bits.append("native execution engine ON" if str(n).lower() == "true"
                        else ("native execution engine off" if n else None))
        bits = [b for b in bits if b]
        rows.append((e, ", ".join(bits) if bits
                     else f"not recorded by dbt run {run.get('id') or '?'}"))
    if not rows:
        return
    print("\n### The hardware it ran on\n")
    print("| engine | compute |")
    print("|:--|:--|")
    for e, what in rows:
        print(f"| {e} | {what} |")
    print(f"\n<sub>Read from the environment the build legs were actually given and recorded by "
          f"`stats.py` in dbt run `{run.get('id') or '?'}` — not from configuration this report "
          f"assumes. A value the run did not record says so rather than showing a default, because "
          f"a filled-in default reads exactly like a measurement. The duckdb-family legs run in one "
          f"Fabric Python notebook each, so their vCores are that notebook's size; spark runs on the "
          f"workspace Livy pool, which this cannot see beyond the profile it asked for.</sub>")


def write_history(path, cells, meta, since, asof, doc):
    """One JSON per generation, committed INTO the repo. The only storage here that outlives
    retention: artifacts expire (90 days, the Pages one sooner) and the Capacity Metrics model keeps
    ~14 days, so without this every number on the page is gone within a fortnight of being measured.

    Deliberately NOT the markdown or the HTML — those are renderings, and a renderer changes. This is
    the numbers, keyed so a later reader can group them the same way the page did.

    `schema` is here from the first file written. A reader two years from now must be able to tell an
    old record from a new one by reading it, rather than by guessing from which keys are present.

    Deliberately excludes benchmark timings: `cu/` does not read `run_report.json` and this is not
    the place to start. If timings are ever wanted historically, `benchmark/` should write its own
    record beside this one.
    """
    per = {}
    for (k, op), cu in cells.items():
        info = meta.get(k) or {}
        cls, eng = info.get("cls", "other"), info.get("engine") or "shared"
        per.setdefault(cls, {}).setdefault(eng, {})[op] = round(cu, 1)
    rec = {
        "schema": 1,
        # Stated in the record, not just on the page. A file read years from now must not need this
        # repo's README to know what its numbers are.
        "unit": "CU (s) — Fabric capacity-unit seconds",
        "written": asof.replace(microsecond=0).isoformat(),
        "since": since.isoformat() if since else None,
        "runs": {"measure": os.environ.get("GITHUB_RUN_ID"),
                 "measure_sha": os.environ.get("GITHUB_SHA"),
                 # Which dbt build these tables came from — the layout and config are that run's,
                 # not this one's, and a record that conflates them is worse than one that omits.
                 "build": (doc.get("run") or {}).get("id"),
                 "build_sha": (doc.get("run") or {}).get("sha")},
        "config": doc.get("config") or {},
        "cu": per,
        "layout": doc.get("stats") or {},
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rec, f, indent=1, sort_keys=True)
    log(f"  wrote {path}: {sum(len(v) for c in per.values() for v in c.values())} (engine, op) "
        f"entries")
    return rec


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
    scope = "Capacity units" if ETL else "Capacity units — semantic models only"
    print(f"## {scope} — {span}, as of {asof:%Y-%m-%d %H:%MZ}\n")
    if not cells:
        render_empty(span, seen, dropped or {"workspace": 0, "workspace_blank": 0, "name": 0,
                                            "kind": 0}, active or {}, near or {})
        return

    # Say the unit once, plainly, BEFORE the first number — charts included. Everything on this page
    # is CU, and a page of unlabelled thousands gets quoted back as "26,128" with no idea of what.
    print("**Every number on this page is capacity units (CU-seconds)** — Fabric's own billing "
          "measure, read from the Capacity Metrics model's `CU (s)` column. Not milliseconds and "
          "not rows: what the work COST, which is what the four engines are being compared on "
          "here.\n")

    # Charts first: two bars per engine — what building cost, what querying cost. `landing` and
    # `shared` are excluded from the bars because neither is an engine, and a bar beside four
    # engines is read as a fifth competitor whatever the caption says. Both keep their table column.
    per_cls = {}
    for (k, _op), cu in cells.items():
        info = meta.get(k) or {}
        if info.get("engine") in (None, "landing"):
            continue
        per_cls[(info.get("cls"), info["engine"])] = per_cls.get(
            (info.get("cls"), info["engine"]), 0.0) + cu
    bars = [e for e in ENGINES if e != "landing"]
    _chart("ETL — what building the tables cost", "capacity units, lower is better",
           [[e, round(per_cls.get(("etl", e), 0.0), 1)] for e in bars])
    _chart("Analytics — what querying them cost", "capacity units, lower is better",
           [[e, round(per_cls.get(("analytics", e), 0.0), 1)] for e in bars])

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
        # Last on the page, deliberately: it is the caveat you check a number against once the
        # number has surprised you, not something to read on the way in.
        render_hardware(doc)
    if HISTORY_JSON:
        write_history(HISTORY_JSON, cells, meta, since, asof, doc)


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
            # A COLLAPSED group can hold items the app catalogued under different kinds — measured:
            # `duckrun-py-*` came back as both `SynapseNotebook` and `Notebook`. First-wins would
            # then make the group's class depend on which row was read first, so a known class
            # always beats `other`. Only that direction, so two known classes never fight.
            if (meta.get(key) or {}).get("cls") == "other" and cls != "other":
                meta[key].update(cls=cls, kind=kind)
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
