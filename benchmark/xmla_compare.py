"""Benchmark ONE engine's semantic model by replaying a USER SESSION against it over the XMLA
endpoint and timing every query. One process, one engine, no comparison of any kind.

Every model exposes identical tables, columns and measures over the SAME 143M-row `mart.fct_summary`
— one copy per engine, at row-count parity. So this is NOT a correctness check: the numbers are
identical by construction. What differs is how each engine physically wrote the table, which changes
how much the Direct Lake engine has to transcode and scan. We measure that as query wall-clock.

**The session is the measurement, and nothing is ever cleared.** `deploy_models.py` deletes and
recreates the semantic model, so it starts with an empty VertiPaq store; this script then walks the
whole suite `BENCH_RUNS` times and the PASS NUMBER is the tier:

    pass 1     -> cold   first visit; pays the whole Delta->memory transcode, once
    pass 2     -> warm   second visit
    pass 3..N  -> hot    settled; median + spread over N-2 samples

Those labels are positions in a session, not engine states. Microsoft uses the same words more
narrowly (warm = data resident, VertiScan caches empty; hot = resident AND caches populated), and by
that definition pass 2 is arguably already hot because pass 1 populated the caches too. A TMSL
`clearCache` between passes would manufacture the strict warm state — it clears query caches without
evicting resident columns, which is exactly that transition. It is DELIBERATELY NOT USED: this
reproduces user behaviour rather than testing the engine, and a user's second visit is simply their
second visit. Do not add it to make the label technically precise.

What this replaced: a per-query dehydrate (`clearValues` + `full` before EVERY cold-tier query). No
user is ever in that state, and `clearValues` clears the data cache — TMSL defines it as no more than
"Clear values in this object and all its dependents" — which is not a statement about transcoding
cost. Nothing here issues a refresh after readiness.

**Nothing touches the model between readiness and pass 1.** That is why the top-DUID resolve happens
AFTER pass 1 (it transcodes DUID and mw, the very columns probe_duid/probe_mw measure) and why the
readiness probe reads a tiny dimension instead of `COUNTROWS(fct_summary)`, which is byte-identical
to probe_rowcount — the control the marginal-column-cost decomposition subtracts.

What is under test: **identical DAX, identical semantic models, four dbt adapters.** Every model is
Direct Lake over its own adapter's copy of the same tables, so every timing is a Delta→memory
transcode and an in-memory scan — shaped by the physical layout that adapter wrote, which is the only
thing that differs. `dwh` included: duckrun 0.4.36's `deploy(mode=)` reads a warehouse's Tables as the
Delta they are, so it is no longer measured as SQL-endpoint pushdown to a different engine. A pushdown
time is not a slow layout, and mixing the two kinds of number in one table invited exactly that
misreading.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless — see .github/workflows/benchmark.yml.

**`BENCH_ENGINES` must name exactly one engine, and this script refuses more.** The workflow runs one
job per engine, because a Fabric/XMLA token lives about an hour and a four-model pass with two 600s
gaps in it does not fit inside one — the expiry would land mid-measurement on whichever engine went
last. So each job mints its own token minutes before using it, writes its own report fragment, and
COMPUTES NOTHING: every number that involves more than one engine is produced by the render layer from
the merged fragments. There is no in-process comparison path here any more (there was one, for running
this from a laptop; the laptop is not a supported way to spend this capacity, and keeping a second
orchestration shape alive to serve it meant two answers to the same question).

Env in:
  BENCH_ENGINES  — exactly ONE engine label. More than one is an error, not a comparison.
  BENCH_TOP_DUID — optional; pins the DUID the hot_only ladder filters on instead of resolving it.
                   Unset is fine: every engine holds the same rows, so each job resolves the same
                   DUID, and the value is recorded per model for the render layer to check.
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — optional; else self-acquired via duckrun (analysis.windows.net/powerbi/api)
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — PASSES over the whole suite (default 6): pass 1 cold, pass 2 warm, the rest hot.
                   At 6 the hot median is over 4 samples and the hot spread is real. Below 3 there
                   is no hot tier at all, which the render layer scopes out per metric rather than
                   guessing at.

Cold and warm are single samples by construction — there is exactly one first visit and one second
visit per deployed model — so neither carries a spread. More cold samples means more dispatches, not
a bigger number here.

Exit 0 always — this is a benchmark, not a pass/fail gate.
"""
import glob
import json
import os
import statistics
import sys
import time

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engines as E  # noqa: E402
import report  # noqa: E402

# The session's query suite. Each entry is (tier, name, dax). Adding a query = adding a tuple.
#
# EVERY query runs in EVERY pass — the tier is descriptive, not a switch, and no query is measured
# differently from any other. (It used to gate a per-query dehydrate; that is gone.)
#   probe      — one column, full scan, scalar result.
#   composite  — realistic multi-column workloads over the mart.
#   raw        — one query per RAW landing table, so every table in the model is measured and none
#                is dead weight. `raw_scada_mw` is the heaviest measurement here.
#   hot_only   — selectivity ladder on the sort-key column. "{duid}" is filled at runtime with the
#                top DUID by MWh, and unless BENCH_TOP_DUID is pinned that resolve happens after
#                pass 1, so these join the session from pass 2 and have no cold number.
#
# ORDER IS LOAD-BEARING, in one specific way: `probe_rowcount` must stay LAST among the probes.
# render_summary's marginal-column-cost table is `probe_<col>` minus `probe_rowcount`, and that
# subtraction only means "the cost of touching one more column" if each probe is the first query to
# touch its column and the rowcount control runs once everything is already resident. Reordering the
# probes silently changes what that table measures. test_verdicts.py pins it.
#
# Every table, column and measure referenced below exists in BOTH templates — benchmark/
# test_templates.py asserts the two semantic surfaces are identical, and that identity is what makes
# one suite portable across every engine's model — they are structurally identical by construction.
QUERIES = [
    # --- Tier 1: per-column probes (rowcount LAST — see the note above) ---
    ("probe", "probe_mw",       'EVALUATE ROW("x", SUM(fct_summary[mw]))'),
    ("probe", "probe_price",    'EVALUATE ROW("x", SUM(fct_summary[price]))'),
    ("probe", "probe_duid",     'EVALUATE ROW("x", DISTINCTCOUNT(fct_summary[DUID]))'),
    ("probe", "probe_date",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[date])))'),
    ("probe", "probe_time",     'EVALUATE ROW("x", COUNTROWS(VALUES(fct_summary[time])))'),
    ("probe", "probe_rowcount", 'EVALUATE ROW("x", COUNTROWS(fct_summary))'),
    # --- Tier 2: composite workloads ---
    ("composite", "region_x_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MWh", [Total MWh], "AvgP", [Avg Price], "Gens", [Generator Count])'),
    ("composite", "fuel_x_region",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[FuelSourceDescriptor], dim_duid[Region], '
     '"MWh", [Total MWh], "MW", [Total MW])'),
    ("composite", "timeofday_x_region",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[time], dim_duid[Region], '
     '"MWh", [Total MWh], "AvgP", [Avg Price])'),
    ("composite", "duid_x_month",
     'EVALUATE SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], dim_calendar[month], '
     '"MWh", [Total MWh])'),
    ("composite", "filtered_nsw_2024_by_duid",
     'EVALUATE CALCULATETABLE('
     'SUMMARIZECOLUMNS(fct_summary[DUID], "MWh", [Total MWh], "AvgP", [Avg Price]), '
     'dim_duid[Region] = "NSW1", dim_calendar[year] = 2024)'),
    ("composite", "scalar_weighted_full_scan",
     'EVALUATE ROW('
     '"RevenueProxy", SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     '"DistinctDUID", DISTINCTCOUNT(fct_summary[DUID]), '
     '"Rows", COUNTROWS(fct_summary))'),
    ("composite", "topn_duid_by_mwh",
     'EVALUATE TOPN(50, SUMMARIZECOLUMNS(fct_summary[DUID], dim_calendar[year], '
     '"MWh", [Total MWh]), [MWh], DESC)'),
    # --- Tier 2 (cont.): column-width at fixed shape (cold scaling with touched columns) ---
    ("composite", "wide_all_measures",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh], "b", [Avg Price], '
     '"c", [Total MW], "d", [Generator Count])'),
    ("composite", "narrow_one_measure",
     'EVALUATE SUMMARIZECOLUMNS(dim_calendar[year], "a", [Total MWh])'),
    # --- Tier 3: the RAW tables, one query per table so nothing in the model goes unmeasured ---
    # These are why the semantic model carries all eight tables and not just the mart three. The
    # first is the single heaviest measurement in the suite: fct_scada is the largest table in the
    # project, so a cold SUM over one of its columns is the biggest Delta->memory transcode any
    # engine here has to do, and it is where a layout difference has the most room to show.
    ("raw", "raw_scada_mw", 'EVALUATE ROW("x", [Scada MW])'),
    ("raw", "raw_scada_x_region_year",
     'EVALUATE SUMMARIZECOLUMNS(dim_duid[Region], dim_calendar[year], '
     '"MW", [Scada MW], "Rows", [Scada Rows])'),
    ("raw", "raw_price_x_region_year",
     'EVALUATE SUMMARIZECOLUMNS(fct_price[REGIONID], dim_calendar[year], '
     '"AvgRRP", [Avg RRP], "Demand", SUM(fct_price[TOTALDEMAND]))'),
    ("raw", "raw_intraday_scada",
     'EVALUATE SUMMARIZECOLUMNS(fct_scada_today[DUID], '
     '"MW", [Scada Today MW], "Rows", [Scada Today Rows])'),
    ("raw", "raw_intraday_price",
     'EVALUATE SUMMARIZECOLUMNS(fct_price_today[REGIONID], '
     '"AvgRRP", AVERAGE(fct_price_today[RRP]), "Rows", [Price Today Rows])'),
    ("raw", "raw_archive_log",
     'EVALUATE SUMMARIZECOLUMNS(stg_csv_archive_log[source_type], '
     '"Files", [Archive Files], "Rows", [Archive Source Rows])'),
    # --- Tier 4: selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
    ("hot_only", "sel_1yr",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024))'),
    ("hot_only", "sel_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
    ("hot_only", "sel_1duid",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = "{duid}"))'),
    ("hot_only", "sel_1duid_1mo",
     'EVALUATE ROW("r", CALCULATE(SUMX(fct_summary, fct_summary[mw] * fct_summary[price]), '
     'fct_summary[DUID] = "{duid}", dim_calendar[year] = 2024, dim_calendar[month] = 6))'),
]

def resolve_queries(top_duid):
    """Fill the "{duid}" placeholder in the hot_only ladder with the actual top DUID. If no top
    DUID could be resolved, drop the DUID-dependent ladder queries rather than run a broken filter."""
    out = []
    for tier, name, dax in QUERIES:
        if "{duid}" in dax:
            if not top_duid:
                continue
            dax = dax.replace("{duid}", top_duid)
        out.append((tier, name, dax))
    return out


def _load_adomd(adomd_dir: str):
    """Make Microsoft.AnalysisServices.AdomdClient importable via pythonnet."""
    import clr  # pythonnet
    hits = glob.glob(os.path.join(adomd_dir, "**", "Microsoft.AnalysisServices.AdomdClient.dll"),
                     recursive=True)
    if not hits:
        sys.exit(f"ADOMD client DLL not found under {adomd_dir!r}")
    hits.sort(key=lambda p: ("netcore" not in p.lower() and "net6" not in p.lower(), len(p)))
    d = os.path.dirname(hits[0])
    if d not in sys.path:
        sys.path.append(d)
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    print(f"Loaded ADOMD from {hits[0]}")


def open_conn(workspace: str, model: str, token: str, tries=5, delay=15):
    """Open an XMLA connection, retrying transient drops. The XMLA endpoint can forcibly close an
    idle connection (SocketException 10054) — especially after the idle gap or under capacity
    throttling — so one blip shouldn't kill the run."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdConnection
    conn_str = (
        f"Data Source=powerbi://api.powerbi.com/v1.0/myorg/{workspace};"
        f"Initial Catalog={model};User ID=;Password={token};"
    )
    last = None
    for i in range(1, tries + 1):
        try:
            conn = AdomdConnection(conn_str)
            conn.Open()
            return conn
        except Exception as e:
            last = e
            print(f"  open_conn {i}/{tries} failed ({str(e).splitlines()[0][:100]}); "
                  f"retrying in {delay}s...", flush=True)
            time.sleep(delay)
    raise last


def _refresh(conn, model, kind):
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    tmsl = json.dumps({"refresh": {"type": kind, "objects": [{"database": model}]}})
    AdomdCommand(tmsl, conn).ExecuteNonQuery()


def warm_up(conn, model, tries=16, delay=30):
    """A freshly-created Direct Lake model can't read its OneLake source until security
    propagates — the first refresh/query fails with 'source tables ... do not exist or access
    was denied'. Reframe (full) + probe a trivial query, looping until it actually reads data
    (or we give up). Returns True once queryable.

    This is the ONLY refresh the run issues, and the only query before pass 1. It matters more now
    that the model is deleted and recreated every run: a brand-new item is exactly the propagation
    case this exists for.

    The probe reads a TINY DIMENSION, not `COUNTROWS(fct_summary)` as it once did. That was
    byte-identical to the `probe_rowcount` query — the zero-column control that render_summary's
    marginal-column-cost table subtracts from every other probe — so the readiness check was
    pre-warming the very control it would later be measured against. dim_calendar is a few thousand
    rows and proves the same thing: the model can reach its OneLake source.

    The refresh is BEST-EFFORT and its failure is explicitly NOT a readiness signal — a model can be
    perfectly queryable while a refresh against it is rejected. Only the probe decides. Retrying the
    pair as one unit spent 16×30s and then skipped the leg entirely."""
    probe = 'EVALUATE ROW("n", COUNTROWS(dim_calendar))'
    for i in range(1, tries + 1):
        try:
            _refresh(conn, model, "full")   # (re)frame Direct Lake against the current Delta
        except Exception as e:
            print(f"  warm-up {i}: refresh unavailable ({str(e).splitlines()[0][:90]}) — "
                  "probing anyway", flush=True)
        try:
            run_query(conn, probe)          # confirm it can actually transcode/read the data
            print(f"  warm-up: queryable after {i} attempt(s)", flush=True)
            return True
        except Exception as e:
            print(f"  warm-up {i}/{tries}: not ready ({str(e).splitlines()[0][:110]})"
                  + (f"; waiting {delay}s..." if i < tries else ""), flush=True)
            if i < tries:
                time.sleep(delay)
    print("  warm-up: model never became queryable — skipping it", flush=True)
    return False


def run_query(conn, dax: str):
    """Execute dax, drain all rows, return (elapsed_ms, row_count)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    t0 = time.perf_counter()
    reader = AdomdCommand(dax, conn).ExecuteReader()
    rows = 0
    try:
        fc = reader.FieldCount
        while reader.Read():
            for i in range(fc):
                reader.GetValue(i)
            rows += 1
    finally:
        reader.Close()
    return (time.perf_counter() - t0) * 1000.0, rows


def run_scalar(conn, dax):
    """Execute dax and return the first cell of the first row (or None)."""
    from Microsoft.AnalysisServices.AdomdClient import AdomdCommand
    reader = AdomdCommand(dax, conn).ExecuteReader()
    try:
        if reader.Read() and reader.FieldCount:
            return reader.GetValue(0)
    finally:
        reader.Close()
    return None


def top_duid(conn):
    """The DUID with the largest Total MWh — used to fill the hot_only selectivity ladder.
    Same underlying data in every engine, so every job resolves the same DUID."""
    v = run_scalar(conn,
                   'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_summary[DUID], "m", [Total MWh]), '
                   '[m], DESC)')
    return None if v is None else str(v)


def _tier_of(pass_no):
    """The tier a pass belongs to. The pass NUMBER is the tier — that is the whole design."""
    return "cold" if pass_no == 1 else ("warm" if pass_no == 2 else "hot")


def _finalize(by_pass, tier, rows):
    """One query's samples, keyed by pass number, reduced to the reported metrics.

    cold and warm are single samples and carry no spread: there is exactly one first visit and one
    second visit per deployed model. Hot is passes 3+, reported as a MEDIAN — a single capacity spike
    (a 2.5s blip among 110ms runs) blows up a mean and fabricates a winner.

    A query can legitimately be missing pass 1: unless BENCH_TOP_DUID is pinned, the ladder joins the
    session at pass 2, so it has warm and hot numbers and no cold one. The render layer scopes each
    metric to the engines and queries that have it, so a missing tier is a gap, not a zero.
    """
    res = {"tier": tier, "rows": rows, "ms_by_pass": {str(p): v for p, v in sorted(by_pass.items())}}
    if 1 in by_pass:
        res["cold_ms"] = by_pass[1]
    if 2 in by_pass:
        res["warm_ms"] = by_pass[2]
    hot = [v for p, v in sorted(by_pass.items()) if p >= 3]
    if hot:
        res["all_hot_ms"] = hot
        res["hot_median_ms"] = statistics.median(hot)
        hlo, hhi, hmed = min(hot), max(hot), statistics.median(hot)
        res["hot_spread_pct"] = 100.0 * (hhi - hlo) / hmed if hmed else 0.0
    return res


def bench_model(workspace, model, token, runs, pinned_duid=None):
    """Replay `runs` passes of the whole suite against one model and return (timings, top_duid).

    The ORDER here is the measurement. Readiness, then pass 1 with nothing in between — no refresh,
    no DMV probe, no DUID resolve — because anything that touches a fact column first would spend
    the cold pass before it starts.
    """
    print(f"\n=== Benchmarking {model} ({runs} passes: 1 cold, 2 warm, 3+ hot) ===")
    conn = open_conn(workspace, model, token)
    if not warm_up(conn, model):
        conn.Close()
        return None, None

    td = pinned_duid
    queries = resolve_queries(td)   # 25 when the DUID is pinned, 21 when it is resolved below
    samples, rows_of, tier_of = {}, {}, {}
    try:
        for p in range(1, runs + 1):
            tier = _tier_of(p)
            print(f"\n  --- pass {p}/{runs} ({tier}) — {len(queries)} queries ---", flush=True)
            for tier_name, name, dax in queries:
                t, rows = run_query(conn, dax)
                samples.setdefault(name, {})[p] = t
                rows_of[name] = rows
                tier_of[name] = tier_name
                print(f"    [{tier_name}] {name}: {t:,.1f}ms (rows={rows})", flush=True)
            if p == 1 and not td:
                # Only now — this transcodes DUID and mw, which probe_duid and probe_mw measure.
                # Free at this point, because pass 1 has already touched both.
                td = top_duid(conn)
                queries = resolve_queries(td)
                print(f"  top DUID resolved after the cold pass: {td} "
                      f"— the ladder joins from pass 2 ({len(queries)} queries)", flush=True)
    finally:
        conn.Close()

    results = {n: _finalize(by_pass, tier_of[n], rows_of[n]) for n, by_pass in samples.items()}
    return results, td


def _write_timings(model, res):
    # res is already keyed by query with the final report keys (tier, rows, ms_by_pass, cold_ms,
    # warm_ms, all_hot_ms, hot_median_ms, hot_spread_pct) — merge as-is.
    report.merge({"timings": {model: res}})


def main():
    # Engine selection FIRST, before a token is minted or the workspace is read: a misconfigured
    # dispatch should fail in a second, not after an auth round trip.
    picked = E.selected()
    if len(picked) != 1:
        sys.exit(f"BENCH_ENGINES must name exactly ONE engine, got {picked}. This script measures "
                 "one model per process by design — the workflow runs one job per engine so that "
                 "each mints its own token, and every comparison is made by the render layer from "
                 "the merged report.")
    engine = picked[0]
    model = E.model_name(engine)

    workspace = os.environ["PBI_WORKSPACE"].strip()
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()  # self-acquire the XMLA token
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    runs = int(os.environ.get("BENCH_RUNS", "6"))
    # Pinning the DUID skips the resolve entirely, so the ladder runs from pass 1 like everything
    # else. Unpinned, it is resolved after the cold pass and the ladder joins at pass 2 — see
    # bench_model. Either way the value is recorded per model, so the render layer can warn if two
    # engines disagreed instead of assuming they did not.
    pinned_duid = (os.environ.get("BENCH_TOP_DUID") or "").strip() or None

    _load_adomd(adomd_dir)
    print(f"Workspace : {workspace}")
    print(f"Engine    : {engine} -> {model} (written by {E.WRITER.get(engine, '?')})")
    print(f"Passes    : {runs} (1 cold, 2 warm, 3+ hot)   "
          f"Top DUID: {pinned_duid or '(resolved after the cold pass)'}")

    res, td = bench_model(workspace, model, token, runs, pinned_duid)
    if res is None:
        sys.exit(f"[{engine}] {model!r} never became queryable — nothing measured.")
    _write_timings(model, res)
    report.merge({"top_duid": {model: td}})
    print(f"\n[{engine}] measured {len(res)} queries over {runs} passes "
          f"-> {os.environ.get('RUN_REPORT', 'run_report.json')}")


if __name__ == "__main__":
    main()
