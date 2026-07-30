"""Benchmark one semantic model per engine by running the SAME heavy DAX queries against each
over the XMLA endpoint and timing them.

Every model exposes identical tables, columns and measures over the SAME 143M-row `mart.fct_summary`
— one copy per engine, at row-count parity. So this is NOT a correctness check: the numbers are
identical by construction. What differs is how each engine physically wrote the table, which changes
how much the Direct Lake engine has to transcode (cold) and scan (hot). We measure both as query
wall-clock.

COLD is forced per query by DEHYDRATING the model first: a TMSL `clearValues` refresh evicts all
transcoded column data from memory, then a `full` refresh reframes (on Direct Lake that's metadata
only — no transcode), so the next query pays the full cold Delta→memory cost. We dehydrate before
EACH query because the queries share the big fact columns (mw/price/DUID/date/time) — without a
per-query dehydrate only the first query would be cold.

The `dwh` engine is served as DirectQuery, not Direct Lake (see benchmark/engines.py). It therefore
has no transcoded data to evict, its dehydrate is expected to fail, and `bench_model` degrades it to
hot-only automatically. Its timings measure SQL endpoint pushdown, not a Delta layout — they are
real, but they are not the same kind of number as the other three.

Uses the XMLA endpoint (ADOMD.NET), NOT the throttled /executeQueries REST endpoint.
Run headless — see .github/workflows/benchmark.yml.

Env in:
  BENCH_ENGINES  — comma-separated engines; the FIRST is the reference every ratio is taken against
  PBI_WORKSPACE  — workspace *display name* (XMLA data source uses the name, not the id)
  PBI_TOKEN      — optional; else self-acquired via duckrun (analysis.windows.net/powerbi/api)
  ADOMD_DIR      — folder containing Microsoft.AnalysisServices.AdomdClient.dll
  BENCH_RUNS     — HOT repetitions per query per model (default 3); run1/run2 dropped as warm.
                   At the default that leaves ONE measured hot sample per query, so the hot
                   "median" is that sample and the hot spread is 0. Raise it to get a real one.
  COLD_REPEATS   — cold dehydrate→query cycles per cold-tier query (default 1); we report the
                   median + spread over these. The defaults are set for capacity cost, not
                   statistical strength: at 1 the cold median IS the single sample and the
                   spread is 0, which also means render_summary's >25%-spread noise filter
                   flags nothing. Raise both when a result actually has to be defended.
  BENCH_COLD     — "true"/"false": measure cold via dehydrate (default true). Falls back to
                   hot-only automatically if the token can't run the refresh (needs write).

Cold is a black box probed only by wall-clock: dehydrate (clearValues+full) forces a full
Delta→memory transcode on the next query, so COLD_REPEATS cycles give a small distribution
instead of an n=1 point — at the default of 1 it IS an n=1 point. Queries are tiered — see
QUERIES below.

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

# Tiered DAX suite. Each entry is (tier, name, dax). Adding a query = adding a tuple.
#   probe      — one column, full scan, scalar result. Cold time ≈ that column's transcode cost +
#                fixed overhead; probe_rowcount is the ~zero-column control (subtract it in P3 to
#                get the marginal per-column cost). Measured cold (COLD_REPEATS×) AND hot.
#   composite  — realistic multi-column workloads over the mart, also measured cold AND hot.
#   raw        — one query per RAW landing table, so every table in the model is measured and none
#                is dead weight. Cold and hot. `raw_scada_mw` is the heaviest measurement here.
#   hot_only   — selectivity ladder on the sort-key column, measured HOT only (segment/row-group
#                elimination is only visible once resident — cold is dominated by full-column
#                transcode). "{duid}" is filled at runtime with the top DUID by MWh.
# Every table, column and measure referenced below exists in BOTH templates — benchmark/
# test_templates.py asserts the two semantic surfaces are identical, and that identity is what makes
# one suite portable across the Direct Lake and DirectQuery models.
QUERIES = [
    # --- Tier 1: per-column cold probes ---
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
    # --- Tier 4: hot-only selectivity ladder (SUMX lifts work above the XMLA noise floor) ---
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

COLD_TIERS = ("probe", "composite", "raw")   # tiers that get the dehydrate→query cold path


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


def dehydrate_model(conn, model):
    """Evict all column data (clearValues) then reframe (full = metadata only on Direct Lake),
    leaving the model cold — the next query pays the full Delta->memory transcode cost.

    Expected to FAIL on a DirectQuery model: there is no transcoded data to evict. bench_model
    catches that and degrades the model to hot-only, which is the correct reading."""
    for kind in ("clearValues", "full"):
        _refresh(conn, model, kind)


def warm_up(conn, model, tries=16, delay=30):
    """A freshly-deployed Direct Lake model can't read its OneLake source until security
    propagates — the first refresh/query fails with 'source tables ... do not exist or access
    was denied'. Reframe (full) + probe a trivial query, looping until it actually reads data
    (or we give up). Returns True once queryable.

    The refresh is BEST-EFFORT and its failure is explicitly NOT a readiness signal: a DirectQuery
    model has nothing to reframe, so its refresh is rejected while the model is perfectly
    queryable. Only the probe decides. Retrying the pair as one unit spent 16×30s and then skipped
    the leg entirely."""
    probe = 'EVALUATE ROW("n", COUNTROWS(fct_summary))'
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
    Same underlying data across engines, so resolve once on the reference model and reuse."""
    v = run_scalar(conn,
                   'EVALUATE TOPN(1, SUMMARIZECOLUMNS(fct_summary[DUID], "m", [Total MWh]), '
                   '[m], DESC)')
    return None if v is None else str(v)


def tie(b, m, b_spread_pct, m_spread_pct):
    """A per-query winner is a TIE when the relative gap between the two times is smaller than
    the larger of their cold spreads — i.e. the difference is inside the measurement noise.
    Returns "base", "model", or "tie". b_spread_pct/m_spread_pct may be None (hot: no spread)."""
    if not b or not m:
        return "tie" if b == m else ("model" if (m or 0) < (b or 0) else "base")
    rel = abs(b - m) / max(b, m)
    noise = max((b_spread_pct or 0.0), (m_spread_pct or 0.0)) / 100.0
    if rel < noise:
        return "tie"
    return "model" if m < b else ("base" if m > b else "tie")


def bench_model(workspace, model, token, runs, want_cold, cold_repeats, queries):
    print(f"\n=== Benchmarking {model} (runs={runs}, cold={want_cold}) ===")
    conn = open_conn(workspace, model, token)
    if not warm_up(conn, model):
        conn.Close()
        return None, False
    can_cold = want_cold
    if want_cold:
        try:
            dehydrate_model(conn, model)
            print("  dehydrate: OK (per-query cold timing enabled)")
        except Exception as e:
            can_cold = False
            print(f"  dehydrate: unavailable ({str(e).splitlines()[0][:120]}) — hot timing only")
    results = {}
    try:
        for tier, name, dax in queries:
            do_cold = can_cold and tier in COLD_TIERS
            rowcount = None
            res = {"tier": tier}
            # COLD: dehydrate → query, COLD_REPEATS times, so cold is a small distribution (median +
            # spread) rather than an n=1 point. Each cycle pays the full Delta→memory transcode.
            cold = []
            if do_cold:
                for _ in range(cold_repeats):
                    dehydrate_model(conn, model)
                    t, rows = run_query(conn, dax)
                    cold.append(t)
                    rowcount = rows
                res["cold_ms_all"] = cold
                res["cold_median_ms"] = statistics.median(cold)
                res["cold_min_ms"] = min(cold)
                lo, hi, med = min(cold), max(cold), statistics.median(cold)
                res["cold_spread_pct"] = 100.0 * (hi - lo) / med if med else 0.0
            # HOT: run WITHOUT dehydrating; drop run1/run2 as the warm transition. Report the
            # MEDIAN of the rest — a single capacity spike (a 2.5s blip among 110ms runs) blows up
            # the mean and fabricates a verdict, so the mean is kept only for continuity.
            hot_times = []
            for _ in range(runs):
                t, rows = run_query(conn, dax)
                hot_times.append(t)
                rowcount = rows
            hot = hot_times[2:] or hot_times[1:] or hot_times[:1]
            res["all_hot_ms"] = hot_times
            res["hot_avg_ms"] = sum(hot) / len(hot)      # continuity only — NOT used for verdicts
            res["hot_median_ms"] = statistics.median(hot)
            hlo, hhi, hmed = min(hot), max(hot), statistics.median(hot)
            res["hot_spread_pct"] = 100.0 * (hhi - hlo) / hmed if hmed else 0.0
            if tier == "hot_only":
                res["first_touch_ms"] = hot_times[0]     # first run = the data-skipping measurement
            res["rows"] = rowcount
            results[name] = res
            # Console trace.
            print(f"  [{tier}] {name}  (rows={rowcount})")
            if do_cold:
                cold_str = ", ".join(f"{c:.1f}" for c in cold)
                print(f"      cold x{cold_repeats}: [{cold_str}]  median={res['cold_median_ms']:.1f}ms"
                      f"  spread={res['cold_spread_pct']:.1f}%")
            hot_str = ", ".join(f"{h:.1f}" for h in hot_times)
            print(f"      hot   x{runs}: [{hot_str}]  median={res['hot_median_ms']:.1f}ms"
                  f"  spread={res['hot_spread_pct']:.1f}%")
    finally:
        conn.Close()
    return results, can_cold


def discover_models():
    """(reference, [challengers]) as deployed semantic-model names.

    Taken from BENCH_ENGINES, where order is significant: the FIRST engine is the reference every
    ratio is measured against, so `base/<engine>` reads the same way in every table and across runs.
    Upstream picked the base by name (`endswith('_auto_sort')`, else shortest) — with one model per
    engine there is no name to key off, and an implicit choice here silently reorients every ratio."""
    picked = E.selected()
    if len(picked) < 2:
        sys.exit(f"need at least 2 engines to compare, got {picked} — "
                 "set BENCH_ENGINES to e.g. 'duckrun,spark'")
    ref = E.reference(picked)
    return E.model_name(ref), [E.model_name(e) for e in picked if e != ref]


def _write_timings(model, res):
    # res is already keyed by query with the final report keys (tier, rows, cold_ms_all,
    # cold_median_ms, cold_min_ms, cold_spread_pct, all_hot_ms, hot_avg_ms) — merge as-is.
    report.merge({"timings": {model: res}})


def _render_console(title, headers, rows, aligns, sep_before_last=False):
    """A boxed, aligned unicode table to stdout."""
    widths = [len(h) for h in headers]
    for r in rows:
        for i, c in enumerate(r):
            widths[i] = max(widths[i], len(str(c)))
    line = lambda l, m, rt: l + m.join("─" * (w + 2) for w in widths) + rt
    def frow(cells):
        parts = []
        for i, c in enumerate(cells):
            c = str(c)
            parts.append(" " + (c.rjust(widths[i]) if aligns[i] == "r" else c.ljust(widths[i])) + " ")
        return "│" + "│".join(parts) + "│"
    print(f"\n{title}")
    print(line("┌", "┬", "┐"))
    print(frow(headers))
    print(line("├", "┼", "┤"))
    body = rows[:-1] if sep_before_last else rows
    for r in body:
        print(frow(r))
    if sep_before_last:
        print(line("├", "┼", "┤"))
        print(frow(rows[-1]))
    print(line("└", "┴", "┘"))


def compare_table(title, base, model, base_res, opt_res, key, spread_key=None):
    base_tot = opt_tot = 0.0
    wins = 0
    counted = 0
    rows = []
    for name in base_res:  # base_res preserves query order; only queries present in BOTH, with key
        if name not in opt_res:
            continue
        b = base_res[name].get(key)
        o = opt_res[name].get(key)
        if b is None or o is None:
            continue
        bs = base_res[name].get(spread_key) if spread_key else None
        os_ = opt_res[name].get(spread_key) if spread_key else None
        w = tie(b, o, bs, os_)                 # "base" / "model" / "tie"
        base_tot += b
        opt_tot += o
        counted += 1
        wins += 1 if w == "model" else 0
        speedup = (b / o) if o else float("inf")
        rows.append((name, b, o, speedup, w))
    if not counted:
        return
    overall = (base_tot / opt_tot) if opt_tot else float("inf")
    total_w = "model" if opt_tot < base_tot else ("base" if opt_tot > base_tot else "tie")
    mshort = E.engine_of(model)
    bshort = E.engine_of(base)
    winner_lbl = {"model": model, "base": base, "tie": "tie"}
    factor = overall if overall >= 1 else (1.0 / overall if overall else 0.0)
    headline = (f"{winner_lbl[total_w]} is {factor:.2f}× faster overall"
                f" — {mshort} wins {wins}/{counted}")

    # ---- boxed console table ----
    mark = {"model": f"{mshort} ✔", "base": f"{bshort} ✔", "tie": "tie"}
    disp = [(n, f"{b:,.1f}", f"{o:,.1f}", f"{s:.2f}×", mark[w]) for (n, b, o, s, w) in rows]
    disp.append(("TOTAL", f"{base_tot:,.1f}", f"{opt_tot:,.1f}", f"{overall:.2f}×", mark[total_w]))
    _render_console(title, ("query", f"{base} (ms)", f"{model} (ms)", f"{bshort}/{mshort}", "winner"),
                    disp, ("l", "r", "r", "r", "l"), sep_before_last=True)
    print(f"  → {headline}")


def main():
    workspace = os.environ["PBI_WORKSPACE"].strip()
    from duckrun import auth
    token = os.environ.get("PBI_TOKEN") or auth.get_powerbi_token()  # self-acquire the XMLA token
    adomd_dir = os.environ.get("ADOMD_DIR", ".")
    runs = int(os.environ.get("BENCH_RUNS", "3"))
    cold_repeats = int(os.environ.get("COLD_REPEATS", "1"))
    want_cold = (os.environ.get("BENCH_COLD", "true").strip().lower() != "false")
    gap = int(os.environ.get("BENCH_GAP_SECONDS", "600"))  # idle gap between models (>CU smoothing)

    _load_adomd(adomd_dir)
    base, others = discover_models()
    print(f"Workspace : {workspace}")
    print(f"Reference : {base} ({E.MODE[E.engine_of(base)]})")
    print(f"Compare   : " + ", ".join(f"{m} ({E.MODE[E.engine_of(m)]})" for m in others))
    print(f"Runs (hot): {runs}   Cold repeats: {cold_repeats}")

    # Resolve the top DUID once on the reference model (same data across engines) to fill the ladder.
    td = None
    try:
        c = open_conn(workspace, base, token)
        if warm_up(c, base):
            td = top_duid(c)
        c.Close()
    except Exception as e:
        print(f"  top DUID resolve failed ({str(e).splitlines()[0][:100]}) — dropping DUID ladder.")
    print(f"Top DUID  : {td}")
    queries = resolve_queries(td)

    base_res, base_cold = bench_model(workspace, base, token, runs, want_cold, cold_repeats, queries)
    if base_res is None:
        sys.exit(f"Reference model {base!r} never became queryable — cannot benchmark.")
    _write_timings(base, base_res)

    for model in others:
        if gap:
            print(f"\n⏳ Idle gap: sleeping {gap}s before {model} so the Fabric capacity chart "
                  f"shows a clean separation between models...", flush=True)
            time.sleep(gap)
        try:
            opt_res, opt_cold = bench_model(workspace, model, token, runs, want_cold,
                                            cold_repeats, queries)
        except Exception as e:
            print(f"  {model}: benchmark failed ({str(e).splitlines()[0][:120]}) — skipping.",
                  flush=True)
            continue
        if opt_res is None:
            print(f"  {model} never became queryable — skipping its comparison.", flush=True)
            continue
        _write_timings(model, opt_res)
        if base_cold and opt_cold:
            compare_table(f"{model} vs {base}  —  COLD (median of {cold_repeats} dehydrate cycles)",
                          base, model, base_res, opt_res, "cold_median_ms", "cold_spread_pct")
        elif want_cold:
            # Naming why is the point: a missing cold table for the DirectQuery leg is expected,
            # not a gap in the run.
            print(f"\n(no COLD table for {model}: "
                  f"{'it has no transcoded data to evict' if E.MODE[E.engine_of(model)] == 'directQuery' else 'dehydrate was unavailable'})")
        compare_table(f"{model} vs {base}  —  HOT (median of hot runs, dropping run1/run2 warm)",
                      base, model, base_res, opt_res, "hot_median_ms", "hot_spread_pct")


if __name__ == "__main__":
    main()
