"""Render run_report.json to the GitHub job summary AND compute the derived `analysis` block,
which is merged back into the same file. Pure post-processing, pure timing — everything here is
recomputable offline from the cold samples and hot runs already in run_report.json.

**There is no reference engine and no baseline.** Every engine is measured the same way against the
same DAX, so the report ranks them and names the fastest per query; nothing is stated as a ratio
against one privileged engine. That used to be the shape — `BENCH_ENGINES[0]` was the reference and
every ratio read `base ÷ challenger` — inherited from upstream, where there genuinely was a baseline:
it built a candidate layout and compared it to the existing one. Here the four engines are four peers,
so a baseline was an arbitrary choice that made every number in the report depend on the order the
dispatch happened to list them in, and made "iceberg 1.30× faster" mean nothing without remembering
which engine the reference was. Ratios are now stated against the FASTEST engine of the metric, which
is a property of the measurement rather than of the input list.

Deliberately NO parquet/geometry analysis. Physical layout (files, row groups, size, v-order,
compression) is already reported per engine by `.github/scripts/stats.py` in the *Parity dashboard* workflow;
duplicating it here would mean a second, slower reader of the same Delta logs saying the same thing.
This measures wall-clock and nothing else.

Env in: RUN_REPORT (the one JSON), GITHUB_STEP_SUMMARY (optional; also prints to stdout).
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engines as E  # noqa: E402
import report  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

_PROBE_COLS = ["mw", "price", "duid", "date", "time"]     # probe_<col> minus probe_rowcount

# The two measured metrics, in report order: (label, per-query median key, per-query spread key).
METRICS = (("COLD", "cold_median_ms", "cold_spread_pct"),
           ("HOT", "hot_median_ms", "hot_spread_pct"))


def _write(md):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as f:
            f.write(md + "\n")
    print(md)


def _fmt(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        return f"{v:,.1f}"
    if isinstance(v, int):
        return f"{v:,}"
    return "" if v is None else str(v)


def _int(v):
    return "" if v is None else f"{v:,.0f}"


def _short(model):
    """Engine label for a semantic-model name (aemo_duckrun -> duckrun)."""
    return E.engine_of(model)


def _order(models):
    """Column order for every side-by-side table: by ENGINE LABEL, alphabetically.

    Not by speed and not by the dispatch's engine list. Alphabetical is the only order that is both
    neutral between peers and stable across runs — ordering by result would move the columns whenever
    the winner changed, which makes two runs impossible to read side by side, and ordering by the
    input list smuggles back the privileged first position this report no longer has. The RANKING
    table is where speed decides the order, and it says so."""
    return sorted(models, key=lambda m: _short(m))


def _query_order(timings, models):
    """Query rows in measurement order, taken from whichever model has the most of them.

    With a reference engine this was `timings[base]`, which silently dropped any query the reference
    failed to answer. There is no base now, so take the longest — every model runs the same suite in
    the same order, so the longest is the most complete view of it."""
    best = []
    for m in models:
        qs = list(timings.get(m, {}))
        if len(qs) > len(best):
            best = qs
    return best


def _totals(timings, models, key):
    """{engine: summed metric} over the queries EVERY participating model answered.

    Restricted to the common query set, because summing over each engine's own set would compare
    different amounts of work and make the column meaningless.

    Participation is per metric, because an engine can be missing one: `BENCH_COLD=false` skips cold
    for everyone, and a dehydrate that fails (a token that cannot refresh) drops that engine to hot
    only. Letting a hot-only engine into the COLD intersection would empty the column for the engines
    that do have cold numbers."""
    have = [m for m in models
            if any((d or {}).get(key) is not None for d in timings.get(m, {}).values())]
    if not have:
        return {}
    common = [q for q in _query_order(timings, have)
              if all((timings[m].get(q) or {}).get(key) is not None for m in have)]
    if not common:
        return {}
    return {_short(m): sum(timings[m][q][key] for q in common) for m in have}


# ---------------------------------------------------------------------------- derived analysis

def _per_query_wins(timings, models, key):
    """{engine: queries it was strictly the fastest on} over the rows every model answered.

    Strictly: an exact tie to the millisecond gives nobody the win, which is the only tie left in
    this report. Note the win count and the summed total can disagree — an engine can win most
    queries and still lose the total by losing the expensive one. Both are reported; neither is
    corrected against the other."""
    wins = {_short(m): 0 for m in models}
    for q in _query_order(timings, models):
        vals = {m: (timings[m].get(q) or {}).get(key) for m in models}
        if any(v is None for v in vals.values()):
            continue
        lo = min(vals.values())
        if sum(1 for v in vals.values() if v == lo) == 1:
            wins[_short(min(vals, key=lambda m: vals[m]))] += 1
    return wins


def rank(timings, models, key):
    """Rank the engines on one metric: fastest first, ratio stated against the FASTEST.

    This replaced the pairwise base-vs-model verdict. Every entry is a statement about the
    measurement (`2.4× the fastest engine's total`) rather than about a chosen engine, so adding or
    reordering engines in the dispatch cannot change what any row means."""
    tot = _totals(timings, models, key)
    if not tot:
        return []
    have = [m for m in models if _short(m) in tot]
    wins = _per_query_wins(timings, have, key)
    fastest = min(tot.values())
    out = []
    for i, lbl in enumerate(sorted(tot, key=lambda e: tot[e]), start=1):
        out.append({"engine": lbl, "rank": i, "total_ms": round(tot[lbl], 1),
                    "x_fastest": round(tot[lbl] / fastest, 3) if fastest else None,
                    "query_wins": wins.get(lbl, 0)})
    return out


def compute_analysis(rep):
    timings = rep.get("timings", {})
    models = _order(list(timings))
    analysis = {"cold_column_cost": {}, "ranking": {}}

    # cold_column_cost: probe_<col>.cold_median - probe_rowcount.cold_median (marginal transcode).
    # Timing-only — a difference between two measured queries, not a claim about the files.
    for m in models:
        base = timings[m].get("probe_rowcount", {}).get("cold_median_ms")
        if base is None:
            continue
        row = {}
        for col in _PROBE_COLS:
            v = timings[m].get(f"probe_{col}", {}).get("cold_median_ms")
            if v is not None:
                row[col] = round(v - base, 1)
        analysis["cold_column_cost"][m] = {"rowcount_overhead_ms": round(base, 1), "columns": row}

    # ranking: one ordered list per metric. Medians only, never a mean.
    for metric, key, _sk in METRICS:
        r = rank(timings, models, key)
        if r:
            analysis["ranking"][metric] = r
    return analysis


# ---------------------------------------------------------------------------- rendering

def rank_sentence(metric, ranking, bold=False):
    """One metric's result in a sentence, naming engines and nothing else.

    No engine is described as beating another by a ratio: the fastest is named, and everyone else is
    quoted as a multiple of it."""
    if not ranking:
        return None
    win = ranking[0]
    shown = f"**{win['engine']}**" if bold else win["engine"]
    rest = ", ".join(f"{r['engine']} {r['x_fastest']:.2f}×" for r in ranking[1:])
    return (f"{metric}: {shown} fastest ({win['total_ms']:,.0f} ms total, "
            f"{win['query_wins']} query wins)" + (f" — then {rest}" if rest else ""))


def _summary_table(rep, analysis):
    """One row per engine: what wrote its table, and its aggregate cold/hot wall-clock with ✔ on the
    fastest. Rows in the alphabetical column order, so this table and the side-by-side ones read
    together; the ranking is stated underneath in speed order."""
    timings = rep.get("timings", {})
    models = _order(list(timings))
    if not models:
        return

    cold_tot = _totals(timings, models, "cold_median_ms")
    hot_tot = _totals(timings, models, "hot_median_ms")

    def _winner(tot):
        # Needs something to beat: with one participant (e.g. COLD when every other engine dropped
        # to hot-only) a ✔ would decorate an uncontested number.
        return min(tot, key=tot.get) if len(tot) > 1 else None

    cold_w, hot_w = _winner(cold_tot), _winner(hot_tot)

    body = []
    for m in models:
        name = _short(m)
        meta = (rep.get("engines", {}) or {}).get(name) or {}
        writer = meta.get("writer") or E.WRITER.get(name, "—")
        c, h = cold_tot.get(name), hot_tot.get(name)
        cc = f"{_int(c)} ✔" if name == cold_w else _int(c)
        hc = f"{_int(h)} ✔" if name == hot_w else _int(h)
        body.append([name, f"`{writer}`", meta.get("item") or "—", cc, hc])
    out = ["## Summary", "",
           "No baseline: every engine is measured the same way and ranked. Totals are over the "
           "queries every participating engine answered; ✔ is the fastest total.", "",
           "| engine | writer | item | cold total (ms) | hot total (ms) |",
           "|:--|:--|:--|--:|--:|"]
    for r in body:
        out.append("| " + " | ".join(r) + " |")
    for metric, _k, _s in METRICS:
        s = rank_sentence(metric, analysis.get("ranking", {}).get(metric), bold=True)
        if s:
            out += ["", f"- {s}"]
    # No `mode` column: every model is Direct Lake, which is the premise rather than a variable —
    # four adapters, one way of reading what they wrote. `writer` is the axis under test.
    out += ["", "<sub>Physical layout per engine — files, row groups, size, v-order, compression — "
                "is the *Parity dashboard* workflow, not this run.</sub>"]
    _write("\n".join(out) + "\n")


def _sidebyside(title, timings, models, key):
    """One table: rows = queries, columns = every engine alphabetically, `best` = the fastest.

    `best` is argmin over the row, full stop. It was once computed best-vs-second-best through a tie
    rule, which printed "tie" on a row where the top two were within noise of each other even when a
    THIRD engine was several times slower — every row of a real four-engine run came out "tie", which
    reads as "all four are equal" and is the opposite of what the row showed. A 1ms win is a win; the
    exact times are in the row for anyone who wants to judge the margin.

    Participation is per metric, for the same reason `_totals` scopes it: a row is dropped when any
    COLUMN lacks the metric, so admitting a hot-only engine (one whose dehydrate failed, or any
    engine at all when BENCH_COLD=false) to the COLD table would drop every row and render
    nothing."""
    models = [m for m in _order(models)
              if any((d or {}).get(key) is not None for d in timings.get(m, {}).values())]
    if len(models) < 2:
        return
    labels = [_short(m) for m in models]
    header = "| Query | " + " | ".join(f"{l} (ms)" for l in labels) + " | best |"
    sep = "|:--|" + "--:|" * len(models) + ":--|"
    out = [f"### {title}", "", header, sep]
    any_row = False
    for q in _query_order(timings, models):
        vals = {m: (timings[m].get(q) or {}).get(key) for m in models}
        if any(vals[m] is None for m in models):
            continue
        any_row = True
        best_lbl = _short(min(models, key=lambda m: vals[m]))
        cells = " | ".join(f"{vals[m]:,.1f}" for m in models)
        out.append(f"| `{q}` | {cells} | {best_lbl} |")
    if any_row:
        _write("\n".join(out) + "\n")


def _cold_cost_table(cc):
    if not cc:
        return
    models = _order(list(cc))
    out = ["### Marginal cold column cost (probe_col − probe_rowcount, ms)", "",
           "| column | " + " | ".join(_short(m) for m in models) + " |",
           "|:--|" + "--:|" * len(models)]
    for col in _PROBE_COLS:
        cells = " | ".join(_fmt(cc[m]["columns"].get(col)) for m in models)
        out.append(f"| `{col}` | {cells} |")
    out.append("| _rowcount overhead_ | "
               + " | ".join(_fmt(cc[m]["rowcount_overhead_ms"]) for m in models) + " |")
    _write("\n".join(out) + "\n")


def _ranking_table(analysis):
    ranking = analysis.get("ranking", {})
    if not ranking:
        return
    out = ["### Ranking (medians; fastest wins, no tie band, no baseline)", "",
           "| metric | rank | engine | total (ms) | × fastest | query wins |",
           "|:--|--:|:--|--:|--:|--:|"]
    for metric, _k, _s in METRICS:
        for r in ranking.get(metric, []):
            out.append(f"| {metric} | {r['rank']} | {r['engine']} | {r['total_ms']:,.0f} | "
                       f"{r['x_fastest']:.2f} | {r['query_wins']} |")
    out += ["", "<sub>`× fastest` is that engine's total divided by the fastest total of the same "
                "metric. Query wins count the rows it was strictly fastest on — it can win most "
                "queries and still lose the total by losing the expensive one.</sub>"]
    _write("\n".join(out) + "\n")


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to render")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    analysis = compute_analysis(rep)
    report.merge({"analysis": analysis})   # derived block lands in the same one file

    run = rep.get("run", {})
    _write(f"# Direct Lake query benchmark — `{run.get('sha')}` "
           f"(run {run.get('run_id')})\n")

    _summary_table(rep, analysis)

    timings = rep.get("timings", {})
    models = _order(list(timings))
    _sidebyside("COLD (median of dehydrate cycles)", timings, models, "cold_median_ms")
    _sidebyside("HOT (median of steady-state runs)", timings, models, "hot_median_ms")
    _cold_cost_table(analysis.get("cold_column_cost", {}))
    _ranking_table(analysis)


if __name__ == "__main__":
    main()
