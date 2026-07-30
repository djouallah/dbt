"""Render run_report.json to the GitHub job summary AND compute the derived `analysis` block,
which is merged back into the same file. Pure post-processing, pure timing — everything here is
recomputable offline from the cold samples and hot runs already in run_report.json.

Deliberately NO parquet/geometry analysis. Physical layout (files, row groups, size, v-order,
compression) is already reported per engine by `.github/scripts/stats.py` in ci.yml's `summary` job;
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


def reference(rep, models):
    """The model every comparison is measured against.

    Recorded by resolve_env.py as run.reference (the first of BENCH_ENGINES); falls back to the env
    and then to the first model present. Never a name-length heuristic — with one model per engine
    there is no name to key off, and an implicit choice silently reorients every ratio in the
    report."""
    if not models:
        return None
    labels = [_short(m) for m in models]
    want = (rep.get("run", {}) or {}).get("reference")
    if want not in labels:
        want = E.reference(labels) if labels else None
    for m in models:
        if _short(m) == want:
            return m
    return models[0]


def _mode(rep, model):
    """directLake / directQuery for a model, from the report (resolve_env.py) or the registry."""
    lbl = _short(model)
    meta = (rep.get("engines", {}) or {}).get(lbl) or {}
    return meta.get("mode") or E.MODE.get(lbl, "directLake")


def _is_lake(rep, model):
    return _mode(rep, model) == "directLake"


def _order(rep, models):
    """Reference first, then the rest by name — a stable reading order across runs."""
    base = reference(rep, models)
    return ([base] + sorted(m for m in models if m != base)) if base else list(models)


def _totals(timings, models, key):
    """{engine: summed metric} over the queries EVERY participating model answered.

    The pairwise `base_total_ms` in each verdict sums over that challenger's own query set, so with
    more than one challenger those totals are not mutually comparable and cannot be read as a
    column. This restricts to the common query set instead.

    Participation is per metric: a DirectQuery model has no cold numbers at all, and letting it into
    the COLD intersection would empty the column for the engines that do have them."""
    have = [m for m in models
            if any((d or {}).get(key) is not None for d in timings.get(m, {}).values())]
    if not have:
        return {}
    common = [q for q in timings[have[0]]
              if all((timings[m].get(q) or {}).get(key) is not None for m in have)]
    if not common:
        return {}
    return {_short(m): sum(timings[m][q][key] for q in common) for m in have}


# ---------------------------------------------------------------------------- derived analysis

def _tie(b, m, b_spread_pct=None, m_spread_pct=None):
    """Winner of base vs model: the FASTER time wins, by any margin (same rule as
    xmla_compare.tie — see there for why the noise band was removed). "tie" now means exactly
    equal, which in practice never happens. Spreads are accepted and ignored."""
    if b is None or m is None or not b or not m:
        return "tie" if b == m else ("model" if (m or 0) < (b or 0) else "base")
    return "model" if m < b else ("base" if m > b else "tie")


def _agg_verdict(base_t, model_t, key, spread_key=None):
    """Aggregate base-vs-model verdict for one metric: per query the faster time wins, and the
    overall verdict follows the summed totals. No tie band — a 1ms win is a win."""
    bt = mt = 0.0
    per = []
    for q, mv in model_t.items():
        bv = base_t.get(q, {})
        b, x = bv.get(key), mv.get(key)
        if b is None or x is None:
            continue
        per.append(_tie(b, x, bv.get(spread_key) if spread_key else None,
                        mv.get(spread_key) if spread_key else None))
        bt += b
        mt += x
    if not per:
        return None
    wins, losses, ties = per.count("model"), per.count("base"), per.count("tie")
    ratio = (bt / mt) if mt else float("inf")
    if wins == 0 and losses == 0:
        # Only reachable when EVERY comparable query came back exactly equal to the millisecond.
        # Kept as a degenerate guard, not as a noise verdict — the old version reached this
        # whenever the deltas sat inside the spread, which is the behaviour that was removed.
        verdict, text = "tie", "identical timings on every query"
    else:
        verdict = "model" if mt < bt else "base"
        fac = ratio if ratio >= 1 else (1 / ratio if ratio else 0)
        text = f"{verdict} {fac:.2f}× faster overall (W/L/T {wins}/{losses}/{ties})"
    return {"base_total_ms": round(bt, 1), "model_total_ms": round(mt, 1),
            "ratio": round(ratio, 3), "wins": wins, "losses": losses, "ties": ties,
            "verdict": verdict, "text": text}


def compute_analysis(rep):
    timings = rep.get("timings", {})
    models = list(timings)
    analysis = {"cold_column_cost": {}, "verdicts": []}

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

    # verdicts: structured, reference vs each challenger, medians, fastest wins (never a mean).
    # Every model participates: a DirectQuery timing is a real measured query time, it just isn't
    # the same kind of number. `mode` on each verdict says which kind it is.
    base = reference(rep, models)
    if base:
        for m in models:
            if m == base:
                continue
            for metric, key, sk in (("COLD", "cold_median_ms", "cold_spread_pct"),
                                    ("HOT", "hot_median_ms", "hot_spread_pct")):
                agg = _agg_verdict(timings[base], timings[m], key, sk)
                if agg:
                    agg.update({"metric": metric, "base": _short(base), "model": _short(m),
                                "base_mode": _mode(rep, base), "model_mode": _mode(rep, m)})
                    analysis["verdicts"].append(agg)
    return analysis


# ---------------------------------------------------------------------------- rendering

def verdict_sentence(v, bold=False):
    """One verdict stated with the ENGINE NAMES, never 'base'/'model'.

    `_agg_verdict` cannot write this itself — it is handed two timing dicts and knows no labels — so
    its own `text` field says "base 1.87× faster", which forces the reader to remember which engine
    the reference was. Both renderers go through here instead."""
    if not v:
        return None
    if v["verdict"] == "tie":
        return f"{v['metric']}: identical timings on every query"
    fac = v["ratio"] if v["ratio"] >= 1 else (1 / v["ratio"] if v["ratio"] else 0)
    if v["verdict"] == "base":
        win, lose, wc, lc = v["base"], v["model"], v["losses"], v["wins"]
    else:
        win, lose, wc, lc = v["model"], v["base"], v["wins"], v["losses"]
    shown = f"**{win}**" if bold else win
    return (f"{v['metric']}: {shown} {fac:.2f}× faster "
            f"({win} wins {wc}, {lose} wins {lc}, ties {v['ties']})")


def _summary_table(rep):
    """One row per engine: what wrote its table, how it is read, and its aggregate cold/hot
    wall-clock with ✔ on the fastest.

    Generalised from upstream's two-layout shape: each engine's total comes from the common query
    set, so adding an engine adds a row and nothing else."""
    timings = rep.get("timings", {})
    models = _order(rep, list(timings))
    base = reference(rep, models)
    if not base:
        return

    cold_tot = _totals(timings, models, "cold_median_ms")
    hot_tot = _totals(timings, models, "hot_median_ms")

    def _winner(tot):
        # Needs something to beat: with one participant (e.g. COLD when the only other engine is
        # DirectQuery) a ✔ would decorate an uncontested number.
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
        body.append([name, f"`{writer}`", _mode(rep, m), meta.get("item") or "—", cc, hc])
    if not body:
        return
    out = ["## Summary", "",
           f"Reference: **{_short(base)}**. Every ratio below reads `{_short(base)} ÷ engine`. "
           f"Totals are over the queries every participating engine answered.", "",
           "| engine | writer | mode | item | cold total (ms) | hot total (ms) |",
           "|:--|:--|:--|:--|--:|--:|"]
    for r in body:
        out.append("| " + " | ".join(r) + " |")
    dq = [_short(m) for m in models if not _is_lake(rep, m)]
    if dq:
        one = len(dq) == 1
        out += ["", f"<sub>{', '.join(dq)} {'reads' if one else 'read'} through **DirectQuery** "
                    f"(SQL endpoint pushdown), not Direct Lake, so "
                    f"{'it has' if one else 'they have'} no cold tier to evict and "
                    f"{'its' if one else 'their'} numbers are not the same kind as the "
                    f"others'.</sub>"]
    out += ["", "<sub>Physical layout per engine — files, row groups, size, v-order, compression — "
                "is the `summary` job of `ci.yml`, not this run.</sub>"]
    _write("\n".join(out) + "\n")


def _sidebyside(title, timings, base, others, key, spread_key=None):
    """One table: rows = queries, columns = base + each challenger, `best` = the fastest engine.

    `best` is simply argmin over the row. It used to be computed as best-vs-second-best through
    the tie rule, which printed "tie" on a row where the top two were within noise of each other
    — even when a THIRD engine was several times slower. Every row of a real four-engine run came
    out "tie", which reads as "all four are equal" and is the opposite of what the row showed.
    A 1ms win is a win; the exact times are in the row for anyone who wants to judge the margin.

    Participation is per metric, for the same reason `_totals` scopes it: a row is dropped when any
    COLUMN lacks the metric, so admitting a DirectQuery engine (no cold tier at all) to the COLD
    table would drop every row and render nothing."""
    models = [m for m in [base] + others
              if any((d or {}).get(key) is not None for d in timings.get(m, {}).values())]
    if len(models) < 2 or base not in models:
        return
    labels = [_short(m) for m in models]
    header = "| Query | " + " | ".join(f"{l} (ms)" for l in labels) + " | best |"
    sep = "|:--|" + "--:|" * len(models) + ":--|"
    out = [f"### {title}", "", header, sep]
    any_row = False
    for q in timings[base]:
        vals = {m: timings[m].get(q, {}).get(key) for m in models}
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
    models = list(cc)
    out = ["### Marginal cold column cost (probe_col − probe_rowcount, ms)", "",
           "| column | " + " | ".join(_short(m) for m in models) + " |",
           "|:--|" + "--:|" * len(models)]
    for col in _PROBE_COLS:
        cells = " | ".join(_fmt(cc[m]["columns"].get(col)) for m in models)
        out.append(f"| `{col}` | {cells} |")
    out.append("| _rowcount overhead_ | "
               + " | ".join(_fmt(cc[m]["rowcount_overhead_ms"]) for m in models) + " |")
    _write("\n".join(out) + "\n")


def _verdicts(vs):
    if not vs:
        return
    lines = []
    for v in vs:
        tag = "" if v.get("model_mode", "directLake") == "directLake" else " _(DirectQuery)_"
        lines.append(f"- {v['model']} vs {v['base']} — {verdict_sentence(v, bold=True)}{tag}")
    _write("### Verdicts (medians; fastest wins, no tie band)\n\n" + "\n".join(lines) + "\n")


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

    _summary_table(rep)

    timings = rep.get("timings", {})
    models = _order(rep, list(timings))
    base = reference(rep, models)
    if base:
        others = [m for m in models if m != base]
        _sidebyside("COLD (median of dehydrate cycles)", timings, base, others,
                    "cold_median_ms", "cold_spread_pct")
        _sidebyside("HOT (median of steady-state runs)", timings, base, others,
                    "hot_median_ms", "hot_spread_pct")
    _cold_cost_table(analysis.get("cold_column_cost", {}))
    _verdicts(analysis.get("verdicts", []))


if __name__ == "__main__":
    main()
