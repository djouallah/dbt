"""Specialist findings for the query benchmark. Reads ONLY run_report.json and recomputes every
number from it (nothing hardcoded); appends to the CI job summary ($GITHUB_STEP_SUMMARY) and prints
to stdout — no file artifact. Medians only — never a mean in any comparison or verdict; ties render
as `=`.

Timing only, by design: physical layout per engine is `.github/scripts/stats.py` in ci.yml's
`summary` job, and is not re-derived here.

Exits 1 on a verdict-direction inversion — the one thing here that can fail the job, and
deliberately so: a report that names the slower engine the winner is worse than no report.

Env in: RUN_REPORT (the one JSON), GITHUB_STEP_SUMMARY (optional).
"""
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_report as rr  # noqa: E402  (pure helpers: compute_analysis, reference, consts)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

OUT = []


def w(line=""):
    OUT.append(line)


def lbl(model):
    return rr._short(model)  # engine label: aemo_duckrun -> duckrun


def _noisy_cols(rep, thresh=25.0):
    """Probe columns whose cold spread exceeds `thresh`% in ANY engine — measurement is too noisy
    to quote (n = cold_repeats over shared capacity). These are excluded from headline sentences."""
    tim = rep.get("timings", {})
    noisy = set()
    for m in tim:
        for col in rr._PROBE_COLS:
            sp = tim[m].get(f"probe_{col}", {}).get("cold_spread_pct")
            if sp is not None and sp > thresh:
                noisy.add(col)
    return noisy


def _verdict_line(v):
    """One metric's verdict, stated with explicit engine names — shared with render_report so the
    two documents can never phrase the same verdict differently."""
    return rr.verdict_sentence(v)


def _verdict_row(by, base_lbl, m):
    chal = lbl(m)
    mv = by.get(chal, {})
    c, h = mv.get("COLD"), mv.get("HOT")
    cr = "=" if (c and c["verdict"] == "tie") else (_ratio(c["ratio"]) if c else "—")
    hr = "=" if (h and h["verdict"] == "tie") else (_ratio(h["ratio"]) if h else "—")
    parts = [p for p in (_verdict_line(c), _verdict_line(h)) if p]
    mode = (h or c or {}).get("model_mode", "directLake")
    tag = "" if mode == "directLake" else " (DirectQuery)"
    w(f"| {chal}{tag} vs {base_lbl} | {cr} | {hr} | {'; '.join(parts)} |")


def _ms(v):
    return "—" if v is None else f"{v:,.0f}"


def _ratio(v):
    return "—" if v is None else f"{v:.2f}"


# ------------------------------------------------------------------------------------- sections

def s1_header(rep):
    run = rep.get("run", {})
    inp = run.get("inputs", {})
    tim = rep.get("timings", {})
    lake = [m for m in tim if rr._is_lake(rep, m)]
    dq = [m for m in tim if not rr._is_lake(rep, m)]
    w("# Specialist findings — engine query benchmark")
    w()
    w(f"- run `{run.get('run_id')}` · sha `{run.get('sha')}` · {run.get('date')}")
    w(f"- duckrun `{run.get('duckrun_version')}` · workspace `{run.get('workspace')}` · "
      f"reference `{run.get('reference')}`")
    w(f"- inputs: engines={inp.get('engines')} · cold_repeats={inp.get('cold_repeats')} · "
      f"runs={inp.get('runs')} · gap_seconds={inp.get('gap_seconds')}")
    w()
    w(f"{len(tim)} engines' own copy of the same `mart.fct_summary`, at row-count parity; identical "
      f"DAX over XMLA against each. {len(lake)} read by Direct Lake"
      + (f" ({inp.get('cold_repeats')} cold cycles per query, medians reported)" if lake else "")
      + (f"; {len(dq)} by DirectQuery (hot only — no transcoded data to evict)" if dq else "") + ".")
    w()


def s2_verdicts(rep, analysis, base, models):
    by = {}
    for v in analysis.get("verdicts", []):
        by.setdefault(v["model"], {})[v["metric"]] = v
    base_lbl = lbl(base)
    headline = [m for m in models if m != base]
    noisy = sorted(_noisy_cols(rep))

    w("## 1. Headline verdict (medians; fastest wins, no tie band)")
    w()
    w(f"Ratio column is `{base_lbl} ÷ challenger` (< 1 ⇒ {base_lbl} faster).")
    w()
    w(f"| pair | COLD {base_lbl}÷chal | HOT {base_lbl}÷chal | verdict |")
    w("|:--|--:|--:|:--|")
    for m in headline:
        _verdict_row(by, base_lbl, m)
    w()
    if noisy:
        cr = rep.get("run", {}).get("inputs", {}).get("cold_repeats")
        agg = []
        for m in headline:
            c = by.get(lbl(m), {}).get("COLD")
            if c and c["verdict"] != "tie":
                tot = c["wins"] + c["losses"] + c["ties"]
                base_won = c["verdict"] == "base"
                # The loser is the OTHER party. Reading `lbl(m)` for both sides printed
                # "spark wins 12/15 vs spark" whenever the CHALLENGER won — which upstream never hit,
                # because there the reference was the layout under test and it won.
                who = base_lbl if base_won else lbl(m)
                against = lbl(m) if base_won else base_lbl
                cnt = c["losses"] if base_won else c["wins"]
                agg.append(f"{who} wins {cnt}/{tot} vs {against}")
        w(f"- ⚠ {len(noisy)} probe columns exceed 25% cold spread (n={cr}, shared capacity; see §2). "
          f"The headline rests on the aggregate, not any single column: "
          f"{'; '.join(agg) or 'see table'} (per-query cold median, fastest wins).")
        w()


def s3_cold_decomp(rep, analysis, models):
    cc = analysis.get("cold_column_cost", {})
    if not cc:
        return
    models = [m for m in models if m in cc]
    noisy = _noisy_cols(rep)
    w("## 2. Cold decomposition (marginal cost per column)")
    w()
    w("<sub>Each cell is that column's probe median minus the `probe_rowcount` control — the "
      "marginal cost of touching one more column, cold.</sub>")
    w()
    w("| column | " + " | ".join(f"{lbl(m)} ms" for m in models) + " |")
    w("|:--|" + "--:|" * len(models))
    cost_by_col = {}  # col -> [cost per engine] for the observations below
    for col in rr._PROBE_COLS:
        cells = []
        for m in models:
            cost = cc.get(m, {}).get("columns", {}).get(col)
            cells.append(_ms(cost))
            cost_by_col.setdefault(col, []).append(cost)
        w(f"| {col} | " + " | ".join(cells) + " |")
    w("| _rowcount overhead_ | "
      + " | ".join(_ms(cc.get(m, {}).get("rowcount_overhead_ms")) for m in models) + " |")
    w()
    # auto observations: CV of cost across engines per column
    cv = {}
    floor = {}
    for col, vals in cost_by_col.items():
        v = [x for x in vals if x is not None]
        if len(v) >= 2 and statistics.mean(v):
            cv[col] = statistics.pstdev(v) / statistics.mean(v)
        if v:
            floor[col] = statistics.mean(v)
    if cv:
        med_cv = statistics.median(cv.values())
        # engine-sensitivity is only quotable for low-noise columns; noisy ones (spread>25%) have
        # cost variance confounded with measurement noise, so name them separately.
        hi = sorted((c for c, x in cv.items() if x >= med_cv and c not in noisy), key=lambda c: -cv[c])
        lo = [c for c, x in cv.items() if x < med_cv and c not in noisy]
        # The cheapest columns overall (candidate "floor"). If any is non-quotable, the irreducible
        # transcode floor isn't measurable at this n — naming a noisy cheap column the floor while
        # its own medians sit below it is the contradiction we refuse to print.
        cheap = sorted(floor, key=floor.get)[:2]
        cheap_noisy = [c for c in cheap if c in noisy]
        w(f"- engine-sensitive (high cross-engine variance, low noise): "
          f"{', '.join(f'{c} (CV {cv[c]:.2f})' for c in hi) or 'none clearly separable at this n'}.")
        w(f"- engine-invariant (low variance, low noise): "
          f"{', '.join(lo) or 'none clearly separable at this n'}.")
        if noisy:
            w(f"- non-quotable (cold spread >25%): {', '.join(sorted(noisy))}.")
        if cheap_noisy:
            w(f"- irreducible floor: not measurable at this n — the cheapest columns "
              f"({', '.join(cheap_noisy)}) are non-quotable; anchor conclusions on the aggregate "
              "cold win (§1), not per-column costs.")
        elif cheap:
            w(f"- irreducible floor (stable): {cheap[0]} (~{floor[cheap[0]]:,.0f} ms across engines).")
        w()


def s4_spread(rep, models):
    """How trustworthy each engine's numbers are. Without this the medians read as exact."""
    tim = rep.get("timings", {})
    rows = []
    for m in models:
        qs = tim.get(m, {})
        cold = [d["cold_spread_pct"] for d in qs.values() if d.get("cold_spread_pct") is not None]
        hot = [d["hot_spread_pct"] for d in qs.values() if d.get("hot_spread_pct") is not None]
        if not (cold or hot):
            continue
        rows.append((lbl(m), len(qs),
                     statistics.median(cold) if cold else None,
                     max(cold) if cold else None,
                     statistics.median(hot) if hot else None,
                     max(hot) if hot else None))
    if not rows:
        return
    w("## 3. Measurement spread")
    w()
    w("<sub>A per-query difference smaller than the larger of the two spreads is scored a **tie**, "
      "not a win. High spread here is why a verdict can read 'no measurable difference'.</sub>")
    w()
    w("| engine | queries | cold spread median % | cold max % | hot spread median % | hot max % |")
    w("|:--|--:|--:|--:|--:|--:|")
    for r in rows:
        w(f"| {r[0]} | {r[1]} | {_ratio(r[2])} | {_ratio(r[3])} | {_ratio(r[4])} | {_ratio(r[5])} |")
    w()


def s5_pointers(rep):
    w("## 4. Raw")
    w()
    w("- artifact `run-report`: `run_report.json` (these findings are in the CI job summary).")
    w("- every number above recomputes from run_report.json (`timings.*`, `analysis.*`) — "
      "`RUN_REPORT=<file> python benchmark/render_report.py`, no credentials.")
    w("- physical layout per engine, and row-count parity: the `summary` job of `ci.yml`.")
    w()


def verify_verdicts(rep, analysis):
    """Orientation guard: the verdict winner must agree with the per-query cold-median majority
    over the SAME queries the verdict aggregates — a disagreement there is a true ratio inversion
    and is fatal. The summed marginal PROBE cost is a second view over a DIFFERENT (probe-only)
    query subset; probes and composites can legitimately point different ways, so a disagreement
    there is a non-fatal note, not a build failure. Returns (errors, notes)."""
    tim = rep.get("timings", {})
    cc = analysis.get("cold_column_cost", {})
    base = rr.reference(rep, list(tim))
    if not base or base not in cc:
        return [], []

    def _cost(m):
        cols = cc.get(m, {}).get("columns")
        return (sum(cols.values()) + cc[m]["rowcount_overhead_ms"]) if cols else None

    base_cost = _cost(base)
    vmap = {v["model"]: v for v in analysis.get("verdicts", []) if v["metric"] == "COLD"}
    errs, notes = [], []
    for m in tim:
        if m == base or m not in cc:
            continue
        v = vmap.get(rr._short(m))
        if not v or v["verdict"] == "tie":
            continue
        verdict_winner = base if v["verdict"] == "base" else m
        # per-query cold-median majority — same query set as the verdict; the orientation invariant.
        bw = mw = 0
        for q, d in tim[m].items():
            b, x = tim[base].get(q, {}).get("cold_median_ms"), d.get("cold_median_ms")
            if b is None or x is None:
                continue
            bw += b < x
            mw += x < b
        median_winner = base if bw > mw else (m if mw > bw else None)
        if median_winner and verdict_winner != median_winner:      # FATAL: real inversion
            errs.append(f"{lbl(m)}: verdict says {lbl(verdict_winner)} but per-query cold-median "
                        f"majority says {lbl(median_winner)}")
            continue
        # summed marginal probe cost — different subset, advisory only.
        mcost = _cost(m)
        cost_winner = (base if base_cost < mcost else m) if (base_cost and mcost) else None
        if cost_winner and verdict_winner != cost_winner:
            notes.append(f"{lbl(m)}: full-query verdict favours {lbl(verdict_winner)} while the "
                         f"probe-only marginal cost favours {lbl(cost_winner)} — probes and "
                         f"composites diverge (not an inversion)")
    return errs, notes


def main():
    path = os.environ.get("RUN_REPORT", "run_report.json")
    if not os.path.exists(path):
        print(f"no report at {path}; nothing to summarize")
        return
    with open(path, encoding="utf-8") as f:
        rep = json.load(f)

    analysis = rep.get("analysis") or rr.compute_analysis(rep)
    models = rr._order(rep, list(rep.get("timings", {})))
    base = rr.reference(rep, models)

    s1_header(rep)
    if base:
        s2_verdicts(rep, analysis, base, models)
    s3_cold_decomp(rep, analysis, models)
    s4_spread(rep, models)
    s5_pointers(rep)

    text = "\n".join(OUT) + "\n"
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(text)
    print(text)

    # Direction guard — the findings are already in the job summary. A genuine verdict inversion
    # (verdict disagrees with the same-query median majority) is fatal; a probe-vs-composite
    # divergence is only a warning.
    errs, notes = verify_verdicts(rep, analysis)
    for n in notes:
        print(f"::warning::{n}")
    if errs:
        for e in errs:
            print(f"::error::verdict direction inversion — {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
