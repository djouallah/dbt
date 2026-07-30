"""Specialist findings for the query benchmark. Reads ONLY run_report.json and recomputes every
number from it (nothing hardcoded); appends to the CI job summary ($GITHUB_STEP_SUMMARY) and prints
to stdout — no file artifact. Medians only — never a mean in any comparison.

**No baseline.** Engines are ranked and the fastest is named; nothing is stated as a ratio against a
privileged engine (see render_report's module docstring for why the reference was removed).

Timing only, by design: physical layout per engine is `.github/scripts/stats.py` in the *Parity
dashboard* workflow, and is not re-derived here.

Exits 1 on a ranking inconsistency — the one thing here that can fail the job, and deliberately so:
a report that names the slower engine the winner is worse than no report.

Env in: RUN_REPORT (the one JSON), GITHUB_STEP_SUMMARY (optional).
"""
import json
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import render_report as rr  # noqa: E402  (pure helpers: compute_analysis, rank, consts)

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


def _ms(v):
    return "—" if v is None else f"{v:,.0f}"


def _ratio(v):
    return "—" if v is None else f"{v:.2f}"


# ------------------------------------------------------------------------------------- sections

def s1_header(rep):
    run = rep.get("run", {})
    inp = run.get("inputs", {})
    tim = rep.get("timings", {})
    w("# Specialist findings — engine query benchmark")
    w()
    w(f"- run `{run.get('run_id')}` · sha `{run.get('sha')}` · {run.get('date')}")
    w(f"- duckrun `{run.get('duckrun_version')}` · workspace `{run.get('workspace')}`")
    w(f"- inputs: engines={inp.get('engines')} · cold_repeats={inp.get('cold_repeats')} · "
      f"runs={inp.get('runs')} · gap_seconds={inp.get('gap_seconds')}")
    w()
    # The experiment in one sentence: identical DAX, identical semantic models, N dbt adapters. The
    # adapter that wrote the parquet is the only variable, which is why no engine is described here
    # as being read differently from the others — or as the one the others are measured against.
    w(f"Identical DAX over XMLA against {len(tim)} semantic models, one per dbt adapter, each over "
      f"that adapter's own copy of the same `mart.fct_summary` at row-count parity. All Direct Lake, "
      f"so every timing is a Delta→memory transcode and an in-memory scan shaped by the physical "
      f"layout — {inp.get('cold_repeats')} cold cycles per query, medians reported. No baseline: the "
      f"engines are peers and are ranked against each other.")
    w()
    # One job per engine and none of them fail-fast, so an engine can be missing entirely. Name it:
    # a report with three columns where the dispatch asked for four otherwise reads as a four-engine
    # result, and the missing one is exactly the interesting case.
    asked = [e.strip() for e in (inp.get("engines") or "").split(",") if e.strip()]
    got = {lbl(m) for m in tim}
    missing = [e for e in asked if e not in got]
    if missing:
        w(f"- ⚠ **no timings for {', '.join(missing)}** — its benchmark job did not report "
          f"(deploy or XMLA failure; see that job's log). Everything below covers "
          f"{', '.join(e for e in asked if e in got) or 'nothing'} only.")
        w()
    # Each engine is measured in its OWN CI job, so each resolves the hot_only ladder's DUID
    # independently. Same rows everywhere means the same answer — but that is an expectation, and an
    # unnoticed disagreement would make `sel_1duid*` compare two different filters across engines.
    tds = {k: v for k, v in (rep.get("top_duid") or {}).items() if v}
    if len(set(tds.values())) > 1:
        w("- ⚠ **the hot-only ladder filtered a DIFFERENT DUID per engine** — "
          + ", ".join(f"`{lbl(m)}`→`{d}`" for m, d in sorted(tds.items()))
          + ". The `sel_1duid*` rows are not comparable; pin `BENCH_TOP_DUID` and re-run.")
        w()


def s2_ranking(rep, analysis):
    ranking = analysis.get("ranking", {})
    if not ranking:
        return
    w("## 1. Headline ranking (medians; fastest wins, no tie band, no baseline)")
    w()
    w("<sub>`× fastest` is the engine's total over the fastest total of the same metric. Query wins "
      "count the rows it was strictly fastest on — an engine can win most queries and lose the "
      "total by losing the expensive one, and neither number is corrected against the other.</sub>")
    w()
    w("| metric | rank | engine | total ms | × fastest | query wins |")
    w("|:--|--:|:--|--:|--:|--:|")
    for metric, _k, _s in rr.METRICS:
        for r in ranking.get(metric, []):
            w(f"| {metric} | {r['rank']} | {r['engine']} | {_ms(r['total_ms'])} | "
              f"{_ratio(r['x_fastest'])} | {r['query_wins']} |")
    w()
    for metric, _k, _s in rr.METRICS:
        s = rr.rank_sentence(metric, ranking.get(metric), bold=True)
        if s:
            w(f"- {s}")
    w()
    noisy = sorted(_noisy_cols(rep))
    if noisy:
        cr = rep.get("run", {}).get("inputs", {}).get("cold_repeats")
        w(f"- ⚠ {len(noisy)} probe columns exceed 25% cold spread (n={cr}, shared capacity; see §2). "
          f"The headline rests on the aggregate totals above, not on any single column.")
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
              "cold ranking (§1), not per-column costs.")
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
    w("<sub>Spread does not decide a winner — the faster time wins by any margin — but a rank gap "
      "smaller than the spread beside it is not a result worth quoting. This is where a default "
      "run (cold_repeats=1, runs=3, so every spread is 0) shows itself as a smoke test.</sub>")
    w()
    w("| engine | queries | cold spread median % | cold max % | hot spread median % | hot max % |")
    w("|:--|--:|--:|--:|--:|--:|")
    for r in rows:
        w(f"| {r[0]} | {r[1]} | {_ratio(r[2])} | {_ratio(r[3])} | {_ratio(r[4])} | {_ratio(r[5])} |")
    w()


def s5_pointers(rep):
    w("## 4. Raw")
    w()
    w("- artifact `run-report`: `run_report.json` (these findings are in the CI job summary); "
      "one `report-fragment-<engine>` per engine, as each job wrote it.")
    w("- every number above recomputes from run_report.json (`timings.*`, `analysis.*`) — "
      "`RUN_REPORT=<file> python benchmark/render_report.py`, no credentials.")
    w("- physical layout per engine, and row-count parity: the *Table layout* workflow.")
    w()


def verify_ranking(rep, analysis):
    """Consistency guard over the numbers about to be printed. Returns (errors, notes).

    The old guard checked a base-vs-model verdict against the per-query median majority, because the
    ratio could be stated with the wrong orientation and name the slower engine the winner. With no
    baseline that inversion is not expressible — a rank is an argmin over the totals — so what is
    left to check is that the ranking really agrees with the timings it was derived from:

      * rank 1 must hold the LOWEST total of its metric, and ranks must ascend by total;
      * `x_fastest` must be ≥ 1, and exactly 1 at rank 1.

    Cheap, and it fails loudly rather than printing a table that contradicts itself. The
    probe-vs-aggregate divergence stays a NOTE: the marginal probe cost is a different (probe-only)
    query subset, and probes and composites can legitimately point different ways.
    """
    errs, notes = [], []

    tds = {k: v for k, v in (rep.get("top_duid") or {}).items() if v}
    if len(set(tds.values())) > 1:
        notes.append("the hot-only ladder used different DUIDs per engine ("
                     + ", ".join(f"{lbl(m)}={d}" for m, d in sorted(tds.items()))
                     + ") — sel_1duid* is not comparable; pin BENCH_TOP_DUID")

    for metric, key, _sk in rr.METRICS:
        ranking = analysis.get("ranking", {}).get(metric) or []
        if not ranking:
            continue
        totals = [r["total_ms"] for r in ranking]
        if totals != sorted(totals):
            shown = ", ".join("{}={:,.0f}".format(r["engine"], r["total_ms"]) for r in ranking)
            errs.append(f"{metric}: ranking is not ordered by total ({shown})")
            continue
        fastest = ranking[0]
        if fastest["x_fastest"] not in (None, 1.0):
            errs.append(f"{metric}: rank 1 ({fastest['engine']}) has "
                        f"x_fastest={fastest['x_fastest']}, must be 1.0")
        for r in ranking[1:]:
            if r["x_fastest"] is not None and r["x_fastest"] < 1.0:
                errs.append(f"{metric}: {r['engine']} is ranked behind {fastest['engine']} but "
                            f"x_fastest={r['x_fastest']} < 1")

    # summed marginal PROBE cost vs the aggregate COLD ranking — advisory only.
    cc = analysis.get("cold_column_cost", {})
    cold = analysis.get("ranking", {}).get("COLD") or []
    if cc and cold:
        def _cost(m):
            cols = cc.get(m, {}).get("columns")
            return (sum(cols.values()) + cc[m]["rowcount_overhead_ms"]) if cols else None
        costs = {lbl(m): _cost(m) for m in cc if _cost(m) is not None}
        if len(costs) > 1:
            cheapest = min(costs, key=costs.get)
            if cheapest != cold[0]["engine"]:
                notes.append(f"the aggregate COLD ranking puts {cold[0]['engine']} first while the "
                             f"probe-only marginal cost is lowest for {cheapest} — probes and "
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
    models = rr._order(list(rep.get("timings", {})))

    s1_header(rep)
    s2_ranking(rep, analysis)
    s3_cold_decomp(rep, analysis, models)
    s4_spread(rep, models)
    s5_pointers(rep)

    text = "\n".join(OUT) + "\n"
    gh = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh:
        with open(gh, "a", encoding="utf-8") as f:
            f.write(text)
    print(text)

    # Consistency guard — the findings are already in the job summary. A ranking that disagrees with
    # its own totals is fatal; a probe-vs-composite divergence is only a warning.
    errs, notes = verify_ranking(rep, analysis)
    for n in notes:
        print(f"::warning::{n}")
    if errs:
        for e in errs:
            print(f"::error::ranking inconsistency — {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
