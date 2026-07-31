"""Regression guards for the Direct Lake benchmark's ranking layer.

Ported from duckrun's `tests/parquet_layout/test_verdicts.py`, where the layer read a base/model
ratio with the wrong orientation and could report the FASTER layout as the loser. That whole shape is
gone: **there is no reference engine and no baseline here** — upstream genuinely had one (it built a
candidate layout and compared it against the existing one), while these four engines are peers, so a
baseline made every number depend on the order the dispatch happened to list them in.

What is pinned now:
  * the ranking is ordered by total, rank 1 is the lowest total, and `× fastest` is ≥ 1 (the direction
    guard, in the only form still expressible);
  * fastest-wins with no tie band — a 1ms win is a win, and the per-query `best` column names an
    engine rather than "tie";
  * column order is neutral and stable (alphabetical), never the dispatch's engine order and never
    the result order;
  * totals are summed over a query set every participating engine answered, and hot-only engines are
    scoped out of the COLD column rather than emptying it;
  * the analysis stays timing-only.

Pure functions only — no Fabric, no XMLA, no network. This is the free CI gate that runs before any
paid capacity is spent.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import render_report as rr          # noqa: E402
import render_summary as rs         # noqa: E402

FAST = "aemo_duckrun"
MID = "aemo_spark"
SLOW = "aemo_iceberg"
WH = "aemo_dwh"                     # used below as a HOT-ONLY fixture: a model with no cold/warm
                                    # numbers — a job that died before reporting them, or a query
                                    # that joined the session late. Not about a storage mode: all
                                    # four are Direct Lake.


def _cold(median, spread=5.0):
    """A query measured through a full session: cold (pass 1), warm (pass 2), hot (passes 3+).

    Cold and warm carry no spread key — they are single samples by construction, one first visit and
    one second visit per deployed model."""
    return {"tier": "composite", "cold_ms": median, "warm_ms": median,
            "hot_median_ms": median, "hot_spread_pct": spread}


def _hot(median, spread=5.0):
    """A query that reported HOT ONLY — no cold or warm keys at all.

    Two ways to get here and both are real: the engine's job died before the report was written, or
    the ladder joined the session at pass 2 (unpinned DUID) so it has no cold number."""
    return {"tier": "composite", "hot_median_ms": median, "hot_spread_pct": spread}


def _rep(timings, **kw):
    rep = {"timings": timings, "run": {}}
    rep.update(kw)
    return rep


# ----------------------------------------------------------------- ranking direction

def test_ranking_is_ordered_by_total_fastest_first():
    tim = {SLOW: {"q1": _cold(300), "q2": _cold(300)},
           FAST: {"q1": _cold(100), "q2": _cold(100)},
           MID: {"q1": _cold(200), "q2": _cold(200)}}
    r = rr.rank(tim, list(tim), "cold_ms")
    assert [x["engine"] for x in r] == ["duckrun", "spark", "iceberg"]
    assert [x["rank"] for x in r] == [1, 2, 3]
    assert r[0]["x_fastest"] == pytest.approx(1.0)
    assert r[1]["x_fastest"] == pytest.approx(2.0)     # 400/200 -> 2x the fastest total
    assert r[2]["x_fastest"] == pytest.approx(3.0)


def test_x_fastest_is_never_below_one():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(101)}}
    r = rr.rank(tim, list(tim), "cold_ms")
    assert all(x["x_fastest"] >= 1.0 for x in r)


def test_ranking_does_not_depend_on_the_engine_order_given():
    """The reference used to be BENCH_ENGINES[0], so reordering the dispatch reoriented every ratio.
    Nothing may depend on the order now."""
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(250)}}
    a = rr.rank(tim, [FAST, MID], "cold_ms")
    b = rr.rank(tim, [MID, FAST], "cold_ms")
    assert a == b


def test_a_one_millisecond_win_is_a_win():
    tim = {FAST: {"q1": _cold(100, spread=50)},      # huge spread, 1ms apart
           MID: {"q1": _cold(101, spread=50)}}
    r = rr.rank(tim, list(tim), "cold_ms")
    assert r[0]["engine"] == "duckrun" and r[0]["query_wins"] == 1
    assert r[1]["query_wins"] == 0


def test_an_exact_tie_gives_nobody_the_query_win():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(100)}}
    r = rr.rank(tim, list(tim), "cold_ms")
    assert [x["query_wins"] for x in r] == [0, 0]


def test_query_wins_and_the_total_may_disagree():
    """An engine can win most queries and still lose the total by losing the expensive one. Both are
    reported and neither is corrected against the other."""
    tim = {FAST: {"q1": _cold(10), "q2": _cold(10), "q3": _cold(9000)},
           MID: {"q1": _cold(20), "q2": _cold(20), "q3": _cold(100)}}
    r = rr.rank(tim, list(tim), "cold_ms")
    assert r[0]["engine"] == "spark"                  # wins the total
    assert r[0]["query_wins"] == 1
    assert r[1]["engine"] == "duckrun" and r[1]["query_wins"] == 2   # won more queries, lost anyway


def test_sidebyside_best_column_names_the_fastest_not_tie(capsys):
    """The `best` column is argmin over the row, full stop.

    Regression for a real report: the label was computed as best-vs-SECOND-best through a tie rule,
    so iceberg beating spark by 2ms printed "tie" — on a row where dwh was 4x slower than both. Every
    row of the HOT table came out "tie", which reads as "all four engines are equal" and is the
    opposite of what the numbers showed."""
    tim = {FAST: {"probe_mw": _hot(110.1, spread=30)},   # the actual numbers from that run
           WH: {"probe_mw": _hot(383.8, spread=30)},
           SLOW: {"probe_mw": _hot(106.4, spread=30)},   # fastest, by 2ms over spark
           MID: {"probe_mw": _hot(108.4, spread=30)}}
    rr._sidebyside("HOT", tim, list(tim), "hot_median_ms")
    row = [ln for ln in capsys.readouterr().out.splitlines() if "probe_mw" in ln][0]
    assert row.rstrip().endswith("| iceberg |"), row
    assert "tie" not in row


# ----------------------------------------------------------------- neutral, stable presentation

def test_column_order_is_alphabetical_not_the_input_order():
    """Neutral between peers AND stable across runs: ordering by the input list restores a
    privileged first column, and ordering by result moves the columns whenever the winner changes."""
    assert rr._order([WH, MID, FAST, SLOW]) == [FAST, WH, SLOW, MID]  # duckrun,dwh,iceberg,spark
    assert rr._order([FAST, MID]) == rr._order([MID, FAST])


def test_no_reference_helper_survives():
    """`reference()` / `BENCH_REFERENCE` are gone from the render layer and from engines.py. A
    re-added baseline would make every ratio depend on the dispatch's engine order again."""
    import engines as E
    assert not hasattr(rr, "reference")
    assert not hasattr(E, "reference")


def test_query_rows_come_from_the_most_complete_model():
    """Row order used to be `timings[base]`, which silently dropped any query the reference failed
    to answer."""
    tim = {FAST: {"q1": _cold(1)}, MID: {"q1": _cold(1), "q2": _cold(1), "q3": _cold(1)}}
    assert rr._query_order(tim, [FAST, MID]) == ["q1", "q2", "q3"]


# ----------------------------------------------------------------- consistency guard

def test_guard_is_silent_on_a_consistent_report():
    rep = _rep({FAST: {"probe_rowcount": _cold(100), "q1": _cold(100)},
                MID: {"probe_rowcount": _cold(100), "q1": _cold(200)}})
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_ranking(rep, analysis)
    assert errs == [] and notes == []


def test_guard_is_fatal_when_the_ranking_contradicts_its_totals():
    """The one thing that can fail the job: a table that names the slower engine the winner."""
    rep = _rep({FAST: {"probe_rowcount": _cold(100), "q1": _cold(100)},
                MID: {"probe_rowcount": _cold(100), "q1": _cold(300)}})
    analysis = rr.compute_analysis(rep)
    analysis["ranking"]["COLD"].reverse()            # corrupt it: slowest presented as rank 1
    errs, _notes = rs.verify_ranking(rep, analysis)
    assert errs and "not ordered by total" in errs[0]


def test_guard_is_fatal_when_x_fastest_is_below_one():
    rep = _rep({FAST: {"probe_rowcount": _cold(100), "q1": _cold(100)},
                MID: {"probe_rowcount": _cold(100), "q1": _cold(300)}})
    analysis = rr.compute_analysis(rep)
    analysis["ranking"]["COLD"][1]["x_fastest"] = 0.5   # ranked behind, yet claimed faster
    errs, _notes = rs.verify_ranking(rep, analysis)
    assert errs and "< 1" in errs[0]


def test_probe_vs_aggregate_divergence_is_a_note_not_fatal():
    """Probes and composites are different query subsets and can legitimately point different ways."""
    rep = _rep({
        FAST: {"probe_rowcount": _cold(100), "probe_mw": _cold(200),    # cheaper on the probe
               "c1": _cold(500), "c2": _cold(500), "c3": _cold(500)},   # slower on composites
        MID: {"probe_rowcount": _cold(100), "probe_mw": _cold(400),     # dearer on the probe
              "c1": _cold(300), "c2": _cold(300), "c3": _cold(300)}})   # faster overall
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_ranking(rep, analysis)
    assert errs == []
    assert notes and "diverge" in notes[0]


def test_a_disagreeing_top_duid_is_a_note_not_fatal():
    """Each engine's job resolves the hot-only ladder's DUID itself; a disagreement invalidates only
    the sel_1duid* rows, so it is reported rather than fatal."""
    rep = _rep({FAST: {"q1": _cold(100)}, MID: {"q1": _cold(200)}},
               top_duid={FAST: "ERGT01", MID: "BW01"})
    errs, notes = rs.verify_ranking(rep, rr.compute_analysis(rep))
    assert errs == []
    assert any("different DUIDs" in n for n in notes)


# ----------------------------------------------------------------- the session's shape

def test_probe_rowcount_runs_last_among_the_probes():
    """The marginal-column-cost table is `probe_<col>` minus `probe_rowcount`, and that subtraction
    only means "the cost of touching one more column" because of WHERE each query sits in the cold
    pass: every probe is the first query to touch its own column, and the rowcount control runs once
    they are all resident, so it measures ~pure overhead.

    Reorder the probes and that table silently starts measuring something else — no error, no
    missing column, just wrong numbers. Hence a test rather than a comment."""
    import xmla_compare as xc
    probes = [name for tier, name, _dax in xc.QUERIES if tier == "probe"]
    assert probes[-1] == "probe_rowcount", probes
    # and every column the decomposition subtracts it from is measured before it
    for col in rr._PROBE_COLS:
        assert probes.index(f"probe_{col}") < probes.index("probe_rowcount")


def test_the_pass_number_is_the_tier():
    """1 cold, 2 warm, 3+ hot — the whole design in one function."""
    import xmla_compare as xc
    assert [xc._tier_of(p) for p in (1, 2, 3, 6)] == ["cold", "warm", "hot", "hot"]


def test_cold_and_warm_are_single_samples_hot_is_a_distribution():
    """A session gives one first visit and one second visit, so cold and warm get no spread key —
    nothing downstream may try to noise-filter them."""
    import xmla_compare as xc
    res = xc._finalize({1: 900.0, 2: 300.0, 3: 100.0, 4: 120.0, 5: 110.0}, "probe", 1)
    assert res["cold_ms"] == 900.0 and res["warm_ms"] == 300.0
    assert "cold_spread_pct" not in res and "warm_spread_pct" not in res
    assert res["all_hot_ms"] == [100.0, 120.0, 110.0]
    assert res["hot_median_ms"] == 110.0
    assert res["hot_spread_pct"] == pytest.approx(100.0 * 20.0 / 110.0)


def test_a_query_that_joins_at_pass_2_has_no_cold_number():
    """The selectivity ladder joins the session at pass 2 when the DUID is not pinned, because
    resolving it transcodes DUID and mw — the columns probe_duid and probe_mw measure. Such a query
    must report warm and hot and simply have no cold entry, not a zero."""
    import xmla_compare as xc
    res = xc._finalize({2: 300.0, 3: 100.0, 4: 110.0}, "hot_only", 1)
    assert "cold_ms" not in res
    assert res["warm_ms"] == 300.0 and res["hot_median_ms"] == 105.0


def test_fewer_than_three_passes_yields_no_hot_tier():
    """`runs=1` or `runs=2` is a legitimate scouting dispatch; it must produce a gap, not a fake."""
    import xmla_compare as xc
    assert set(xc._finalize({1: 900.0}, "probe", 1)) == {"tier", "rows", "ms_by_pass", "cold_ms"}
    two = xc._finalize({1: 900.0, 2: 300.0}, "probe", 1)
    assert "hot_median_ms" not in two and two["warm_ms"] == 300.0


def test_nothing_touches_the_model_between_readiness_and_pass_1(monkeypatch):
    """The single most important guard here: the cold pass is only cold if nothing warms a fact
    column first.

    Two ways that has already been true and had to be fixed — the readiness probe was
    `COUNTROWS(fct_summary)`, byte-identical to `probe_rowcount`, and the top-DUID resolve
    (`TOPN` over DUID and Total MWh) ran BEFORE the measurement. Both would spend the cold pass
    before it starts, and neither would fail loudly; the numbers would just be wrong.

    So this replays a whole session against a stub connection and asserts the exact sequence."""
    import xmla_compare as xc
    seen = []

    class FakeConn:
        def Close(self):
            seen.append("CLOSE")

    monkeypatch.setattr(xc, "open_conn", lambda *a, **k: FakeConn())
    monkeypatch.setattr(xc, "_refresh", lambda *a, **k: seen.append("REFRESH"))
    monkeypatch.setattr(xc, "run_query", lambda conn, dax: (seen.append(dax), (1.0, 1))[1])
    monkeypatch.setattr(xc, "top_duid", lambda conn: (seen.append("TOP_DUID"), "ERGT01")[1])

    res, td = xc.bench_model("ws", "aemo_duckrun", "tok", 3, None)
    assert td == "ERGT01"

    before = [q for _t, _n, q in xc.resolve_queries(None)]       # pass 1: no DUID yet
    after = [q for _t, _n, q in xc.resolve_queries("ERGT01")]    # pass 2+: the full suite
    readiness = 'EVALUATE ROW("n", COUNTROWS(dim_calendar))'

    # readiness (one reframe + one probe), then pass 1, then the resolve — nothing in between.
    assert seen[:2] == ["REFRESH", readiness]
    assert seen[2:2 + len(before)] == before
    assert seen[2 + len(before)] == "TOP_DUID"
    # and the passes that follow carry the ladder
    tail = seen[3 + len(before):]
    assert tail[:len(after)] == after
    assert tail[len(after):2 * len(after)] == after
    assert tail[-1] == "CLOSE"

    # exactly ONE refresh in the whole session, and it is the readiness reframe
    assert seen.count("REFRESH") == 1
    # the readiness probe must not be a query the suite also measures
    assert readiness not in before
    # the ladder really did join late, and the queries that need no DUID did not wait
    assert len(after) - len(before) == 2
    assert "cold_ms" in res["probe_mw"] and "cold_ms" not in res["sel_1duid"]


def test_think_time_pauses_between_queries_and_is_not_measured(monkeypatch):
    """A user reads a visual before clicking the next one, so the suite does not fire back-to-back.

    Two things must hold and only one is obvious: the pause happens between EVERY consecutive pair
    of queries (including across the pass boundary — the user does not know where that is) and never
    before the first; and it sits OUTSIDE the timed region, so it changes what is reproduced and not
    what is measured. A pause inside `run_query`'s clock would add itself to every number."""
    import xmla_compare as xc
    slept, order = [], []

    class FakeConn:
        def Close(self):
            pass

    def fake_query(conn, dax):
        order.append("Q")
        return 7.0, 1                       # a fixed, small measured time

    monkeypatch.setattr(xc, "open_conn", lambda *a, **k: FakeConn())
    monkeypatch.setattr(xc, "_refresh", lambda *a, **k: None)
    monkeypatch.setattr(xc, "run_query", fake_query)
    monkeypatch.setattr(xc, "top_duid", lambda conn: "ERGT01")
    monkeypatch.setattr(xc.time, "sleep", lambda s: (slept.append(s), order.append("T"))[0])

    res, _td = xc.bench_model("ws", "aemo_duckrun", "tok", 2, "ERGT01", think_seconds=4)

    n = len(xc.resolve_queries("ERGT01")) * 2          # 2 passes, DUID pinned so nothing joins late
    assert slept == [4] * (n - 1)                      # between every pair, never before the first
    assert order[0] == "Q" and "TT" not in "".join(order)   # never two pauses back to back
    # the pause is not in the timings: every measured value is still the query's own 7.0ms
    assert res["probe_mw"]["cold_ms"] == 7.0 and res["probe_mw"]["warm_ms"] == 7.0


def test_think_time_of_zero_sleeps_not_at_all(monkeypatch):
    """Scouting dispatches set it to 0; a 0-second sleep per query would still be 150 syscalls and,
    worse, would read as "think time is on" in a trace."""
    import xmla_compare as xc
    slept = []

    class FakeConn:
        def Close(self):
            pass

    monkeypatch.setattr(xc, "open_conn", lambda *a, **k: FakeConn())
    monkeypatch.setattr(xc, "_refresh", lambda *a, **k: None)
    monkeypatch.setattr(xc, "run_query", lambda conn, dax: (7.0, 1))
    monkeypatch.setattr(xc, "top_duid", lambda conn: "ERGT01")
    monkeypatch.setattr(xc.time, "sleep", lambda s: slept.append(s))
    xc.bench_model("ws", "aemo_duckrun", "tok", 2, "ERGT01", think_seconds=0)
    assert slept == []


def test_nothing_dehydrates_any_more():
    """The per-query `clearValues` cycle is gone and must not come back: no user is ever in that
    state. `clearCache` is likewise absent — it would manufacture the strict warm state, and this
    measures a user's second visit rather than an engine state."""
    import xmla_compare as xc
    src = open(xc.__file__, encoding="utf-8").read()
    body = src.split('"""', 2)[2]          # skip the module docstring, which discusses both by name
    assert "clearValues" not in body
    assert "clearCache" not in body
    assert not hasattr(xc, "dehydrate_model")


# ----------------------------------------------------------------- scoping (port)

def test_analysis_is_timing_only():
    """No parquet/geometry analysis: physical layout is `.github/scripts/stats.py` in the
    *Table layout* workflow. Re-deriving it here would be a second, slower reader of the same Delta logs
    saying the same thing — and it is the reason this run reads nothing but the XMLA endpoint."""
    rep = _rep({FAST: {"probe_rowcount": _cold(100), "q1": _cold(100)},
                MID: {"probe_rowcount": _cold(100), "q1": _cold(200)}})
    a = rr.compute_analysis(rep)
    assert set(a) == {"cold_column_cost", "ranking"}


def test_totals_use_the_common_query_set():
    tim = {FAST: {"q1": _cold(100), "q2": _cold(100)},
           MID: {"q1": _cold(200)}}                     # answered only q1
    tot = rr._totals(tim, [FAST, MID], "cold_ms")
    assert tot == {"duckrun": 100, "spark": 200}        # q2 excluded: spark has no q2


def test_cold_totals_ignore_engines_that_reported_hot_only():
    """A model with no cold numbers — a job that died early, or `runs<2`. Letting it into the COLD
    intersection would empty the column for the engines that DO have them."""
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(300)}, WH: {"q1": _hot(50)}}
    cold = rr._totals(tim, [FAST, MID, WH], "cold_ms")
    assert cold == {"duckrun": 100, "spark": 300}       # dwh absent, others intact
    hot = rr._totals(tim, [FAST, MID, WH], "hot_median_ms")
    assert set(hot) == {"duckrun", "spark", "dwh"}      # hot: everyone participates


def test_ranking_scopes_cold_to_engines_that_measured_it():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(300)}, WH: {"q1": _hot(50)}}
    cold = rr.rank(tim, [FAST, MID, WH], "cold_ms")
    assert [x["engine"] for x in cold] == ["duckrun", "spark"]
    hot = rr.rank(tim, [FAST, MID, WH], "hot_median_ms")
    assert [x["engine"] for x in hot] == ["dwh", "duckrun", "spark"]   # 50 < 100 < 300
