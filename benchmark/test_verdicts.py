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
WH = "aemo_dwh"                     # used below as a HOT-ONLY fixture: an engine whose dehydrate
                                    # could not run (BENCH_COLD=false, or a token that cannot
                                    # refresh). Not about a storage mode — all four are Direct Lake.


def _cold(median, spread=5.0):
    return {"tier": "composite", "cold_median_ms": median, "cold_spread_pct": spread,
            "hot_median_ms": median, "hot_spread_pct": spread}


def _hot(median, spread=5.0):
    """An engine that reported HOT ONLY — no cold keys at all."""
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
    r = rr.rank(tim, list(tim), "cold_median_ms")
    assert [x["engine"] for x in r] == ["duckrun", "spark", "iceberg"]
    assert [x["rank"] for x in r] == [1, 2, 3]
    assert r[0]["x_fastest"] == pytest.approx(1.0)
    assert r[1]["x_fastest"] == pytest.approx(2.0)     # 400/200 -> 2x the fastest total
    assert r[2]["x_fastest"] == pytest.approx(3.0)


def test_x_fastest_is_never_below_one():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(101)}}
    r = rr.rank(tim, list(tim), "cold_median_ms")
    assert all(x["x_fastest"] >= 1.0 for x in r)


def test_ranking_does_not_depend_on_the_engine_order_given():
    """The reference used to be BENCH_ENGINES[0], so reordering the dispatch reoriented every ratio.
    Nothing may depend on the order now."""
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(250)}}
    a = rr.rank(tim, [FAST, MID], "cold_median_ms")
    b = rr.rank(tim, [MID, FAST], "cold_median_ms")
    assert a == b


def test_a_one_millisecond_win_is_a_win():
    tim = {FAST: {"q1": _cold(100, spread=50)},      # huge spread, 1ms apart
           MID: {"q1": _cold(101, spread=50)}}
    r = rr.rank(tim, list(tim), "cold_median_ms")
    assert r[0]["engine"] == "duckrun" and r[0]["query_wins"] == 1
    assert r[1]["query_wins"] == 0


def test_an_exact_tie_gives_nobody_the_query_win():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(100)}}
    r = rr.rank(tim, list(tim), "cold_median_ms")
    assert [x["query_wins"] for x in r] == [0, 0]


def test_query_wins_and_the_total_may_disagree():
    """An engine can win most queries and still lose the total by losing the expensive one. Both are
    reported and neither is corrected against the other."""
    tim = {FAST: {"q1": _cold(10), "q2": _cold(10), "q3": _cold(9000)},
           MID: {"q1": _cold(20), "q2": _cold(20), "q3": _cold(100)}}
    r = rr.rank(tim, list(tim), "cold_median_ms")
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


def test_floor_uses_only_quotable_columns():
    """A cheap-but-noisy column (spread>25%) must not be named the irreducible floor."""
    noisy = {"tier": "probe", "cold_median_ms": 50, "cold_spread_pct": 110,
             "hot_median_ms": 50, "hot_spread_pct": 110}     # date-like: cheap + very noisy
    stable = {"tier": "probe", "cold_median_ms": 300, "cold_spread_pct": 5,
              "hot_median_ms": 300, "hot_spread_pct": 5}
    rep = _rep({FAST: {"probe_rowcount": stable, "probe_date": noisy, "probe_mw": stable}})
    assert "date" in rs._noisy_cols(rep)                # flagged non-quotable
    assert "mw" not in rs._noisy_cols(rep)


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
    tot = rr._totals(tim, [FAST, MID], "cold_median_ms")
    assert tot == {"duckrun": 100, "spark": 200}        # q2 excluded: spark has no q2


def test_cold_totals_ignore_engines_that_reported_hot_only():
    """An engine whose dehydrate could not run has no cold numbers at all. Letting it into the COLD
    intersection would empty the column for the engines that DO have them."""
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(300)}, WH: {"q1": _hot(50)}}
    cold = rr._totals(tim, [FAST, MID, WH], "cold_median_ms")
    assert cold == {"duckrun": 100, "spark": 300}       # dwh absent, others intact
    hot = rr._totals(tim, [FAST, MID, WH], "hot_median_ms")
    assert set(hot) == {"duckrun", "spark", "dwh"}      # hot: everyone participates


def test_ranking_scopes_cold_to_engines_that_measured_it():
    tim = {FAST: {"q1": _cold(100)}, MID: {"q1": _cold(300)}, WH: {"q1": _hot(50)}}
    cold = rr.rank(tim, [FAST, MID, WH], "cold_median_ms")
    assert [x["engine"] for x in cold] == ["duckrun", "spark"]
    hot = rr.rank(tim, [FAST, MID, WH], "hot_median_ms")
    assert [x["engine"] for x in hot] == ["dwh", "duckrun", "spark"]   # 50 < 100 < 300
