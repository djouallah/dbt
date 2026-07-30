"""Regression guards for the Direct Lake benchmark verdict layer.

Ported from duckrun's `tests/parquet_layout/test_verdicts.py`. The verdict layer once read a
base/model ratio with the wrong orientation, so a lower total (the FASTER layout) could be reported
as the loser. These tests pin the direction so it can't recur: the winner is always the lower-total
engine, ratios are oriented base÷model, and the summed-cost winner must equal the verdict winner.

The port added its own risks, so they are pinned here too: reference selection is now explicit
(no name-length heuristic), the analysis must stay timing-only, DirectQuery must be labelled, and a
column of totals must be summed over a query set every participating engine answered.

Pure functions only — no Fabric, no XMLA, no network. This is the free CI gate that runs before any
paid capacity is spent.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import render_report as rr          # noqa: E402
import render_summary as rs         # noqa: E402

REF = "aemo_duckrun"                # the reference engine in these fixtures
CHAL = "aemo_spark"                 # a Direct Lake challenger
DQ = "aemo_dwh"                     # the DirectQuery engine


def _cold(median, spread=5.0):
    return {"tier": "composite", "cold_median_ms": median, "cold_spread_pct": spread,
            "hot_median_ms": median, "hot_spread_pct": spread}


def _hot(median, spread=5.0):
    """Hot-only, as a DirectQuery engine reports: no cold keys at all."""
    return {"tier": "composite", "hot_median_ms": median, "hot_spread_pct": spread}


def _rep(timings, **kw):
    rep = {"timings": timings, "run": {"reference": rr._short(REF)}}
    rep.update(kw)
    return rep


# ----------------------------------------------------------------- ratio orientation (upstream)

def test_ratio_orientation_base_slower_loses():
    """base is the SLOWER engine (higher total) -> the verdict must say base loses (model wins)."""
    base_t = {"q1": _cold(200), "q2": _cold(200)}      # base total 400 (slow)
    model_t = {"q1": _cold(100), "q2": _cold(100)}     # model total 200 (fast)
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["verdict"] == "model"                     # faster (lower total) wins
    assert v["ratio"] == pytest.approx(2.0)            # base/model = 400/200
    assert v["wins"] == 2 and v["losses"] == 0


def test_ratio_orientation_base_faster_wins():
    base_t = {"q1": _cold(100), "q2": _cold(100)}      # base fast
    model_t = {"q1": _cold(200), "q2": _cold(200)}     # model slow
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["verdict"] == "base"
    assert v["ratio"] == pytest.approx(0.5)            # base/model = 100/200 < 1 => base faster


def test_within_spread_is_a_tie_not_a_win():
    base_t = {"q1": _cold(100, spread=30)}             # 30% spread
    model_t = {"q1": _cold(104, spread=30)}            # 4% apart << 30% noise
    v = rr._agg_verdict(base_t, model_t, "cold_median_ms", "cold_spread_pct")
    assert v["ties"] == 1 and v["wins"] == 0 and v["losses"] == 0
    assert v["verdict"] == "tie"


# ----------------------------------------------------------------- direction guard (upstream)

def test_verify_verdicts_passes_when_consistent():
    """Direction guard is silent when the verdict agrees with the per-query median majority."""
    rep = _rep({REF: {"probe_rowcount": _cold(100), "q1": _cold(100), "q2": _cold(100)},
                CHAL: {"probe_rowcount": _cold(100), "q1": _cold(200), "q2": _cold(200)}})
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_verdicts(rep, analysis)     # reference faster everywhere, consistent
    assert errs == [] and notes == []


def test_verify_verdicts_flags_a_true_inversion():
    """Fatal only when the verdict disagrees with the SAME-query median majority."""
    rep = _rep({REF: {"probe_rowcount": _cold(100), "q1": _cold(100), "q2": _cold(100)},
                CHAL: {"probe_rowcount": _cold(100), "q1": _cold(300), "q2": _cold(300)}})
    analysis = rr.compute_analysis(rep)
    # the reference is clearly faster; corrupt the verdict to claim the challenger won.
    for v in analysis["verdicts"]:
        if v["metric"] == "COLD":
            v["verdict"] = "model"
    errs, notes = rs.verify_verdicts(rep, analysis)
    assert errs and "verdict says spark" in errs[0]


def test_probe_vs_composite_divergence_is_a_note_not_fatal():
    """When the verdict agrees with the median majority but the probe-only cost points the other
    way (composites and probes diverge), it must be a non-fatal note — not a build failure."""
    rep = _rep({
        REF: {"probe_rowcount": _cold(100), "probe_mw": _cold(200),    # base cheaper on the probe
              "c1": _cold(500), "c2": _cold(500), "c3": _cold(500)},   # base slower on composites
        CHAL: {"probe_rowcount": _cold(100), "probe_mw": _cold(400),   # challenger dearer on probe
               "c1": _cold(300), "c2": _cold(300), "c3": _cold(300)}}) # challenger faster elsewhere
    analysis = rr.compute_analysis(rep)
    errs, notes = rs.verify_verdicts(rep, analysis)
    assert errs == []                                   # not fatal
    assert notes and "diverge" in notes[0]               # surfaced as a note


def test_floor_uses_only_quotable_columns():
    """A cheap-but-noisy column (spread>25%) must not be named the irreducible floor."""
    noisy = {"tier": "probe", "cold_median_ms": 50, "cold_spread_pct": 110,
             "hot_median_ms": 50, "hot_spread_pct": 110}     # date-like: cheap + very noisy
    stable = {"tier": "probe", "cold_median_ms": 300, "cold_spread_pct": 5,
              "hot_median_ms": 300, "hot_spread_pct": 5}
    rep = _rep({REF: {"probe_rowcount": stable, "probe_date": noisy, "probe_mw": stable}})
    assert "date" in rs._noisy_cols(rep)                # flagged non-quotable
    assert "mw" not in rs._noisy_cols(rep)


# ----------------------------------------------------------------- reference selection (port)

def test_reference_comes_from_the_report_not_name_length():
    """Upstream picked the base by name (`endswith('_auto_sort')`, else SHORTEST name). With one
    model per engine that heuristic would silently make `aemo_dwh` the reference — it is the
    shortest — and reorient every ratio in the report."""
    rep = _rep({DQ: _hot(100), REF: {"q1": _cold(100)}, CHAL: {"q1": _cold(100)}})
    assert rr.reference(rep, [DQ, REF, CHAL]) == REF
    assert min([DQ, REF, CHAL], key=len) == DQ          # the trap the explicit reference avoids


def test_reference_falls_back_to_first_model_when_unrecorded():
    rep = {"timings": {}, "run": {}}
    assert rr.reference(rep, [CHAL, REF]) in (CHAL, REF)
    assert rr.reference(rep, []) is None


def test_verdicts_are_all_taken_against_the_one_reference():
    rep = _rep({REF: {"q1": _cold(100)}, CHAL: {"q1": _cold(200)}, DQ: {"q1": _hot(300)}})
    analysis = rr.compute_analysis(rep)
    assert {v["base"] for v in analysis["verdicts"]} == {"duckrun"}
    assert {v["model"] for v in analysis["verdicts"]} == {"spark", "dwh"}


# ----------------------------------------------------------------- DirectQuery handling (port)

def test_analysis_is_timing_only():
    """No parquet/geometry analysis: physical layout is `.github/scripts/stats.py` in ci.yml's
    `summary` job. Re-deriving it here would be a second, slower reader of the same Delta logs
    saying the same thing — and it is the reason this run reads nothing but the XMLA endpoint."""
    rep = _rep({REF: {"probe_rowcount": _cold(100), "q1": _cold(100)},
                CHAL: {"probe_rowcount": _cold(100), "q1": _cold(200)}})
    a = rr.compute_analysis(rep)
    assert set(a) == {"cold_column_cost", "verdicts"}


def test_directquery_verdict_carries_its_mode():
    """The report must be able to label a DirectQuery number as such — otherwise the summary reads
    as 'dwh has a slow layout' about a query that never touched the files."""
    rep = _rep({REF: {"q1": _cold(100)}, DQ: {"q1": _hot(400)}},
               engines={"duckrun": {"mode": "directLake"}, "dwh": {"mode": "directQuery"}})
    a = rr.compute_analysis(rep)
    dq = [v for v in a["verdicts"] if v["model"] == "dwh"]
    assert dq and all(v["model_mode"] == "directQuery" for v in dq)
    assert all(v["base_mode"] == "directLake" for v in dq)


# ----------------------------------------------------------------- comparable totals (port)

def test_totals_use_the_common_query_set():
    """With more than one challenger, each pairwise verdict sums over its OWN challenger's queries,
    so those totals are not mutually comparable. The Summary column must use the intersection."""
    tim = {REF: {"q1": _cold(100), "q2": _cold(100)},
           CHAL: {"q1": _cold(200)}}                    # answered only q1
    tot = rr._totals(tim, [REF, CHAL], "cold_median_ms")
    assert tot == {"duckrun": 100, "spark": 200}        # q2 excluded: spark has no q2


def test_cold_totals_ignore_engines_with_no_cold_tier():
    """A DirectQuery engine has no cold numbers. Letting it into the COLD intersection would empty
    the column for the engines that DO have them."""
    tim = {REF: {"q1": _cold(100)}, CHAL: {"q1": _cold(300)}, DQ: {"q1": _hot(50)}}
    cold = rr._totals(tim, [REF, CHAL, DQ], "cold_median_ms")
    assert cold == {"duckrun": 100, "spark": 300}       # dwh absent, others intact
    hot = rr._totals(tim, [REF, CHAL, DQ], "hot_median_ms")
    assert set(hot) == {"duckrun", "spark", "dwh"}      # hot: everyone participates
