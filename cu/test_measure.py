"""Offline tests for the CU ledger. No token, no network, no Fabric. ~1s.

Every rule here fails the same way when it is wrong: a plausible number, printed with confidence.
Summing instead of overwriting multiplies an hour by the number of times it was read; removing on
absence silently erases everything past the app's 14-day retention; settling too early freezes a
partial total forever. None of those raise.

    python -m pytest cu/test_measure.py -q
"""
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure  # noqa: E402

COLS = {"item_id": "Item", "workspace_id": "Workspace Id", "operation": "Operation",
        "cu": "CU (s)", "when": "Date Hour"}
WS = "WORKSPACE-1"


def row(guid, op, hour, cu, ws=WS):
    return {"Item": guid, "Workspace Id": ws, "Operation": op, "Date Hour": hour, "CU": cu}


@pytest.fixture(autouse=True)
def _workspace(monkeypatch):
    monkeypatch.setattr(measure, "WS_FILTER", WS)


def _now():
    from datetime import datetime
    return datetime(2026, 8, 2, 18, 0, 0)


def empty():
    return {"schema": 1, "updated": None, "reads": [], "cu": {}, "settled": {}}


def test_a_re_read_overwrites_an_hour_rather_than_adding_to_it():
    """An hour's CU keeps growing for ~70 minutes after the fact, so re-reading is normal and
    correct. Summing would multiply it by the number of reads — and still look plausible."""
    led = empty()
    measure.merge_rows(led, [row("G1", "Query", "2026-08-02T10:00:00", 100.0)], COLS, _now())
    measure.merge_rows(led, [row("G1", "Query", "2026-08-02T10:00:00", 140.0)], COLS, _now())
    assert led["cu"]["G1"]["Query"] == {"2026-08-02T10:00:00": 140.0}


def test_an_hour_that_stops_being_returned_keeps_its_value():
    """The app retains ~14 days. Deleting on absence would erase exactly the history this exists for."""
    led = empty()
    measure.merge_rows(led, [row("G1", "Query", "2026-08-02T10:00:00", 100.0)], COLS, _now())
    measure.merge_rows(led, [row("G1", "Query", "2026-08-02T14:00:00", 7.0)], COLS, _now())
    assert led["cu"]["G1"]["Query"] == {"2026-08-02T10:00:00": 100.0, "2026-08-02T14:00:00": 7.0}


def test_a_settled_item_is_never_rewritten():
    """Frozen means frozen: re-reading a window the app has begun to age out could only take value
    away, and the settled number is the one that was true when the item stopped accruing."""
    led = empty()
    led["cu"]["G1"] = {"Query": {"2026-08-02T10:00:00": 100.0}}
    led["settled"]["G1"] = "unchanged and quiet"
    kept, changed, skipped, _hours = measure.merge_rows(
        led, [row("G1", "Query", "2026-08-02T10:00:00", 3.0)], COLS, _now())
    assert (kept, changed, skipped) == (0, 0, 1)
    assert led["cu"]["G1"]["Query"]["2026-08-02T10:00:00"] == 100.0


def test_rows_outside_the_workspace_are_dropped():
    led = empty()
    measure.merge_rows(led, [row("G1", "Query", "2026-08-02T10:00:00", 5.0, ws="SOMEONE-ELSE")],
                       COLS, _now())
    assert led["cu"] == {}


def test_settling_needs_both_unchanged_and_quiet():
    """Unchanged alone is trivially true of an hour that has not finished smoothing and has not been
    written to yet either — freezing on it would pin a partial total."""
    led = empty()
    # Quiet (8 hours old) but CHANGED by this read: not settled.
    led["cu"]["G1"] = {"Query": {"2026-08-02T10:00:00": 100.0}}
    assert measure.settle(led, {"G1"}, _now()) == []
    # Unchanged but RECENT (one hour old, inside the 3h settle window): not settled.
    led["cu"]["G2"] = {"Query": {"2026-08-02T17:00:00": 100.0}}
    assert measure.settle(led, set(), _now()) == ["G1"]
    assert "G2" not in led["settled"]


def test_two_agreeing_reads_settle_an_item():
    """This is also what covers the missing metrics-model refresh: an item the first read could not
    see is picked up by the second, because nothing is final until two reads agree."""
    led = empty()
    rows = [row("G1", "Query", "2026-08-02T10:00:00", 100.0)]
    measure.merge_rows(led, rows, COLS, _now())
    assert measure.settle(led, {"G1"}, _now()) == []          # first read: changed
    measure.merge_rows(led, rows, COLS, _now())
    assert measure.settle(led, set(), _now()) == ["G1"]       # second read: identical, and quiet


def test_the_floor_is_the_earliest_open_run_in_the_model_clock(tmp_path, monkeypatch):
    """The record stamps UTC; the metrics model stamps its own offset. Getting this wrong reads as
    'no activity' rather than as an error."""
    from datetime import datetime, timedelta
    monkeypatch.setattr(measure, "MODEL_OFFSET", timedelta(hours=10))
    led = empty()
    runs = [{"_file": "a.json", "run": {"started": "2026-08-02T05:19:24+00:00"},
             "items": {"G1": {"role": "output"}}},
            {"_file": "b.json", "run": {"started": "2026-08-02T09:00:00+00:00"},
             "items": {"G2": {"role": "output"}}}]
    floor, guids, _why = measure.pending(runs, led, datetime(2026, 8, 2, 20, 0))
    assert floor == datetime(2026, 8, 2, 15, 0)               # 05:19 UTC -> 15:19 model -> 15:00
    assert guids == {"G1", "G2"}


def test_a_run_whose_items_are_all_settled_is_not_re_read(tmp_path):
    from datetime import datetime
    led = empty()
    led["settled"] = {"G1": "done"}
    runs = [{"_file": "a.json", "run": {"started": "2026-08-02T05:00:00+00:00"},
             "items": {"G1": {"role": "output"}}}]
    floor, guids, _why = measure.pending(runs, led, datetime(2026, 8, 2, 20, 0))
    assert (floor, guids) == (None, set())


def test_a_run_past_retention_is_force_settled_with_a_reason():
    """No further read can improve it, and leaving it open would re-query time the app has
    forgotten, forever."""
    from datetime import datetime
    led = empty()
    runs = [{"_file": "old.json", "run": {"started": "2026-06-01T05:00:00+00:00"},
             "items": {"G1": {"role": "output"}}}]
    floor, guids, why = measure.pending(runs, led, datetime(2026, 8, 2, 20, 0))
    assert (floor, guids) == (None, set())
    assert "retention" in led["settled"]["G1"]
    assert "aged out" in why["old.json"]


def test_the_ledger_round_trips_and_sorts_its_keys(tmp_path):
    """sort_keys so a re-read that moves one hour is a one-line diff in the commit."""
    p = tmp_path / "cu.json"
    led = empty()
    led["cu"] = {"B": {"Query": {"h": 1.0}}, "A": {"Query": {"h": 2.0}}}
    measure.save_ledger(led, str(p))
    assert list(json.loads(p.read_text(encoding="utf-8"))["cu"]) == ["A", "B"]
    assert measure.load_ledger(str(p))["cu"] == led["cu"]


def test_a_missing_ledger_starts_empty_rather_than_raising(tmp_path):
    led = measure.load_ledger(str(tmp_path / "nope.json"))
    assert led["cu"] == {} and led["settled"] == {} and led["schema"] == measure.SCHEMA
