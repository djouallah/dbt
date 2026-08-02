"""Offline tests for the CU ledger. No token, no network, no Fabric. ~1s.

Every rule here fails the same way when it is wrong: a plausible number, printed with confidence.
Adding instead of taking the max multiplies an item's cost by how often it was read. Overwriting
blindly lets a truncated window erase a complete total. Neither raises.

    python -m pytest cu/test_measure.py -q
"""
import json
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import measure  # noqa: E402

COLS = {"item_id": "Item Id", "workspace_id": "Workspace Id", "cu": "CU (s)",
        "when": "Datetime", "operation": "Operation name"}
WS = "WORKSPACE-1"
NOW = datetime(2026, 8, 2, 18, 0, 0)


def row(guid, value, op="Warehouse Query", first="2026-08-02T15:00:00", ws=WS):
    return {"Item Id": guid, "Workspace Id": ws, "Operation name": op, "CU": value,
            "FirstHour": first}


def cu(led, guid):
    """The item's total, which is what most of these tests are really about."""
    return measure.total(led, guid)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(measure, "WS_FILTER", WS)
    monkeypatch.setattr(measure, "MODEL_OFFSET", timedelta(hours=10))


def read(led, rows):
    """One full read: fold the rows, merge them in. Returns how many items moved."""
    folded, _stamps = measure.fold(rows, COLS)
    return measure.apply(led, folded)


def test_one_row_per_item_is_taken_as_that_item_s_total():
    led = measure.blank()
    read(led, [row("G1", 31080.4), row("G2", 2041.0)])
    assert cu(led, "G1") == 31080.4 and cu(led, "G2") == 2041.0


def test_a_re_read_of_the_same_number_changes_nothing():
    """Idempotent, so dispatching the dashboard twice costs a query and produces no diff."""
    led = measure.blank()
    read(led, [row("G1", 100.0)])
    assert read(led, [row("G1", 100.0)]) == 0
    assert cu(led, "G1") == 100.0


def test_an_undercounted_first_read_is_raised_by_the_next_one():
    """An hour keeps growing for ~70 minutes after the fact, so the first read after a run is a
    lower bound. Nothing has to be reconciled — the bigger number simply wins."""
    led = measure.blank()
    read(led, [row("G1", 40.0)])
    read(led, [row("G1", 125.0)])
    assert cu(led, "G1") == 125.0


def test_a_smaller_later_read_never_lowers_a_total():
    """The floor walks forward as old runs age out, so a later query can cover LESS of an item's
    life than an earlier one did. max() keeps the fuller number; a blind overwrite would throw it
    away and look perfectly reasonable doing it."""
    led = measure.blank()
    read(led, [row("G1", 125.0)])
    assert read(led, [row("G1", 4.0)]) == 0
    assert cu(led, "G1") == 125.0


def test_repeated_reads_never_accumulate():
    """The query already summed server-side, so what came back IS the total. Adding would multiply
    it by the number of reads."""
    led = measure.blank()
    for _ in range(5):
        read(led, [row("G1", 100.0)])
    assert cu(led, "G1") == 100.0


def test_an_item_absent_from_a_read_keeps_its_value():
    """Past retention an item stops being returned. Deleting on absence would erase exactly the
    history this ledger exists to keep."""
    led = measure.blank()
    read(led, [row("G1", 100.0), row("G2", 7.0)])
    read(led, [row("G2", 9.0)])
    assert cu(led, "G1") == 100.0 and cu(led, "G2") == 9.0


def test_rows_outside_the_workspace_are_dropped():
    led = measure.blank()
    read(led, [row("G1", 5.0, ws="SOMEONE-ELSE")])
    assert led["items"] == {}


def test_the_floor_is_the_earliest_run_start_in_the_model_clock():
    """The record stamps UTC; the metrics model stamps its own offset. Getting this wrong reads as
    'no activity' rather than as an error."""
    runs = [{"run": {"started": "2026-08-02T09:00:00+00:00"}},
            {"run": {"started": "2026-08-02T05:19:24+00:00"}}]
    # 05:19 UTC -> 15:19 model -> floored to 15:00.
    assert measure.floor_for(runs, NOW) == datetime(2026, 8, 2, 15, 0)


def test_the_floor_is_clamped_to_retention():
    """Reading further back returns nothing — the app has forgotten it — and an unbounded floor
    would grow the query for the life of the repo."""
    runs = [{"run": {"started": "2026-01-01T00:00:00+00:00"}}]
    floor = measure.floor_for(runs, NOW)
    assert floor == datetime(2026, 7, 19, 18, 0), "14 days before now, on the hour"


def test_no_runs_still_yields_a_bounded_floor():
    assert measure.floor_for([], NOW) == datetime(2026, 7, 19, 18, 0)


def test_the_ledger_round_trips_and_sorts_its_keys(tmp_path):
    """sort_keys so a read that moves one number is a one-line diff in the commit."""
    p = tmp_path / "cu.json"
    led = measure.blank()
    led["items"] = {"B": {"Query": 1.0}, "A": {"Query": 2.0}}
    measure.save_ledger(led, str(p))
    assert list(json.loads(p.read_text(encoding="utf-8"))["items"]) == ["A", "B"]
    assert measure.load_ledger(str(p))["items"] == {"A": {"Query": 2.0}, "B": {"Query": 1.0}}


def test_a_missing_ledger_starts_empty_rather_than_raising(tmp_path):
    led = measure.load_ledger(str(tmp_path / "nope.json"))
    assert led["items"] == {} and led["schema"] == measure.SCHEMA


def test_coverage_names_the_recorded_items_a_read_did_not_find():
    """The standing check on the one assumption the no-refresh design rests on: the metrics FACT
    table is DirectQuery, so a brand-new item GUID should be summable without the model being
    refreshed to catalogue it. A run whose items are all found minutes after it ended proves it; one
    whose items are still missing hours later disproves it. Either way the log says so."""
    runs = [{"_file": "r.json", "items": {
        "OUT": {"role": "output", "name": "dbt_spark"},
        "SEM": {"role": "semantic_model", "name": "aemo_spark"},
        "F": {"role": "folder", "name": "dbt"}}}]
    assert measure.coverage(runs, {"OUT": 1.0, "SEM": 2.0}) == [("r.json", 2, [])]
    # A folder never accrues a capacity unit, so its absence means nothing and must not be reported.
    assert measure.coverage(runs, {"OUT": 1.0}) == [("r.json", 1, ["semantic_model/aemo_spark"])]


def test_compute_and_storage_are_kept_apart_within_one_item():
    """The whole reason the operation is still in the grain: a spark lakehouse bills its Livy session
    and its OneLake reads against the SAME GUID, and no per-item total can separate them."""
    led = measure.blank()
    read(led, [row("G1", 188635.8, op="High Concurrency Session Livy Run"),
               row("G1", 20267.9, op="OneLake Write via Redirect")])
    assert led["items"]["G1"] == {"High Concurrency Session Livy Run": 188635.8,
                                  "OneLake Write via Redirect": 20267.9}
    assert round(cu(led, "G1"), 1) == 208903.7


def test_a_ledger_written_before_operations_is_dropped_not_guessed(tmp_path):
    """An entry stored as one NUMBER mixes compute and storage, so it cannot be bucketed. Filing it
    under a guessed operation would put storage-heavy items in the compute half; dropping it is
    safe, because the floor reaches back to the earliest run and the next read repopulates it."""
    p = tmp_path / "cu.json"
    p.write_text(json.dumps({"schema": 1, "items": {"OLD": 123.4, "NEW": {"Query": 5.0}}}),
                 encoding="utf-8")
    led = measure.load_ledger(str(p))
    assert led["items"] == {"NEW": {"Query": 5.0}}
