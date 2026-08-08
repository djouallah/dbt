"""Offline tests for the warehouse V-Order read. No Fabric, no ODBC driver, no credentials.

WHY THIS IS WORTH PINNING. The value it records is the ONLY V-Order signal that can see a Fabric
Warehouse — the other two are Spark-shaped (a `TBLPROPERTIES` key and the Spark writer's per-file
`add.tags.VORDER`) and both read `false` for dwh against parquet that was V-Ordered throughout. So
this one number is what stops the page repeating that mistake, and every way it can go wrong is
silent:

- a `0` misread as "no answer" would turn a warehouse someone deliberately disabled into an absent
  key, and the page would fall back to the blind property and print V-Order anyway;
- a missing row misread as `False` would assert the opposite of the default on a run that could not
  be measured at all;
- a write into `layout.config` instead of `layout.ordering` would split dwh's dashboard column and its
  layout bar on a value that is measured rather than configured.

    python -m pytest .github/scripts/test_dwh_vorder.py -q
"""
import json
import os
import struct
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import dwh_vorder  # noqa: E402


class Cur:
    def __init__(self, row):
        self.row, self.sql = row, None

    def execute(self, q):
        self.sql = q

    def fetchone(self):
        return self.row


class Con:
    """A stub connection — the interesting failures are in reading the row, not in the network."""

    def __init__(self, row):
        self.cur = Cur(row)
        self.closed = False

    def cursor(self):
        return self.cur

    def close(self):
        self.closed = True


@pytest.mark.parametrize("row,want", [
    ((1,), True),                 # the documented "enabled"
    ((0,), False),                # someone ran the irreversible ALTER — a real answer, not a no-answer
    ((True,), True),
    ((False,), False),
    (None, None),                 # no row: nobody could ask, which is not the same as disabled
    ((None,), None),              # column present but NULL
])
def test_the_row_is_read_without_collapsing_zero_into_no_answer(row, want):
    """`0` and `None` are DIFFERENT and a truthiness test would fold them together. `0` means V-Order
    was disabled and must beat the blind table property on the page; `None` means unmeasured and must
    leave the key absent so the page falls back instead of asserting."""
    assert dwh_vorder.read_vorder(Con(row)) is want


def test_it_asks_sys_databases_for_this_database_only():
    """`DB_NAME()` rather than a literal name: the flag is per warehouse and a connection landing
    somewhere unexpected must not report a sibling's state as this run's."""
    con = Con((1,))
    dwh_vorder.read_vorder(con)
    assert "sys.databases" in con.cur.sql
    assert "is_vorder_enabled" in con.cur.sql
    assert "DB_NAME()" in con.cur.sql


def test_the_token_is_encoded_exactly_as_the_adapter_encodes_it():
    """THE FIRST VERSION OF THIS FILE USED pyodbc AND WOULD HAVE FAILED EVERY RUN. The dwh leg pins
    `dbt-fabric-samdebruyn`, whose dependency is `mssql-python` — pyodbc is not installed there, and
    because this step is best-effort the failure would have been silent: no key in the record, and the
    page falling back to the blind property exactly as before.

    So the connection is the risky part, and the token encoding is the half that can be checked
    offline. The adapter builds it by interleaving zero bytes
    (`fabric_token_provider.py:224-227`); this asserts our `utf-16-le` spelling is byte-identical, so
    a future reader cannot "tidy" either one into something the driver rejects."""
    from itertools import chain, repeat
    tok = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.payload.sig"
    adapter_bytes = bytes(chain.from_iterable(zip(bytes(tok, "UTF-8"), repeat(0))))
    attrs = dwh_vorder.token_attrs(tok)
    assert list(attrs) == [1256], "SQL_COPT_SS_ACCESS_TOKEN, the adapter's own constant"
    assert attrs[1256] == struct.pack("<i", len(adapter_bytes)) + adapter_bytes
    # The length prefix counts the ENCODED bytes, not the characters — twice the JWT's length.
    assert struct.unpack("<i", attrs[1256][:4])[0] == 2 * len(tok)


def test_a_failure_records_nothing_and_never_fails_the_leg(tmp_path, monkeypatch):
    """Best-effort, like every other layout signal. A build that succeeded must not go red because a
    metadata query did — and the key must be ABSENT rather than `false`, because `false` is a claim
    (V-Order was disabled) and absence is the truth (nobody could ask)."""
    rec = tmp_path / "record-20-build-dwh.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.setattr(dwh_vorder, "connect", lambda: (_ for _ in ()).throw(RuntimeError("no driver")))
    assert dwh_vorder.main(["dwh_vorder.py", "dwh"]) == 0
    assert not rec.exists(), "nothing measured -> nothing written, not a false"

    # A connection that opens but answers nothing is the same outcome.
    monkeypatch.setattr(dwh_vorder, "connect", lambda: Con(None))
    assert dwh_vorder.main(["dwh_vorder.py", "dwh"]) == 0
    assert not rec.exists()


def test_it_lands_in_layout_ordering_and_never_in_layout_config(tmp_path, monkeypatch):
    """`layout.config` is walked by the dashboard's `variant()` into a column name, so a MEASURED
    value there would split dwh's column and its layout bar. `layout.ordering` is the sibling that
    exists for measurements — and it must deep-merge with `stats.py`'s own `ordering.dwh`, which
    arrives in a LATER fragment (`-30-layout` after `-20-build`)."""
    rec = tmp_path / "record-20-build-dwh.json"
    monkeypatch.setenv("RUN_RECORD", str(rec))
    monkeypatch.setattr(dwh_vorder, "connect", lambda: Con((1,)))
    assert dwh_vorder.main(["dwh_vorder.py", "dwh"]) == 0
    doc = json.loads(rec.read_text(encoding="utf-8"))
    assert doc["layout"]["ordering"]["dwh"]["vorder_enabled"] is True
    assert "config" not in doc["layout"], "a measured value must not reach layout.config"

    # The union with the layout fragment's own ordering block keeps both — `record.deep_update`
    # unions dicts, which is why `items`/`ordering` are dicts and never lists.
    import record
    record.deep_update(doc, {"layout": {"ordering": {"dwh": {"table": "mart.fct_summary"}}}})
    assert doc["layout"]["ordering"]["dwh"] == {"vorder_enabled": True, "table": "mart.fct_summary"}


def test_run_record_unset_is_a_no_op_so_this_stays_runnable_by_hand(tmp_path, monkeypatch, capsys):
    """Same rule as `provision.py` and `stats.py`: reproducing a CI reading from a laptop must not
    need a record path."""
    monkeypatch.delenv("RUN_RECORD", raising=False)
    monkeypatch.setattr(dwh_vorder, "connect", lambda: Con((0,)))
    assert dwh_vorder.main(["dwh_vorder.py", "dwh"]) == 0
    assert "is_vorder_enabled = False" in capsys.readouterr().err
