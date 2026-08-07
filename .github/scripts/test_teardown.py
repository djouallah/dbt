"""Offline tests for `provision.py teardown`, against a stubbed Fabric. No network, no token.

This is a DELETE path with no undo, so it is exercised before it is dispatched. What matters is not
that it deletes — it is what it refuses to delete, and whether it notices when a delete did not take.

`provision.py` does its work at import time (mode dispatch is module-level), so each test imports it
fresh with a stubbed `requests`, a stubbed token and its own argv. Ugly, but it tests the real
module rather than a copy of its logic, which for a delete path is the trade worth making.

    python -m pytest .github/scripts/test_teardown.py -q
"""
import importlib
import json
import os
import sys
import types

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

WS = "00000000-0000-0000-0000-0000000000ws"
# The two workspace folders: `benchmark` for everything a run creates and `landing` for the one
# lakehouse that outlives it. provision.py used to resolve BOTH at import, in every mode — see
# test_the_teardown_never_reads_folders for what that cost. They are resolved on first use now, so
# these are here for the provisioning paths and a teardown should never ask for them.
FOLDERS = [{"id": "BENCH-FOLDER", "displayName": "benchmark"},
           {"id": "LANDING-FOLDER", "displayName": "landing"}]


class Resp:
    def __init__(self, status=200, payload=None):
        self.status_code = status
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise AssertionError(f"unexpected raise_for_status on {self.status_code}")


class Fabric:
    """A workspace holding items by GUID. Records every DELETE it is asked for."""

    def __init__(self, items, undeletable=()):
        self.items = dict(items)                 # guid -> displayName
        self.undeletable = set(undeletable)      # guids whose DELETE is accepted but never takes
        self.deletes = []
        self.folder_gets = 0                     # see test_the_teardown_never_reads_folders

    def get(self, url, headers=None, **kw):
        if url.endswith("/folders"):
            self.folder_gets += 1
            return Resp(200, {"value": FOLDERS})
        if "/items/" in url:
            guid = url.rsplit("/", 1)[1]
            return Resp(200, {"id": guid}) if guid in self.items else Resp(404)
        raise AssertionError(f"unexpected GET {url}")

    def delete(self, url, headers=None, **kw):
        guid = url.rsplit("/", 1)[1]
        self.deletes.append(guid)
        if guid in self.undeletable:
            return Resp(202)                     # accepted, and the item stays listed
        self.items.pop(guid, None)
        return Resp(200)

    def post(self, url, headers=None, **kw):
        raise AssertionError(f"teardown must not POST: {url}")


def run_teardown(tmp_path, monkeypatch, items, record_items, undeletable=()):
    """Import provision.py in `teardown` mode against a stubbed Fabric; return (fabric, record)."""
    fab = Fabric(items, undeletable)
    fake_requests = types.SimpleNamespace(get=fab.get, delete=fab.delete, post=fab.post)
    auth = types.ModuleType("duckrun.auth")
    auth.get_fabric_token = lambda: "TEST-TOKEN"
    monkeypatch.setitem(sys.modules, "requests", fake_requests)
    monkeypatch.setitem(sys.modules, "duckrun", types.ModuleType("duckrun"))
    monkeypatch.setitem(sys.modules, "duckrun.auth", auth)
    monkeypatch.setattr("time.sleep", lambda *_: None)

    src = tmp_path / "sofar.json"
    src.write_text(json.dumps({"items": record_items}), encoding="utf-8")
    out = tmp_path / "frag.json"
    monkeypatch.setenv("WS_ID", WS)
    monkeypatch.setenv("RUN_RECORD", str(out))
    monkeypatch.setattr(sys, "argv", ["provision.py", "teardown", str(src)])
    sys.modules.pop("provision", None)
    importlib.import_module("provision")
    written = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    return fab, written.get("items", {})


def test_deletes_the_run_s_items_and_records_each_deletion(tmp_path, monkeypatch):
    fab, rec = run_teardown(
        tmp_path, monkeypatch,
        items={"OUT": "dbt_spark", "SRC": "dbt_dwh_src", "SEM": "aemo_spark", "LAND": "dbt_landing"},
        record_items={
            "OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"},
            "SRC": {"role": "dwh_src", "kind": "Lakehouse", "name": "dbt_dwh_src"},
            "SEM": {"role": "semantic_model", "kind": "SemanticModel", "name": "aemo_spark"},
        })
    assert sorted(fab.deletes) == ["OUT", "SEM", "SRC"]
    assert all(rec[g]["deleted"] for g in ("OUT", "SEM", "SRC"))


def test_landing_and_the_folder_are_never_touched(tmp_path, monkeypatch):
    """dbt_landing holds the downloaded AEMO archive — the one thing here that cannot be rebuilt
    from the workspace. A folder holds no data and costs nothing."""
    fab, _ = run_teardown(
        tmp_path, monkeypatch,
        items={"LAND": "dbt_landing", "BENCH-FOLDER": "benchmark", "OUT": "dbt_spark"},
        record_items={
            "LAND": {"role": "landing", "kind": "Lakehouse", "name": "dbt_landing"},
            "BENCH-FOLDER": {"role": "folder", "kind": "Folder", "name": "benchmark"},
            "OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"},
        })
    assert fab.deletes == ["OUT"]


def test_an_item_not_in_the_record_is_not_deleted(tmp_path, monkeypatch):
    """The safety property. Teardown deletes by GUID from THIS run's record, so a concurrent
    dispatch's freshly created dbt_spark cannot be caught by a name match."""
    fab, _ = run_teardown(
        tmp_path, monkeypatch,
        items={"MINE": "dbt_spark", "SOMEONE-ELSES": "dbt_spark"},
        record_items={"MINE": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"}})
    assert fab.deletes == ["MINE"]


def test_an_already_deleted_item_is_skipped_not_re_deleted(tmp_path, monkeypatch):
    """An entry that already carries a `deleted` timestamp is not re-deleted — whoever wrote it
    (a re-run of the teardown, a by-hand cleanup) already confirmed the item is gone."""
    fab, _ = run_teardown(
        tmp_path, monkeypatch, items={},
        record_items={"NB": {"role": "compute", "kind": "Notebook", "name": "dbt-spark-ab12",
                             "deleted": "2026-08-02T10:00:00+00:00"}})
    assert fab.deletes == []


def test_a_404_counts_as_deleted(tmp_path, monkeypatch):
    """Already gone is the outcome wanted, whoever got there first."""
    fab, rec = run_teardown(
        tmp_path, monkeypatch, items={},
        record_items={"OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"}})
    assert fab.deletes == ["OUT"] and rec["OUT"]["deleted"]


def test_an_item_that_survives_its_delete_fails_the_job(tmp_path, monkeypatch):
    """A DELETE is accepted asynchronously, and an item still listed is still BILLABLE. Failing
    loudly is the point — a leftover lakehouse costs capacity silently otherwise."""
    with pytest.raises(SystemExit) as ex:
        run_teardown(tmp_path, monkeypatch,
                     items={"OUT": "dbt_spark", "SEM": "aemo_spark"},
                     record_items={
                         "OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"},
                         "SEM": {"role": "semantic_model", "kind": "SemanticModel",
                                 "name": "aemo_spark"}},
                     undeletable={"OUT"})
    assert "STILL BILLABLE" in str(ex.value) and "dbt_spark" in str(ex.value)


def test_the_other_items_are_still_deleted_when_one_refuses(tmp_path, monkeypatch):
    """A warehouse that will not delete must not leave the lakehouses standing behind it: the
    failures are collected and raised at the end, not one at a time."""
    with pytest.raises(SystemExit):
        run_teardown(tmp_path, monkeypatch,
                     items={"OUT": "dbt_dwh", "SEM": "aemo_dwh"},
                     record_items={
                         "OUT": {"role": "output", "kind": "Warehouse", "name": "dbt_dwh"},
                         "SEM": {"role": "semantic_model", "kind": "SemanticModel",
                                 "name": "aemo_dwh"}},
                     undeletable={"OUT"})
    frag = json.loads((tmp_path / "frag.json").read_text(encoding="utf-8"))["items"]
    assert "deleted" in frag["SEM"], "the semantic model should have gone down anyway"
    assert "OUT" not in frag, "an item that survived must not be recorded as deleted"


def test_landing_is_refused_even_if_its_role_is_wrong(tmp_path, monkeypatch):
    """Belt and braces: the role decides what is skipped, the NAME is a second, independent
    refusal. A fragment that mislabelled dbt_landing would otherwise delete the archive."""
    with pytest.raises(SystemExit) as ex:
        run_teardown(tmp_path, monkeypatch, items={"LAND": "dbt_landing"},
                     record_items={"LAND": {"role": "output", "kind": "Lakehouse",
                                            "name": "dbt_landing"}})
    assert "refusing to drop dbt_landing" in str(ex.value)


def test_the_sql_endpoint_is_recorded_but_never_deleted(tmp_path, monkeypatch):
    """Fabric creates a SQL analytics endpoint alongside every lakehouse and removes it WITH the
    lakehouse, so a DELETE here would either fail or race the parent's. Its CU is real — 245-306 CU
    per engine of `SQL Endpoint Query`, invisible until it was measured — so it is recorded and then
    left alone."""
    fab, rec = run_teardown(
        tmp_path, monkeypatch,
        items={"OUT": "dbt_spark", "EP": "dbt_spark"},
        record_items={"OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"},
                      "EP": {"role": "sql_endpoint", "kind": "Warehouse", "name": "dbt_spark"}})
    assert fab.deletes == ["OUT"], "the endpoint goes down with its lakehouse, not separately"
    assert "EP" not in rec


def test_both_folders_survive_the_teardown(tmp_path, monkeypatch):
    """`benchmark` holds what a run creates and `landing` holds what outlives it, and neither is
    ever deleted — a folder holds no data and costs nothing, and deleting the one landing lives in
    would be the same mistake as deleting landing."""
    fab, _ = run_teardown(
        tmp_path, monkeypatch,
        items={"BENCH-FOLDER": "benchmark", "LANDING-FOLDER": "landing", "OUT": "dbt_spark"},
        record_items={
            "BENCH-FOLDER": {"role": "folder", "kind": "Folder", "name": "benchmark"},
            "LANDING-FOLDER": {"role": "folder", "kind": "Folder", "name": "landing"},
            "OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_spark"}})
    assert fab.deletes == ["OUT"]


# --------------------------------------------------------- transient Fabric failures, and the folders

def test_the_teardown_never_reads_folders(tmp_path, monkeypatch):
    """THE regression test for run 31144099879.

    `FOLDER_ID` and `LANDING_FOLDER_ID` were module-level assignments, so importing provision.py in
    ANY mode fired two `GET /workspaces/<ws>/folders` calls before `mode` was dispatched. `teardown`
    reads neither id — only `ensure()` does — so a transient Fabric 500 on that call killed the one
    job in the workflow that must always run, left the run's items standing, and shipped a record
    with no deletion stamps. They resolve on first use now, and a teardown has no first use.
    """
    fab, rec = run_teardown(
        tmp_path, monkeypatch,
        items={"OUT": "dbt_delta"},
        record_items={"OUT": {"role": "output", "kind": "Lakehouse", "name": "dbt_delta"}})
    assert fab.deletes == ["OUT"]
    assert rec["OUT"]["deleted"]
    assert fab.folder_gets == 0, "a teardown must not call an endpoint it reads nothing from"


def _reload_req(tmp_path, monkeypatch):
    """Import provision.py (teardown mode, empty record) and hand back the live module."""
    run_teardown(tmp_path, monkeypatch, items={}, record_items={})
    return sys.modules["provision"]


def test_a_transient_status_is_retried_and_an_answer_is_not(tmp_path, monkeypatch):
    """5xx and 429 are retried; every 4xx is returned on the first call.

    A 4xx here is a real answer that a caller already reads — `409 ItemDisplayNameNotAvailableYet`
    has its own poll in `ensure()`, and a 404 from a DELETE means the item is already gone, which
    `drop_guid()` counts as success. Retrying those would fight a caller that handles them.
    """
    prov = _reload_req(tmp_path, monkeypatch)
    for status in (500, 502, 503, 504, 429):
        calls = []

        def flaky(url, _s=status, _c=calls, **kw):
            _c.append(url)
            return Resp(_s) if len(_c) < 3 else Resp(200, {"ok": True})

        monkeypatch.setattr(prov.requests, "get", flaky, raising=False)
        assert prov._req("GET", "https://f/x").status_code == 200
        assert len(calls) == 3, f"{status} should have been retried"

    for status in (400, 403, 404, 409):
        calls = []
        monkeypatch.setattr(
            prov.requests, "get",
            lambda url, _s=status, _c=calls, **kw: (_c.append(url), Resp(_s))[1], raising=False)
        assert prov._req("GET", "https://f/x").status_code == status
        assert len(calls) == 1, f"{status} is an answer, not a transient failure"


def test_exhausted_retries_return_the_last_response_rather_than_raising(tmp_path, monkeypatch):
    """The blast radius of adding retries is a DELAY, never a different control flow: every
    `raise_for_status()` and `status_code in (...)` check downstream must see what it saw before."""
    prov = _reload_req(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(
        prov.requests, "get",
        lambda url, **kw: (calls.append(url), Resp(500))[1], raising=False)
    r = prov._req("GET", "https://f/x")
    assert r.status_code == 500, "the caller still gets the failure to handle its own way"
    assert len(calls) == 3, "and it was tried three times first"


def test_a_transport_error_is_retried_then_re_raised(tmp_path, monkeypatch):
    """A connection blip has no response to hand back, so it is the one case that re-raises —
    swallowing it would turn a network failure into a silent success."""
    prov = _reload_req(tmp_path, monkeypatch)
    calls = []

    def boom(url, **kw):
        calls.append(url)
        raise OSError("connection reset")

    monkeypatch.setattr(prov.requests, "get", boom, raising=False)
    with pytest.raises(OSError):
        prov._req("GET", "https://f/x")
    assert len(calls) == 3
