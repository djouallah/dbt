"""Is V-Order on for this run's warehouse? One row from `sys.databases`, into the run record.

WHY THIS EXISTS, because the obvious answer is that the layout job already reports V-Order and it
does not — not for a Warehouse. The two signals `stats.py` carries are both SPARK-shaped:

- `stats[dwh][*].vorder` is the Delta table property `delta.parquet.vorder.enabled`, read by
  duckrun's `get_stats`. It is a `TBLPROPERTIES` key. Fabric's warehouse writer does not set it.
- `ordering.dwh.vorder_files` was the per-file Delta `add.tags.VORDER` marker, which the Fabric SPARK
  writer stamps and the warehouse writer does not.

So both read "no V-Order" for dwh, and both were WRONG: the warehouse V-Orders **by default** on
every new warehouse. Measured against runs 31148571096 and 31167379761 — freshly created warehouses,
0 of 77 and 0 of 78 `mart.fct_summary` files tagged, `unknown: 0`, i.e. a completely successful read
of a log that simply does not carry the marker. Indistinguishable, on the page, from a writer that
did not V-Order. That is the false negative this script closes; `ordering_for` now skips the tag read
for a warehouse so the absence is honest, and this supplies the answer that probe cannot see.

`sys.databases.is_vorder_enabled` is the ONLY authoritative source — it is what Microsoft's own
`disable-v-order` doc tells you to query, `1` enabled and `0` disabled. Nothing in this repo runs the
`ALTER DATABASE CURRENT SET VORDER = OFF` that would flip it (and that is irreversible, so it could
never be flipped back), which means the expected reading here is `True` on every run. Recording it
anyway rather than hardcoding the default: it costs one round trip on a leg that is already connected,
and a value read is a value that would NOTICE if the default ever changed or if someone disabled it
by hand. The whole reason this file was needed is that this repo asserted a V-Order default from
documentation for months and had it backwards.

Runs on the dwh leg, after the build, on the runner that is already the dbt client — so pyodbc, an
ODBC driver and the `database.windows.net` token are all in place. **Best-effort and never fatal:** a
failure leaves the key ABSENT, never `false`, because `false` here is a claim (V-Order was disabled)
and absence is the truth (nobody could ask). It writes into the leg's own record fragment, whose
`layout.ordering.dwh` deep-merges with `stats.py`'s — `record.deep_update` unions dicts, and the
fragments merge in basename order (`-20-build` before `-30-layout`), so both survive.

It must NOT land in `layout.config`: the dashboard's `variant()` walks every key of that block into a
column name, so a measured value there would split dwh's column and its layout bar whenever it moved.
`layout.ordering` is the correct sibling — the same rule `ordering_for` documents.

Env in: `FABRIC_DWH_SERVER`, `FABRIC_DWH_NAME`, `FABRIC_ACCESS_TOKEN` (all set by `provision.py` and
the token step), `RUN_RECORD`. **`RUN_RECORD` unset is a no-op**, so this stays runnable by hand to
reproduce a CI reading. Diagnostics -> stderr.

    python .github/scripts/dwh_vorder.py dwh
"""
import os
import struct
import sys

import record

# What `disable-v-order` documents: 1 = enabled, 0 = disabled. `DB_NAME()` rather than a literal so
# this cannot read a sibling warehouse's flag if the connection lands somewhere unexpected.
QUERY = "SELECT [is_vorder_enabled] FROM sys.databases WHERE [name] = DB_NAME()"

# SQL_COPT_SS_ACCESS_TOKEN. Passing the token as a connection attribute is how dbt-fabric does it
# too; a token in the connection STRING is not supported by the driver.
SQL_COPT_SS_ACCESS_TOKEN = 1256


def read_vorder(con):
    """`True`/`False` from a live connection, or `None` when the row or column is not there.

    Split from `connect()` so a stub connection can pin it offline — the interesting failure is not
    the network, it is misreading the row (a `0` is a real answer and must not become `None`, and a
    missing row must not become `False`).
    """
    cur = con.cursor()
    cur.execute(QUERY)
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return bool(row[0])


def driver():
    """The newest installed `ODBC Driver NN for SQL Server`, discovered rather than hardcoded.

    The runner image's driver version moves and dbt-fabric picks its own; pinning a number here would
    make this fail on an image bump while the leg beside it kept working.
    """
    import pyodbc
    found = sorted(d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d)
    if not found:
        raise RuntimeError(f"no SQL Server ODBC driver among {pyodbc.drivers()}")
    return found[-1]


def connect():
    import pyodbc
    server, db = os.environ["FABRIC_DWH_SERVER"], os.environ["FABRIC_DWH_NAME"]
    tok = os.environ["FABRIC_ACCESS_TOKEN"].encode("utf-16-le")
    return pyodbc.connect(
        f"DRIVER={{{driver()}}};SERVER={server};DATABASE={db};Encrypt=yes;TrustServerCertificate=no",
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: struct.pack("<i", len(tok)) + tok},
        timeout=60)


def main(argv):
    engine = (argv[1] if len(argv) > 1 else "dwh").strip()
    try:
        con = connect()
        try:
            v = read_vorder(con)
        finally:
            con.close()
    except Exception as e:                              # noqa: BLE001 — never fail the build leg
        sys.stderr.write(f"  v-order state unavailable for {engine} "
                         f"({type(e).__name__}: {e})\n")
        return 0
    if v is None:
        sys.stderr.write(f"  v-order state unavailable for {engine} "
                         "(sys.databases returned no is_vorder_enabled for DB_NAME())\n")
        return 0
    record.merge({"layout": {"ordering": {engine: {"vorder_enabled": v}}}})
    sys.stderr.write(f"  {engine}: is_vorder_enabled = {v}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
