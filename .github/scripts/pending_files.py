"""How many source files this engine still has to fold — the signal that decides WHERE the
DuckDB-family build runs (argv[1]: `duckrun` | `iceberg`). Prints `PENDING_FILES=<n>` on stdout
for $GITHUB_ENV; diagnostics -> stderr.

Replaces `new_daily` (did the downloader fetch a new PUBLIC_DAILY file THIS run?) which was a
proxy for "is the fold small", and a bad one: it describes the download, not the backlog. A
from-scratch lakehouse has the whole archive — ~3000 daily files — waiting even when nothing
new landed, and the old signal would read 0 and put that on a 7GB GitHub runner.

Pending is measured the same way the models choose their own work: rows in the archive log
whose `csv_filename` is not yet in the consuming table's `[file]` column. A daily file feeds
BOTH fct_scada and fct_price, so it counts as pending while EITHER is missing it. A table that
does not exist yet means every file of that type is pending — and that costs no scan, which is
exactly the from-scratch case above.

Both engines are read through Delta (`delta_scan`): the iceberg lakehouse is surfaced as Delta
on OneLake, the same way stats.py grades it. An XTable sync lag can only make the table look
further behind than it is, which biases toward Fabric — the safe direction.

Any failure prints the SAFE sentinel instead of erroring: Fabric handles a fold of any size,
the runner does not, so an unknown workload must not be placed locally.
"""
import contextlib
import os
import sys

import duckrun

# (archive-log source_type, tables that consume it). Marts land in the `landing` schema.
SOURCES = [("daily", ["fct_scada", "fct_price"]),
           ("scada_today", ["fct_scada_today"]),
           ("price_today", ["fct_price_today"])]
SCHEMA = "landing"
SAFE = 999999          # "assume huge" -> workflow picks Fabric


def tables_path(engine):
    """The engine's OneLake Tables root, from what provision.py exported for it."""
    if engine == "duckrun":
        return os.environ["ONELAKE_TABLES_PATH"]
    # iceberg gets WAREHOUSE_PATH ('<workspace-guid>/<lakehouse-guid>') instead, because its
    # dbt profile addresses the Iceberg REST catalog, not an abfss Tables path.
    ws, lh = os.environ["WAREHOUSE_PATH"].split("/", 1)
    return f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh}/Tables"


def main():
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"
    log = os.environ["FILES_PATH"].rstrip("/") + "/csv_raw_archive_log.parquet"
    base = tables_path(engine).rstrip("/")

    con = duckrun.connect(base, read_only=True)
    try:
        con.con.sql("SET GLOBAL azure_transport_option_type='"
                    + os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "curl") + "'")
    except Exception:
        pass
    q = con.con.sql

    def readable(loc):
        """Does this Delta table exist? A missing one raises — and means nothing is folded yet."""
        try:
            q(f"SELECT * FROM delta_scan('{loc}') LIMIT 0")
            return True
        except Exception as e:
            sys.stderr.write(f"  {loc.rsplit('/', 1)[-1]}: not readable ({type(e).__name__})\n")
            return False

    total = 0
    for stype, tables in SOURCES:
        landed = q(f"SELECT count(*) FROM read_parquet('{log}') "
                   f"WHERE source_type = '{stype}'").fetchone()[0]
        present = [f"{base}/{SCHEMA}/{t}" for t in tables]
        present = [p for p in present if readable(p)]
        if not present:
            pending = landed          # nothing folded yet: the whole type is outstanding
        else:
            # Pending while ANY consumer is missing it — one pass, no per-table round trip.
            pred = " OR ".join(
                f"l.csv_filename NOT IN (SELECT DISTINCT file FROM delta_scan('{p}'))"
                for p in present)
            pending = q(f"SELECT count(*) FROM read_parquet('{log}') l "
                        f"WHERE l.source_type = '{stype}' AND ({pred})").fetchone()[0]
        sys.stderr.write(f"  {stype}: {pending} pending of {landed} landed\n")
        total += pending

    sys.stderr.write(f"[pending_files] {engine}: {total} files to fold\n")
    return total


if __name__ == "__main__":
    try:
        # stdout is appended verbatim to $GITHUB_ENV, so nothing but the KEY=VALUE line may
        # reach it — any chatter a library decides to print goes to stderr with the rest.
        with contextlib.redirect_stdout(sys.stderr):
            count = main()
    except Exception as e:
        sys.stderr.write(f"[pending_files] FAILED ({type(e).__name__}: {e}) — "
                         f"reporting {SAFE} so the fold is placed on Fabric\n")
        count = SAFE
    print(f"PENDING_FILES={count}")
