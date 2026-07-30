"""The engine registry for the benchmark — one place that knows what the four engines are.

Deliberately mirrors `.github/scripts/stats.py`'s `ENGINES` / `WRITER`: that script is the parity
dashboard over the same four items, and the two must never disagree about which Fabric item belongs
to which engine. If an item is renamed, both change together.

`MODE` is the one thing stats.py has no opinion about and this does: how each engine's tables are
READ by the semantic model. All four are now **Direct Lake** — an in-memory transcode from the Delta
files, so the physical layout is what shapes the timing, which is the only question this benchmark
asks.

The warehouse used to be the exception, read by a **DirectQuery** model (pushdown to the SQL
endpoint — a different engine, not a different layout), because that was the only way duckrun could
deploy a model over a warehouse. `deploy(mode=)` (duckrun 0.4.36) removed the constraint: a
warehouse's tables are Delta in OneLake like any other, so it can be read by Direct Lake too, and
one authored .bim now serves either mode. That makes the four legs directly comparable for the first
time — dwh has a cold tier now, so it appears in the COLD tables instead of being scoped out of them.

MODE is still per-engine and still consulted everywhere, so flipping one engine back to
`"directQuery"` is a one-line change here — the same template deploys either way. Everything
downstream (cold-tier scoping in `render_report._totals`, the `_(DirectQuery)_` label on a verdict,
the hot-only note) reads this and keeps working; it just has nothing to report while all four match.
"""
import json
import os

# (engine label, Fabric item display name, item kind) — same triple as stats.py's ENGINES.
ENGINES = [("duckrun", "dbt_delta", "lakehouses"),
           ("iceberg", "dbt_iceberg", "lakehouses"),
           ("spark", "dbt_spark", "lakehouses"),
           ("dwh", "dbt_dwh", "warehouses")]

ITEM = {e: item for e, item, _ in ENGINES}
KIND = {e: kind for e, _, kind in ENGINES}

# What actually wrote the parquet behind each engine's Delta log (stats.py's WRITER, verbatim).
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)",
          "spark": "spark", "dwh": "warehouse"}

# How the semantic model READS it — independent of the item kind since duckrun 0.4.36. A warehouse's
# tables are Delta in OneLake exactly like a lakehouse's, so all four are read the same way and the
# comparison is layout-vs-layout rather than layout-vs-pushdown. Set an engine to "directQuery" to
# put it back on the SQL endpoint; the same .bim deploys either way via deploy(mode=).
MODE = {e: "directLake" for e, _, _ in ENGINES}

# The TMSL spellings above, in the spelling duckrun's deploy(mode=) takes.
DEPLOY_MODE = {"directLake": "direct_lake", "directQuery": "direct_query"}

ALL = [e for e, _, _ in ENGINES]

# Semantic models are named <PREFIX><engine>. The DAX suite is identical across them, so the model
# name is the ONLY thing that identifies which engine's table a timing came from.
PREFIX = "aemo_"


def model_name(engine):
    return f"{PREFIX}{engine}"


def engine_of(model):
    """Inverse of model_name. Tolerates being handed a bare engine label already."""
    return model[len(PREFIX):] if model.startswith(PREFIX) else model


def selected(default=None):
    """The engines this run covers, from BENCH_ENGINES (comma-separated). Order is significant:
    the FIRST one is the reference every ratio is taken against."""
    raw = (os.environ.get("BENCH_ENGINES") or "").strip()
    if not raw:
        return list(default if default is not None else ALL)
    out = []
    for part in raw.split(","):
        e = part.strip().lower()
        if not e:
            continue
        if e not in ITEM:
            raise SystemExit(f"unknown engine {e!r}; known: {', '.join(ALL)}")
        if e not in out:
            out.append(e)
    if not out:
        raise SystemExit("BENCH_ENGINES was set but named no engine")
    return out


def reference(candidates=None):
    """The engine every comparison is measured against — BENCH_REFERENCE if it is one of the
    candidates, else the first candidate. Never guesses by name length."""
    cands = list(candidates) if candidates else selected()
    want = (os.environ.get("BENCH_REFERENCE") or "").strip().lower()
    if want and want in cands:
        return want
    return cands[0] if cands else None


def items():
    """The {engine: {item, kind, guid, mode, writer}} map that resolve_env.py wrote to
    BENCH_ITEMS. Raises with a pointer rather than a KeyError, because every consumer of this needs
    resolve_env.py to have run first."""
    raw = os.environ.get("BENCH_ITEMS")
    if not raw:
        raise SystemExit("BENCH_ITEMS is not set — run benchmark/resolve_env.py first")
    return json.loads(raw)
