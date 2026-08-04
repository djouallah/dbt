"""Run `dbt build` for one DuckDB-family engine (argv[1]: `duckrun` = Delta | `iceberg`) against
the landed data, writing to that engine's OneLake lakehouse.

CI always runs this inside a throwaway Fabric Python notebook, as the entry script fabric_run.py
ships via duckrun.run_python — a fresh interpreter whose cwd is the unpacked project root, with
duckrun / dbt-duckdb already pip-installed. dbt.yml used to invoke it on the GitHub runner too for
folds a pending-file count called small; that path is gone.

Nothing below is location-specific, and it is worth keeping that way — it is what makes this
runnable by hand when you need to reproduce a CI failure. duckrun.auth resolves the OneLake token
from whatever is there (the Fabric runtime in a notebook, GitHub OIDC on a runner), so the token
is never shipped, and config (FILES_PATH, the output path, schema, limits) always arrives via env.

Tests run HERE, in the same invocation: `dbt build` interleaves model and test, so a broken model
stops the leg at the node that broke. There is no separate neutral-reader test job any more.
"""
import json
import os
import shutil
import subprocess
import sys
import threading
import time

_MAX_ATTEMPTS = 3

_SAMPLE_INTERVAL = 15


def _gib(n) -> str:
    """Auto-scaled, because the spill figure is the one being read for "is it still zero" and a
    fixed GiB unit prints 12 MiB of real spilling as `0.0GiB`."""
    if n is None:
        return "?"
    for unit, size in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
        if n >= size:
            return f"{n / size:.1f}{unit}"
    return f"{n}B"


def _meminfo(key: str):
    """One /proc/meminfo field in bytes, or None off Linux."""
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith(key + ":"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None


def _cgroup_limit():
    """The container's own memory ceiling, which is what actually kills us — /proc/meminfo
    reports the HOST's RAM and can be far larger."""
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(path) as fh:
                raw = fh.read().strip()
            return None if raw == "max" else int(raw)
        except Exception:
            continue
    return None


def _spill_usage(path: str):
    """(bytes, file count) currently in the DuckDB temp dir. Zero while it is climbing to the
    memory limit means nothing ever spilled."""
    total = files = 0
    try:
        for root, _, names in os.walk(path):
            for name in names:
                try:
                    total += os.path.getsize(os.path.join(root, name))
                    files += 1
                except OSError:
                    pass
    except Exception:
        return None, None
    return total, files


def _rss(pid: int):
    try:
        with open(f"/proc/{pid}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    return None


def _log_node_facts(engine: str) -> None:
    """Everything about the machine and the spill directory that the leg log cannot otherwise say.

    A `cores: 4` dispatch OOMed on fct_summary at 24.6 GiB while `cores: 8` passes, and nothing in
    the log distinguished "DuckDB is misconfigured" from "this node has less RAM". These are the
    inputs to that question: how much memory the container really has, where the spill directory
    is, and how much free disk backs it — max_temp_directory_size defaults to 90% of THAT, so a
    small work disk is a small spill budget, silently.
    """
    tmp = os.environ["DUCKDB_TEMP_DIR"]
    parent = tmp if os.path.isdir(tmp) else os.path.dirname(tmp) or "."
    try:
        usage = shutil.disk_usage(parent)
        disk = f"free={_gib(usage.free)} of {_gib(usage.total)}"
    except Exception as ex:
        disk = f"unreadable ({ex})"
    try:
        cwd_usage = shutil.disk_usage(".")
        cwd_disk = f"free={_gib(cwd_usage.free)} of {_gib(cwd_usage.total)}"
    except Exception as ex:
        cwd_disk = f"unreadable ({ex})"

    print(f"[fabric_build] node: engine={engine} cpu_count={os.cpu_count()} "
          f"MemTotal={_gib(_meminfo('MemTotal'))} MemAvailable={_gib(_meminfo('MemAvailable'))} "
          f"cgroup_max={_gib(_cgroup_limit())}", flush=True)
    print(f"[fabric_build] spill: DUCKDB_TEMP_DIR={tmp} exists={os.path.isdir(tmp)} "
          f"writable={os.access(parent, os.W_OK)} disk[{parent}] {disk}", flush=True)
    print(f"[fabric_build] cwd: {os.getcwd()} TMPDIR={os.environ.get('TMPDIR')} disk {cwd_disk}",
          flush=True)

    # duckrun pip-installs itself with -q, so the notebook's actual versions appear nowhere.
    try:
        frozen = subprocess.run([sys.executable, "-m", "pip", "freeze"],
                                capture_output=True, text=True, timeout=120).stdout
        wanted = ("duckdb", "duckrun", "dbt-", "deltalake", "obstore")
        pkgs = sorted(l.strip() for l in frozen.splitlines()
                      if l.lower().startswith(wanted))
        print(f"[fabric_build] versions: {', '.join(pkgs)}", flush=True)
    except Exception as ex:
        print(f"[fabric_build] versions: unreadable ({ex})", flush=True)


class _Sampler:
    """One line every 15s while dbt runs: process RSS, free memory, and how much has spilled.

    OS reads only — /proc and a walk of the temp dir, no DuckDB connection and no query — so it
    cannot perturb what the leg is measuring. It is the timeline the OOM stack trace does not
    give: if spill stays at 0B while RSS climbs to the memory limit, the memory in use was never
    evictable and the answer is more RAM, not a setting.
    """

    def __init__(self, pid: int, tmp: str):
        self._pid, self._tmp = pid, tmp
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self):
        self._t0 = time.monotonic()
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5)
        self._emit(final=True)
        return False

    def _emit(self, final: bool = False) -> None:
        spill, files = _spill_usage(self._tmp)
        try:
            free = _gib(shutil.disk_usage(
                self._tmp if os.path.isdir(self._tmp) else ".").free)
        except Exception:
            free = "?"
        print(f"[fabric_build] {'final ' if final else ''}t=+{time.monotonic() - self._t0:.0f}s "
              f"rss={_gib(_rss(self._pid))} mem_avail={_gib(_meminfo('MemAvailable'))} "
              f"spill={_gib(spill)}/{'?' if files is None else files} files disk_free={free}",
              flush=True)

    def _run(self) -> None:
        while not self._stop.wait(_SAMPLE_INTERVAL):
            self._emit()


def _only_tests_failed() -> bool:
    """True when every failing node of the last invocation was a data test.

    The retry ladder below exists for transient OneLake commit conflicts, which are a property of
    the WRITE. A data assertion is deterministic — it reads back the table this same invocation
    just wrote — so replaying it buys a second Fabric-side scan and the identical verdict, and
    under `dbt build` the downstream nodes it skipped stay skipped. Unreadable or absent
    run_results means we learned nothing, so fall through to retrying (the old behaviour).
    """
    try:
        with open("target/run_results.json") as fh:
            results = json.load(fh)["results"]
    except Exception:
        return False
    bad = [r for r in results if r["status"] in ("error", "fail")]
    return bool(bad) and all(r["unique_id"].startswith("test.") for r in bad)


def main() -> int:
    engine = sys.argv[1] if len(sys.argv) > 1 else "duckrun"

    # The iceberg target (type: duckdb) reads ONELAKE_TOKEN from the env for its Iceberg REST
    # catalog + azure secret. get_onelake_token() picks the right source for wherever this is
    # running — notebookutils in Fabric, GitHub OIDC on the runner. duckrun (Delta) self-acquires
    # its own token, so setting it is harmless there.
    from duckrun import auth
    os.environ.setdefault("ONELAKE_TOKEN", auth.get_onelake_token())

    # Spill DuckDB temp files to the notebook's big work disk (the harness points TMPDIR there),
    # not the cramped /tmp overlay — a large iceberg aggregation / delta merge would fill /tmp.
    # setdefault, not assignment: a caller that already picked a spill dir keeps it.
    scratch = os.environ.get("TMPDIR") or "/tmp"
    os.environ.setdefault("DUCKDB_TEMP_DIR", os.path.join(scratch, "duckdb_spill"))

    # No `pip install --pre duckdb` here, and it is not an oversight: a nightly resolves fine for
    # both adapters (dbt-duckdb wants duckdb>=1.0.0, duckrun >=1.5.4) but its extensions come from
    # a nightly repo that does not carry `azure` — and every read here goes through OneLake.
    _log_node_facts(engine)

    # `dbt build`: models and their tests in one DAG walk. The singular tests in tests/ are gated to
    # the duckdb-family targets by `data_tests: +enabled`, so this is the only place they run.
    #
    # No `--exclude tag:heavy`: nothing carries the tag now that the suite is one grain check on
    # fct_summary plus the dimension keys. Do not re-add the flag without re-adding a heavy test —
    # a selector matching zero nodes just warns and misdescribes what ran.
    base = ["--target", engine, "--profiles-dir", "."]

    # Retry ladder: the OneLake Iceberg REST catalog intermittently rejects a commit with
    # 409 Conflict ("One or more requirements failed. The client may retry.") under optimistic
    # concurrency — the same transient the standalone iceberg pipeline retries.
    #
    # Each attempt is a FRESH dbt subprocess, NOT an in-process dbtRunner re-invoke:
    # dbt-duckdb caches the DuckDB connection at module level, so a second invoke re-runs the
    # on-run-start `SET GLOBAL temp_directory` on a session whose temp dir is already in use —
    # "Cannot switch temporary directory after the current one has been used" — and every retry
    # is dead on arrival. Retries use `dbt retry` (with --target, else it renders the profile's
    # default target) so only the failed nodes re-run, not the whole idempotent build. `base` is
    # safe to pass to retry only because it is now just --target/--profiles-dir; a selection flag
    # in there would break it, since retry replays the selection recorded in run_results and
    # rejects --select/--exclude outright.
    ok = False
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        if attempt == 1 or not os.path.exists("target/run_results.json"):
            cmd = ["dbt", "build", *base]
        else:
            cmd = ["dbt", "retry", *base]
        print(f"[fabric_build] $ {' '.join(cmd)}", flush=True)
        # Popen + wait, not subprocess.run: identical behaviour (run is that wrapper), but the pid
        # is needed to sample the child's RSS while it works.
        proc = subprocess.Popen(cmd)
        with _Sampler(proc.pid, os.environ["DUCKDB_TEMP_DIR"]):
            ok = proc.wait() == 0
        if ok:
            break
        if _only_tests_failed():
            print(f"[fabric_build] {engine} attempt {attempt}/{_MAX_ATTEMPTS} failed on data "
                  f"tests only — deterministic, not a transient commit conflict; not retrying",
                  flush=True)
            break
        if attempt < _MAX_ATTEMPTS:
            backoff = 15 * attempt
            print(f"[fabric_build] {engine} attempt {attempt}/{_MAX_ATTEMPTS} failed; "
                  f"retrying in {backoff}s (transient OneLake commit conflicts)", flush=True)
            time.sleep(backoff)

    # A REBUILD_SUMMARY=1 step used to follow (dbt build --select fct_summary --full-refresh).
    # Removed with the workflow input: it fired for BOTH duckdb engines, and --full-refresh on
    # iceberg fails every time (`Table fct_summary__dbt_tmp does not exist`, dbt-duckdb's swap
    # materialization) and leaves a __dbt_backup behind.
    print(f"[fabric_build] {engine} build success={ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
