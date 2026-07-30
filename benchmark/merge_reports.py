"""Merge the per-engine report fragments back into one run_report.json.

The benchmark runs ONE JOB PER ENGINE — that is what keeps every Fabric token minted minutes before
it is used, instead of one token trying to cover a four-model pass with two 600s gaps in it. The cost
of that split is that no single process holds all four engines' timings any more, so the ratios cannot
be computed where they used to be. They are computed by the render layer instead, from this file.

Nothing here is engine-aware: each fragment is a `report.merge`-shaped dict and they are deep-merged
in FILENAME ORDER, which is why the resolve job's fragment is named to sort first — it carries the
`run` block (inputs, sha, workspace) that a per-engine fragment must not be allowed to overwrite.
The engine fragments only ever add their own key under `engines`, `deploy`, `timings`, `top_duid`.

Usage: python benchmark/merge_reports.py <dir-or-file> [...]   (writes $RUN_REPORT)
"""
import glob
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report  # noqa: E402


def fragments(paths):
    """Every *.json under the given files/directories, sorted by BASENAME.

    Basename, not full path: the artifact download lands each fragment in its own directory named
    after the artifact, so sorting on the path would order by directory and put the meta fragment
    wherever the alphabet happened to place it."""
    out = []
    for p in paths:
        if os.path.isdir(p):
            out += glob.glob(os.path.join(p, "**", "*.json"), recursive=True)
        elif os.path.exists(p):
            out.append(p)
    return sorted(out, key=lambda p: (os.path.basename(p), p))


def main():
    args = sys.argv[1:] or ["fragments"]
    dest = os.environ.get("RUN_REPORT", "run_report.json")
    found = fragments(args)
    if not found:
        sys.exit(f"no report fragments found under {args} — nothing to merge")
    for p in found:
        with open(p, encoding="utf-8") as f:
            frag = json.load(f)
        report.merge(frag, dest)
        engines = ", ".join(sorted(frag.get("timings", {}))) or "-"
        print(f"merged {os.path.basename(p)} (timings: {engines})")

    with open(dest, encoding="utf-8") as f:
        rep = json.load(f)
    print(f"\n{dest}: {len(rep.get('timings', {}))} engine(s) with timings "
          f"({', '.join(sorted(rep.get('timings', {}))) or 'none'})")


if __name__ == "__main__":
    main()
