"""Render `capacity_cu.py`'s markdown as ONE standalone HTML page. `python cu/report_html.py in.md`.

Why a converter and not a markdown library: `requests` is this directory's whole runtime dependency
and the point of that is that `rm -rf cu/` costs nothing. The markdown here is not arbitrary — it is
emitted by `capacity_cu.py` a few lines away, so the subset is fixed and small (headings, pipe
tables, `<sub>` notes, `**bold**`, `` `code` ``). A parser for a grammar you also write is a fair
trade; pulling in a markdown package for it is not.

Self-contained by construction: the CSS is inline, there are no external fonts, scripts or images.
It is a build artifact, so it has to survive being downloaded and opened from a local disk with no
network, and being read on a phone.

Numbers are right-aligned and never re-formatted — they arrive already rounded and thousands-separated
from the report, and a renderer that reformats numbers is a renderer that can change them.
"""
import html
import json
import re
import sys


def _inline(text):
    """`**bold**`, `` `code` ``, `[text](url)`, `<br>`, and nothing else. Escaped first, so a stray
    `<` in an item name cannot inject markup.

    Links are restricted to `http(s)://` — the report only ever emits GitHub URLs, and a scheme
    allowlist is what keeps that true even if an item NAME ever reaches this function looking like
    markdown. A non-matching link is left as literal text rather than dropped.
    """
    out = html.escape(text, quote=False)
    out = out.replace("&lt;br&gt;", "<br>").replace("&lt;sub&gt;", "").replace("&lt;/sub&gt;", "")
    out = re.sub(r"\[([^\]]+)\]\((https?://[^\s)]+)\)",
                 lambda m: f'<a href="{html.escape(m.group(2), quote=True)}">{m.group(1)}</a>', out)
    out = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", out)
    out = re.sub(r"`(.+?)`", r"<code>\1</code>", out)
    return out


def _cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


# Horizontal bars, one series, drawn as inline SVG.
#
# Bars because the job is magnitude across a handful of named categories; HORIZONTAL because the
# categories are words. ONE series per chart, so there is no legend and no categorical palette to
# validate — the engine names are on the axis, and colouring each bar differently would encode
# nothing the label does not already say. Colour by RANK would be worse: it repaints when the
# numbers move.
#
# Geometry follows the mark spec: bar ≤ 24px thick, square at the baseline, 4px rounded data-end,
# and a gap between bands wider than the 2px surface-gap minimum. No gridlines — every bar carries
# its value at the tip, so ticks would be a second copy of the same number.
#
# The value label wears text ink, never the bar colour, and each bar carries a <title> so hovering
# gives a native tooltip with NO script — which is the only kind of interactivity a page under this
# repo's no-external-anything rule can have.
#
# A row may carry a CAPTION — the adapter and the compute the engine ran on — drawn as a second,
# dimmer line under the name. It is the difference between "iceberg cost 2.3x duckrun" and "the same
# DuckDB writing to an Iceberg catalog instead of through delta-rs, at the same notebook size, cost
# 2.3x". The label gutter widens and the band grows only when a caption is actually present, so a
# plain chart keeps the geometry it had.
BAR_H, BAND, PAD_T, LABEL_W, VALUE_W, WIDTH = 18, 30, 26, 96, 74, 660
SUB_BAND, SUB_LABEL_W = 36, 224


def _bar_path(w, h, r=4):
    """Square at the baseline, rounded at the data end — so the bar reads as growing from zero."""
    if w <= r:
        return f"M0,0 h{w:.1f} v{h} h-{w:.1f} Z"
    return (f"M0,0 H{w - r:.1f} A{r},{r} 0 0 1 {w:.1f},{r} V{h - r} "
            f"A{r},{r} 0 0 1 {w - r:.1f},{h} H0 Z")


def chart_svg(spec):
    rows = [(str(r[0]), float(r[1]), str(r[2]) if len(r) > 2 and r[2] else "")
            for r in spec.get("rows") or []]
    if not rows:
        return ""
    subs = any(s for _l, _v, s in rows)
    band, label_w = (SUB_BAND, SUB_LABEL_W) if subs else (BAND, LABEL_W)
    top = max(v for _l, v, _s in rows) or 1.0
    plot = WIDTH - label_w - VALUE_W
    height = PAD_T + len(rows) * band + 6
    out = [f'<figure class="chart"><figcaption><span class="chart-title">'
           f'{html.escape(spec.get("title", ""))}</span>'
           f'<span class="chart-sub">{html.escape(spec.get("subtitle", ""))}</span></figcaption>',
           f'<svg viewBox="0 0 {WIDTH} {height}" width="100%" height="{height}" role="img" '
           f'aria-label="{html.escape(spec.get("title", ""))}">']
    for i, (label, value, sub) in enumerate(rows):
        y = PAD_T + i * band
        w = plot * (value / top)
        # With a caption the name sits on the bar's upper half and the caption under it, so the pair
        # reads as one block against the bar rather than as two columns.
        ly = (BAR_H / 2 if subs else BAR_H / 2 + 4)
        out.append(f'<g transform="translate(0,{y})">'
                   f'<title>{html.escape(label)}'
                   + (f' ({html.escape(sub)})' if sub else "")
                   + f': {value:,.1f} CU</title>'
                     f'<text class="bar-label" x="{label_w - 10}" y="{ly:.0f}" '
                     f'text-anchor="end">{html.escape(label)}</text>'
                   + (f'<text class="bar-caption" x="{label_w - 10}" y="{ly + 13:.0f}" '
                      f'text-anchor="end">{html.escape(sub)}</text>' if sub else "")
                   + f'<g transform="translate({label_w},0)">'
                     f'<path class="bar" d="{_bar_path(w, BAR_H)}"/></g>'
                     f'<text class="bar-value" x="{label_w + w + 8:.1f}" '
                     f'y="{BAR_H / 2 + 4:.0f}">{value:,.1f}</text></g>')
    out.append(f'<line class="axis" x1="{label_w}" y1="{PAD_T - 6}" x2="{label_w}" '
               f'y2="{PAD_T + len(rows) * band - band + BAR_H + 4}"/>')
    out.append("</svg></figure>")
    return "\n".join(out)


def _is_rule(line):
    """A markdown alignment row — `|:--|---:|` — which is what proves the line above was a header."""
    return bool(re.fullmatch(r"\|(\s*:?-{2,}:?\s*\|)+", line.strip()))


def to_html(md):
    """Markdown (this report's subset) -> the page BODY. Tables keep their alignment row's intent:
    a trailing `-:` means the column is numeric, so it is right-aligned."""
    lines = md.splitlines()
    out, i = [], 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line.strip():
            i += 1
            continue
        # The chart marker. It is an HTML comment so the same markdown stays clean in the GitHub job
        # summary, where inline SVG is sanitised away anyway.
        m = re.match(r"^<!--chart:(.*)-->$", line.strip())
        if m:
            try:
                out.append(chart_svg(json.loads(m.group(1))))
            except Exception as ex:   # a malformed spec must not cost the whole report
                out.append(f'<p class="note">chart skipped ({type(ex).__name__})</p>')
            i += 1
            continue
        m = re.match(r"^(#{2,4})\s+(.*)", line)
        if m:
            lvl = len(m.group(1))
            out.append(f"<h{lvl}>{_inline(m.group(2))}</h{lvl}>")
            i += 1
            continue
        if line.lstrip().startswith("<sub>"):
            # A note, possibly wrapped over several lines by the report's own line width.
            buf = [line.strip()]
            while not buf[-1].endswith("</sub>") and i + 1 < len(lines):
                i += 1
                buf.append(lines[i].strip())
            out.append(f'<p class="note">{_inline(" ".join(buf))}</p>')
            i += 1
            continue
        if line.startswith("|") and i + 1 < len(lines) and _is_rule(lines[i + 1]):
            head = _cells(line)
            align = ["right" if c.endswith(":") and not c.startswith(":") else "left"
                     for c in _cells(lines[i + 1])]
            rows = []
            i += 2
            while i < len(lines) and lines[i].startswith("|"):
                rows.append(_cells(lines[i]))
                i += 1
            out.append("<div class=\"scroll\"><table>")
            out.append("<thead><tr>" + "".join(
                f'<th class="{a}">{_inline(c)}</th>' for c, a in zip(head, align)) + "</tr></thead>")
            out.append("<tbody>")
            for r in rows:
                # A row whose first cell is bold is a subtotal — the report's own emphasis, carried
                # through as a class so the page can rule it off rather than only embolden it.
                cls = ' class="sub"' if r and r[0].startswith("**") else ""
                out.append(f"<tr{cls}>" + "".join(
                    f'<td class="{a}">{_inline(c)}</td>'
                    for c, a in zip(r, align + ["left"] * len(r))) + "</tr>")
            out.append("</tbody></table></div>")
            continue
        out.append(f"<p>{_inline(line)}</p>")
        i += 1
    return "\n".join(out)


# One stylesheet, both themes. `prefers-color-scheme` is the default signal; nothing here needs JS.
CSS = """
:root { color-scheme: light dark;
  --bg:#fff; --fg:#1a1a1a; --dim:#5b6472; --line:#e3e6ea; --head:#f6f7f9; --sub:#eef1f5;
  --code:#f2f3f5;
  /* Series hue, validated against both surfaces with the dataviz palette validator: lightness
     band, chroma floor and contrast all pass. The dark value is a SELECTED step for the dark
     surface, not an automatic flip of the light one. */
  --series:#2a78d6; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#14171a; --fg:#e8eaed; --dim:#9aa4b2; --line:#2a2f36; --head:#1c2025; --sub:#20252b;
    --code:#22262c; --series:#3987e5; } }
:root[data-theme="dark"] { --bg:#14171a; --fg:#e8eaed; --dim:#9aa4b2; --line:#2a2f36;
  --head:#1c2025; --sub:#20252b; --code:#22262c; --series:#3987e5; }
* { box-sizing: border-box; }
body { margin:0; padding:2rem 1.25rem 4rem; background:var(--bg); color:var(--fg);
  font:15px/1.55 ui-sans-serif, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width: 62rem; margin: 0 auto; }
h2 { font-size:1.5rem; line-height:1.25; margin:0 0 1.25rem; letter-spacing:-0.01em; }
h3 { font-size:1.1rem; margin:2.5rem 0 .85rem; }
h4 { font-size:.95rem; margin:2rem 0 .6rem; color:var(--dim); }
p { margin:.75rem 0; }
a { color:var(--series); text-decoration:none; border-bottom:1px solid var(--line); }
a:hover { border-bottom-color:var(--series); }
p.note { color:var(--dim); font-size:.82rem; line-height:1.5; margin:.7rem 0 0; }
code { background:var(--code); border-radius:4px; padding:.08em .32em; font-size:.88em;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
.scroll { overflow-x:auto; -webkit-overflow-scrolling:touch; margin:.5rem 0 .25rem; }
table { border-collapse:collapse; width:100%; font-variant-numeric: tabular-nums; font-size:.92rem; }
th, td { padding:.42rem .7rem; border-bottom:1px solid var(--line); white-space:nowrap; }
thead th { background:var(--head); font-weight:600; font-size:.82rem; letter-spacing:.02em;
  text-transform:lowercase; color:var(--dim); position:sticky; top:0; }
tbody tr.sub td { background:var(--sub); font-weight:600; }
tbody tr:last-child td { border-bottom:none; }
.left { text-align:left; } .right { text-align:right; }
footer { color:var(--dim); font-size:.78rem; margin-top:3rem; padding-top:1rem;
  border-top:1px solid var(--line); }
/* Charts. The bar is the only thing wearing the series colour — labels and values stay in text ink,
   so identity never depends on being able to see the hue. */
figure.chart { margin:1.25rem 0 1.75rem; }
figure.chart figcaption { display:flex; flex-wrap:wrap; align-items:baseline; gap:.6rem;
  margin-bottom:.35rem; }
.chart-title { font-weight:600; font-size:.95rem; }
.chart-sub { color:var(--dim); font-size:.78rem; }
figure.chart svg { max-width:100%; height:auto; display:block; overflow:visible; }
.bar { fill:var(--series); }
.bar-label { fill:var(--fg); font-size:12px; font-weight:600; }
.bar-caption { fill:var(--dim); font-size:10px; }
.bar-value { fill:var(--dim); font-size:12px; font-variant-numeric:tabular-nums; }
.axis { stroke:var(--line); stroke-width:1; }
"""


def page(md, title="Capacity units", footer=""):
    return (f"<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
            f"<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
            f"<title>{html.escape(title)}</title>\n<style>{CSS}</style></head>\n<body><main>\n"
            f"{to_html(md)}\n"
            + (f"<footer>{_inline(footer)}</footer>\n" if footer else "")
            + "</main></body></html>\n")


def main(argv):
    md = (open(argv[1], encoding="utf-8").read() if len(argv) > 1 else sys.stdin.read())
    footer = argv[2] if len(argv) > 2 else ""
    sys.stdout.write(page(md, footer=footer))
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(main(sys.argv))
