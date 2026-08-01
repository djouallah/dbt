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
import re
import sys


def _inline(text):
    """`**bold**`, `` `code` ``, `<br>`, and nothing else. Escaped first, so a stray `<` in an item
    name cannot inject markup."""
    out = html.escape(text, quote=False)
    out = out.replace("&lt;br&gt;", "<br>").replace("&lt;sub&gt;", "").replace("&lt;/sub&gt;", "")
    out = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", out)
    out = re.sub(r"`(.+?)`", r"<code>\1</code>", out)
    return out


def _cells(line):
    return [c.strip() for c in line.strip().strip("|").split("|")]


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
  --code:#f2f3f5; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#14171a; --fg:#e8eaed; --dim:#9aa4b2; --line:#2a2f36; --head:#1c2025; --sub:#20252b;
    --code:#22262c; } }
* { box-sizing: border-box; }
body { margin:0; padding:2rem 1.25rem 4rem; background:var(--bg); color:var(--fg);
  font:15px/1.55 ui-sans-serif, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; }
main { max-width: 62rem; margin: 0 auto; }
h2 { font-size:1.5rem; line-height:1.25; margin:0 0 1.25rem; letter-spacing:-0.01em; }
h3 { font-size:1.1rem; margin:2.5rem 0 .85rem; }
h4 { font-size:.95rem; margin:2rem 0 .6rem; color:var(--dim); }
p { margin:.75rem 0; }
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
