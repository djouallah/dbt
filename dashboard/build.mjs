/**
 * One HTML file out of `index.html` + `app.js`, twice — the live page and the frozen copy.
 *
 *     node dashboard/build.mjs --out site/index.html               # live: fetches history/ at load time
 *     node dashboard/build.mjs --out dashboard.html --snapshot     # offline: history/ inlined
 *
 * **The live page is the ONLY thing Pages publishes, and it carries no data.** That is the point of
 * the whole arrangement: `Benchmark` commits a run record, `Dashboard`'s measure job commits the
 * ledger, and the published page picks both up on the next load. Publishing became what you do when
 * the VISUALISATION changes.
 *
 * **The snapshot copy exists because the artifact has to survive.** `dashboard.html` is uploaded per
 * run and has to open from a local disk with no network years later, which a page that fetches cannot
 * do. Same file, same render path, data from a different place — `app.js` prefers an inlined
 * `#snapshot` over the network when one is present, so there is exactly one implementation of the join
 * and no way for the two copies to disagree.
 *
 * No dependencies, and no bundler: `app.js` is a single ES module and this concatenates it into the
 * shell. Node's own `--test` runner covers the module; this file is 80 lines of string substitution.
 */
import { readFileSync, writeFileSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, "..");
/** Where the live page is published — only the offline copy needs it, to reach `dag.html`. */
const PAGES = "https://djouallah.github.io/fabric-dbt-benchmark";

const argv = process.argv.slice(2);
const flag = (name, dflt = null) => {
  const i = argv.indexOf(`--${name}`);
  return i < 0 ? dflt : (argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : true);
};

/** `</script>` anywhere in an inlined payload ends the script element early, whatever the quoting
 *  around it — so it is refused rather than escaped. Neither input has ever contained one; this is
 *  here so that if one ever does, the build fails instead of the page silently truncating. */
function refuseScriptClose(what, text) {
  if (/<\/script/i.test(text)) throw new Error(`${what} contains </script — cannot be inlined`);
  return text;
}

/** Every whole run record plus the ledger, exactly as the live loader would have fetched them. The
 *  `legacy/` subdirectory is skipped for the same reason the loader skips it: those records predate
 *  the item GUIDs and cannot be joined to a ledger at all. */
function snapshot() {
  const runsDir = join(ROOT, "history", "runs");
  const records = [];
  let names = [];
  try {
    names = readdirSync(runsDir, { withFileTypes: true })
      .filter((e) => e.isFile() && e.name.endsWith(".json")).map((e) => e.name).sort();
  } catch { names = []; }
  for (const n of names) {
    // `index.json` is the page's directory listing — a JSON ARRAY of record filenames, written by
    // record.py so the live page can list this directory over raw instead of the rate-limited
    // contents API. Skipped by name AND by shape: assigning `_file` to an array is legal in JS, so
    // without the shape check a phantom "record" would reach the snapshot silently.
    if (n === "index.json") continue;
    try {
      const rec = JSON.parse(readFileSync(join(runsDir, n), "utf8"));
      if (!rec || typeof rec !== "object" || Array.isArray(rec)) {
        process.stderr.write(`  skipping ${n}: not a record\n`);
        continue;
      }
      rec._file = n;
      records.push(rec);
    } catch (ex) {
      process.stderr.write(`  skipping ${n}: unreadable (${ex.message})\n`);
    }
  }
  let ledger = null;
  try { ledger = JSON.parse(readFileSync(join(ROOT, "history", "cu.json"), "utf8")); } catch { }
  return { built: new Date().toISOString().slice(0, 16).replace("T", " ") + " UTC", records, ledger };
}

const out = flag("out");
if (!out || out === true) {
  process.stderr.write("usage: node dashboard/build.mjs --out <file.html> [--snapshot]\n");
  process.exit(2);
}

let html = readFileSync(join(HERE, "index.html"), "utf8");
const app = refuseScriptClose("app.js", readFileSync(join(HERE, "app.js"), "utf8"));

const before = html;
html = html.replace('<script type="module" src="app.js"></script>',
  `<script type="module">\n${app}\n</script>`);
if (html === before) throw new Error("index.html no longer carries the app.js script tag");

if (flag("snapshot")) {
  const data = snapshot();
  // `<` is escaped rather than the payload being trusted: JSON inside a `<script>` element only has to
  // avoid the closing tag, and escaping every `<` is the one rule that cannot be got subtly wrong.
  const json = JSON.stringify(data).replace(/</g, "\\u003c");
  const swapped = html.replace('<script id="snapshot" type="application/json"></script>',
    `<script id="snapshot" type="application/json">${json}</script>`);
  if (swapped === html) throw new Error("index.html no longer carries the snapshot script tag");
  html = swapped;
  // Read it back out of the finished document. The offline copy is the one output nobody looks at
  // until they need it — years later, off a disk, with no network — so the escaping is verified here
  // rather than trusted. A truncated or unparseable snapshot renders as "no run records", which is
  // indistinguishable from a repo that has never been measured.
  const back = html.match(/<script id="snapshot" type="application\/json">([\s\S]*?)<\/script>/);
  const parsed = JSON.parse(back[1]);
  if (parsed.records.length !== data.records.length) {
    throw new Error("the inlined snapshot did not survive the round trip");
  }
  process.stderr.write(`  snapshot: ${data.records.length} record(s), ` +
    `${data.ledger ? Object.keys(data.ledger.items || {}).length : 0} ledger item(s)\n`);

  // The DAG link is RELATIVE on the live page, where `dag.html` sits beside `index.html` in the
  // Pages artifact. The offline copy is one loose file with no sibling, so the same href would
  // dangle — silently, since a 404 off a disk looks like nothing happened. Point it at the published
  // copy instead: it needs a network, which is exactly what the GitHub link beside it already needs.
  const linked = html.replace('<a id="daglink" href="dag.html">',
    `<a id="daglink" href="${PAGES}/dag.html">`);
  if (linked === html) throw new Error("index.html no longer carries the relative dag.html link");
  html = linked;
}

mkdirSync(dirname(resolve(out)), { recursive: true });
writeFileSync(resolve(out), html, "utf8");
process.stderr.write(`  wrote ${out} (${(html.length / 1024).toFixed(0)} KB)\n`);
