/**
 * Regenerate networks.json from the AUTHORITATIVE SQD docs network matrix.
 *   node --experimental-strip-types harness/compat/fetch-networks.ts
 *
 * The docs page (https://docs.sqd.dev/en/data/all-networks) is the source of truth
 * for per-network capability flags (traces / stateDiffs / realtime) and block-range
 * caveats (the `tooltip`). The Portal /datasets endpoint does NOT expose these — it
 * only lists which datasets a given portal serves. So we snapshot the docs here and
 * check per-portal existence live at report time.
 */
import { writeFileSync } from "node:fs";
import { join } from "node:path";

const DOCS = "https://docs.sqd.dev/en/data/all-networks";

const html = await fetch(DOCS).then((r) => r.text());
const unescaped = html
  .replace(/\\n/g, "\n")
  .replace(/\\"/g, '"')
  .replace(/\\u003e/g, ">")
  .replace(/\\u003c/g, "<")
  .replace(/\\\\/g, "\\");

const re = /\{[^{}]*?"slug"\s*:\s*"[^"]+"[^{}]*?\}/g;
const out: Record<string, any> = {};
let m: RegExpExecArray | null;
let rows = 0;
while ((m = re.exec(unescaped))) {
  const t = m[0];
  if (!/"traces"/.test(t) || !/"stateDiffs"/.test(t)) continue;
  let o: any;
  try {
    o = JSON.parse(t);
  } catch {
    continue;
  }
  rows++;
  const cid = Number(o.chainId);
  if (!Number.isFinite(cid) || cid === 0) continue; // skip non-numeric / missing chainId
  out[cid] = {
    name: o.name,
    slug: o.slug,
    type: o.type,
    portal: !!o.portal,
    realtime: !!o.realtime,
    traces: !!o.traces,
    stateDiffs: !!o.stateDiffs,
    ...(o.tooltip ? { note: o.tooltip } : {}),
  };
}

const path = join(import.meta.dirname, "networks.json");
writeFileSync(
  path,
  JSON.stringify(
    {
      _source: DOCS,
      _generated: "run fetch-networks.ts to refresh",
      networks: out,
    },
    null,
    2,
  ) + "\n",
);
console.log(
  `parsed ${rows} rows → wrote ${Object.keys(out).length} networks (with numeric chainId) to ${path}`,
);
