// Pure window-generation core for the validation campaign (harness/validate/cells.json).
//
// A "cell" declares its windows either as literal {from,to} blocks or as a strategy descriptor
// that this module expands deterministically:
//   - format-era   : eth format-boundary blocks (pre-Byzantium receipts, Merge, blob/type-3, type-4)
//   - chunk-grid    : windows straddling multiples of PORTAL_CHUNK_BLOCKS (default 500k) by ±delta
//   - deploy-floor  : a window straddling a contract's deploy block
//   - seeded-random : `count` uniform windows of `size` blocks from a fixed seed (reproducible)
//   - frontier      : the live Portal head at run time (resolved by run-cell.sh, marked here)
//
// Everything here is a pure function of its inputs (no I/O, no clock) so it is unit-testable and
// the same cells.json always produces the same windows. Auto-shrink (halve a window whose matched
// row count exceeds a threshold) is exposed as a pure rule that run-cell.sh applies at run time.

// Ethereum mainnet format-era boundary blocks — each marks a change in the on-wire receipt/tx shape
// that the Portal→sync-store transform must reproduce byte-for-byte.
export const ETH_FORMAT_ERAS = {
  // pre-Byzantium receipts carry a post-state `root`, not a `status` (Byzantium = 4,370,000).
  preByzantiumReceipts: 4_200_000,
  // The Merge: the first PoS block; total_difficulty freezes, header fields shift.
  merge: 15_537_393,
  // Dencun / EIP-4844: blob (type-3) transactions first allowed.
  blobType3: 19_426_587,
  // Pectra / EIP-7702: set-code (type-4) transactions first allowed.
  type4Prague: 22_431_084,
};

// Deterministic PRNG (mulberry32) — a fixed seed yields a fixed window set, so a campaign run is
// reproducible from cells.json alone.
export function mulberry32(seed) {
  let a = seed >>> 0;

  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// `count` uniform-random windows of `size` blocks within [from, to], driven by `seed`.
export function seededRandomWindows({ seed, from, to, count, size }) {
  if (!Number.isInteger(from) || !Number.isInteger(to) || from > to) {
    throw new Error(`seededRandomWindows: bad range [${from},${to}]`);
  }

  const span = to - from;
  if (size > span) {
    throw new Error(`seededRandomWindows: size ${size} exceeds range ${span}`);
  }

  const rand = mulberry32(seed);
  const out = [];
  for (let i = 0; i < count; i++) {
    const start = from + Math.floor(rand() * (span - size + 1));
    // Tag is qualified by (seed, size) as well as index so that a cell expanding TWO seeded-random
    // specs cannot emit colliding tags (#79): a bare `rand#${i}` repeated across specs let one spec's
    // window get folded into an unrelated spec's `attempts` history in record-result.mjs. `+shrunk`
    // suffixing (run-cell.sh) composes on top of this unchanged.
    out.push({
      from: start,
      to: start + size,
      tag: `rand#${seed}.${i}@${size}`,
    });
  }

  return out;
}

// Windows straddling every chunk-grid edge (multiple of `chunk`) inside (from, to) by ±delta, so a
// fetch has to cross the chunk-cache boundary and reassemble both sides.
export function chunkGridWindows({ from, to, chunk = 500_000, delta = 2 }) {
  const out = [];
  const first = Math.ceil((from + delta) / chunk);
  const last = Math.floor((to - delta) / chunk);
  for (let k = first; k <= last; k++) {
    const edge = k * chunk;
    out.push({ from: edge - delta, to: edge + delta, tag: `grid@${edge}` });
  }

  return out;
}

// A window straddling a contract's deploy block by ±pad — exercises the deploy floor and the empty
// pre-deploy prefix in one range.
export function deployFloorWindow({ deploy, pad = 100 }) {
  const from = Math.max(0, deploy - pad);

  return { from, to: deploy + pad, tag: `deploy@${deploy}` };
}

// Ethereum format-era windows (empty for non-eth chains — these boundaries are eth-specific).
export function formatEraWindows({ chainId, span = 50 }) {
  if (chainId !== 1) {
    return [];
  }

  return Object.entries(ETH_FORMAT_ERAS).map(([name, block]) => ({
    from: block,
    to: block + span,
    tag: `era:${name}`,
  }));
}

// Auto-shrink rule: a window whose matched-row count exceeds `threshold` is halved (lower half kept),
// keeping the byte-diff bounded. Pure — returns the (possibly) shrunk window and whether it fired.
export function halveWindow(window) {
  const half = Math.floor((window.to - window.from) / 2);

  return { ...window, to: window.from + half, halvedFrom: window.to };
}

export function autoShrink({ window, matchedRows, threshold = 50_000 }) {
  if (matchedRows <= threshold) {
    return { window, shrunk: false };
  }

  return { window: halveWindow(window), shrunk: true };
}

// Expand a single cells.json window entry. Literal entries pass through; strategy entries dispatch.
// A `frontier` entry needs the live head: if `head` is supplied it resolves, else it returns an
// unresolved marker for run-cell.sh to fill in.
export function resolveWindowEntry(entry, { head, chainId } = {}) {
  if (entry.from !== undefined && entry.to !== undefined && !entry.strategy) {
    return [{ from: entry.from, to: entry.to, tag: entry.tag ?? 'literal' }];
  }

  switch (entry.strategy) {
    case 'format-era':
      return formatEraWindows({
        chainId: entry.chainId ?? chainId,
        span: entry.span,
      });
    case 'chunk-grid':
      return chunkGridWindows(entry);
    case 'deploy-floor':
      return [deployFloorWindow(entry)];
    case 'seeded-random':
      return seededRandomWindows(entry);
    case 'frontier': {
      const span = entry.span ?? 30;
      if (head === undefined) {
        return [{ frontier: true, span, tag: 'frontier' }];
      }

      return [{ from: head - span + 1, to: head, tag: 'frontier' }];
    }
    case 'full-range': {
      // [deploy → pinned Portal head] — the F-full cell. Needs the head, resolved by run-cell.sh.
      if (head === undefined) {
        return [{ fullRange: true, from: entry.from, tag: 'full-range' }];
      }

      return [{ from: entry.from, to: head, tag: 'full-range' }];
    }
    default:
      throw new Error(
        `resolveWindowEntry: unknown strategy '${entry.strategy}'`,
      );
  }
}

// Expand every window entry of a cell into a flat, ordered list of concrete windows.
export function resolveWindows(cell, opts = {}) {
  const context = { chainId: cell.chainId, ...opts };
  const out = [];
  for (const entry of cell.windows ?? []) {
    for (const w of resolveWindowEntry(entry, context)) {
      out.push(w);
    }
  }

  return out;
}
