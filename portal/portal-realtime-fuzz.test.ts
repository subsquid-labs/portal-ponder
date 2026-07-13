/**
 * RG2 — silent-gap property fuzzer (realtime campaign plan item (e).2).
 *
 * WHAT IT PROVES — the TRICHOTOMY. The fork's brand is "loud-restart, never-wrong-data". A stream of
 * adversarial block deliveries (dropped / duplicated / out-of-order / mutated hash|parentHash /
 * interleaved 204-idle & 409-fork phases) driven through the REAL realtime consumer
 * `portalRealtimeEvents` must resolve EVERY delivery into exactly one of three outcomes:
 *   1. APPENDED    — the code's realized chain grew by that block (model chain matches), or
 *   2. RECONCILED  — a reorg (window rolled back to the right ancestor, new fork adopted) or a
 *                    duplicate (idempotent no-op, no state change), or
 *   3. FATAL       — the consumer THREW (a `gap`: parent unknown / beyond the window).
 * A FOURTH outcome — the realized chain SILENTLY DIVERGES from the canonical chain with no throw (a
 * skipped/dropped block that is neither reorged, deduped, nor fatal) — is a SILENT GAP and is the
 * exact defect this test exists to make impossible. Any such divergence FAILS with a repro seed.
 *
 * INDEPENDENCE (load-bearing — a wrong reference model gives a false green that proves nothing). The
 * expected outcome for each delivery is derived PURELY from hash/parentHash relationships against an
 * independently-maintained canonical chain — it NEVER calls `reconcile()` to decide the expected
 * answer, so the check is not circular. The code's realized chain is then reconstructed from the
 * consumer's OWN emitted event stream (`block` appends; `reorg` rolls back to its commonAncestor) and
 * asserted EQUAL to the model — or the model predicted `gap` and the consumer must have thrown.
 *
 * SEAM (why the whole trichotomy, not just the pure function). We fuzz `portalRealtimeEvents`, the
 * async consumer that drives `reconcile` AND advances the private `unfinalized` window, emits the
 * block/reorg events, and THROWS on `gap`. A silent skip can only occur in THAT loop (reconcile is
 * pure and separately unit-tested); reconstructing the realized chain from the events the consumer
 * actually emits exercises the append + rollback + fatal control flow end-to-end, so a regression that
 * dropped a block, mis-rolled a reorg, or swallowed a gap would be caught here where the pure-function
 * test could not see it. We feed blocks via a scripted `fetchImpl` /stream mock (the same seam every
 * existing realtime test uses) and interleave 204 (idle) and 409 (fork-negotiation) responses so the
 * I/O shell's own paths are on the wire too.
 *
 * FINALITY IS HELD INERT for the core trichotomy (`finalizedHead` stays at/below the window base, poll
 * disabled): a mid-sequence finalize prunes the private window and advances the anchor, which changes
 * what `reconcile` sees and would make the reference model depend on finalize timing rather than pure
 * chain structure. Finality/finalize is exhaustively covered by the hand-written realtime tests and
 * the B1/RT-G10 watchdog suites; this fuzzer isolates the append|reorg|dup|gap decision, which is where
 * a silent gap lives. (A separate 409/204 interleave still drives the shell's negotiation paths.)
 *
 * REPRODUCIBILITY. A seeded mulberry32 PRNG (NOT Math.random). Each sequence's seed is derived from a
 * base seed; on ANY trichotomy violation the failing seed is printed so the exact sequence repros. The
 * default run does N = 2500 sequences (a few seconds; the test sets an explicit timeout above vitest's
 * 15000ms global). A heavy/nightly mode runs the acceptance bar of >= 10^5 sequences via `FUZZ_N`:
 *     FUZZ_N=100000 <run the portal suite>          # >= 10^5 sequences, zero trichotomy violations
 * `FUZZ_SEED` overrides the base seed (default 0x1234_5678) to replay or extend a run.
 */
import { expect, test } from 'vitest';
import { type Light, portalRealtimeEvents } from './portal-realtime.js';

// ─────────────────────────────── seeded PRNG (mulberry32) ───────────────────────────────

/** mulberry32 — a small, fast, seeded PRNG. Deterministic given the seed; never Math.random. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;

  return () => {
    a += 0x6d2b79f5;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);

    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// ─────────────────────────────── wire types & helpers ───────────────────────────────

/** A raw /stream batch — the shape `streamHotBlocks` parses off the NDJSON wire. */
type Batch = {
  header: {
    number: number;
    hash: string;
    parentHash: string;
    timestamp: number;
  };
  logs: unknown[];
};

/** One scripted /stream connection: a 200 that streams blocks, a 204 idle, or a 409 fork-negotiation. */
type Conn =
  | { status: 200; blocks: Batch[] }
  | { status: 204 }
  | { status: 409; previousBlocks: Array<{ number: number; hash: string }> };

const enc = new TextEncoder();

/**
 * A scripted Portal /stream mock: one entry per connection. A 200 streams its NDJSON batches then
 * closes; a 204 is a no-data idle re-poll; a 409 returns `{ previousBlocks }`. Past the last entry it
 * returns 204 forever and calls `onExhausted` (the caller aborts). Mirrors the existing tests'
 * `mockForkFetch` / `mockFetch` shape so the shell's real reconnect / negotiation paths are exercised.
 */
function mockStreamFetch(conns: Conn[], onExhausted: () => void): typeof fetch {
  let i = 0;

  return (async (_url: string, _init: { body: string }) => {
    if (i >= conns.length) {
      onExhausted();

      return { status: 204, ok: false, body: null };
    }
    const conn = conns[i++]!;
    if (conn.status === 204) {
      return { status: 204, ok: false, body: null };
    }
    if (conn.status === 409) {
      const text = JSON.stringify({ previousBlocks: conn.previousBlocks });
      const body = new ReadableStream({
        start(c) {
          c.enqueue(enc.encode(text));
          c.close();
        },
      });

      return { status: 409, ok: false, body };
    }
    const lines = conn.blocks.map((b) => `${JSON.stringify(b)}\n`).join('');
    const body = new ReadableStream({
      start(c) {
        c.enqueue(enc.encode(lines));
        c.close();
      },
    });

    return { status: 200, ok: true, body };
  }) as unknown as typeof fetch;
}

const batch = (b: Light): Batch => ({
  header: {
    number: b.number,
    hash: b.hash,
    parentHash: b.parentHash,
    timestamp: b.timestamp,
  },
  logs: [],
});

// ─────────────────────────────── the independent reference model ───────────────────────────────

type Expected = 'append' | 'duplicate' | 'reorg' | 'gap';

/**
 * The canonical-chain reference model. Maintains the "true" unfinalized window above a fixed finalized
 * `anchor`, mirroring the semantics of `reconcile` PURELY from hash/parentHash relationships — WITHOUT
 * ever calling `reconcile`. `classify` decides the expected trichotomy class for a delivery; `apply`
 * mutates the model to what a CORRECT consumer's realized chain must then be (append grows it, reorg
 * rolls back the suffix and adopts the fork, duplicate/gap leave it unchanged — a gap fatals the
 * consumer, ending the sequence).
 */
class ChainModel {
  readonly anchor: Light;
  window: Light[] = [];
  /** every hash ever adopted into the window (for the "known duplicate re-delivery" case) */
  private readonly seen = new Set<string>();

  constructor(anchor: Light) {
    this.anchor = anchor;
    this.seen.add(anchor.hash);
  }

  private tip(): Light | undefined {
    return this.window[this.window.length - 1];
  }

  /**
   * The expected trichotomy class for `next`, derived purely from chain structure. Deliberately
   * parallels `reconcile`'s CONTRACT (append if it extends the tip / the anchor on an empty window;
   * duplicate if it IS the tip / the anchor re-delivered; reorg if the parent is a known earlier
   * window block or the anchor; gap otherwise) but is written independently against the model's own
   * state so it is not a copy of the code under test.
   */
  classify(next: Light): Expected {
    const tip = this.tip();
    if (tip === undefined) {
      if (next.hash === this.anchor.hash) return 'duplicate';
      if (next.parentHash === this.anchor.hash) return 'append';

      return 'gap';
    }
    if (next.hash === tip.hash) return 'duplicate';
    if (next.parentHash === tip.hash) return 'append';
    // Fork off a known earlier window block → reorg with that as the common ancestor.
    const idx = this.window.findIndex((b) => b.hash === next.parentHash);
    if (idx !== -1) return 'reorg';
    // Fork at the finality boundary: parent IS the anchor → the whole window is the reorged suffix.
    if (next.parentHash === this.anchor.hash) return 'reorg';

    return 'gap';
  }

  /** Advance the model to the realized chain a CORRECT consumer must hold after processing `next`. */
  apply(next: Light, cls: Expected): void {
    if (cls === 'append') {
      this.window.push(next);
      this.seen.add(next.hash);

      return;
    }
    if (cls === 'reorg') {
      if (next.parentHash === this.anchor.hash) {
        this.window = [next];
      } else {
        const idx = this.window.findIndex((b) => b.hash === next.parentHash);
        this.window = this.window.slice(0, idx + 1);
        this.window.push(next);
      }
      this.seen.add(next.hash);

      return;
    }
    // duplicate / gap: no state change (a gap additionally fatals the consumer — handled by the caller).
  }
}

// ─────────────────────────────── code-side realized-chain reconstruction ───────────────────────────────

type RtEvent =
  | {
      type: 'block';
      block: { number: string; hash: string; parentHash: string };
    }
  | { type: 'reorg'; block: Light; reorgedBlocks: Light[] }
  | { type: 'finalize'; block: Light };

/**
 * Reconstruct the consumer's realized unfinalized chain (as `{hash, parentHash}` from its EMITTED
 * events, independent of its private state): a `block` event appends that block; a `reorg` event rolls
 * the window back to (and including) its `commonAncestor`. We key on `hash` (globally unique per block)
 * so the block event's hex `number` never has to be decoded. `finalize` is inert here (finality held
 * below the window base) and asserted absent.
 */
function replayRealized(
  events: RtEvent[],
): Array<{ hash: string; parentHash: string }> {
  const chain: Array<{ hash: string; parentHash: string }> = [];
  for (const e of events) {
    if (e.type === 'block') {
      chain.push({ hash: e.block.hash, parentHash: e.block.parentHash });
      continue;
    }
    if (e.type === 'reorg') {
      const anchorHash = e.block.hash;
      let cut = chain.length;
      for (let k = chain.length - 1; k >= 0; k--) {
        if (chain[k]!.hash === anchorHash) {
          cut = k + 1;
          break;
        }
        cut = k;
      }
      chain.length = cut;
    }
    // 'finalize' — inert (see header); asserted not to appear.
  }

  return chain;
}

// ─────────────────────────────── adversarial sequence generator ───────────────────────────────

const hexHash = (rng: () => number): string =>
  `0x${Math.floor(rng() * 0xffffffff)
    .toString(16)
    .padStart(8, '0')}`;

/**
 * Build ONE adversarial delivery sequence from a seed. Starts from a canonical anchor, then repeatedly
 * emits a delivery drawn from the enumerated classes — dropped block (skip a parent → gap), duplicate,
 * out-of-order, mutated parentHash / hash (→ gap or reorg), and reorg (fork off an earlier block). The
 * returned `blocks` are the raw batches to feed the consumer, in order; the model is advanced in
 * lockstep and each delivery's EXPECTED class is recorded so the harness can assert the trichotomy.
 * Generation STOPS at the first delivery the model predicts is a `gap` (the consumer fatals there, so
 * anything after it would never be processed).
 */
function buildSequence(
  seed: number,
  anchor: Light,
): { blocks: Batch[]; expected: Expected[]; endsInGap: boolean } {
  const rng = mulberry32(seed);
  const model = new ChainModel(anchor);
  const blocks: Batch[] = [];
  const expected: Expected[] = [];
  const steps = 4 + Math.floor(rng() * 24); // 4..27 deliveries
  // canonical continuation: the block that would legitimately extend the current model tip.
  const canonicalNext = (): Light => {
    const tip = model.window[model.window.length - 1] ?? anchor;

    return {
      number: tip.number + 1,
      hash: hexHash(rng),
      parentHash: tip.hash,
      timestamp: tip.number + 1,
    };
  };

  for (let s = 0; s < steps; s++) {
    const roll = rng();
    let next: Light;
    if (roll < 0.5) {
      // append the canonical next block (the happy path; keeps the chain growing so other classes have
      // material to fork off / duplicate).
      next = canonicalNext();
    } else if (roll < 0.62) {
      // DUPLICATE: re-deliver the current tip (or the anchor when the window is empty).
      const tip = model.window[model.window.length - 1] ?? anchor;
      next = { ...tip };
    } else if (roll < 0.74) {
      // DROPPED BLOCK: skip a parent — a block two heights above the tip with an unknown parent → gap.
      const tip = model.window[model.window.length - 1] ?? anchor;
      next = {
        number: tip.number + 2,
        hash: hexHash(rng),
        parentHash: hexHash(rng), // a parent we never delivered
        timestamp: tip.number + 2,
      };
    } else if (roll < 0.84) {
      // REORG: fork off a known earlier block (a window entry or the anchor).
      const pool = [anchor, ...model.window];
      const forkOff = pool[Math.floor(rng() * pool.length)]!;
      next = {
        number: forkOff.number + 1,
        hash: hexHash(rng),
        parentHash: forkOff.hash,
        timestamp: forkOff.number + 1,
      };
    } else if (roll < 0.92) {
      // MUTATED parentHash: the canonical next block, but its parentHash is rewritten to garbage → gap.
      next = canonicalNext();
      next.parentHash = hexHash(rng);
    } else {
      // OUT-OF-ORDER / MUTATED hash: re-emit an already-delivered EARLIER block, or a canonical block
      // with a mutated hash (both are either a duplicate/reorg/gap by structure — the model decides).
      if (model.window.length > 1 && rng() < 0.5) {
        const old =
          model.window[Math.floor(rng() * (model.window.length - 1))]!;
        next = { ...old };
      } else {
        next = canonicalNext();
        next.hash = hexHash(rng); // a new hash extending the tip is still a plain append
      }
    }

    const cls = model.classify(next);
    blocks.push(batch(next));
    expected.push(cls);
    if (cls === 'gap') {
      return { blocks, expected, endsInGap: true };
    }
    model.apply(next, cls);
  }

  return { blocks, expected, endsInGap: false };
}

// ─────────────────────────────── the property ───────────────────────────────

/**
 * Run ONE sequence end-to-end and assert the trichotomy. Returns nothing; throws (via expect) with the
 * seed embedded on any violation. Re-derives the model here (in lockstep with the consumer's events) so
 * the assertion is against a freshly-computed independent chain, not the generator's bookkeeping.
 */
async function runSequence(seed: number, anchor: Light): Promise<void> {
  const { blocks, expected, endsInGap } = buildSequence(seed, anchor);

  const ac = new AbortController();
  // Deliver the whole adversarial NDJSON on ONE 200 (so reconcile sees the exact generated order on a
  // single connection), then one 204 idle re-poll (exercising the shell's no-data path), then exhaust
  // → the caller aborts. `finalizePollMs: 2` keeps the shell's inter-poll `tickSleep` at ~1ms (it is
  // `min(500, floor(pollMs/2))`), so 10^5 sequences stay fast; finality is still INERT because the
  // probed head equals the anchor (never above the window base ⇒ `takeFinalized` yields no finalizedTip
  // and no finalize event fires however often the cadence polls). See the file header.
  const conns: Conn[] = [{ status: 200, blocks }, { status: 204 }];
  const iter = portalRealtimeEvents({
    portalUrl: 'http://portal',
    headers: {},
    fromBlock: anchor.number + 1,
    anchor,
    logs: [],
    fetchImpl: mockStreamFetch(conns, () => ac.abort()),
    signal: ac.signal,
    // finality held INERT below the window base — see the file header.
    finalizedHead: async () => ({ number: anchor.number }),
    finalizePollMs: 2,
  });

  const events: RtEvent[] = [];
  let threw = false;
  let thrownMessage = '';
  try {
    for await (const e of iter) {
      events.push(e as RtEvent);
    }
  } catch (err) {
    threw = true;
    thrownMessage = err instanceof Error ? err.message : String(err);
  }

  const seedTag = `seed=0x${(seed >>> 0).toString(16)}`;

  // 1. FATAL branch of the trichotomy: the model predicts a gap ⟺ the consumer must throw. A gap that
  //    did NOT throw is a SILENT SKIP (the defect). A throw where the model predicted no gap is an
  //    over-eager fatal (also a violation — it means a legitimately appendable/reconcilable delivery
  //    was treated as fatal).
  if (endsInGap) {
    expect(
      threw,
      `expected a FATAL gap but the consumer did not throw (${seedTag})`,
    ).toBe(true);
    expect(
      thrownMessage,
      `gap must fatal with the loud unknown-parent restart message (${seedTag})`,
    ).toMatch(/unknown parent|reconcile safely/i);
  } else {
    expect(
      threw,
      `no gap was expected but the consumer threw: ${thrownMessage} (${seedTag})`,
    ).toBe(false);
  }

  // finalize is held inert — none must be emitted (a stray finalize would mutate the window and void
  // the reconstruction below).
  expect(
    events.some((e) => e.type === 'finalize'),
    `no finalize expected while finality is held below the window base (${seedTag})`,
  ).toBe(false);

  // 2. APPEND / RECONCILE branch: reconstruct the consumer's realized chain from its emitted events and
  //    assert it EQUALS the independently-derived canonical chain (processing every delivery the model
  //    did NOT predict as a terminating gap). This is the load-bearing assertion — the ONLY way the two
  //    can differ with no throw is a silently-skipped block, which is exactly a trichotomy violation.
  const model = new ChainModel(anchor);
  const processed = endsInGap ? expected.length - 1 : expected.length;
  for (let k = 0; k < processed; k++) {
    const b = blocks[k]!;
    const light: Light = {
      number: b.header.number,
      hash: b.header.hash,
      parentHash: b.header.parentHash,
      timestamp: b.header.timestamp,
    };
    model.apply(light, expected[k]!);
  }
  const modelChain = model.window.map((b) => ({
    hash: b.hash,
    parentHash: b.parentHash,
  }));
  const realized = replayRealized(events);

  expect(
    realized,
    `SILENT-GAP TRICHOTOMY VIOLATION: the consumer's realized chain diverged from the canonical model with no fatal (${seedTag}). ` +
      `expected classes=${expected.join(',')}${endsInGap ? ' [last=gap→fatal]' : ''}`,
  ).toEqual(modelChain);
}

// ─────────────────────────────── the driver ───────────────────────────────

const DEFAULT_N = 2500;
const N = Number(process.env.FUZZ_N ?? DEFAULT_N);
const BASE_SEED = Number(process.env.FUZZ_SEED ?? 0x12345678);

// A fixed finalized anchor well below every generated block; the whole fuzz window lives above it.
const ANCHOR: Light = {
  number: 1000,
  hash: '0xanchor',
  parentHash: '0xpre',
  timestamp: 1000,
};

// Per-sequence cost is a few ms (one 200 + one 204 idle at ~1ms tickSleep, then abort). Budget ~5ms/seq
// plus a 10s floor so the DEFAULT run (N=2500 ⇒ ~22.5s cap) clears the vitest 15000ms global testTimeout
// via this explicit override, and the heavy bar (FUZZ_N=100000 ⇒ ~510s cap) has headroom.
const TIMEOUT_MS = Math.max(15_000, N * 5 + 10_000);

test(
  `RG2 silent-gap fuzzer: ${N} adversarial sequences, trichotomy (append | reorg/dup | fatal) never silently skips`,
  async () => {
    let violations = 0;
    let firstFailSeed: number | undefined;
    for (let i = 0; i < N; i++) {
      const seed = (BASE_SEED + i * 0x9e3779b1) >>> 0; // golden-ratio stride → well-spread seeds
      try {
        await runSequence(seed, ANCHOR);
      } catch (err) {
        violations += 1;
        if (firstFailSeed === undefined) {
          firstFailSeed = seed;
          // Surface the first failure loudly and immediately with its repro seed.
          const msg = err instanceof Error ? err.message : String(err);
          throw new Error(
            `RG2 fuzzer FAILED on sequence ${i} (seed=0x${seed.toString(16)}). Repro exactly this sequence: ` +
              `FUZZ_N=1 FUZZ_SEED=${seed} (BASE_SEED becomes this seed and i=0 reuses it). Underlying: ${msg}`,
          );
        }
      }
    }

    expect(violations).toBe(0);
  },
  TIMEOUT_MS,
);
