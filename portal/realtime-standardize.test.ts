import { expect, test } from 'vitest';
import { standardizeTransactions } from '@/rpc/actions.js';
import { encodeTransaction } from '@/sync-store/encode.js';

// Regression for issue #27. A non-compliant RPC provider returned full-tx `eth_getBlockByNumber`
// responses whose typed transactions OMIT the `accessList` key entirely. Upstream ponder's
// `standardizeTransactions` validates/defaults eight other tx fields but never requires nor
// defaults `accessList`; the `undefined` then reaches `encodeTransaction`
// (`accessList: transaction.accessList ? JSON.stringify(...) : null`) and, via
// `onConflictDoNothing`, becomes a permanent NULL — silent column-level data loss even on txs
// that carry a real on-chain access list. The fork's wiring patch makes a missing `accessList`
// on typed-envelope txs (EIP-2930/1559/4844/7702 → 0x1/0x2/0x3/0x4) a loud `RpcProviderError`,
// while leaving legacy (0x0), OP deposit (0x7e), and unknown/system envelopes untouched.

// A fully-valid transaction that carries every property `standardizeTransactions` inspects, so a
// throw can only be attributed to the field the case deliberately mutates. `type`/`accessList`
// are supplied by the caller. Numbers stay small so the PG_*_MAX ceiling checks never fire.
const tx = (over: Record<string, unknown>): any => ({
  hash: '0xabc',
  transactionIndex: '0x0',
  blockNumber: '0x1',
  blockHash: '0xblock',
  from: '0xfrom',
  to: '0xto',
  input: '0x',
  value: '0x0',
  nonce: '0x0',
  r: '0x0',
  s: '0x0',
  v: '0x0',
  gas: '0x0',
  ...over,
});

// `standardizeTransactions(transactions, request)` — `request` only feeds the error `meta`.
const REQUEST = {
  method: 'eth_getBlockByNumber',
  params: ['0x1', true],
} as any;

const standardize = (t: any) => standardizeTransactions([t], REQUEST);

// ─────────────────────── (a) the exact provider shape → throws (mutation check) ───────────────────────

// This is the mutation check: it MUST fail on the unfixed upstream code (which lets the missing
// key sail through and returns the tx unchanged). A type-0x2 tx missing ONLY `accessList`.
test('a valid type-0x2 tx missing only accessList → throws, message names accessList (issue #27)', () => {
  const t = tx({ type: '0x2' });
  expect(t.accessList).toBeUndefined();
  expect(() => standardize(t)).toThrowError(/transaction\.accessList/);
  // and it names the offending type, not a generic message
  expect(() => standardize(t)).toThrowError(/0x2/);
});

// An explicit `accessList: null` (as opposed to an omitted key) must fail the same way: encode.ts
// treats null and undefined identically (`transaction.accessList ? … : null`), so a non-compliant
// provider that sends `null` would still land the same permanent NULL. The guard uses `== null`.
test('a type-0x2 tx with explicit accessList: null → throws (message names accessList)', () => {
  const t = tx({ type: '0x2', accessList: null });
  expect(t.accessList).toBeNull();
  expect(() => standardize(t)).toThrowError(/transaction\.accessList/);
});

// ─────────────────────── (b) every other typed envelope missing accessList → throws ───────────────────────

test('type 0x1/0x3/0x4 missing accessList → throws too (all carry accessList per spec)', () => {
  for (const type of ['0x1', '0x3', '0x4']) {
    const t = tx({ type });
    expect(() => standardize(t)).toThrowError(/transaction\.accessList/);
  }
});

// ─────────────────────── (c) types that legitimately carry no accessList → NO throw ───────────────────────

test('type 0x0 (legacy) and 0x7e (OP deposit) without accessList → no throw', () => {
  const legacy = tx({ type: '0x0' });
  const deposit = tx({ type: '0x7e' });
  expect(() => standardize(legacy)).not.toThrow();
  expect(() => standardize(deposit)).not.toThrow();
  // untouched: the fix must NOT fabricate an accessList on these
  expect(standardize(legacy)[0]!.accessList).toBeUndefined();
  expect(standardize(deposit)[0]!.accessList).toBeUndefined();
});

// ─────────────────────── (d) an unknown / system envelope → NO throw ───────────────────────

test('an unknown tx type (0x6a) without accessList → no throw (system/Arbitrum-internal shapes)', () => {
  const t = tx({ type: '0x6a' });
  expect(() => standardize(t)).not.toThrow();
  expect(standardize(t)[0]!.accessList).toBeUndefined();
});

// ─────────────────────── (e) compliant typed txs pass through + encode contract ───────────────────────

test('type-0x2 with accessList ([] and non-empty) passes through unchanged and encodes exactly', () => {
  // empty list — the common shape; must round-trip to the JSON string '[]', never NULL
  const empty = tx({ type: '0x2', accessList: [] });
  const outEmpty = standardize(empty);
  expect(outEmpty[0]!.accessList).toEqual([]);
  expect(
    encodeTransaction({ transaction: outEmpty[0]!, chainId: 1 }).accessList,
  ).toBe('[]');

  // a real, non-empty access list — the 4 on-chain txs in issue #27's observed window
  const list = [
    {
      address: '0x000000000000000000000000000000000000dead',
      storageKeys: [
        '0x0000000000000000000000000000000000000000000000000000000000000001',
      ],
    },
  ];
  const full = tx({ type: '0x2', accessList: list });
  const outFull = standardize(full);
  expect(outFull[0]!.accessList).toEqual(list);
  expect(
    encodeTransaction({ transaction: outFull[0]!, chainId: 1 }).accessList,
  ).toBe(JSON.stringify(list));
});

// ─────────────────────── (f) opt-in: PORTAL_ALLOW_MISSING_ACCESS_LIST ───────────────────────
// The default guard is right for a compliant provider, but SQD's own zkSync RPC proxy returns
// EIP-1559 (0x2) txs with NO `accessList` key at all — so on that documented-supported endpoint
// the guard crashes the whole app. `PORTAL_ALLOW_MISSING_ACCESS_LIST` (default OFF) lets an
// operator who knows the omission is legitimate proceed and store the honest NULL, exactly as the
// Portal path already does for datasets that lack the column entirely (#27, #110/#111).

// Run `body` with the knob forced to `value`, restoring the prior env afterwards (no cross-test leak).
const withKnob = (value: string | undefined, body: () => void): void => {
  const prev = process.env.PORTAL_ALLOW_MISSING_ACCESS_LIST;
  if (value === undefined) {
    delete process.env.PORTAL_ALLOW_MISSING_ACCESS_LIST;
  } else {
    process.env.PORTAL_ALLOW_MISSING_ACCESS_LIST = value;
  }

  try {
    body();
  } finally {
    if (prev === undefined) {
      delete process.env.PORTAL_ALLOW_MISSING_ACCESS_LIST;
    } else {
      process.env.PORTAL_ALLOW_MISSING_ACCESS_LIST = prev;
    }
  }
};

// The loud default error must NAME the escape hatch — an operator hitting the zkSync crash needs to
// be told the exact knob, not left to grep the source. The guidance rides on `RpcProviderError.meta`
// (surfaced by the logger, like every other error here), not the terse message line.
test('default (knob unset) → missing accessList still throws, and the guidance names the knob', () => {
  withKnob(undefined, () => {
    const t = tx({ type: '0x2' });
    expect(() => standardize(t)).toThrowError(/transaction\.accessList/);

    let meta: string[] = [];
    try {
      standardize(t);
    } catch (err) {
      meta = (err as { meta?: string[] }).meta ?? [];
    }

    expect(meta.join(' ')).toMatch(/PORTAL_ALLOW_MISSING_ACCESS_LIST/);
  });
});

// With the knob set, EVERY typed envelope missing accessList flows through instead of throwing, and
// the absent value stays `undefined` → encodeTransaction maps it to a NULL (honest absence, not []).
test('knob set → missing accessList on 0x1/0x2/0x3/0x4 passes through, encodes to NULL', () => {
  withKnob('1', () => {
    for (const type of ['0x1', '0x2', '0x3', '0x4']) {
      const t = tx({ type });
      expect(() => standardize(t)).not.toThrow();
      const out = standardize(t);
      expect(out[0]!.accessList).toBeUndefined();
      expect(
        encodeTransaction({ transaction: out[0]!, chainId: 1 }).accessList,
      ).toBeNull();
    }
  });
});

// The knob is an escape hatch for ABSENT lists only — it must never fabricate or drop a REAL one:
// a present access list still round-trips unchanged when the knob is on.
test('knob set → a present accessList is still preserved unchanged', () => {
  withKnob('1', () => {
    const list = [
      {
        address: '0x000000000000000000000000000000000000dead',
        storageKeys: [
          '0x0000000000000000000000000000000000000000000000000000000000000001',
        ],
      },
    ];
    const t = tx({ type: '0x2', accessList: list });
    const out = standardize(t);
    expect(out[0]!.accessList).toEqual(list);
    expect(
      encodeTransaction({ transaction: out[0]!, chainId: 1 }).accessList,
    ).toBe(JSON.stringify(list));
  });
});

// ─────────────────────── (g) strict parse: falsy strings must NOT enable the opt-in ───────────────────────
// The knob gates a DATA-INTEGRITY guard, so its parse must be strict: `Boolean(process.env.X)` is a
// footgun — `X=0`, `X=false`, `X=no` are all non-empty strings, so `Boolean(...)` is `true` and the
// guard would be silently DISABLED by a value an operator plainly means as "off". The parse is
// `=== '1'` (mirroring PORTAL_REALTIME's `=== 'stream'`): only the exact string `'1'` opts in;
// everything else — including `'0'`/`'false'`/`'no'`/`''` and unset — keeps the guard fail-loud.
// This test is the mutation lock: flipping the parse to always-enabled (e.g. `Boolean(...)` or a
// constant `true`) turns every `.toThrow()` below RED.
test('strict parse → only "1" enables; "0"/"false"/"no"/"" (and unset) stay fail-loud', () => {
  // Every value an operator could reasonably intend as OFF must leave the guard throwing.
  for (const off of ['0', 'false', 'no', '', undefined]) {
    withKnob(off, () => {
      const t = tx({ type: '0x2' });
      expect(t.accessList).toBeUndefined();
      expect(() => standardize(t)).toThrowError(/transaction\.accessList/);
      expect(() => standardize(t)).toThrowError(/0x2/);
    });
  }

  // And a non-"1" truthy-looking string is still OFF (guards the exact-match, not merely "non-empty").
  withKnob('true', () => {
    const t = tx({ type: '0x2' });
    expect(() => standardize(t)).toThrowError(/transaction\.accessList/);
  });

  // Only the exact "1" opts in — the missing list flows through and encodes to an honest NULL.
  withKnob('1', () => {
    const t = tx({ type: '0x2' });
    expect(() => standardize(t)).not.toThrow();
    expect(
      encodeTransaction({ transaction: standardize(t)[0]!, chainId: 1 })
        .accessList,
    ).toBeNull();
  });
});
