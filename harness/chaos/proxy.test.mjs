import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  DEFAULT_SCENARIO,
  mergeScenario,
  pickFault,
  resolveCut,
} from './proxy.mjs';

// ── #13 deep-merge: a partial faults/head override must not NaN-out the unspecified defaults ─────

test('mergeScenario: a partial faults override keeps every other default (no NaN)', () => {
  const s = mergeScenario(DEFAULT_SCENARIO, { faults: { pReset: 1 } });
  assert.equal(s.faults.pReset, 1, 'the override is applied');
  // the shallow merge dropped these → undefined → the probability arithmetic went NaN, disabling
  // the very fault the operator asked for. Deep-merge must preserve them.
  assert.equal(s.faults.p429, 0);
  assert.equal(s.faults.p5xx, 0);
  assert.equal(s.faults.pStall, 0);
  assert.equal(s.faults.stallMs, 90_000);
  assert.equal(s.faults.pMalformedNdjson, 0);

  // head defaults survive a faults-only override
  assert.equal(s.head.mode, 'passthrough');
  assert.equal(s.head.regressBy, 100_000);
});

test('mergeScenario: a partial head override keeps the other head + all faults', () => {
  const s = mergeScenario(DEFAULT_SCENARIO, { head: { mode: 'freeze' } });
  assert.equal(s.head.mode, 'freeze');
  assert.equal(s.head.delta, 16, 'unspecified head fields survive');
  assert.equal(s.faults.p429, 0, 'faults untouched by a head-only override');
});

test('pickFault: with the partial override, the requested fault ACTUALLY fires (banding sound)', () => {
  const s = mergeScenario(DEFAULT_SCENARIO, { faults: { pReset: 1 } });
  // pReset=1 with every other probability 0 → any roll must select 'reset'. Under the shallow-merge
  // bug the other probabilities were undefined and `roll < NaN` is false everywhere, so 'reset' was
  // never reached — the fault silently disabled.
  for (const roll of [0, 0.3, 0.99]) {
    assert.equal(pickFault(s.faults, roll).kind, 'reset', `roll=${roll}`);
  }
});

test('pickFault: cumulative bands select each fault at the right roll', () => {
  const faults = {
    p429: 0.1,
    p5xx: 0.1,
    p204: 0.1,
    pTruncatedGzip: 0.1,
    pReset: 0.1,
    pStall: 0.1,
    pMalformedNdjson: 0.1,
    retryAfter: 2,
    stallMs: 1000,
  };
  assert.equal(pickFault(faults, 0.05).kind, '429');
  assert.equal(pickFault(faults, 0.15).kind, '5xx');
  assert.equal(pickFault(faults, 0.25).kind, '204');
  assert.equal(pickFault(faults, 0.35).kind, 'gzip');
  assert.equal(pickFault(faults, 0.45).kind, 'reset');
  assert.equal(pickFault(faults, 0.55).kind, 'stall');
  assert.equal(pickFault(faults, 0.65).kind, 'ndjson');
  assert.equal(pickFault(faults, 0.95).kind, 'pass');
});

// ── #14 resolveCut: the cut must land inside observed bytes, else count a missed injection ───────

test('resolveCut: clamps the desired cut into the body so the fault FIRES', () => {
  // desired cut 4000 but the body is only 500 bytes → clamp to 500 (fires, not skipped)
  const clamped = resolveCut(500, 4000);
  assert.equal(clamped.missed, false);
  assert.equal(clamped.cutAt, 500);

  // a normal case: desired cut inside the body is used verbatim
  const inside = resolveCut(10_000, 3000);
  assert.equal(inside.missed, false);
  assert.equal(inside.cutAt, 3000);

  // a 1-byte body still fires (cutAt clamped up to at least 1)
  const tiny = resolveCut(1, 4000);
  assert.equal(tiny.missed, false);
  assert.equal(tiny.cutAt, 1);
});

test('resolveCut: an EMPTY upstream body is a MISSED injection (cannot fire) — reported', () => {
  const missed = resolveCut(0, 4000);
  assert.equal(
    missed.missed,
    true,
    'an empty body cannot fire the cut → counted as missed',
  );
});
