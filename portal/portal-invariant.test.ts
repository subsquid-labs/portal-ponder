import { afterEach, expect, test } from 'vitest';
import { InvariantViolation } from './portal-errors.js';
import {
  getCheckMode,
  invariant,
  invariantStrict,
  setCheckMode,
} from './portal-invariant.js';

afterEach(() => setCheckMode('on')); // restore default

test("mode 'on': O(1) invariant throws InvariantViolation carrying id + context", () => {
  setCheckMode('on');
  let thrown: unknown;
  try {
    invariant('INV-8', false, 'limit out of range', () => ({
      limit: 99,
      max: 48,
    }));
  } catch (e) {
    thrown = e;
  }
  expect(thrown).toBeInstanceOf(InvariantViolation);
  const v = thrown as InvariantViolation;
  expect(v.id).toBe('INV-8');
  expect(v.context).toEqual({ limit: 99, max: 48 });
  expect(v.message).toContain('INV-8');
  expect(v.message).toContain('limit out of range');
});

test("mode 'on': a satisfied invariant is a no-op", () => {
  setCheckMode('on');
  expect(() => invariant('INV-2', true, 'ok')).not.toThrow();
});

test("mode 'off': all checks disabled", () => {
  setCheckMode('off');
  expect(getCheckMode()).toBe('off');
  expect(() => invariant('INV-2', false, 'should be skipped')).not.toThrow();
  expect(() =>
    invariantStrict('INV-5', () => false, 'should be skipped'),
  ).not.toThrow();
});

test('O(n) invariantStrict runs ONLY in strict mode (predicate not even evaluated otherwise)', () => {
  let evaluated = 0;
  const pred = () => {
    evaluated++;
    return false;
  };

  setCheckMode('on'); // O(n) check must be skipped AND its predicate not evaluated
  expect(() => invariantStrict('INV-5', pred, 'strict only')).not.toThrow();
  expect(evaluated).toBe(0);

  setCheckMode('strict');
  expect(() => invariantStrict('INV-5', pred, 'strict only')).toThrow(
    InvariantViolation,
  );
  expect(evaluated).toBe(1);
});
