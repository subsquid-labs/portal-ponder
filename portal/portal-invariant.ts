/**
 * portal-invariant.ts — runtime invariant checks.
 *
 * The Portal layer is organised around explicit, provable invariants (portal/INVARIANTS.md,
 * INV-1…INV-16). This module lets the code ASSERT them at runtime and cross-reference the
 * catalog by id, so a violated assumption is a loud, attributable crash rather than silent
 * data corruption.
 *
 * Modes (set once by the shell from `PortalConfig.checks`, i.e. the PORTAL_CHECKS env knob):
 *   - "off":    all checks disabled (perf escape hatch).
 *   - "on":     O(1) checks run (default). A loud crash beats silent corruption.
 *   - "strict": additionally enables O(n) whole-structure checks (tests/CI).
 *
 * Pure w.r.t. I/O and env: the mode is injected by the shell via `setCheckMode`, never read
 * from `process.env` here — so the functional core stays testable and side-effect-free.
 */
import { InvariantViolation } from './portal-errors.js';

export type CheckMode = 'off' | 'on' | 'strict';

let mode: CheckMode = 'on';

/** Set the active check mode (called once by the shell after loading PortalConfig). */
export const setCheckMode = (m: CheckMode): void => {
  mode = m;
};

export const getCheckMode = (): CheckMode => mode;

/**
 * Assert an O(1) invariant. Throws `InvariantViolation` (carrying `id` + `ctx`) when `cond`
 * is false and checks are enabled. `ctx` is a thunk so the (possibly non-trivial) context
 * object is only built on the failure path.
 */
export function invariant(
  id: string,
  cond: boolean,
  msg: string,
  ctx?: () => Record<string, unknown>,
): void {
  if (mode === 'off') return;
  if (!cond) throw new InvariantViolation(id, msg, ctx?.());
}

/**
 * Assert an O(n) invariant — the predicate is only EVALUATED in "strict" mode, so the
 * whole-structure scan costs nothing in production ("on") or when disabled ("off").
 */
export function invariantStrict(
  id: string,
  cond: () => boolean,
  msg: string,
  ctx?: () => Record<string, unknown>,
): void {
  if (mode !== 'strict') return;
  if (!cond()) throw new InvariantViolation(id, msg, ctx?.());
}

export { InvariantViolation };
