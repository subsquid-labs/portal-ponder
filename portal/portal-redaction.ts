/**
 * portal-redaction.ts — the Portal layer's endpoint-redaction policy for emitted diagnostics.
 *
 * Threat model (LOW severity, defense-in-depth): the raw per-chain Portal URL is interpolated into
 * several Portal-layer emitted-text sites — log lines AND fatal-error messages. Those texts routinely get
 * pasted into a public GitHub issue or a support ticket. For an operator on a DEDICATED / private Portal,
 * the private HOST leaking that way is an infra-info leak. The host is NOT a credential (the `x-api-key` is
 * carried separately and never enters any URL or message), so this is hardening, not a vuln fix.
 *
 * Policy invariant: when the request is AUTHENTICATED (a Portal API key is set — our principled proxy for
 * a private/dedicated Portal), the Portal URL is NEVER emitted; it is replaced by a fixed placeholder. When
 * UNKEYED (the public default Portal), the URL is not secret and IS a useful diagnostic ("you're hammering
 * the free public Portal"), so it is shown in full. The chain name is always carried alongside, so the
 * redacted form loses no routing context.
 *
 * Why a SEPARATE module (not folded into portal-errors.ts): this policy is shared by BOTH the log sites and
 * the error sites, and keeping it here lets `portal-errors.ts` stay a pure error TAXONOMY (dumb carriers of
 * a final, already-sanitized string). Kept pure — no I/O, no env reads; the `keyed` signal is passed in by
 * the caller, which owns the authentication state.
 */

/** The fixed stand-in emitted in place of a private Portal URL when the request is authenticated. */
export const AUTHENTICATED_PORTAL_PLACEHOLDER = '<authenticated Portal>';

/**
 * True when a headers map carries a non-empty `x-api-key` (case-insensitive; header casing is not
 * normalized at every call site). An empty-string value is treated as absent — it authenticates nothing.
 */
export const hasPortalApiKeyHeader = (
  headers: Record<string, string | undefined>,
): boolean =>
  Object.entries(headers).some(
    ([k, v]) => k.toLowerCase() === 'x-api-key' && v !== undefined && v !== '',
  );

/**
 * Resolve the Portal URL as it should appear in a diagnostic: the placeholder when authenticated
 * (`keyed`), else the URL verbatim.
 */
export const portalUrlForLog = (portalUrl: string, keyed: boolean): string =>
  keyed ? AUTHENTICATED_PORTAL_PLACEHOLDER : portalUrl;
