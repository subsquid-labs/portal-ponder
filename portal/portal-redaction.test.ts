import { expect, test } from 'vitest';
import {
  AUTHENTICATED_PORTAL_PLACEHOLDER,
  hasPortalApiKeyHeader,
  portalUrlForLog,
} from './portal-redaction.js';

// A stand-in private/dedicated Portal host: it MUST never survive redaction into a diagnostic.
const PRIVATE_URL = 'https://portal.internal.example/datasets/ethereum-mainnet';

test('hasPortalApiKeyHeader: a non-empty x-api-key is detected regardless of header casing', () => {
  expect(hasPortalApiKeyHeader({ 'x-api-key': 'k' })).toBe(true);
  expect(hasPortalApiKeyHeader({ 'X-API-KEY': 'k' })).toBe(true);
  expect(hasPortalApiKeyHeader({ 'X-Api-Key': 'k' })).toBe(true);
});

test('hasPortalApiKeyHeader: absent, empty-string, or undefined x-api-key is NOT authenticated', () => {
  // no key header at all → unauthenticated (the public default Portal).
  expect(hasPortalApiKeyHeader({ 'content-type': 'application/json' })).toBe(
    false,
  );
  expect(hasPortalApiKeyHeader({})).toBe(false);
  // a present-but-empty value authenticates nothing — treat as absent.
  expect(hasPortalApiKeyHeader({ 'x-api-key': '' })).toBe(false);
  expect(hasPortalApiKeyHeader({ 'x-api-key': undefined })).toBe(false);
});

test('portalUrlForLog: keyed → the fixed placeholder, and the private host never leaks', () => {
  const shown = portalUrlForLog(PRIVATE_URL, true);

  expect(shown).toBe(AUTHENTICATED_PORTAL_PLACEHOLDER);
  expect(shown).not.toContain('portal.internal.example');
  expect(shown).not.toContain(PRIVATE_URL);
});

test('portalUrlForLog: unkeyed → the URL verbatim (a useful public-Portal diagnostic)', () => {
  expect(portalUrlForLog('http://portal.example', false)).toBe(
    'http://portal.example',
  );
  // the full private URL would be shown verbatim too if unkeyed — redaction is strictly a keyed-path policy.
  expect(portalUrlForLog(PRIVATE_URL, false)).toBe(PRIVATE_URL);
});
