import path from 'node:path';
import { defineConfig } from 'vitest/config';
// run the Portal-layer unit tests in isolation (no Foundry globalSetup): copied next to
// packages/core/ by scripts/sync-upstream.sh, where ./src points at the patched core.
export default defineConfig({
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
      '@ponder/utils': path.resolve(__dirname, '../utils/src'),
    },
  },
  test: {
    include: [
      'src/sync-historical/portal*.test.ts',
      'src/sync-historical/realtime*.test.ts',
    ],
    testTimeout: 15000,
    // Coverage is scoped to the Portal layer (the whole diff vs upstream ponder). Inert on a plain
    // `--test` run: it only activates under `--coverage` (see scripts/sync-upstream.sh --coverage),
    // so the normal path needs no coverage provider installed.
    coverage: {
      provider: 'v8',
      reporter: ['json-summary', 'text-summary'],
      reportsDirectory: 'portal-coverage',
      include: [
        'src/sync-historical/portal.ts',
        'src/sync-historical/portal-transform.ts',
        'src/sync-historical/portal-realtime.ts',
        'src/sync-historical/portal-realtime-wire.ts',
        'src/sync-historical/realtime.ts',
      ],
    },
  },
});
