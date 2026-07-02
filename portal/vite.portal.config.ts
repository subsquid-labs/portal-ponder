import path from "node:path";
import { defineConfig } from "vitest/config";
// run the Portal-layer unit tests in isolation (no Foundry globalSetup): copied next to
// packages/core/ by scripts/sync-upstream.sh, where ./src points at the patched core.
export default defineConfig({
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      "@ponder/utils": path.resolve(__dirname, "../utils/src"),
    },
  },
  test: {
    include: [
      "src/sync-historical/portal*.test.ts",
      "src/sync-historical/realtime*.test.ts",
    ],
    testTimeout: 15000,
  },
});
