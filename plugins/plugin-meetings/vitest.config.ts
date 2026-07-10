/**
 * Vitest config for @elizaos/plugin-meetings: aliases the `@elizaos/*` packages
 * to their workspace sources (matching the sibling plugin-local-inference
 * config) so the suite runs without built dists, and scopes coverage to this
 * plugin's own `src/**` — the changed-file coverage lane merges per-package
 * LCOV reports, so an unscoped run would charge core/shared files this plugin
 * merely imports at import-graph coverage.
 */
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    extensions: [".ts", ".tsx", ".mts", ".js", ".mjs", ".json"],
    alias: {
      "@elizaos/core": fileURLToPath(
        new URL("../../packages/core/src/index.node.ts", import.meta.url),
      ),
      "@elizaos/logger": fileURLToPath(
        new URL("../../packages/logger/src/index.ts", import.meta.url),
      ),
      // Deep subpath must precede the bare alias — the bare entry
      // prefix-matches and would rewrite this to `src/index.ts/<subpath>`.
      "@elizaos/shared/transcripts": fileURLToPath(
        new URL("../../packages/shared/src/transcripts.ts", import.meta.url),
      ),
      "@elizaos/shared": fileURLToPath(
        new URL("../../packages/shared/src/index.ts", import.meta.url),
      ),
    },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
    exclude: [
      "dist/**",
      "node_modules/**",
      "**/*.e2e.test.ts",
      "**/*.live.test.ts",
      "**/*.real.test.ts",
    ],
    coverage: {
      include: ["src/**/*.ts"],
      exclude: ["src/**/*.test.ts", "src/__e2e__/**", "src/test-support.ts"],
    },
  },
});
