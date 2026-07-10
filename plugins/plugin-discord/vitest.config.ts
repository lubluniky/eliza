/**
 * Vitest config for the Discord plugin's unit tests. Aliases unbuilt workspace
 * deps (`@elizaos/core`, command/meeting plugins, and core's workspace
 * re-exports) to source so tests resolve without prebuilding those packages.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const pluginRoot = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(pluginRoot, "../..");
const coreSrc = path.join(repoRoot, "packages/core/src");
const sharedSrc = path.join(repoRoot, "packages/shared/src");
const loggerSrc = path.join(repoRoot, "packages/logger/src");
const cloudRoutingSrc = path.join(repoRoot, "packages/cloud/routing/src");

export default defineConfig({
	resolve: {
		// @elizaos/plugin-commands publishes only a built `dist/` entry. The
		// per-package test lanes prebuild just core/shared/logger/contracts/prompts,
		// so plugin-commands has no dist and vitest fails to resolve it ("Failed to
		// resolve entry for package @elizaos/plugin-commands"). Resolve it from
		// source instead — mirrors the same alias pattern other plugins use for
		// unbuilt workspace deps.
		alias: [
			{
				find: /^@elizaos\/shared\/(.*)\.js$/,
				replacement: path.join(sharedSrc, "$1.ts"),
			},
			{
				find: /^@elizaos\/shared\/(.*)$/,
				replacement: path.join(sharedSrc, "$1.ts"),
			},
			{
				find: "@elizaos/shared",
				replacement: path.join(sharedSrc, "index.ts"),
			},
			{
				find: /^@elizaos\/core\/(.*)\.js$/,
				replacement: path.join(coreSrc, "$1.ts"),
			},
			{
				find: "@elizaos/core",
				replacement: path.join(coreSrc, "index.node.ts"),
			},
			{
				find: "@elizaos/cloud-routing",
				replacement: path.join(cloudRoutingSrc, "index.ts"),
			},
			{
				find: "@elizaos/logger",
				replacement: path.join(loggerSrc, "index.ts"),
			},
			{
				find: /^@elizaos\/plugin-commands$/,
				replacement: path.join(
					repoRoot,
					"plugins/plugin-commands/src/index.ts",
				),
			},
			{
				find: /^@elizaos\/plugin-commands\/(.+)$/,
				replacement: path.join(repoRoot, "plugins/plugin-commands/src/$1"),
			},
			// Same source-resolution story for @elizaos/plugin-meetings (voice
			// meeting transcription seams; only a dynamic import at runtime, but
			// vite's import-analysis still needs to resolve the specifier).
			{
				find: /^@elizaos\/plugin-meetings$/,
				replacement: path.join(
					repoRoot,
					"plugins/plugin-meetings/src/index.ts",
				),
			},
		],
	},
	test: {
		include: [
			"__tests__/**/*.test.ts",
			"actions/**/*.test.ts",
			"test/**/*.test.ts",
		],
		// `*.harness.test.ts` boot a real PGLite runtime and need the workspace
		// source aliases from vitest.harness.config.ts — run via `test:harness`.
		exclude: ["**/node_modules/**", "dist/**", "**/*.harness.test.ts"],
		environment: "node",
		testTimeout: 60_000,
	},
});
