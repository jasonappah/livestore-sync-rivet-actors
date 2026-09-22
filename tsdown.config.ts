import { defineConfig } from 'tsdown'

/**
 * Emit `.mjs` / `.d.mts` so the hand-written `exports` map in package.json
 * (`./dist/client.mjs`, `./dist/client.d.mts`, …) matches the build output.
 */
const outExtensions = () => ({ js: '.mjs', dts: '.d.mts' })

export default defineConfig([
  {
    // Browser/worker-safe entries. These must never pull in `rivetkit` (server
    // entry) — only `rivetkit/client` is allowed, and it stays external.
    entry: {
      client: 'src/client/mod.ts',
      common: 'src/common/mod.ts',
    },
    platform: 'neutral',
    format: 'esm',
    dts: { tsgo: true },
    outExtensions,
    clean: ['dist/client.*', 'dist/common.*'],
  },
  {
    entry: {
      server: 'src/server/mod.ts',
    },
    platform: 'node',
    format: 'esm',
    dts: { tsgo: true },
    outExtensions,
    clean: ['dist/server.*'],
  },
])
