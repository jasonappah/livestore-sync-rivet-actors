import { configDefaults, defineConfig } from 'vitest/config'

/** Files that need a running Rivet engine (auto-spawned by `Registry.test`). */
const conformanceFiles = [
  'tests/sync-provider.test.ts',
  'tests/sync-provider-properties.test.ts',
  'tests/rivet-specific.test.ts',
  'src/**/*.integration.test.ts',
  'tests/**/*.integration.test.ts',
]

const shared = {
  testTimeout: 60_000,
  hookTimeout: 120_000,
  pool: 'forks',
  // `@effect/vitest` ships untranspiled-for-node ESM helpers; inlining keeps
  // vitest from double-loading the effect runtime.
  server: { deps: { inline: ['@effect/vitest'] } },
  passWithNoTests: true,
} as const

export default defineConfig({
  test: {
    ...shared,
    // One Rivet engine per run: never run test files in parallel.
    fileParallelism: false,
    // NOTE: no root-level `include` — the two projects below own file
    // discovery, and a root `include` gets merged into every project on some
    // vitest versions (which makes each project pick up the other's files).
    projects: [
      {
        test: {
          ...shared,
          name: 'unit',
          include: ['tests/**/*.test.ts', 'src/**/*.test.ts'],
          exclude: [...configDefaults.exclude, ...conformanceFiles],
        },
      },
      {
        test: {
          ...shared,
          name: 'conformance',
          include: conformanceFiles,
        },
      },
    ],
  },
})
