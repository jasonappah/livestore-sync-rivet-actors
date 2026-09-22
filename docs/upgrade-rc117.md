# Upgrade: effect 4.0.0-rc.117 + LiveStore snapshot

From `effect@4.0.0-rc.112` + `@livestore/*@0.5.0-dev.0` to `effect@4.0.0-rc.117` +
`@livestore/*@0.0.0-snapshot-bf25b4d9b7bf0a8be73d61e68f8f2c8af439be2d` (npm dist-tag `snapshot`,
published 2026-09-20, built against effect rc.113, no longer imports `effect/testing/FastCheck`).

## Version changes

| Package | Before | After |
|---|---|---|
| `effect`, every `@effect/*` | `4.0.0-rc.112` | `4.0.0-rc.117` |
| `@livestore/common`, `@livestore/utils`, `@livestore/livestore` | `0.5.0-dev.0` | `0.0.0-snapshot-bf25b4d…` |
| `@livestore/adapter-node` (example only) | `0.5.0-dev.0` | unchanged, deps overridden (see below) |
| `vitest` | `^4.1.10` | `^5.0.1` (forced by `@effect/vitest@4.0.0-rc.117`, which peers `vitest >=5 <6`) |
| peer `effect` | `>=4.0.0-rc.111 <4.0.0-rc.113` | `>=4.0.0-rc.113 <5` |
| peer `@livestore/common`, `@livestore/utils` | `^0.5.0-dev.0` | exact `0.0.0-snapshot-bf25b4d…` |

Why the `@livestore/*` peers are exact pins: `^0.5.0-dev.0` does not match `0.0.0-snapshot-…`
(different major/minor, and a prerelease tag), and no semver range can express "this snapshot or a
later 0.5 release". The snapshot build requires rc.113+ and 0.5.0-dev.0 requires rc.112, so there is
no range that covers both.

## API deltas handled

1. **`Config.string` → `Config.String`** (effect rc.113+ capitalised the `Config` constructors:
   `String`, `Number`, `Boolean`, `Int`, `Literal`, …). `@rivetkit/effect@2.3.17` calls
   `Config.string("RIVET_LOG_LEVEL")` in `src/internal/logging.ts`; on rc.117 that is `undefined`, so
   the SDK throws at module load and `tsc` reports five errors in that file. Fixed by extending
   `patches/@rivetkit__effect@2.3.17.patch` (src and dist). Our own code does not use `Config`.
2. **`it.prop` options: `fastCheck` → `arbitrary`.** `@effect/vitest@4.0.0-rc.117` no longer uses
   fast-check; property tests run on Effect's own engine (`effect/unstable/arbitrary/Arbitrary`), and the
   options type is `Arbitrary.CheckOptions` (`runs`, `size`, `maxDiscards`, `maxShrinks`, `seed`,
   `replay`). In `tests/sync-provider-properties.test.ts`: `numRuns` → `runs`,
   `endOnFailure: true` → `maxShrinks: 0` (both mean "stop at the first counterexample, do not
   shrink"), `seed` unchanged. The Schema-based generator (`[LargeBatchScenarioSchema]`) and the
   callback shape are unchanged, so no `fast-check` devDependency is needed. The `FC_NUM_RUNS` /
   `FC_SEED` env var names are kept.
3. **vitest 5.** No config change needed: the `unit` / `conformance` projects discover the same 18 / 6
   files as on vitest 4.1 (checked with `vitest list --project … --filesOnly`), disjoint and covering
   every `*.test.ts`.
4. **`@livestore/adapter-node` has no build at this snapshot** (its `snapshot` tag is an older
   `0.0.0-snapshot-9fd312cd….8aa073fb…` against rc.111). The example keeps `0.5.0-dev.0` and
   `pnpm-workspace.yaml` → `overrides` forces its `@livestore/{common,utils,webmesh,sqlite-wasm}` deps
   onto the snapshot, so one FastCheck-free `@livestore/utils` is installed. This is an example-only
   hack; `adapter-node` code itself is not rebuilt against the snapshot.

No other source changes: no `Schema`, `KeyValueStore`, `Stream`, `Layer` or `Effect` renames hit this
codebase between rc.112 and rc.117. `@livestore/utils@snapshot` declares `@effect/platform-node`
`4.0.0-rc.113` as a hard dependency; pnpm resolved it to the single rc.117 copy (one `effect`, one
`@effect/platform-node` in the lockfile).

## Verification (macOS arm64, local Rivet engine 2.3.17 on :6420)

- `pnpm typecheck`, `pnpm build`, `pnpm --filter example-node-todo run check-types`: clean.
- `pnpm test:unit`: 18 files, 247/247 passed.
- `pnpm test:conformance`: 36/36 passed in a full run (47.9 s, same as the rc.112 baseline), and
  every conformance file passed on its own (properties test, including replays of the failing seeds).
  Other full runs hit one or two 90–120 s timeouts in a live-pull wait (`rivet-specific` "syncs two
  clients…", the large-batch property, `actor.integration` "push acks…"). The same `stack/09` commit
  on rc.112, run in a separate worktree alternating with rc.117 against the same engine, failed the
  same way (0 of 3 full runs green: the same tests, the same timeouts), so the flakiness is environmental (most likely the
  long-running shared engine), not caused by rc.117.
- `examples/node-todo` end to end (`server` + `start`): both stores converge in both directions.
  `examples/node-server` boots.

## Trade-off

- **Snapshot pins are unstable.** The `snapshot` dist-tag moves; a given `0.0.0-snapshot-<sha>` may
  never become a release, and consumers must install exactly the same snapshot (and effect ≥ rc.113)
  or they get two `@livestore/common` copies / broken peers.
- **Not publishable as-is.** A release of this package with exact snapshot peers forces every consumer
  onto a pre-release build that LiveStore does not support. Prefer waiting for a LiveStore `0.5.0-dev.N`
  (or 0.5.0) built on rc.113+ and switching the peers back to a normal range.
- **`@livestore/adapter-node` lags** the snapshot; any app on Node needs the same override trick.
- **vitest 5 for consumers.** LiveStore peers `@effect/vitest`, and `@effect/vitest@4.0.0-rc.117` peers
  `vitest >=5 <6`, so apps following the README's peer install move to vitest 5 as well.
