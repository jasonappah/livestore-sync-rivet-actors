/**
 * Service shape of a sync provider under conformance test.
 *
 * Mirrors LiveStore's `tests/sync-provider/src/types.ts` verbatim (plus an
 * `endpoint` in `providerSpecific`) so `tests/sync-provider.test.ts` stays
 * diffable against the upstream suite.
 */

import type { SyncBackend, UnknownError } from '@livestore/common'
import { Context, type Effect, type HttpClient, type Layer, type Schedule } from '@livestore/utils/effect'

/**
 * Vitest budget for `beforeAll`/`afterAll` hooks that boot a provider runtime.
 *
 * `Registry.test` auto-spawns the Rivet engine and returns before it is ready;
 * the first action then waits for the engine (~2.5 s cold). Vitest's default
 * `hookTimeout` (10 s) would be tight on a slow machine, so hooks get their
 * own budget (suite-level `{ timeout }` options do not apply to hooks).
 */
export const HOOK_TIMEOUT_MS = 120_000

export interface SyncProviderOptions {
  pingSchedule?: Schedule.Schedule<unknown>
}

export class SyncProviderImpl extends Context.Service<
  SyncProviderImpl,
  {
    // TODO support simulatation of latency and offline mode etc
    makeProvider: (
      args: SyncBackend.MakeBackendArgs,
      options?: SyncProviderOptions,
    ) => ReturnType<SyncBackend.SyncBackendConstructor<any>>
    turnBackendOffline: Effect.Effect<void>
    turnBackendOnline: Effect.Effect<void>
    providerSpecific: { port?: number; endpoint?: string }
  }
>()('SyncProviderImpl') {}

export type SyncProviderLayer = Layer.Layer<SyncProviderImpl, UnknownError, HttpClient.HttpClient>
