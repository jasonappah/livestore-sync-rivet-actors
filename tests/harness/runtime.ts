/**
 * Suite runtime shared by the conformance files.
 *
 * Call {@link useProviderRuntime} inside a `describe` block: it registers the
 * `beforeAll`/`afterAll` hooks that boot the provider layer once (a
 * `ManagedRuntime`, started eagerly) and dispose it, and returns helpers that
 * provide that runtime plus fresh per-test services (`KeyValueStore.layerMemory`,
 * logger) to a test body.
 */

import type { SyncBackend, UnknownError } from '@livestore/common'
import { EventFactory } from '@livestore/common/testing'
import {
  type Context,
  Effect,
  FetchHttpClient,
  type HttpClient,
  KeyValueStore,
  Layer,
  Logger,
  ManagedRuntime,
  type Scope,
} from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { afterAll, beforeAll } from '@effect/vitest'

import { HOOK_TIMEOUT_MS, SyncProviderImpl, type SyncProviderLayer, type SyncProviderOptions } from './types.ts'

export type RuntimeServices = SyncProviderImpl | HttpClient.HttpClient

/** Per-test services LiveStore would normally provide to a `SyncBackendConstructor`. */
export const testLayer = Layer.mergeAll(Logger.layer([Logger.consolePretty()]), KeyValueStore.layerMemory)

export const defaultClient = EventFactory.clientIdentity('test-client', 'test-session')

/** Actor keys tolerate spaces, but a plain id keeps engine logs readable. */
export const sanitize = (name: string) =>
  name.replaceAll(/[^a-zA-Z0-9_-]+/g, '-').replaceAll(/^-+|-+$/g, '').slice(0, 60)

type TestCtxServices = RuntimeServices | KeyValueStore.KeyValueStore | Scope.Scope

type MakeBackend = Effect.Effect<SyncBackend.SyncBackend<any>, UnknownError, TestCtxServices>

export interface ProviderRuntime {
  /** Provides the suite runtime + per-test services to a test body. */
  readonly withTestCtx: () => <A, E>(self: Effect.Effect<A, E, TestCtxServices>) => Effect.Effect<A, E, Scope.Scope>
  /** Isolated store id per call (and per run: the engine persists actor state). */
  readonly storeIdFor: (testName: string) => string
  /** A backend on a fresh, isolated store. */
  readonly makeProvider: (testName: string, options?: SyncProviderOptions) => MakeBackend
  /** A backend for an explicit store/client, e.g. to attach a second client to the same store. */
  readonly makeProviderFor: (
    args: { storeId: string; clientId?: string },
    options?: SyncProviderOptions,
  ) => MakeBackend
}

export const useProviderRuntime = (layer: SyncProviderLayer): ProviderRuntime => {
  let runtime: ManagedRuntime.ManagedRuntime<RuntimeServices, never>
  let runtimeContext: Context.Context<RuntimeServices>
  let testId: string

  beforeAll(async () => {
    testId = nanoid()
    runtime = ManagedRuntime.make(layer.pipe(Layer.provideMerge(FetchHttpClient.layer), Layer.orDie))
    // Eagerly start the runtime
    runtimeContext = await runtime.context()
  }, HOOK_TIMEOUT_MS)

  afterAll(async () => await runtime.dispose(), HOOK_TIMEOUT_MS)

  /** Provides the suite runtime + per-test services to a test body. */
  const withTestCtx =
    () =>
    <A, E>(
      self: Effect.Effect<A, E, RuntimeServices | KeyValueStore.KeyValueStore | Scope.Scope>,
    ): Effect.Effect<A, E, Scope.Scope> =>
      Effect.suspend(() => self.pipe(Effect.provide(testLayer), Effect.provide(runtimeContext)))

  /** Isolated store id per call (and per run: the engine persists actor state). */
  const storeIdFor = (testName: string) => `test-store-${sanitize(testName)}-${testId}-${nanoid(6)}`

  /** A backend for an explicit store/client, e.g. to attach a second client to the same store. */
  const makeProviderFor = (
    { storeId, clientId = defaultClient.clientId }: { storeId: string; clientId?: string },
    options?: SyncProviderOptions,
  ) =>
    Effect.suspend(() =>
      Effect.andThen(SyncProviderImpl, (_) => _.makeProvider({ storeId, clientId, payload: undefined }, options)),
    )

  /** A backend on a fresh, isolated store. */
  const makeProvider = (testName: string, options?: SyncProviderOptions) =>
    Effect.suspend(() => makeProviderFor({ storeId: storeIdFor(testName) }, options))

  return { withTestCtx, storeIdFor, makeProvider, makeProviderFor }
}
