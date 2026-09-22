/**
 * Conformance provider for the Rivet-backed sync backend.
 *
 * Boots the real `LiveStoreSync` actor through `Registry.test` (auto-spawns /
 * reuses the local Rivet engine when `RIVET_ENDPOINT` is unset; see
 * `docs/spike-results.md` §B1) and hands the suite `makeRivetSync` backends
 * pointed at it.
 *
 * Isolation: the engine routes an actor to *any* runner registered for its
 * name in the same namespace, so another `LiveStoreSync` runner on the shared
 * local engine (e.g. `examples/node-todo`'s server with its own
 * `validatePayload`) would receive our actors. The suite therefore runs in its
 * own namespace (`RIVET_NAMESPACE`, default `livestore-conformance`), created
 * idempotently through the engine API. The layer only resolves once that
 * runner is listed by the engine and routes a `TestInfo` action on a
 * throwaway store (`tests/harness/engine.ts`), so the first test never races
 * a cold engine or a still-registering runner.
 *
 * `turnBackendOffline` simulates an outage by invoking the actor's test-only
 * `TestDisconnectAll` action for every store a backend was created for. It
 * goes through a stateless rivetkit *handle* (HTTP), not over a WebSocket
 * conn: an in-flight action on a connection that is being closed would reject
 * with "Connection closed" before the response arrives. `turnBackendOnline`
 * is a no-op — rivetkit clients reconnect on their own.
 *
 * The engine outlives vitest and persists actor state, so callers must use a
 * unique `storeId` per test.
 */

import { UnknownError } from '@livestore/common'
import { Effect, Layer } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { Registry } from '@rivetkit/effect'
import { createClient } from 'rivetkit/client'

import { makeRivetSync } from '../../../src/client/mod.ts'
import {
  ACTION_TEST_DISCONNECT_ALL,
  ACTION_TEST_INFO,
  ACTOR_NAME,
  encodeTestDisconnectAllRequest,
  encodeTestInfoRequest,
} from '../../../src/common/mod.ts'
import {
  makeLiveStoreSyncActor,
  RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  registryOptions,
} from '../../../src/server/mod.ts'
import { bootClock, engineTargetFromEnv, preflightLayer, waitForRunner } from '../engine.ts'
import { SyncProviderImpl, type SyncProviderLayer } from '../types.ts'

export const name = 'Rivet Actors'

export const prepare = Effect.void

/** Dedicated namespace so other `LiveStoreSync` runners on the same engine cannot pick up our actors. */
const NAMESPACE = process.env.RIVET_NAMESPACE ?? 'livestore-conformance'
const TARGET = engineTargetFromEnv(NAMESPACE)
const ENDPOINT = TARGET.endpoint
const TOKEN = TARGET.token

/** Identity the harness uses for its own `TestDisconnectAll` calls. */
const HARNESS_CLIENT_ID = 'conformance-harness'

/**
 * Raised so the large-batch cases (120 KB events, 900 KB push chunks) fit.
 * The registry's `maxIncomingMessageSize` below is sized against the same limit.
 */
const MAX_PUSH_BYTES = 900_000

/**
 * Marker a test can put in its `syncPayload` to make the shared harness actor
 * reject it (`tests/rivet-specific.test.ts`, "validatePayload rejection").
 *
 * Every other test passes `payload: undefined`, which
 * {@link harnessValidatePayload} accepts, so the validator is a no-op for them.
 */
export const REJECTED_PAYLOAD = { reject: true } as const

/** Accepts everything except {@link REJECTED_PAYLOAD}. */
const harnessValidatePayload = (payload: unknown): void => {
  if (typeof payload === 'object' && payload !== null && (payload as { reject?: unknown }).reject === true) {
    throw new Error('conformance harness: payload rejected')
  }
}

const ActorsLayer = makeLiveStoreSyncActor({
  testing: { enabled: true },
  pullPageSize: 100,
  maxMessageBytes: 900_000,
  validatePayload: harnessValidatePayload,
})

const RegistryLayer = Registry.layer(
  registryOptions({
    noWelcome: true,
    endpoint: process.env.RIVET_ENDPOINT,
    token: TOKEN,
    namespace: NAMESPACE,
    maxIncomingMessageSize: RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  }),
)

/**
 * Registers the actor and starts the (leaked-until-exit) rivetkit registry,
 * creating {@link NAMESPACE} first when the engine is already up.
 */
const EngineLayer = Registry.test.pipe(
  Layer.provide(ActorsLayer),
  Layer.provide(preflightLayer(TARGET)),
  Layer.provideMerge(RegistryLayer),
)

export const layer: SyncProviderLayer = Layer.effect(
  SyncProviderImpl,
  Effect.gen(function* () {
    // Depend on the registry so the engine layer (and its runner) is started before we touch the engine.
    yield* Registry.Registry

    /** Stores with a live backend; `turnBackendOffline` only needs to reach those. */
    const storeIds = new Set<string>()

    const clientOptions = {
      endpoint: ENDPOINT,
      ...(TOKEN === undefined ? {} : { token: TOKEN }),
      namespace: NAMESPACE,
    }

    // Raw client for the out-of-band `TestDisconnectAll` calls. Disposed with
    // the layer so vitest can exit.
    const rawClient = createClient(clientOptions)
    yield* Effect.addFinalizer(() => Effect.promise(() => rawClient.dispose()).pipe(Effect.ignore))

    // Cold start: engine healthy -> namespace -> our runner listed -> an action routes.
    yield* waitForRunner(TARGET, {
      since: bootClock,
      probe: Effect.suspend(() => {
        const storeId = `harness-ready-${nanoid()}`
        return Effect.tryPromise(() =>
          rawClient.getOrCreate(ACTOR_NAME, [storeId]).action({
            name: ACTION_TEST_INFO,
            args: [encodeTestInfoRequest({ storeId, clientId: HARNESS_CLIENT_ID })],
          }),
        )
      }),
    }).pipe(Effect.orDie)

    const disconnectAll = (storeId: string) =>
      Effect.promise(() =>
        rawClient.getOrCreate(ACTOR_NAME, [storeId]).action({
          name: ACTION_TEST_DISCONNECT_ALL,
          args: [encodeTestDisconnectAllRequest({ storeId, clientId: HARNESS_CLIENT_ID })],
        }),
      ).pipe(
        Effect.tapCause((cause) =>
          Effect.logDebug('rivet conformance provider: TestDisconnectAll failed (ignored)', { storeId, cause }),
        ),
        Effect.ignore,
      )

    return {
      makeProvider: (args) =>
        Effect.gen(function* () {
          storeIds.add(args.storeId)
          yield* Effect.addFinalizer(() => Effect.sync(() => storeIds.delete(args.storeId)))
          return yield* makeRivetSync({
            ...clientOptions,
            maxPushBytes: MAX_PUSH_BYTES,
            ping: { requestInterval: '2 seconds', requestTimeout: '5 seconds' },
          })(args)
        }),
      turnBackendOffline: Effect.suspend(() =>
        Effect.forEach([...storeIds], disconnectAll, { concurrency: 'unbounded', discard: true }),
      ),
      turnBackendOnline: Effect.void,
      providerSpecific: { endpoint: ENDPOINT },
    }
  }),
).pipe(Layer.provide(EngineLayer), UnknownError.mapToUnknownErrorLayer)
