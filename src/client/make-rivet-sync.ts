/**
 * Assembly: turns {@link RivetSyncOptions} into LiveStore's
 * `SyncBackendConstructor`.
 *
 * Wiring (all bound to the `Scope` LiveStore provides):
 * - `makeConnection` owns the rivetkit client + the lazy `ActorConn`. The conn
 *   is deliberately *not* created here: LiveStore's conformance suite expects
 *   `isConnected === false` right after construction, so nothing in this
 *   module may touch the wire before `connect`/`pull`/`push` is called.
 * - `SyncBackend.makeBackendIdHelper` caches the backend id in the
 *   `KeyValueStore` LiveStore provides (declared in the constructor's `R`), so
 *   pull cursors and push requests can carry it across restarts.
 * - A single `Semaphore(1)` serialises pushes.
 * - The liveness ping fiber (enabled by default) starts after one interval so
 *   it does not force the lazy connection open.
 *
 * Browser-safe: the only rivetkit import lives in `connection.ts`
 * (`rivetkit/client`).
 */

import { SyncBackend, UnknownError } from '@livestore/common'
import { Effect, type Schema, Schedule, Semaphore } from '@livestore/utils/effect'

import { ConnParams, encodeConnParams, type SyncMetadata } from '../common/mod.ts'
import { makeActionClient } from './action-client.ts'
import { type CreateRivetClient, makeConnection } from './connection.ts'
import { type RivetSyncOptions, resolveRivetSyncOptions } from './options.ts'
import { makePing } from './ping.ts'
import { makePull } from './pull.ts'
import { makePush } from './push.ts'

/** Test seams. Not part of the public API. */
export interface RivetSyncInternalOptions {
  /** Replaces rivetkit's `createClient` (used by unit tests to assert no connection is attempted). */
  readonly createClient?: CreateRivetClient
}

/**
 * Like {@link makeRivetSync}, but with internal seams exposed. Exported for
 * this package's own tests only — the public entry point is
 * {@link makeRivetSync}.
 */
export const makeRivetSyncWith =
  <TPayload extends Schema.Json = Schema.Json>(
    options: RivetSyncOptions,
    internal: RivetSyncInternalOptions = {},
  ): SyncBackend.SyncBackendConstructor<SyncMetadata, TPayload> =>
  ({ storeId, clientId, payload }) =>
    Effect.gen(function* () {
      const resolved = resolveRivetSyncOptions(options)

      // `payload` is an `optionalKey`: absent must mean an absent key.
      const payloadFields = payload !== undefined ? { payload } : {}
      const connParams = encodeConnParams(ConnParams.make({ storeId, clientId, ...payloadFields }))

      const conn = yield* makeConnection({
        options: resolved,
        storeId,
        connParams,
        ...(internal.createClient !== undefined ? { createClient: internal.createClient } : {}),
      })

      const actions = makeActionClient(conn)
      const backendIdHelper = yield* SyncBackend.makeBackendIdHelper
      const semaphore = yield* Semaphore.make(1)

      const deps = { conn, actions, backendIdHelper, storeId, clientId, payload, options: resolved } as const

      const pull = makePull(deps)
      const push = makePush({ ...deps, semaphore })
      const ping = makePing(deps)

      if (resolved.ping.enabled === true) {
        // The first ping waits one interval: pinging immediately would force
        // the lazy connection open during construction.
        yield* Effect.sleep(resolved.ping.requestInterval).pipe(
          Effect.andThen(
            ping.pipe(
              Effect.tapCauseLogPretty,
              Effect.ignore,
              Effect.repeat(Schedule.spaced(resolved.ping.requestInterval)),
            ),
          ),
          Effect.forkScoped,
        )
      }

      return SyncBackend.of<SyncMetadata>({
        isConnected: conn.isConnected,
        connect: conn.awaitConnected,
        pull,
        push,
        ping,
        metadata: {
          name: 'livestore-sync-rivet-actors',
          description: 'LiveStore sync backend implementation using Rivet Actors',
          protocol: 'rivet-actor-ws',
          endpoint: resolved.endpoint,
          actorName: resolved.actorName,
        },
        supports: {
          pullPageInfoKnown: true,
          pullLive: true,
        },
      })
    }).pipe(UnknownError.mapToUnknownError, Effect.withSpanScoped('rivet-sync:makeRivetSync', { attributes: { storeId, clientId } }))

/**
 * Creates the Rivet-backed `SyncBackend` constructor LiveStore's adapters take
 * as `sync.backend`.
 *
 * @example
 * ```ts
 * import { makeRivetSync } from 'livestore-sync-rivet-actors/client'
 *
 * const adapter = makeAdapter({
 *   storage: { type: 'in-memory' },
 *   sync: { backend: makeRivetSync({ endpoint: 'http://127.0.0.1:6420' }) },
 * })
 * ```
 */
export const makeRivetSync = <TPayload extends Schema.Json = Schema.Json>(
  options: RivetSyncOptions,
): SyncBackend.SyncBackendConstructor<SyncMetadata, TPayload> => makeRivetSyncWith<TPayload>(options)
