/**
 * `SyncBackend['ping']` for the Rivet transport.
 *
 * A successful `Ping` action is the only positive liveness signal that does
 * not depend on rivetkit's status callbacks, so it also (re)asserts
 * `isConnected`. A ping that does not answer within
 * `options.ping.requestTimeout` marks the connection as offline and fails with
 * `Cause.TimeoutError` (part of `SyncBackend['ping']`'s declared error type).
 *
 * Browser-safe: no rivetkit imports.
 */

import type { SyncBackend } from '@livestore/common'
import { Cause, Duration, Effect, type Schema, SubscriptionRef } from '@livestore/utils/effect'

import { PingRequest, type SyncMetadata } from '../common/mod.ts'
import type { ActionClient } from './action-client.ts'
import type { ResolvedRivetSyncOptions } from './options.ts'
import type { Connection } from './types.ts'

export interface PingDeps {
  readonly actions: ActionClient
  readonly conn: Connection
  readonly storeId: string
  readonly clientId: string
  readonly payload: Schema.Json | undefined
  readonly options: ResolvedRivetSyncOptions
}

export const makePing = (deps: PingDeps): SyncBackend.SyncBackend<SyncMetadata>['ping'] => {
  const { actions, clientId, conn, options, payload, storeId } = deps

  const request = PingRequest.make({
    storeId,
    clientId,
    // `payload` is an `optionalKey`: absent must mean an absent key.
    ...(payload !== undefined ? { payload } : {}),
  })

  return actions.ping(request).pipe(
    Effect.andThen(SubscriptionRef.set(conn.isConnected, true)),
    Effect.timeoutOrElse({
      duration: options.ping.requestTimeout,
      orElse: () =>
        SubscriptionRef.set(conn.isConnected, false).pipe(
          Effect.andThen(
            Effect.fail(
              new Cause.TimeoutError(
                `rivet-sync: ping timed out after ${Duration.toMillis(options.ping.requestTimeout)}ms`,
              ),
            ),
          ),
        ),
    }),
    Effect.withSpan('rivet-sync:ping', { attributes: { storeId, clientId } }),
  )
}
