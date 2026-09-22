/**
 * In-memory stand-in for the rivetkit-backed `Connection` (T11), so the
 * action client / pull / push layers can be unit-tested without an engine.
 *
 * Deliberately minimal: status + `isConnected` refs, an unbounded pull-event
 * PubSub and a programmable action handler map. T11/T12 extend it (e.g. with
 * connect-timeout and mid-flight-drop simulation).
 */

import type { IsOfflineError } from '@livestore/common'
import { Effect, PubSub, SubscriptionRef } from '@livestore/utils/effect'

import type { Connection, ConnStatus, RawActionFailure } from '../../src/client/types.ts'

export type FakeActionHandler = (payload: unknown) => Effect.Effect<unknown, RawActionFailure | IsOfflineError>

export interface RecordedCall {
  readonly name: string
  readonly payload: unknown
}

export interface FakeConnection {
  readonly conn: Connection
  /** Sets `status` and keeps `isConnected` in sync (as the real manager does). */
  readonly setStatus: (status: ConnStatus) => Effect.Effect<void>
  /** Publishes a raw (still encoded) live `pull` event. */
  readonly emitPullEvent: (raw: unknown) => Effect.Effect<void>
  /** Registers/replaces the handler for an action name. */
  readonly onAction: (name: string, handler: FakeActionHandler) => void
  /** Every `conn.action` invocation, in order, with the payload as it went on the wire. */
  readonly calls: Array<RecordedCall>
}

export const makeFakeConnection = (
  options: { readonly initialStatus?: ConnStatus } = {},
): Effect.Effect<FakeConnection> =>
  Effect.gen(function* () {
    const initialStatus = options.initialStatus ?? 'connected'
    const status = yield* SubscriptionRef.make<ConnStatus>(initialStatus)
    const isConnected = yield* SubscriptionRef.make(initialStatus === 'connected')
    const pullEvents = yield* PubSub.unbounded<unknown>()

    const handlers = new Map<string, FakeActionHandler>()
    const calls: Array<RecordedCall> = []

    const conn: Connection = {
      status,
      isConnected,
      pullEvents,
      action: (name, encodedPayload) =>
        Effect.suspend(() => {
          calls.push({ name, payload: encodedPayload })
          const handler = handlers.get(name)
          if (handler === undefined) {
            return Effect.die(new Error(`fake-connection: no handler registered for action '${name}'`))
          }
          return handler(encodedPayload)
        }),
      awaitConnected: SubscriptionRef.waitUntil(isConnected, (connected) => connected === true).pipe(Effect.asVoid),
    }

    return {
      conn,
      setStatus: (next) =>
        Effect.flatMap(SubscriptionRef.set(status, next), () =>
          SubscriptionRef.set(isConnected, next === 'connected'),
        ),
      emitPullEvent: (raw) => Effect.asVoid(PubSub.publish(pullEvents, raw)),
      onAction: (name, handler) => {
        handlers.set(name, handler)
      },
      calls,
    }
  })
