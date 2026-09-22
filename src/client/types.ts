/**
 * Transport contract between the Rivet `ActorConn` wrapper
 * (`connection.ts`, T11) and everything built on top of it (action client,
 * pull, push).
 *
 * Only `rivetkit/client` may ever be imported from `src/client/**` — this
 * module imports nothing from rivetkit at all, so the pull/push/action layers
 * can be unit-tested against a fake connection.
 */

import type { IsOfflineError } from '@livestore/common'
import { Data, type Effect, type PubSub, type SubscriptionRef } from '@livestore/utils/effect'

/**
 * Mirror of rivetkit's `ActorConn` status values (`conn.onStatusChange`).
 *
 * `idle` means rivetkit stopped trying (disposal / `connection_open_failed`),
 * not "about to connect".
 */
export type ConnStatus = 'idle' | 'connecting' | 'connected' | 'disconnected'

/**
 * A rejected `conn.action(...)` promise, captured together with the connection
 * status observed at rejection time. `errors.ts#classifyActionFailure` turns
 * it into a LiveStore-facing error.
 *
 * `cause` is whatever rivetkit threw: an `ActorError`-like `{ group, code,
 * message, metadata }`, or a plain `Error('Connection closed (code: …)')` when
 * a non-structured close dropped the in-flight action.
 */
export class RawActionFailure extends Data.TaggedError('RawActionFailure')<{
  readonly cause: unknown
  readonly statusAtFailure: ConnStatus
}> {}

/** Lazily-established, self-reconnecting connection to one store's actor. */
export interface Connection {
  readonly status: SubscriptionRef.SubscriptionRef<ConnStatus>
  readonly isConnected: SubscriptionRef.SubscriptionRef<boolean>
  /** args[0] of each live `pull` event, in arrival order. Subscribe BEFORE issuing the first Pull. */
  readonly pullEvents: PubSub.PubSub<unknown>
  /** Waits for `connected` (bounded by connectTimeout → IsOfflineError), sends the action, fails with IsOfflineError if the connection drops mid-flight. */
  readonly action: (name: string, encodedPayload: unknown) => Effect.Effect<unknown, IsOfflineError | RawActionFailure>
  readonly awaitConnected: Effect.Effect<void, IsOfflineError>
}
