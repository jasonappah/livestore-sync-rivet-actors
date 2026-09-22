/**
 * Maps raw rivetkit action failures onto the error channel LiveStore's
 * `SyncBackend` understands (`IsOfflineError | UnknownError |
 * ServerAheadError | BackendIdMismatchError`).
 *
 * Rivet errors are duck-typed (`{ group, code, message?, metadata? }`) rather
 * than imported: `rivetkit/errors` is a server-side entry and
 * `@rivetkit/effect` pulls in the whole actor runtime, neither of which may
 * reach a browser bundle. `rivetkit/client` does re-export `ActorError`, but
 * duck-typing keeps this module dependency-free and testable.
 */

import { IsOfflineError, UnknownError } from '@livestore/common'
import { Effect, Option, Result } from '@livestore/utils/effect'

import { InvalidPayloadError } from '../common/mod.ts'
import type { RawActionFailure } from './types.ts'

/** Structural shape of rivetkit's `ActorError` (a.k.a. `RivetError`). */
export interface RivetErrorLike {
  readonly group: string
  readonly code: string
  readonly message?: string
  readonly metadata?: unknown
}

/**
 * `@rivetkit/effect` wraps every declared action error into this envelope and
 * puts it in `UserError.metadata` (`internal_ActionErrorEnvelope.ts`).
 */
export const ACTION_ERROR_ENVELOPE_TAG = 'EffectActionError' as const
export const ACTION_ERROR_ENVELOPE_VERSION = 1 as const

/** Rivet error codes in group `actor` that mean "the actor went away" → offline. */
const OFFLINE_ACTOR_CODES: ReadonlySet<string> = new Set(['aborted', 'not_found', 'stopping', 'restarting'])

/**
 * Non-structured WS closes reject in-flight actions with a plain
 * `Error('Connection closed (code: 1000, reason: …)')` (`docs/spike-results.md` §B3).
 */
const CONNECTION_DROPPED_MESSAGE = /Connection (closed|lost)/i

export const isRivetErrorLike = (u: unknown): u is RivetErrorLike =>
  typeof u === 'object' &&
  u !== null &&
  typeof (u as { group?: unknown }).group === 'string' &&
  typeof (u as { code?: unknown }).code === 'string'

/**
 * Validates the `EffectActionError` envelope carried in `ActorError.metadata`
 * and returns the (still encoded) inner error.
 *
 * `None` means the failure is not a user-declared action error — a transport
 * or runtime error from rivetkit itself.
 */
export const decodeActionErrorEnvelope = (metadata: unknown): Option.Option<unknown> => {
  if (typeof metadata !== 'object' || metadata === null) return Option.none()
  const envelope = metadata as { _tag?: unknown; version?: unknown; error?: unknown }
  if (envelope._tag !== ACTION_ERROR_ENVELOPE_TAG || envelope.version !== ACTION_ERROR_ENVELOPE_VERSION) {
    return Option.none()
  }
  return Option.some(envelope.error)
}

const isInvalidPayloadError = (error: unknown): error is InvalidPayloadError =>
  error instanceof InvalidPayloadError ||
  (typeof error === 'object' && error !== null && (error as { _tag?: unknown })._tag === 'InvalidPayloadError')

/**
 * Builds the failure classifier for one action, given the decoder of that
 * action's declared error union (e.g. `Schema.decodeUnknownEffect(PullErrorJson)`).
 *
 * `InvalidPayloadError` is intentionally excluded from the result type: it is
 * an internal, auth-ish server error that LiveStore has no channel for, so it
 * is re-mapped to `UnknownError`. Callers therefore only ever see
 * `IsOfflineError | UnknownError | ServerAheadError | BackendIdMismatchError`.
 */
export const classifyActionFailure =
  <E>(decodeError: (u: unknown) => Effect.Effect<E, unknown>) =>
  (
    failure: RawActionFailure | IsOfflineError,
  ): Effect.Effect<never, Exclude<E, InvalidPayloadError> | IsOfflineError | UnknownError> => {
    // The connection manager already decided this one.
    if (failure._tag === 'IsOfflineError') return Effect.fail(failure)

    const { cause, statusAtFailure } = failure

    // Anything that failed while the socket was not up is an offline failure,
    // whatever rivetkit attached to it.
    if (statusAtFailure !== 'connected') return Effect.fail(new IsOfflineError({ cause }))

    if (!isRivetErrorLike(cause)) {
      // A non-structured close (`conn.disconnect('reason')`) rejects in-flight
      // actions with a plain `Error`, not an `ActorError`.
      if (cause instanceof Error && CONNECTION_DROPPED_MESSAGE.test(cause.message)) {
        return Effect.fail(new IsOfflineError({ cause }))
      }
      return Effect.fail(new UnknownError({ cause }))
    }

    const envelope = decodeActionErrorEnvelope(cause.metadata)

    if (Option.isSome(envelope)) {
      return Effect.flatMap(
        Effect.result(decodeError(envelope.value)),
        (result): Effect.Effect<never, Exclude<E, InvalidPayloadError> | UnknownError> => {
          if (Result.isFailure(result)) {
            return Effect.fail(new UnknownError({ cause, note: 'undecodable action error' }))
          }
          const error = result.success
          if (isInvalidPayloadError(error)) {
            return Effect.fail(new UnknownError({ cause: error, note: 'validatePayload rejected' }))
          }
          return Effect.fail(error as Exclude<E, InvalidPayloadError>)
        },
      )
    }

    // No envelope → rivetkit's own error. `actor/*` lifecycle codes and every
    // `guard/*` code mean the actor is momentarily unreachable; the leader
    // retries on `IsOfflineError`. `ws/*` codes are structured WebSocket
    // closes issued by the engine for a hibernated connection it cannot
    // resume (e.g. `ws.message_index_skip` on the first message after a wake,
    // see README "Sleep & hibernation"): rivetkit reconnects at once and the
    // request is safe to retry, unlike `message/incoming_too_long`, where
    // retrying the same oversize frame would loop.
    if (
      cause.group === 'guard' ||
      cause.group === 'ws' ||
      (cause.group === 'actor' && OFFLINE_ACTOR_CODES.has(cause.code))
    ) {
      return Effect.fail(new IsOfflineError({ cause }))
    }

    return Effect.fail(new UnknownError({ cause, payload: { group: cause.group, code: cause.code } }))
  }
