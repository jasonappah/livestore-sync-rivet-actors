/**
 * Caller authorization, shared by every action handler and by the
 * per-connection check in `connections.ts`.
 *
 * Three things can reject a caller, all of them surfacing as
 * `InvalidPayloadError` so the client can tell an auth failure apart from a
 * transport or sync error:
 *
 * 1. the request's `storeId` does not match the actor key,
 * 2. the payload fails `options.syncPayloadSchema`,
 * 3. `options.validatePayload` throws, rejects, or fails.
 */

import type { UnknownError } from '@livestore/common'
import { Cause, Effect, Result, Schema } from '@livestore/utils/effect'

import { decodeConnParamsEffect, InvalidPayloadError } from '../common/mod.ts'
import type { ResolvedOptions } from './options.ts'

/** Everything `validateSyncPayload` needs from the actor's wake context. */
export type ValidateContext<TSyncPayload = Schema.Json> = {
  /** The actor key's `storeId`; every request must match it. */
  readonly storeId: string
  readonly options: ResolvedOptions<TSyncPayload>
}

/** The request fields carrying the caller's identity. */
export type ValidateRequest = {
  readonly storeId: string
  readonly clientId: string
  readonly payload?: Schema.Json
}

export const STORE_ID_MISMATCH_REASON = 'storeId does not match actor key'
export const SCHEMA_FAILURE_REASON = 'payload failed schema'
export const VALIDATE_REJECTED_REASON = 'validatePayload rejected'
export const CONN_PARAMS_FAILURE_REASON = 'connection params failed schema'

/**
 * Validates `req` against `ctx` and returns the decoded sync payload
 * (`undefined` when the caller sent none and no schema is configured).
 */
export const validateSyncPayload = <TSyncPayload = Schema.Json>(
  ctx: ValidateContext<TSyncPayload>,
  req: ValidateRequest,
): Effect.Effect<TSyncPayload | undefined, InvalidPayloadError | UnknownError> =>
  Effect.suspend(() => {
    if (req.storeId !== ctx.storeId) {
      return Effect.fail(
        new InvalidPayloadError({
          storeId: req.storeId,
          reason: STORE_ID_MISMATCH_REASON,
          cause: new Error(`expected storeId '${ctx.storeId}', received '${req.storeId}'`),
        }),
      )
    }

    const { syncPayloadSchema, validatePayload } = ctx.options

    // Decode even when the payload is absent: a schema may require one.
    let decoded: TSyncPayload | undefined
    if (syncPayloadSchema === undefined) {
      decoded = req.payload as TSyncPayload | undefined
    } else {
      const result = Schema.decodeUnknownResult(syncPayloadSchema)(req.payload)
      if (Result.isFailure(result) === true) {
        return Effect.fail(
          new InvalidPayloadError({
            storeId: req.storeId,
            reason: SCHEMA_FAILURE_REASON,
            cause: result.failure,
          }),
        )
      }
      decoded = result.success
    }

    if (validatePayload === undefined) return Effect.succeed(decoded)

    const payload = decoded
    return Effect.trySyncOrPromiseOrEffect(() =>
      validatePayload(payload, { storeId: req.storeId, clientId: req.clientId }),
    ).pipe(
      Effect.catchCause((cause) =>
        Effect.fail(
          new InvalidPayloadError({
            storeId: req.storeId,
            reason: VALIDATE_REJECTED_REASON,
            cause: Cause.squash(cause),
          }),
        ),
      ),
      Effect.as(payload),
    )
  })

/**
 * Same as {@link validateSyncPayload}, but starting from the raw
 * `conn.params` value handed to the actor by rivetkit. Used once per
 * connection before it is allowed to receive live-pull events.
 */
export const validateConnParams = <TSyncPayload = Schema.Json>(
  ctx: ValidateContext<TSyncPayload>,
  rawParams: unknown,
): Effect.Effect<TSyncPayload | undefined, InvalidPayloadError | UnknownError> =>
  decodeConnParamsEffect(rawParams).pipe(
    Effect.catchCause((cause) =>
      Effect.fail(
        new InvalidPayloadError({
          storeId: ctx.storeId,
          reason: CONN_PARAMS_FAILURE_REASON,
          cause: Cause.squash(cause),
        }),
      ),
    ),
    Effect.flatMap((params) => validateSyncPayload(ctx, params)),
  )
