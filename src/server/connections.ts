/**
 * Live-pull fan-out over the actor's connections.
 *
 * Rivet gives an action handler no per-connection context, so authorization
 * of a *connection* (as opposed to a request) happens lazily here: the first
 * time a connection is about to receive an event, its `conn.params` are
 * decoded as `ConnParams` and run through the same `validateSyncPayload`
 * every action uses. The verdict is cached in `ctx.connAuth` for the wake, so
 * a connection is validated once and a rejected one is disconnected once.
 *
 * `conn.send` is synchronous, so events reach each connection in push order
 * as long as pushes themselves are serialized (they are, by `pushSemaphore`).
 */

import { Cause, Effect, Exit } from '@livestore/utils/effect'

import { LIVE_PULL_EVENT } from '../common/mod.ts'
import type { RawConn, StoreCtx } from './store-ctx.ts'
import { validateConnParams } from './validate-payload.ts'

/** Close reason handed to `conn.disconnect` for a connection that fails validation. */
export const UNAUTHORIZED_DISCONNECT_REASON = 'unauthorized'

/**
 * Validates one connection that has no cached verdict yet and records the
 * result. Never fails: a rejected connection is logged and disconnected.
 */
const authorizeFresh = <TSyncPayload>(ctx: StoreCtx<TSyncPayload>, conn: RawConn): Effect.Effect<void> =>
  Effect.gen(function* () {
    const exit = yield* Effect.exit(validateConnParams({ storeId: ctx.storeId, options: ctx.options }, conn.params))

    // Another fan-out may have raced us to a verdict; the first one wins so
    // the connection is never disconnected twice.
    if (ctx.connAuth.has(conn.id) === true) return

    if (Exit.isSuccess(exit)) {
      ctx.connAuth.set(conn.id, 'ok')
      return
    }

    ctx.connAuth.set(conn.id, 'rejected')
    ctx.log('warn', 'disconnecting unauthorized connection', {
      storeId: ctx.storeId,
      connId: conn.id,
      reason: describeError(Cause.squash(exit.cause)),
    })
    // `tryPromise` (not `promise`): a rejecting `disconnect` must not become a defect.
    yield* Effect.tryPromise(() => conn.disconnect(UNAUTHORIZED_DISCONNECT_REASON)).pipe(Effect.ignore)
  })

/**
 * One-line description of a validation failure for the log. Prefers our own
 * `reason` field (schema tagged errors carry an empty `message`).
 */
const describeError = (error: unknown): string => {
  if (typeof error === 'object' && error !== null) {
    const { _tag, reason } = error as { _tag?: unknown; reason?: unknown }
    if (typeof reason === 'string') return typeof _tag === 'string' ? `${_tag}: ${reason}` : reason
  }
  if (error instanceof Error) return error.message === '' ? error.name : error.message
  return String(error)
}

/**
 * The connections that may currently receive live-pull events.
 *
 * Snapshots `ctx.conns()`, drops cache entries for connections that have
 * gone away, validates every connection without a verdict (concurrently),
 * and returns the live connections whose verdict is `'ok'`.
 */
export const authorizedConns = <TSyncPayload>(ctx: StoreCtx<TSyncPayload>): Effect.Effect<ReadonlyArray<RawConn>> =>
  Effect.gen(function* () {
    const live = [...ctx.conns()]
    const liveIds = new Set(live.map((conn) => conn.id))

    for (const cachedId of [...ctx.connAuth.keys()]) {
      if (liveIds.has(cachedId) === false) ctx.connAuth.delete(cachedId)
    }

    const fresh = live.filter((conn) => ctx.connAuth.has(conn.id) === false)
    if (fresh.length > 0) {
      yield* Effect.forEach(fresh, (conn) => authorizeFresh(ctx, conn), { concurrency: 'unbounded', discard: true })
    }

    return live.filter((conn) => ctx.connAuth.get(conn.id) === 'ok')
  })

/**
 * Sends every encoded pull response, in order, to every authorized
 * connection. A connection whose `send` throws is logged and skipped; it can
 * never fail the push that triggered the fan-out.
 */
export const fanOut = <TSyncPayload>(
  ctx: StoreCtx<TSyncPayload>,
  encodedResponses: ReadonlyArray<unknown>,
): Effect.Effect<void> =>
  Effect.gen(function* () {
    const conns = yield* authorizedConns(ctx)
    if (conns.length === 0 || encodedResponses.length === 0) return

    for (const conn of conns) {
      try {
        for (const encoded of encodedResponses) {
          conn.send(LIVE_PULL_EVENT, encoded)
        }
      } catch (cause) {
        ctx.log('warn', 'failed to send live pull event to connection', {
          storeId: ctx.storeId,
          connId: conn.id,
          reason: cause instanceof Error ? cause.message : String(cause),
        })
      }
    }
  })
