/**
 * Admin actions, the Rivet counterpart of `@livestore/sync-cf`'s
 * `AdminInfoRequest` / `AdminResetRoomRequest`.
 *
 * Both are registered on the actor unconditionally (the contract is static)
 * but refused unless `options.admin.secret` is set. A request must pass the
 * usual caller validation (`storeId` match, `syncPayloadSchema`,
 * `validatePayload`) *and* carry the matching `adminSecret`, compared in
 * constant time.
 *
 * `AdminReset` wipes the store and mints a new `backendId`. Every connected
 * client is disconnected (`'store-reset'`); on reconnect its live pull
 * re-catches-up with a cursor carrying the old id and fails with
 * `BackendIdMismatchError`, which makes LiveStore's leader apply
 * `onBackendIdMismatch` (default `'reset'`: wipe local state and resync).
 */

import { timingSafeEqual } from 'node:crypto'

import { UnknownError } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import { Effect, Option } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'

import {
  type AdminInfoRequest,
  AdminInfoResponse,
  type AdminResetRequest,
  AdminResetResponse,
  AdminUnauthorizedError,
  type InvalidPayloadError,
  PERSISTENCE_FORMAT_VERSION,
} from '../common/mod.ts'
import { mapToDeclaredErrors } from './hooks.ts'
import type { StoreCtx } from './store-ctx.ts'
import { validateSyncPayload } from './validate-payload.ts'

export type AdminHandlerError = UnknownError | InvalidPayloadError | AdminUnauthorizedError

export const ADMIN_DISABLED_NOTE = 'admin actions are disabled (set admin.secret)'

/** Close reason handed to `conn.disconnect` for every connection when a store is reset. */
export const STORE_RESET_DISCONNECT_REASON = 'store-reset'

const ADMIN_ERROR_TAGS = ['AdminUnauthorizedError', 'InvalidPayloadError', 'UnknownError'] as const

const textEncoder = new TextEncoder()

/** Constant-time string comparison; `false` for inputs of different byte length. */
export const secretsMatch = (expected: string, received: string): boolean => {
  const a = textEncoder.encode(expected)
  const b = textEncoder.encode(received)
  if (a.byteLength !== b.byteLength) return false
  return timingSafeEqual(a, b)
}

/** Shared gate: admin enabled → caller validation → secret check. */
const authorizeAdmin = <TSyncPayload>(
  ctx: StoreCtx<TSyncPayload>,
  req: AdminInfoRequest | AdminResetRequest,
): Effect.Effect<void, AdminHandlerError> =>
  Effect.gen(function* () {
    const secret = ctx.options.adminSecret
    if (secret === undefined) {
      return yield* new UnknownError({ cause: new Error(ADMIN_DISABLED_NOTE), note: ADMIN_DISABLED_NOTE })
    }

    yield* validateSyncPayload(ctx, req)

    if (secretsMatch(secret, req.adminSecret) === false) {
      ctx.log('warn', 'rejected admin request with a wrong adminSecret', {
        storeId: ctx.storeId,
        clientId: req.clientId,
      })
      return yield* new AdminUnauthorizedError({ storeId: req.storeId })
    }
  })

export const makeAdminInfo =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: AdminInfoRequest): Effect.Effect<AdminInfoResponse, AdminHandlerError> =>
    Effect.gen(function* () {
      yield* authorizeAdmin(ctx, req)

      const eventCount = yield* ctx.storage.countAfter(Option.none())

      return AdminInfoResponse.make({
        storeId: ctx.storeId,
        backendId: ctx.backendId,
        currentHead: ctx.headRef.current,
        eventCount,
        connectionCount: [...ctx.conns()].length,
        persistenceFormatVersion: PERSISTENCE_FORMAT_VERSION,
      })
    }).pipe(
      mapToDeclaredErrors(ADMIN_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:admin-info', { attributes: { storeId: ctx.storeId } }),
    )

export const makeAdminReset =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: AdminResetRequest): Effect.Effect<AdminResetResponse, AdminHandlerError> =>
    Effect.gen(function* () {
      yield* authorizeAdmin(ctx, req)

      // Under the push gate, uninterruptibly: no push can interleave with the
      // wipe, and a half-applied reset (storage wiped, in-memory head still
      // old) can never be observed.
      const reset = Effect.gen(function* () {
        const previousBackendId = ctx.backendId
        const previousHead = ctx.headRef.current

        yield* ctx.storage.resetStore(ctx.storeId)

        const backendId = nanoid()
        const root = EventSequenceNumber.Client.ROOT.global
        // In-memory state first: the storage is already empty, so the old
        // head/backendId must not be served any more. If `saveContext` fails
        // below, the next push persists the new context anyway (and a wake
        // before that mints yet another id, which clients handle the same way).
        ctx.backendIdRef.current = backendId
        ctx.headRef.current = root
        ctx.connAuth.clear()

        yield* ctx.storage.saveContext({ storeId: ctx.storeId, currentHead: root, backendId })

        const conns = [...ctx.conns()]
        yield* Effect.forEach(
          conns,
          // `tryPromise` (not `promise`): an already-closed conn rejecting must not become a defect.
          (conn) => Effect.tryPromise(() => conn.disconnect(STORE_RESET_DISCONNECT_REASON)).pipe(Effect.ignore),
          { concurrency: 'unbounded', discard: true },
        )

        ctx.log('info', 'store reset by admin', {
          storeId: ctx.storeId,
          clientId: req.clientId,
          previousBackendId,
          previousHead,
          backendId,
          disconnected: conns.length,
        })

        return AdminResetResponse.make({ backendId })
      })

      return yield* ctx.pushSemaphore.withPermits(1)(reset.pipe(Effect.uninterruptible))
    }).pipe(
      mapToDeclaredErrors(ADMIN_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:admin-reset', { attributes: { storeId: ctx.storeId } }),
    )
