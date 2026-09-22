/**
 * Test-only actions. Registered on the actor unconditionally (the contract
 * is static) but refused at runtime unless `options.testing.enabled` is set.
 *
 * `TestDisconnectAll` lets the conformance suite simulate a backend outage
 * (`turnBackendOffline`): every connection is closed server-side, so clients
 * observe a transport drop and exercise their reconnect path.
 *
 * `TestInfo` reports the per-wake state (`wakeCount`, head, `backendId`, the
 * connections with the `clientId` decoded from their `conn.params`) so the
 * hibernation tests can tell whether the actor slept and what a rehydrated
 * connection looks like. Calling it is itself an action, i.e. it wakes a
 * sleeping actor and resets its idle timer.
 */

import { UnknownError } from '@livestore/common'
import { Effect } from '@livestore/utils/effect'

import {
  decodeConnParams,
  type InvalidPayloadError,
  type TestDisconnectAllRequest,
  TestDisconnectAllResponse,
  type TestInfoRequest,
  TestInfoResponse,
  type TestSleepRequest,
  TestSleepResponse,
} from '../common/mod.ts'
import { mapToDeclaredErrors } from './hooks.ts'
import type { RawConn, StoreCtx } from './store-ctx.ts'
import { validateSyncPayload } from './validate-payload.ts'

export type TestActionError = UnknownError | InvalidPayloadError

export const TEST_ACTIONS_DISABLED_NOTE = 'TestDisconnectAll is disabled (set testing.enabled)'

export const TEST_INFO_DISABLED_NOTE = 'TestInfo is disabled (set testing.enabled)'

export const TEST_SLEEP_DISABLED_NOTE = 'TestSleep is disabled (set testing.enabled)'

/** Close reason handed to `conn.disconnect` by `TestDisconnectAll`. */
export const TEST_DISCONNECT_REASON = 'test-disconnect-all'

const TEST_ACTION_ERROR_TAGS = ['InvalidPayloadError', 'UnknownError'] as const

export const makeTestDisconnectAll =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: TestDisconnectAllRequest): Effect.Effect<TestDisconnectAllResponse, TestActionError> =>
    Effect.gen(function* () {
      if (ctx.options.testingEnabled !== true) {
        return yield* new UnknownError({
          cause: new Error(TEST_ACTIONS_DISABLED_NOTE),
          note: TEST_ACTIONS_DISABLED_NOTE,
        })
      }

      yield* validateSyncPayload(ctx, req)

      const conns = [...ctx.conns()]
      yield* Effect.forEach(
        conns,
        // `tryPromise` (not `promise`): an already-closed conn rejecting must not become a defect.
        (conn) => Effect.tryPromise(() => conn.disconnect(TEST_DISCONNECT_REASON)).pipe(Effect.ignore),
        { concurrency: 'unbounded', discard: true },
      )
      // Every cached verdict belongs to a connection that no longer exists.
      ctx.connAuth.clear()

      ctx.log('info', 'TestDisconnectAll closed every connection', { storeId: ctx.storeId, count: conns.length })

      return TestDisconnectAllResponse.make({ disconnected: conns.length })
    }).pipe(
      mapToDeclaredErrors(TEST_ACTION_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:test-disconnect-all', { attributes: { storeId: ctx.storeId } }),
    )

/** `clientId` from a connection's params, or `null` when they are not valid `ConnParams`. */
const connClientId = (params: unknown): string | null => {
  try {
    return decodeConnParams(params).clientId
  } catch {
    return null
  }
}

/** rivetkit's `Conn.isHibernatable` (a native getter; absent on the structural `RawConn` fakes). */
const connHibernatable = (conn: RawConn): boolean | null => {
  try {
    const value = (conn as { readonly isHibernatable?: unknown }).isHibernatable
    return typeof value === 'boolean' ? value : null
  } catch {
    return null
  }
}

export const makeTestInfo =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: TestInfoRequest): Effect.Effect<TestInfoResponse, TestActionError> =>
    Effect.gen(function* () {
      if (ctx.options.testingEnabled !== true) {
        return yield* new UnknownError({
          cause: new Error(TEST_INFO_DISABLED_NOTE),
          note: TEST_INFO_DISABLED_NOTE,
        })
      }

      yield* validateSyncPayload(ctx, req)

      return TestInfoResponse.make({
        wakeCount: ctx.wake.count,
        wokeAt: ctx.wake.at,
        previousSleptAt: ctx.wake.previousSleptAt,
        head: ctx.headRef.current,
        backendId: ctx.backendId,
        conns: [...ctx.conns()].map((conn) => ({
          id: conn.id,
          clientId: connClientId(conn.params),
          hibernatable: connHibernatable(conn),
        })),
      })
    }).pipe(
      mapToDeclaredErrors(TEST_ACTION_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:test-info', { attributes: { storeId: ctx.storeId } }),
    )

/**
 * `sleep` is rivetkit's `c.sleep()` (the SDK's `Actor.Sleep` service). It is
 * requested *after* the response is produced — the caller's own connection
 * may be one of the connections that get hibernated — but rivetkit only
 * begins the sleep once the running action has completed anyway.
 */
export const makeTestSleep =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>, sleep: Effect.Effect<void>) =>
  (req: TestSleepRequest): Effect.Effect<TestSleepResponse, TestActionError> =>
    Effect.gen(function* () {
      if (ctx.options.testingEnabled !== true) {
        return yield* new UnknownError({
          cause: new Error(TEST_SLEEP_DISABLED_NOTE),
          note: TEST_SLEEP_DISABLED_NOTE,
        })
      }

      yield* validateSyncPayload(ctx, req)

      const conns = [...ctx.conns()].length
      ctx.log('info', 'TestSleep requested', { storeId: ctx.storeId, conns })
      yield* sleep
      return TestSleepResponse.make({ conns })
    }).pipe(
      mapToDeclaredErrors(TEST_ACTION_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:test-sleep', { attributes: { storeId: ctx.storeId } }),
    )
