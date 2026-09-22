/**
 * The Rivet actor contract: one `Action` per wire operation plus the
 * `Actor` definition that carries them.
 *
 * This is the only place (besides `actor.ts`) that may import
 * `@rivetkit/effect` — that package pulls in the `rivetkit` server entry,
 * which must never reach a browser bundle. Every schema referenced here comes
 * from `src/common/`, so the client can call the same actions with plain
 * string names and the same codecs.
 *
 * NOTE: `@rivetkit/effect@2.3.17` targets `effect@4.0.0-beta.66`; consumers of
 * `./server` need the pnpm patch described in `docs/spike-results.md`
 * (`patches/@rivetkit__effect@2.3.17.patch`).
 */

import { Action, Actor } from '@rivetkit/effect'

import {
  ACTION_ADMIN_INFO,
  ACTION_ADMIN_RESET,
  ACTION_PING,
  ACTION_PULL,
  ACTION_PUSH,
  ACTION_TEST_DISCONNECT_ALL,
  ACTION_TEST_INFO,
  ACTION_TEST_SLEEP,
  ACTOR_NAME,
  AdminError,
  AdminInfoRequest,
  AdminInfoResponse,
  AdminResetRequest,
  AdminResetResponse,
  PingError,
  PingRequest,
  Pong,
  PullError,
  PullRequest,
  PullResponse,
  PushAck,
  PushError,
  PushRequest,
  TestDisconnectAllRequest,
  TestDisconnectAllResponse,
  TestInfoRequest,
  TestInfoResponse,
  TestSleepRequest,
  TestSleepResponse,
} from '../common/mod.ts'

/** Catch-up pull. Client-driven pagination; live updates arrive as events. */
export const Pull = Action.make(ACTION_PULL, {
  payload: PullRequest,
  success: PullResponse,
  error: PullError,
})

/** Append a chained batch of events and fan the result out to every conn. */
export const Push = Action.make(ACTION_PUSH, {
  payload: PushRequest,
  success: PushAck,
  error: PushError,
})

/** Liveness check; also revalidates the caller's sync payload. */
export const Ping = Action.make(ACTION_PING, {
  payload: PingRequest,
  success: Pong,
  error: PingError,
})

/**
 * Admin: reports the store's head, backend id, event and connection counts.
 * Refused unless `options.admin.secret` is set and the request's
 * `adminSecret` matches it.
 */
export const AdminInfo = Action.make(ACTION_ADMIN_INFO, {
  payload: AdminInfoRequest,
  success: AdminInfoResponse,
  error: AdminError,
})

/**
 * Admin: wipes the store's eventlog, mints a new `backendId` and disconnects
 * every connection (`'store-reset'`). Refused unless `options.admin.secret`
 * is set and the request's `adminSecret` matches it.
 */
export const AdminReset = Action.make(ACTION_ADMIN_RESET, {
  payload: AdminResetRequest,
  success: AdminResetResponse,
  error: AdminError,
})

/**
 * Test-only: disconnects every connection so a client observes a transport
 * drop (used by the conformance suite's `turnBackendOffline`). The handler
 * refuses unless `options.testing.enabled` is set.
 */
export const TestDisconnectAll = Action.make(ACTION_TEST_DISCONNECT_ALL, {
  payload: TestDisconnectAllRequest,
  success: TestDisconnectAllResponse,
  error: PingError,
})

/**
 * Test-only: reports the actor's per-wake state (`wakeCount`, head,
 * `backendId`, connections) so tests can observe sleep/wake cycles. The
 * handler refuses unless `options.testing.enabled` is set.
 */
export const TestInfo = Action.make(ACTION_TEST_INFO, {
  payload: TestInfoRequest,
  success: TestInfoResponse,
  error: PingError,
})

/**
 * Test-only: asks rivetkit to put the actor to sleep immediately (`c.sleep()`),
 * so tests can exercise the sleep/wake path without waiting for
 * `sleepTimeout`. The handler refuses unless `options.testing.enabled` is set.
 */
export const TestSleep = Action.make(ACTION_TEST_SLEEP, {
  payload: TestSleepRequest,
  success: TestSleepResponse,
  error: PingError,
})

/** Union of every action on {@link LiveStoreSync}. */
export type LiveStoreSyncActions =
  | typeof Pull
  | typeof Push
  | typeof Ping
  | typeof AdminInfo
  | typeof AdminReset
  | typeof TestDisconnectAll
  | typeof TestInfo
  | typeof TestSleep

/**
 * The actor contract. Implementation (and the `db` / `name` / `icon` options)
 * is supplied by `makeLiveStoreSyncActor` in `actor.ts`.
 */
export const LiveStoreSync = Actor.make(ACTOR_NAME, {
  actions: [Pull, Push, Ping, AdminInfo, AdminReset, TestDisconnectAll, TestInfo, TestSleep],
})
