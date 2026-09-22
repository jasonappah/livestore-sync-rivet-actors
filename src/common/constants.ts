/**
 * Wire-protocol constants shared by the Rivet actor (server) and the
 * `SyncBackend` implementation (client).
 *
 * This module must stay free of `rivetkit` / `@rivetkit/effect` imports so it
 * can be bundled for browsers and workers.
 */

/** Rivet actor name. One actor instance per `storeId` (key `[storeId]`). */
export const ACTOR_NAME = 'LiveStoreSync' as const

/** Action name: catch-up pull (client-driven pagination). */
export const ACTION_PULL = 'Pull' as const

/** Action name: push a chained batch of events. */
export const ACTION_PUSH = 'Push' as const

/** Action name: liveness check. */
export const ACTION_PING = 'Ping' as const

/**
 * Action name: admin-only store introspection (head, event count,
 * connections). Requires the server's `admin.secret`.
 */
export const ACTION_ADMIN_INFO = 'AdminInfo' as const

/**
 * Action name: admin-only store reset — drops the eventlog, mints a new
 * `backendId` and disconnects every connection. Requires the server's
 * `admin.secret`.
 */
export const ACTION_ADMIN_RESET = 'AdminReset' as const

/**
 * Action name: test-only helper that disconnects every connection.
 * Only registered when the server options enable `testing`.
 */
export const ACTION_TEST_DISCONNECT_ALL = 'TestDisconnectAll' as const

/**
 * Action name: test-only introspection of the actor's per-wake state
 * (`wakeCount`, connections, head). Refused unless the server options enable
 * `testing`.
 */
export const ACTION_TEST_INFO = 'TestInfo' as const

/**
 * Action name: test-only request to put the actor to sleep right away
 * (`c.sleep()`), regardless of `sleepTimeout`. Refused unless the server
 * options enable `testing`.
 */
export const ACTION_TEST_SLEEP = 'TestSleep' as const

/**
 * Connection event name used for live pull fan-out
 * (`conn.send(LIVE_PULL_EVENT, encodePullResponse(res))`).
 */
export const LIVE_PULL_EVENT = 'pull' as const

/** Upper bound on events the server puts into a single pull page / live event. */
export const MAX_PULL_EVENTS_PER_MESSAGE = 100

/** Upper bound on events a single `Push` action may carry. */
export const MAX_PUSH_EVENTS_PER_REQUEST = 100

/** Defensive byte budget for a single action result / connection event. */
export const MAX_TRANSPORT_PAYLOAD_BYTES = 900_000

/** Default page size for catch-up pulls (clamped to `MAX_PULL_EVENTS_PER_MESSAGE`). */
export const DEFAULT_PULL_PAGE_SIZE = 100

/** Bumped whenever the actor's SQLite layout changes incompatibly. */
export const PERSISTENCE_FORMAT_VERSION = 1
