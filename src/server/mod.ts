/**
 * Server entry point (`livestore-sync-rivet-actors/server`).
 *
 * Importing this module pulls in `@rivetkit/effect` and therefore the
 * `rivetkit` server runtime — never import it from browser code. The wire
 * contract lives in `livestore-sync-rivet-actors/common`.
 *
 * Typical usage:
 *
 * ```ts
 * import { Registry } from '@rivetkit/effect'
 * import { Layer } from 'effect'
 * import { makeLiveStoreSyncActor, registryOptions } from 'livestore-sync-rivet-actors/server'
 *
 * const MainLayer = Registry.serve(makeLiveStoreSyncActor({ validatePayload })).pipe(
 *   Layer.provide(Registry.layer(registryOptions({ endpoint: process.env.RIVET_ENDPOINT }))),
 * )
 * ```
 */

// Actor layer + registry helpers
export {
  DEFAULT_ACTOR_DISPLAY_NAME,
  DEFAULT_ACTOR_ICON,
  makeLiveStoreSyncActor,
  RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  type RegistryOptionsInput,
  registryOptions,
} from './actor.ts'

// Actor contract (actions + actor definition)
export {
  AdminInfo,
  AdminReset,
  LiveStoreSync,
  type LiveStoreSyncActions,
  Ping,
  Pull,
  Push,
  TestDisconnectAll,
  TestInfo,
  TestSleep,
} from './actions.ts'

// Options
export {
  type CallbackContext,
  type LiveStoreSyncActorOptions,
  type LiveStoreSyncRivetActorOptions,
  type ResolvedOptions,
  resolveOptions,
  type ValidatePayloadContext,
} from './options.ts'

// Errors a caller can observe (re-exported from the common contract)
export { AdminUnauthorizedError, InvalidPayloadError } from '../common/mod.ts'

// Advanced: handler-level building blocks for hosting the handlers in a
// custom actor (e.g. a raw `Rivetkit.actor`) or for testing without an engine.
export {
  type ConnAuthState,
  firstWake,
  type LogLevel,
  makeStoreCtx,
  type RawConn,
  type StoreCtx,
  type WakeInfo,
} from './store-ctx.ts'
export {
  type ContextRow,
  type EventlogRow,
  makeSyncStorage,
  migrate,
  migrationSql,
  type RawAccessLike,
  type SyncStorage,
} from './sqlite.ts'
export { makePull, type PullHandlerError } from './pull.ts'
export { makePush, type PushHandlerError } from './push.ts'
export { makePing, type PingHandlerError } from './ping.ts'
export {
  ADMIN_DISABLED_NOTE,
  type AdminHandlerError,
  makeAdminInfo,
  makeAdminReset,
  STORE_RESET_DISCONNECT_REASON,
} from './admin.ts'
export { makeTestDisconnectAll, makeTestInfo, makeTestSleep, type TestActionError } from './test-actions.ts'
