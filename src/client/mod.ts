/**
 * Browser/worker-safe client entry (`livestore-sync-rivet-actors/client`).
 *
 * Only `rivetkit/client` is reachable from here — never `rivetkit`,
 * `rivetkit/errors` or `@rivetkit/effect` (all server entries).
 */

export { makeRivetSync } from './make-rivet-sync.ts'
export type { RivetSyncOptions } from './options.ts'
export type { SyncMetadata } from '../common/mod.ts'
/** Re-exported for convenience: the actor name and live-event name the server registers. */
export { ACTOR_NAME, LIVE_PULL_EVENT } from '../common/mod.ts'

// Admin helper for ops scripts / tooling (`AdminInfo`, `AdminReset`).
export {
  DEFAULT_ADMIN_CLIENT_ID,
  makeRivetSyncAdmin,
  type RivetSyncAdmin,
  type RivetSyncAdminOptions,
} from './admin.ts'
export {
  type AdminError,
  type AdminInfoResponse,
  type AdminResetResponse,
  AdminUnauthorizedError,
  InvalidPayloadError,
} from '../common/mod.ts'
