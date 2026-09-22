/**
 * Public options of the Rivet-backed `SyncBackend` and their resolved
 * (defaults applied, durations normalised) counterpart.
 *
 * Browser-safe: no rivetkit imports.
 */

import { Duration } from '@livestore/utils/effect'

import { ACTOR_NAME, DEFAULT_PULL_PAGE_SIZE } from '../common/mod.ts'

export interface RivetSyncOptions {
  /** Rivet endpoint, e.g. `https://api.rivet.dev` or `http://127.0.0.1:6420`. Supports `https://namespace:token@host` URL auth syntax. */
  readonly endpoint: string
  /** Rivet access token (omit for local engines). */
  readonly token?: string
  /** Rivet namespace (rivetkit defaults to `default`). */
  readonly namespace?: string
  /** Actor name the server registered the sync actor under. Default: `LiveStoreSync`. */
  readonly actorName?: string
  /** Bound on waiting for `connected` before an action fails with `IsOfflineError`. Default: 10 s. */
  readonly connectTimeout?: Duration.Input
  /** Liveness pings. Default: enabled, 10 s timeout, every 10 s. */
  readonly ping?: {
    readonly enabled?: boolean
    readonly requestTimeout?: Duration.Input
    readonly requestInterval?: Duration.Input
  }
  /** Events per catch-up pull page (server clamps to `MAX_PULL_EVENTS_PER_MESSAGE`). Default: 100. */
  readonly pullPageSize?: number
  /**
   * Byte budget for one `Push` action payload. Default: 60 000, which fits
   * under rivetkit's default 64 KB `maxIncomingMessageSize`; raise it only when
   * the server was started with a larger limit.
   */
  readonly maxPushBytes?: number
  /** Backoff used when rivetkit gives up reconnecting (`idle`) and the connection is recreated. Default: 1 s → 30 s, jittered. */
  readonly reconnect?: {
    readonly baseDelay?: Duration.Input
    readonly maxDelay?: Duration.Input
  }
}

export interface ResolvedRivetSyncOptions {
  readonly endpoint: string
  readonly token: string | undefined
  readonly namespace: string | undefined
  readonly actorName: string
  readonly connectTimeout: Duration.Duration
  readonly ping: {
    readonly enabled: boolean
    readonly requestTimeout: Duration.Duration
    readonly requestInterval: Duration.Duration
  }
  readonly pullPageSize: number
  readonly maxPushBytes: number
  readonly reconnect: {
    readonly baseDelay: Duration.Duration
    readonly maxDelay: Duration.Duration
  }
}

export const DEFAULT_CONNECT_TIMEOUT = Duration.seconds(10)
export const DEFAULT_PING_REQUEST_TIMEOUT = Duration.seconds(10)
export const DEFAULT_PING_REQUEST_INTERVAL = Duration.seconds(10)
export const DEFAULT_MAX_PUSH_BYTES = 60_000
export const DEFAULT_RECONNECT_BASE_DELAY = Duration.seconds(1)
export const DEFAULT_RECONNECT_MAX_DELAY = Duration.seconds(30)

const duration = (input: Duration.Input | undefined, fallback: Duration.Duration): Duration.Duration =>
  input === undefined ? fallback : Duration.fromInputUnsafe(input)

export const resolveRivetSyncOptions = (options: RivetSyncOptions): ResolvedRivetSyncOptions => ({
  endpoint: options.endpoint,
  token: options.token,
  namespace: options.namespace,
  actorName: options.actorName ?? ACTOR_NAME,
  connectTimeout: duration(options.connectTimeout, DEFAULT_CONNECT_TIMEOUT),
  ping: {
    enabled: options.ping?.enabled ?? true,
    requestTimeout: duration(options.ping?.requestTimeout, DEFAULT_PING_REQUEST_TIMEOUT),
    requestInterval: duration(options.ping?.requestInterval, DEFAULT_PING_REQUEST_INTERVAL),
  },
  pullPageSize: options.pullPageSize ?? DEFAULT_PULL_PAGE_SIZE,
  maxPushBytes: options.maxPushBytes ?? DEFAULT_MAX_PUSH_BYTES,
  reconnect: {
    baseDelay: duration(options.reconnect?.baseDelay, DEFAULT_RECONNECT_BASE_DELAY),
    maxDelay: duration(options.reconnect?.maxDelay, DEFAULT_RECONNECT_MAX_DELAY),
  },
})
