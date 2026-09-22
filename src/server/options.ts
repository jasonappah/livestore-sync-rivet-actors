/**
 * Server-side configuration for the `LiveStoreSync` actor.
 *
 * Mirrors `@livestore/sync-cf`'s Durable Object / worker options
 * (`cf-worker/shared.ts`, `cf-worker/worker.ts`) so the two providers stay
 * interchangeable from an application's point of view.
 */

import type { UnknownError } from '@livestore/common'
import type { Effect, Schema } from '@livestore/utils/effect'
import type { ActorOptionsInput } from 'rivetkit'

import {
  DEFAULT_PULL_PAGE_SIZE,
  MAX_PULL_EVENTS_PER_MESSAGE,
  MAX_PUSH_EVENTS_PER_REQUEST,
  MAX_TRANSPORT_PAYLOAD_BYTES,
  type PullRequest,
  type PullResponse,
  type PushAck,
  type PushRequest,
} from '../common/mod.ts'

/** Context handed to {@link LiveStoreSyncActorOptions.validatePayload}. */
export type ValidatePayloadContext = {
  readonly storeId: string
  readonly clientId: string
}

/**
 * Context handed to the `onPush` / `onPull` callbacks. `payload` is the
 * *decoded* sync payload (through `syncPayloadSchema` when configured) and
 * the key is absent when the caller sent none.
 */
export type CallbackContext<TSyncPayload = Schema.Json> = {
  readonly storeId: string
  readonly clientId: string
  readonly payload?: TSyncPayload
}

/**
 * rivetkit actor runtime options forwarded verbatim to `Rivetkit.actor`
 * (`options`), minus `name` / `icon`, which are top-level options here.
 *
 * The ones that matter for a sync actor:
 * - `sleepTimeout` (ms, default 30 000): idle time before the actor sleeps.
 *   Hibernatable WebSocket connections (every rivetkit action/event
 *   connection, i.e. every `makeRivetSync` client) survive sleep; the next
 *   action wakes the actor and its per-wake state is rebuilt from SQLite.
 * - `sleepGracePeriod` (ms, default 15 000): budget for the graceful
 *   shutdown window (`onSleep`, disconnect callbacks, final state save).
 * - `actionTimeout` (ms, default 60 000): per-action handler timeout.
 * - `noSleep` (deprecated upstream): keep the actor awake indefinitely.
 *
 * `@rivetkit/effect@2.3.17` only forwards `name` and `icon`; this package's
 * pnpm patch forwards every key (see README, "The `@rivetkit/effect` patch").
 */
export type LiveStoreSyncRivetActorOptions = Omit<NonNullable<ActorOptionsInput>, 'name' | 'icon'>

export interface LiveStoreSyncActorOptions<TSyncPayload = Schema.Json> {
  /**
   * Optionally decode the client-provided sync payload into a typed value
   * before {@link LiveStoreSyncActorOptions.validatePayload} sees it. Decoding
   * runs even when the payload is absent, so a schema can make it required.
   */
  readonly syncPayloadSchema?: Schema.Decoder<TSyncPayload>
  /**
   * Authorizes a caller. Runs on every action *and* once per connection
   * (before that connection receives any live-pull event). Throwing,
   * rejecting or failing rejects the request with `InvalidPayloadError`;
   * a failing connection is disconnected.
   */
  readonly validatePayload?: (
    payload: TSyncPayload | undefined,
    ctx: ValidatePayloadContext,
  ) => Effect.SyncOrPromiseOrEffect<void>

  readonly onPush?: (message: PushRequest, ctx: CallbackContext<TSyncPayload>) => Effect.SyncOrPromiseOrEffect<void>
  readonly onPushRes?: (message: PushAck | UnknownError) => Effect.SyncOrPromiseOrEffect<void>
  readonly onPull?: (message: PullRequest, ctx: CallbackContext<TSyncPayload>) => Effect.SyncOrPromiseOrEffect<void>
  readonly onPullRes?: (message: PullResponse | UnknownError) => Effect.SyncOrPromiseOrEffect<void>

  /**
   * Events per catch-up pull page.
   * @default 100 — clamped to `[1, MAX_PULL_EVENTS_PER_MESSAGE]`
   */
  readonly pullPageSize?: number
  /**
   * Hard cap on events a single `Push` may carry.
   * @default 100 — clamped to `[1, MAX_PUSH_EVENTS_PER_REQUEST]`
   */
  readonly maxPushEventsPerRequest?: number
  /**
   * Byte budget for one pull page / live event. Note rivetkit's *incoming*
   * message limit (client → actor) defaults to 64 KB, which is what bounds
   * pushes; this only bounds what the actor sends back.
   * @default 900_000 — clamped to at least 1 KB
   */
  readonly maxMessageBytes?: number

  /** Display name for the Rivet actor (forwarded to `Rivetkit.actor`). */
  readonly name?: string
  /** Display icon for the Rivet actor (forwarded to `Rivetkit.actor`). */
  readonly icon?: string
  /**
   * rivetkit actor runtime options (`sleepTimeout`, `sleepGracePeriod`,
   * `actionTimeout`, …), see {@link LiveStoreSyncRivetActorOptions}.
   */
  readonly actor?: LiveStoreSyncRivetActorOptions

  /** Enables the `TestDisconnectAll` / `TestInfo` actions. Never enable in production. */
  readonly testing?: { readonly enabled: boolean }

  /**
   * Enables the `AdminInfo` / `AdminReset` actions, guarded by `secret`
   * (compared in constant time against the request's `adminSecret`). Admin
   * requests still go through `validatePayload`. Omitted, or an empty
   * `secret`, disables both actions.
   */
  readonly admin?: { readonly secret: string }
}

/**
 * {@link LiveStoreSyncActorOptions} with every tunable resolved to a concrete
 * value. Handlers only ever see this shape.
 */
export interface ResolvedOptions<TSyncPayload = Schema.Json> extends LiveStoreSyncActorOptions<TSyncPayload> {
  readonly pullPageSize: number
  readonly maxPushEventsPerRequest: number
  readonly maxMessageBytes: number
  /** Convenience mirror of `testing?.enabled === true`. */
  readonly testingEnabled: boolean
  /** `admin.secret` when admin actions are enabled (non-empty), `undefined` otherwise. */
  readonly adminSecret: string | undefined
}

/** Smallest byte budget that can still carry a single (small) event. */
const MIN_MESSAGE_BYTES = 1024

const clampInt = (value: number | undefined, fallback: number, min: number, max: number): number => {
  if (value === undefined || Number.isFinite(value) === false) return fallback
  return Math.min(Math.max(Math.trunc(value), min), max)
}

/**
 * Applies defaults and clamps. Safe to call repeatedly — resolving an already
 * resolved options object is a no-op.
 */
export const resolveOptions = <TSyncPayload = Schema.Json>(
  options: LiveStoreSyncActorOptions<TSyncPayload>,
): ResolvedOptions<TSyncPayload> => ({
  ...options,
  pullPageSize: clampInt(options.pullPageSize, DEFAULT_PULL_PAGE_SIZE, 1, MAX_PULL_EVENTS_PER_MESSAGE),
  maxPushEventsPerRequest: clampInt(
    options.maxPushEventsPerRequest,
    MAX_PUSH_EVENTS_PER_REQUEST,
    1,
    MAX_PUSH_EVENTS_PER_REQUEST,
  ),
  maxMessageBytes: clampInt(
    options.maxMessageBytes,
    MAX_TRANSPORT_PAYLOAD_BYTES,
    MIN_MESSAGE_BYTES,
    Number.MAX_SAFE_INTEGER,
  ),
  testingEnabled: options.testing?.enabled === true,
  adminSecret: options.admin?.secret === undefined || options.admin.secret === '' ? undefined : options.admin.secret,
})
