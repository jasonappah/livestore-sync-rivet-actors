/**
 * The `LiveStoreSync` Rivet actor: binds the contract in `actions.ts` to the
 * pull / push / ping handlers through `@rivetkit/effect`'s `Actor.toLayer`.
 *
 * One actor instance exists per store, addressed as
 * `getOrCreate(ACTOR_NAME, [storeId])`.
 *
 * Concurrency & liveness notes
 * - An actor instance runs on a single thread and every action handler runs
 *   as an Effect inside it. Push admission (head check → durable append →
 *   in-memory head → live fan-out) is additionally serialized by the
 *   per-wake `pushSemaphore`, so concurrent pushes for the same parent
 *   resolve to exactly one winner and the losers see `ServerAheadError`
 *   *after* the winner's events were already fanned out to them.
 * - All per-wake state (`backendId`, `headRef`, connection-auth cache) is
 *   rebuilt from SQLite in `makeStoreCtx` on every wake. Rivet puts the
 *   actor to sleep after `options.actor.sleepTimeout` of inactivity even
 *   while connections are open: action/event WebSockets are hibernatable,
 *   the engine keeps them up, and the next action wakes the actor with the
 *   same connections (same `conn.id`, same `conn.params`) back in `c.conns`.
 *   Nothing below relies on in-memory state surviving a sleep; the auth
 *   cache is simply rebuilt lazily on the first fan-out after wake.
 * - Live-pull events are only ever sent to connections whose `conn.params`
 *   passed `validatePayload`; the verdict is cached per connection id for
 *   the wake and rejected connections are disconnected once.
 * - The wake effect must not fail (`toLayer` requires `never`): storage
 *   errors during wake are turned into defects, which makes the wake fail
 *   loudly instead of serving a half-initialised store.
 *
 * NOTE: `@rivetkit/effect@2.3.17` targets `effect@4.0.0-beta.66`; consumers
 * of `./server` need the pnpm patch described in `docs/spike-results.md`.
 */

import { Effect, type Layer, type Schema } from '@livestore/utils/effect'
import { Actor, type Registry } from '@rivetkit/effect'
import { db } from 'rivetkit/db'

import { LiveStoreSync } from './actions.ts'
import { makeAdminInfo, makeAdminReset } from './admin.ts'
import type { LiveStoreSyncActorOptions } from './options.ts'
import { makePing } from './ping.ts'
import { makePull } from './pull.ts'
import { makePush } from './push.ts'
import { migrate } from './sqlite.ts'
import { defaultLog, makeStoreCtx, type StoreCtx, type WakeInfo } from './store-ctx.ts'
import { makeTestDisconnectAll, makeTestInfo, makeTestSleep } from './test-actions.ts'

/** Default display name forwarded to `Rivetkit.actor` (`options.name`). */
export const DEFAULT_ACTOR_DISPLAY_NAME = 'LiveStore Sync'

/** Default display icon forwarded to `Rivetkit.actor` (`options.icon`). */
export const DEFAULT_ACTOR_ICON = 'database'

/**
 * Adapts rivetkit's per-actor pino logger (`c.log`) to the `(level, msg,
 * data)` shape the handlers use. pino accepts a single merged object with a
 * `msg` key, which is also rivetkit's own logging convention. Falls back to
 * the console logger if the actor logger is missing a level method.
 */
const makeActorLog = (actorLog: unknown): StoreCtx['log'] => {
  const logger = actorLog as Record<string, unknown> | null | undefined
  return (level, msg, data) => {
    const method = logger?.[level]
    if (typeof method !== 'function') return defaultLog(level, msg, data)
    try {
      method.call(logger, { msg, ...data })
    } catch (cause) {
      defaultLog('warn', 'actor logger threw', { cause: cause instanceof Error ? cause.message : String(cause) })
      defaultLog(level, msg, data)
    }
  }
}

/**
 * Wake bookkeeping per store for this runner process, keyed by `storeId`.
 * Diagnostic only (surfaced through the test-only `TestInfo` action); it is
 * deliberately not persisted, so a `count` above 1 proves the actor slept and
 * woke again *in this process*.
 */
const wakeLog = new Map<string, { count: number; sleptAt: number | null }>()

const beginWake = (storeId: string): WakeInfo => {
  const previous = wakeLog.get(storeId)
  const wake: WakeInfo = {
    count: (previous?.count ?? 0) + 1,
    at: Date.now(),
    previousSleptAt: previous?.sleptAt ?? null,
  }
  wakeLog.set(storeId, { count: wake.count, sleptAt: null })
  return wake
}

const endWake = (storeId: string): void => {
  const entry = wakeLog.get(storeId)
  if (entry !== undefined) entry.sleptAt = Date.now()
}

/**
 * Builds the `LiveStoreSync` actor layer. Register it with
 * `Registry.serve(...)` (long-running server) or `Registry.test` (in-process
 * engine for tests):
 *
 * ```ts
 * const ActorsLayer = makeLiveStoreSyncActor({ validatePayload })
 * const MainLayer = Registry.serve(ActorsLayer).pipe(
 *   Layer.provide(Registry.layer(registryOptions({ endpoint: process.env.RIVET_ENDPOINT }))),
 * )
 * Layer.launch(MainLayer).pipe(NodeRuntime.runMain)
 * ```
 *
 * The explicit `Layer.Layer<never, never, Registry.Registry>` annotation is
 * required for `declaration: true` builds (see `docs/spike-results.md`).
 */
export const makeLiveStoreSyncActor = <TSyncPayload = Schema.Json>(
  options: LiveStoreSyncActorOptions<TSyncPayload> = {},
): Layer.Layer<never, never, Registry.Registry> =>
  LiveStoreSync.toLayer(
    Effect.fnUntraced(function* ({ rawRivetkitContext: c }) {
      const address = yield* Actor.CurrentAddress

      const wakeKey = typeof address.key === 'string' ? address.key : (address.key[0] ?? JSON.stringify(address.key))

      const ctx = yield* makeStoreCtx({
        key: address.key,
        db: c.db,
        conns: () => c.conns.values(),
        log: makeActorLog(c.log),
        options,
        wake: beginWake(wakeKey),
      }).pipe(Effect.orDie)

      ctx.log('debug', 'LiveStoreSync actor awake', {
        storeId: ctx.storeId,
        backendId: ctx.backendId,
        head: ctx.headRef.current,
        actorId: address.actorId,
        wakeCount: ctx.wake.count,
        conns: c.conns.size,
      })

      // The wake scope closes on sleep / destroy (`onSleep` / `onDestroy`).
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          endWake(wakeKey)
          ctx.log('debug', 'LiveStoreSync actor sleeping', {
            storeId: ctx.storeId,
            head: ctx.headRef.current,
            wakeCount: ctx.wake.count,
            conns: c.conns.size,
          })
        }),
      )

      const pull = makePull(ctx)
      const push = makePush(ctx)
      const ping = makePing(ctx)
      const adminInfo = makeAdminInfo(ctx)
      const adminReset = makeAdminReset(ctx)
      const testDisconnectAll = makeTestDisconnectAll(ctx)
      const testInfo = makeTestInfo(ctx)
      const testSleep = makeTestSleep(ctx, Effect.sync(() => c.sleep()))

      return LiveStoreSync.of({
        Pull: ({ payload }) => pull(payload),
        Push: ({ payload }) => push(payload),
        Ping: ({ payload }) => ping(payload),
        AdminInfo: ({ payload }) => adminInfo(payload),
        AdminReset: ({ payload }) => adminReset(payload),
        TestDisconnectAll: ({ payload }) => testDisconnectAll(payload),
        TestInfo: ({ payload }) => testInfo(payload),
        TestSleep: ({ payload }) => testSleep(payload),
      })
    }),
    {
      // rivetkit runtime options (`sleepTimeout`, `sleepGracePeriod`, …) are
      // forwarded by the patched `@rivetkit/effect` (upstream drops all but
      // `name` / `icon`).
      ...options.actor,
      db: db({ onMigrate: migrate }),
      name: options.name ?? DEFAULT_ACTOR_DISPLAY_NAME,
      icon: options.icon ?? DEFAULT_ACTOR_ICON,
    },
  )

// -----------------------------------------------------------------------------
// Registry options
// -----------------------------------------------------------------------------

/**
 * Recommended `maxIncomingMessageSize` (client → actor, bytes) for the
 * registry hosting the sync actor: 4 MiB.
 *
 * rivetkit's default is 64 KiB. An oversize push makes the engine close the
 * WebSocket (`message.incoming_too_long`), so the client-side `maxPushBytes`
 * default is tuned to fit the default; raising the limit here lets clients
 * use larger chunks (`maxPushBytes: 900_000`, matching the server's
 * `maxMessageBytes`).
 */
export const RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE = 4 * 1024 * 1024

/** Inputs accepted by {@link registryOptions}. `undefined` values are dropped. */
export interface RegistryOptionsInput {
  /** Rivet engine endpoint, e.g. `http://127.0.0.1:6420`. Omit to use rivetkit's default. */
  readonly endpoint?: string | undefined
  readonly token?: string | undefined
  readonly namespace?: string | undefined
  /** Suppress rivetkit's startup banner. */
  readonly noWelcome?: boolean | undefined
  /** @default RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE */
  readonly maxIncomingMessageSize?: number | undefined
  /**
   * Actor → client limit (bytes). rivetkit's default is 1 MiB, which already
   * fits the server's default 900 KB pull pages; raise it together with
   * `maxMessageBytes` if you need larger pages.
   */
  readonly maxOutgoingMessageSize?: number | undefined
}

/**
 * Builds the options for `Registry.layer(...)` with the message-size limit
 * the sync actor needs.
 *
 * `Registry.Options` only *types* `endpoint | token | namespace | noWelcome |
 * sqlite`, but `@rivetkit/effect` spreads the whole object into
 * `Rivetkit.setup(...)`, so registry-level knobs such as
 * `maxIncomingMessageSize` pass through at runtime (verified in
 * `docs/spike-results.md` §B4) — hence the cast.
 *
 * ```ts
 * Registry.layer(registryOptions({ endpoint: process.env.RIVET_ENDPOINT }))
 * ```
 */
export const registryOptions = (input: RegistryOptionsInput = {}): Registry.Options => {
  const out: Record<string, unknown> = {
    maxIncomingMessageSize: input.maxIncomingMessageSize ?? RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  }
  // Never materialise `undefined` keys: `Registry.test` checks
  // `options.endpoint === undefined` and zod would otherwise see the key.
  if (input.endpoint !== undefined) out.endpoint = input.endpoint
  if (input.token !== undefined) out.token = input.token
  if (input.namespace !== undefined) out.namespace = input.namespace
  if (input.noWelcome !== undefined) out.noWelcome = input.noWelcome
  if (input.maxOutgoingMessageSize !== undefined) out.maxOutgoingMessageSize = input.maxOutgoingMessageSize
  return out as Registry.Options
}
