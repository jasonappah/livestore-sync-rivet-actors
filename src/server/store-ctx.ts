/**
 * Per-wake actor state.
 *
 * Every handler (`pull.ts`, `push.ts`, `connections.ts`) is written against
 * the plain {@link StoreCtx} record built here rather than against rivetkit's
 * `ActorContext`, so the handlers are unit-testable without an engine and the
 * raw-rivetkit fallback described in the plan only has to swap `actor.ts`.
 *
 * Wake also reconciles persisted state, which closes the stale-head gap
 * `@livestore/sync-cf` documents: the head is `max(persisted head,
 * MAX(seqNum))`, so a crash between the eventlog insert and the context
 * update cannot make the actor hand out a head that is behind the log.
 */

import type { UnknownError } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import { Effect, type Schema, Semaphore } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'

import type { LiveStoreSyncActorOptions, ResolvedOptions } from './options.ts'
import { resolveOptions } from './options.ts'
import { makeSyncStorage, type RawAccessLike, type SyncStorage } from './sqlite.ts'

// -----------------------------------------------------------------------------
// Connections
// -----------------------------------------------------------------------------

/**
 * The part of rivetkit's `Conn` the sync actor uses. Declared structurally so
 * fan-out can be unit-tested with plain objects.
 */
export type RawConn = {
  readonly id: string
  /** Whatever the client passed to `handle.connect(params)`; validated lazily. */
  readonly params: unknown
  send(name: string, ...args: unknown[]): void
  disconnect(reason?: string): Promise<void>
}

/** Type-level proof that a real rivetkit `Conn` satisfies {@link RawConn}. */
const _connIsAssignable: RawConn = null as unknown as import('rivetkit').AnyConn
void _connIsAssignable

/**
 * Per-connection authorization verdict, cached for the lifetime of the wake.
 * `rejected` connections are disconnected once and never sent events.
 */
export type ConnAuthState = 'ok' | 'rejected'

// -----------------------------------------------------------------------------
// Store context
// -----------------------------------------------------------------------------

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface StoreCtx<TSyncPayload = Schema.Json> {
  /** The actor key's single element. */
  readonly storeId: string
  /**
   * Current backend id (reads {@link StoreCtx.backendIdRef}). Stable across
   * wakes; only `AdminReset` replaces it. Clients detect a reset store by
   * comparing it.
   */
  readonly backendId: string
  /** Mutable cell behind {@link StoreCtx.backendId}. Written only by `AdminReset`, under `pushSemaphore`. */
  readonly backendIdRef: { current: string }
  /** Highest `seqNum` the backend has accepted. Mutated under `pushSemaphore`. */
  readonly headRef: { current: EventSequenceNumber.Global.Type }
  /** Serializes push admission so the head check and the append are atomic. */
  readonly pushSemaphore: Semaphore.Semaphore
  readonly storage: SyncStorage
  /** Live view of the actor's connections (`() => c.conns.values()`). */
  readonly conns: () => Iterable<RawConn>
  readonly connAuth: Map<string, ConnAuthState>
  readonly options: ResolvedOptions<TSyncPayload>
  readonly log: (level: LogLevel, msg: string, data?: Record<string, unknown>) => void
  /** Diagnostic wake bookkeeping, see {@link WakeInfo}. */
  readonly wake: WakeInfo
}

/**
 * Per-process wake bookkeeping for one store's actor. Kept in module scope
 * by `actor.ts` (never persisted) and reported by the test-only `TestInfo`
 * action so tests can observe sleep/wake cycles and their timing.
 */
export interface WakeInfo {
  /** 1-based ordinal of this wake within the current runner process. */
  readonly count: number
  /** `Date.now()` when this wake started. */
  readonly at: number
  /** `Date.now()` when the previous wake's scope closed (sleep/destroy), `null` on the first wake. */
  readonly previousSleptAt: number | null
}

/** {@link WakeInfo} for a context whose caller does not track wakes. */
export const firstWake = (): WakeInfo => ({ count: 1, at: Date.now(), previousSleptAt: null })

const LOG_PREFIX = '[livestore-sync-rivet-actors]'

/** Console-backed default: quiet at `debug`, routed by level otherwise. */
export const defaultLog: StoreCtx['log'] = (level, msg, data) => {
  if (level === 'debug') return
  const line = `${LOG_PREFIX} ${msg}`
  if (level === 'error') console.error(line, data ?? {})
  else if (level === 'warn') console.warn(line, data ?? {})
  else console.info(line, data ?? {})
}

/**
 * The actor key must address exactly one store. Anything else is a
 * programming error at the call site (`getOrCreate(ACTOR_NAME, [storeId])`),
 * not something a client can trigger, so it dies rather than failing.
 */
const resolveStoreId = (key: ReadonlyArray<string> | string): Effect.Effect<string> => {
  if (typeof key === 'string') return Effect.succeed(key)
  const [storeId, ...rest] = key
  if (storeId === undefined || rest.length > 0) {
    return Effect.die(
      new Error(
        `${LOG_PREFIX} LiveStoreSync actor key must be a single-element array [storeId], received ${JSON.stringify(key)}`,
      ),
    )
  }
  return Effect.succeed(storeId)
}

export const makeStoreCtx = <TSyncPayload = Schema.Json>(args: {
  readonly key: ReadonlyArray<string> | string
  readonly db: RawAccessLike
  readonly conns: () => Iterable<RawConn>
  readonly log?: StoreCtx['log']
  readonly options: LiveStoreSyncActorOptions<TSyncPayload>
  /** See {@link WakeInfo}. @default firstWake() */
  readonly wake?: WakeInfo
}): Effect.Effect<StoreCtx<TSyncPayload>, UnknownError> =>
  Effect.gen(function* () {
    const storeId = yield* resolveStoreId(args.key)
    const log = args.log ?? defaultLog
    const storage = makeSyncStorage(args.db)

    const row = yield* storage.loadContext(storeId)
    const maxSeqNum = yield* storage.maxSeqNum

    const backendId = row?.backendId ?? nanoid()
    const persistedHead = EventSequenceNumber.Global.make(row?.currentHead ?? EventSequenceNumber.Client.ROOT.global)
    const loggedHead = maxSeqNum ?? EventSequenceNumber.Client.ROOT.global
    const currentHead = EventSequenceNumber.Global.make(Math.max(persistedHead, loggedHead))

    if (row === undefined || row.currentHead !== currentHead || row.backendId !== backendId) {
      if (row !== undefined && row.currentHead !== currentHead) {
        log('warn', 'reconciled stale persisted head', {
          storeId,
          persistedHead: row.currentHead,
          loggedHead,
        })
      }
      yield* storage.saveContext({ storeId, currentHead, backendId })
    }

    const pushSemaphore = yield* Semaphore.make(1)
    const wake = args.wake ?? firstWake()

    log('debug', 'actor woke', { storeId, backendId, currentHead, wakeCount: wake.count })

    const backendIdRef = { current: backendId }

    return {
      storeId,
      get backendId() {
        return backendIdRef.current
      },
      backendIdRef,
      headRef: { current: currentHead },
      pushSemaphore,
      storage,
      conns: args.conns,
      connAuth: new Map<string, ConnAuthState>(),
      options: resolveOptions(args.options),
      log,
      wake,
    }
  })
