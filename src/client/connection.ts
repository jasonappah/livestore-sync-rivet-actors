/**
 * ConnectionManager: wraps one rivetkit `ActorConn` per store behind the
 * transport-agnostic {@link Connection} contract.
 *
 * Responsibilities
 * - `createClient` / `client.dispose()` bound to the surrounding `Scope`.
 * - The `ActorConn` is created **lazily** on first use: `handle.connect()`
 *   starts connecting immediately and LiveStore's conformance suite expects
 *   `isConnected === false` before the backend is used.
 * - rivetkit callbacks (`onStatusChange`, `on('pull')`, `onError`) are bridged
 *   into `SubscriptionRef` / `PubSub` synchronously with the runtime captured
 *   at construction time (`Effect.context` + `Effect.runSyncWith`), so status
 *   updates are observable in the same tick rivetkit reports them.
 * - Actions issued while not `connected` are queued by rivetkit and never
 *   rejected, so `action` races the promise against `isConnected → false`.
 * - When rivetkit gives up (`idle` while we did not dispose — e.g.
 *   `connection_open_failed`) an idle watchdog recreates the conn with the
 *   same params after an exponential, jittered backoff.
 *
 * Only `rivetkit/client` may be imported here (browser export condition).
 */

import { IsOfflineError } from '@livestore/common'
import { Duration, Effect, PubSub, Scope, Stream, SubscriptionRef } from '@livestore/utils/effect'
import {
  type ActorConnRaw,
  type ActorConnStatus,
  type ClientRaw,
  createClient as createRivetClient,
} from 'rivetkit/client'

import { LIVE_PULL_EVENT } from '../common/mod.ts'
import type { ResolvedRivetSyncOptions } from './options.ts'
import { type Connection, type ConnStatus, RawActionFailure } from './types.ts'

// ---------------------------------------------------------------------------
// Structural view of the rivetkit client (so tests can substitute a fake)
// ---------------------------------------------------------------------------

/** Subset of rivetkit's `ActorConnRaw` used by the manager. */
export interface RivetConnLike {
  readonly connStatus: ConnStatus
  on(event: string, cb: (...args: unknown[]) => void): () => void
  onStatusChange(cb: (status: ConnStatus) => void): () => void
  onError(cb: (error: unknown) => void): () => void
  action(opts: { readonly name: string; readonly args: unknown[]; readonly signal?: AbortSignal }): Promise<unknown>
  dispose(): Promise<void>
}

/** Subset of rivetkit's `ActorHandle`, with `connect` already widened to the raw conn. */
export interface RivetHandleLike {
  connect(params?: unknown): RivetConnLike
}

/** Subset of rivetkit's `ClientRaw`. */
export interface RivetClientLike {
  getOrCreate(name: string, key: string | string[]): RivetHandleLike
  dispose(): Promise<void>
}

export interface CreateRivetClientOptions {
  readonly endpoint: string
  readonly token?: string | undefined
  readonly namespace?: string | undefined
}

export type CreateRivetClient = (options: CreateRivetClientOptions) => RivetClientLike

// Type-level checks: the real rivetkit classes must satisfy the `*Like`
// shapes (the `ActorConn<AD>` returned by `handle.connect` omits `on`/`once`
// from its public type, hence the cast to `ActorConnRaw` in the adapter).
type _AssertConnRawIsLike = ActorConnRaw extends RivetConnLike ? true : never
type _AssertStatusesMatch = [ActorConnStatus] extends [ConnStatus]
  ? [ConnStatus] extends [ActorConnStatus]
    ? true
    : never
  : never
type _AssertClientHasDispose = ClientRaw extends { dispose(): Promise<void> } ? true : never
const _typeChecks: [_AssertConnRawIsLike, _AssertStatusesMatch, _AssertClientHasDispose] = [true, true, true]
void _typeChecks

/** Default `createClient`: rivetkit's browser-safe client, adapted to the `*Like` shapes. */
export const defaultCreateRivetClient: CreateRivetClient = (options) => {
  const client: ClientRaw = createRivetClient({
    endpoint: options.endpoint,
    ...(options.token === undefined ? {} : { token: options.token }),
    ...(options.namespace === undefined ? {} : { namespace: options.namespace }),
  })
  return {
    getOrCreate: (name, key) => {
      const handle = client.getOrCreate(name, key)
      return {
        // The typed `ActorConn` hides `on`/`once`; the runtime object is an `ActorConnRaw`.
        connect: (params) => handle.connect(params) as unknown as ActorConnRaw,
      }
    },
    dispose: () => client.dispose(),
  }
}

// ---------------------------------------------------------------------------
// Manager
// ---------------------------------------------------------------------------

export interface MakeConnectionArgs {
  readonly options: ResolvedRivetSyncOptions
  readonly storeId: string
  /** Already JSON-encoded `ConnParams`, passed verbatim to `handle.connect(params)`. */
  readonly connParams: unknown
  /** Test seam; defaults to rivetkit's `createClient`. */
  readonly createClient?: CreateRivetClient
}

interface ActiveConn {
  readonly conn: RivetConnLike
  readonly detach: () => void
}

/** `min(maxDelay, baseDelay · 2^attempt) · U[0.8, 1.2]`, clamped to `maxDelay`. Exported for tests. */
export const backoffDelay = (
  attempt: number,
  reconnect: ResolvedRivetSyncOptions['reconnect'],
  random: () => number = Math.random,
): Duration.Duration => {
  const base = Duration.toMillis(reconnect.baseDelay)
  const max = Duration.toMillis(reconnect.maxDelay)
  const raw = Math.min(max, base * 2 ** Math.min(attempt, 30))
  const jittered = raw * (0.8 + 0.4 * random())
  return Duration.millis(Math.min(max, jittered))
}

export const makeConnection = (args: MakeConnectionArgs): Effect.Effect<Connection, never, Scope.Scope> =>
  Effect.gen(function* () {
    const { options, storeId, connParams } = args
    const createClient = args.createClient ?? defaultCreateRivetClient

    const status = yield* SubscriptionRef.make<ConnStatus>('idle')
    const isConnected = yield* SubscriptionRef.make(false)
    const pullEvents = yield* PubSub.unbounded<unknown>()

    // Captured runtime for rivetkit callbacks. Status/pull updates are pure
    // in-memory operations, so they run synchronously; anything else is forked.
    const context = yield* Effect.context<never>()
    const runSync = Effect.runSyncWith(context)
    const runFork = Effect.runForkWith(context)

    const client = yield* Effect.acquireRelease(
      Effect.sync(() => createClient({ endpoint: options.endpoint, token: options.token, namespace: options.namespace })),
      (c) =>
        Effect.promise(() => c.dispose()).pipe(
          Effect.catchCause((cause) => Effect.logDebug('rivet-sync: client.dispose failed', cause)),
        ),
    )
    const handle = client.getOrCreate(options.actorName, [storeId])

    let active: ActiveConn | undefined
    let disposing = false

    const setStatus = (next: ConnStatus): void => {
      runSync(
        Effect.andThen(SubscriptionRef.set(status, next), SubscriptionRef.set(isConnected, next === 'connected')),
      )
    }

    const createConn = (): ActiveConn => {
      const conn = handle.connect(connParams)
      const unsubscribers = [
        conn.onStatusChange((next) => setStatus(next)),
        conn.on(LIVE_PULL_EVENT, (...eventArgs) => {
          runSync(PubSub.publish(pullEvents, eventArgs[0]))
        }),
        conn.onError((error) => {
          runFork(Effect.logDebug('rivet-sync: connection error', error))
        }),
      ]
      const detach = () => {
        for (const unsubscribe of unsubscribers) unsubscribe()
      }
      // `handle.connect` starts connecting synchronously (`connecting`).
      setStatus(conn.connStatus)
      return { conn, detach }
    }

    /** Creates the conn on first use; afterwards returns the current one. Synchronous, so no double-create race. */
    const ensureConn: Effect.Effect<RivetConnLike> = Effect.sync(() => {
      if (active === undefined) active = createConn()
      return active.conn
    })

    const disposeActive: Effect.Effect<void> = Effect.suspend(() => {
      const current = active
      active = undefined
      if (current === undefined) return Effect.void
      current.detach()
      return Effect.promise(() => current.conn.dispose()).pipe(
        Effect.catchCause((cause) => Effect.logDebug('rivet-sync: conn.dispose failed', cause)),
      )
    })

    // Disposes the conn before the client (finalizers run in reverse order).
    yield* Effect.addFinalizer(() =>
      Effect.suspend(() => {
        disposing = true
        return disposeActive
      }),
    )

    // Idle watchdog: rivetkit only reports `idle` when it stopped trying
    // (dispose or `connection_open_failed`). If we did not dispose, recreate.
    let attempt = 0
    const watchdog = SubscriptionRef.changes(status).pipe(
      Stream.runForEach((next) =>
        Effect.suspend(() => {
          if (next === 'connected') {
            attempt = 0
            return Effect.void
          }
          if (next !== 'idle' || disposing || active === undefined) return Effect.void
          const delay = backoffDelay(attempt, options.reconnect)
          attempt += 1
          return Effect.logDebug('rivet-sync: connection idle, recreating', { storeId, delay }).pipe(
            Effect.andThen(Effect.sleep(delay)),
            Effect.andThen(
              Effect.suspend(() => {
                // The scope may have started closing while we slept.
                if (disposing || active === undefined) return Effect.void
                return disposeActive.pipe(
                  Effect.andThen(
                    Effect.sync(() => {
                      active = createConn()
                    }),
                  ),
                )
              }),
            ),
          )
        }),
      ),
    )
    yield* Effect.forkScoped(watchdog)

    const awaitConnected: Effect.Effect<void, IsOfflineError> = ensureConn.pipe(
      Effect.andThen(SubscriptionRef.waitUntil(isConnected, (connected) => connected === true)),
      Effect.asVoid,
      Effect.timeoutOrElse({
        duration: options.connectTimeout,
        orElse: () =>
          Effect.fail(
            new IsOfflineError({
              cause: new Error(`rivet-sync: not connected within ${Duration.toMillis(options.connectTimeout)}ms`),
            }),
          ),
      }),
    )

    const action = (name: string, encodedPayload: unknown): Effect.Effect<unknown, IsOfflineError | RawActionFailure> =>
      awaitConnected.pipe(
        Effect.andThen(ensureConn),
        Effect.flatMap((conn) => {
          const send = Effect.tryPromise({
            try: (signal) => conn.action({ name, args: [encodedPayload], signal }),
            catch: (cause) => new RawActionFailure({ cause, statusAtFailure: conn.connStatus }),
          })
          // rivetkit queues (never rejects) actions while offline; fail fast instead.
          const dropped = SubscriptionRef.waitUntil(isConnected, (connected) => connected === false).pipe(
            Effect.andThen(Effect.fail(new IsOfflineError({ cause: new Error('connection lost during action') }))),
          )
          return Effect.raceFirst(send, dropped)
        }),
      )

    return { status, isConnected, pullEvents, action, awaitConnected } satisfies Connection
  })
