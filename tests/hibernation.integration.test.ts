/**
 * Sleep / hibernation behaviour of the `LiveStoreSync` actor, end to end
 * through `makeRivetSync`.
 *
 * Boots its own actor layer with a short `sleepTimeout` (1.5 s) in its own
 * namespace (`livestore-hibernation`) so it never shares actors with the
 * conformance harness runner (`livestore-conformance`): the engine routes an
 * actor to any runner registered for its name in a namespace, and two runners
 * with different `sleepTimeout`s in one namespace would make these tests
 * flaky.
 *
 * What is asserted (empirical findings in `docs/spike-results.md`, Part C):
 * 1. rivetkit 2.3.17 does **not** put the actor to sleep on its own while an
 *    action/event WebSocket connection is open — even though such connections
 *    are hibernatable (`conn.isHibernatable === true`). This test pins that
 *    behaviour so the docs get updated when upstream changes it;
 * 2. once the last connection is gone the actor sleeps after `sleepTimeout`,
 *    and the next action wakes it with head / `backendId` rebuilt from SQLite;
 * 3. when the actor *does* sleep with a connection open (forced through the
 *    test-only `TestSleep` action, i.e. rivetkit's `c.sleep()`), the
 *    connection hibernates: the client observes no `isConnected` transition,
 *    the next action wakes the actor with the same connection (same id, same
 *    `conn.params`) back in `c.conns`, and a push from another client is
 *    delivered to it over that hibernated socket;
 * 4. the hibernated client's own push after the wake is admitted against the
 *    head reloaded from SQLite and a stale push is still rejected. Upstream
 *    intermittently refuses the *first message after the wake* on such a
 *    socket (`1008 ws.message_index_skip`: the persisted message index is one
 *    behind); the client then sees a transport blip (`IsOfflineError`, one
 *    reconnect) and the retried push lands — the test accepts both outcomes.
 *
 * Needs a Rivet engine: run through `pnpm test:conformance`. The engine
 * outlives vitest and persists actor state, so every test uses a unique
 * `storeId`.
 */

import type { SyncBackend } from '@livestore/common'
import { EventFactory } from '@livestore/common/testing'
import {
  Context,
  Deferred,
  Duration,
  Effect,
  FetchHttpClient,
  type HttpClient,
  KeyValueStore,
  Layer,
  ManagedRuntime,
  Option,
  Schedule,
  type Scope,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { Registry } from '@rivetkit/effect'
import { createClient } from 'rivetkit/client'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { makeRivetSync, type RivetSyncOptions } from '../src/client/mod.ts'
import {
  ACTION_TEST_INFO,
  ACTION_TEST_SLEEP,
  ACTOR_NAME,
  decodeTestInfoResponse,
  decodeTestSleepResponse,
  encodeTestInfoRequest,
  encodeTestSleepRequest,
  type TestInfoResponse,
  type TestSleepResponse,
} from '../src/common/mod.ts'
import { makeLiveStoreSyncActor, RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE, registryOptions } from '../src/server/mod.ts'
import { bootClock, engineTargetFromEnv, preflightLayer, waitForRunner } from './harness/engine.ts'
import { makeEventFactory } from './harness/events.ts'
import { HOOK_TIMEOUT_MS } from './harness/types.ts'

// -----------------------------------------------------------------------------
// Boot (mirrors tests/harness/providers/rivet.ts, different namespace)
// -----------------------------------------------------------------------------

const NAMESPACE = 'livestore-hibernation'
const TARGET = engineTargetFromEnv(NAMESPACE)
const ENDPOINT = TARGET.endpoint
const TOKEN = TARGET.token

/** Short enough to sleep several times per test, long enough to stay clear of the runner's own bookkeeping. */
const SLEEP_TIMEOUT_MS = 1_500

/** Idle window that must contain at least one sleep whenever sleeping is possible. */
const IDLE_MS = SLEEP_TIMEOUT_MS * 4

/**
 * Quiet period before a forced sleep, longer than rivetkit's
 * `stateSaveInterval` (1 s) so the persisted connection state is not racing
 * the sleep. (It does not avoid the `ws.message_index_skip` off-by-one, see
 * test 4 — that one is independent of timing.)
 */
const SETTLE_BEFORE_SLEEP = Duration.millis(1_500)

const HARNESS_CLIENT_ID = 'hibernation-harness'

const ActorsLayer = makeLiveStoreSyncActor({
  testing: { enabled: true },
  actor: { sleepTimeout: SLEEP_TIMEOUT_MS },
  // Runs for each rehydrated connection on the first fan-out after a wake: a
  // connection whose params fail is disconnected, so if hibernated
  // connections came back without their params the idle client would be
  // kicked instead of receiving the event.
  validatePayload: (payload, { clientId }) => {
    if (typeof clientId !== 'string' || clientId.length === 0) throw new Error('missing clientId')
    if (payload !== undefined && (payload as { reject?: unknown }).reject === true) throw new Error('rejected')
  },
})

const RegistryLayer = Registry.layer(
  registryOptions({
    noWelcome: true,
    endpoint: process.env.RIVET_ENDPOINT,
    token: TOKEN,
    namespace: NAMESPACE,
    maxIncomingMessageSize: RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  }),
)

const EngineLayer = Registry.test.pipe(
  Layer.provide(ActorsLayer),
  // Creates the namespace before the runner starts whenever the engine is already up.
  Layer.provide(preflightLayer(TARGET)),
  Layer.provideMerge(RegistryLayer),
)

const clientOptions = {
  endpoint: ENDPOINT,
  ...(TOKEN === undefined ? {} : { token: TOKEN }),
  namespace: NAMESPACE,
} satisfies Partial<RivetSyncOptions>

class Harness extends Context.Service<
  Harness,
  {
    /** `TestInfo` through a stateless handle (HTTP): like any action, it wakes a sleeping actor. */
    readonly info: (storeId: string) => Effect.Effect<TestInfoResponse>
    /** `TestSleep` through a stateless handle: rivetkit's `c.sleep()`, regardless of `sleepTimeout`. */
    readonly sleep: (storeId: string) => Effect.Effect<TestSleepResponse>
  }
>()('HibernationHarness') {}

const HarnessLayer = Layer.effect(
  Harness,
  Effect.gen(function* () {
    yield* Registry.Registry

    const rawClient = createClient(clientOptions)
    yield* Effect.addFinalizer(() => Effect.promise(() => rawClient.dispose()).pipe(Effect.ignore))

    const info = (storeId: string) =>
      Effect.promise(() =>
        rawClient.getOrCreate(ACTOR_NAME, [storeId]).action({
          name: ACTION_TEST_INFO,
          args: [encodeTestInfoRequest({ storeId, clientId: HARNESS_CLIENT_ID })],
        }),
      ).pipe(Effect.map(decodeTestInfoResponse))

    const sleep = (storeId: string) =>
      Effect.promise(() =>
        rawClient.getOrCreate(ACTOR_NAME, [storeId]).action({
          name: ACTION_TEST_SLEEP,
          args: [encodeTestSleepRequest({ storeId, clientId: HARNESS_CLIENT_ID })],
        }),
      ).pipe(Effect.map(decodeTestSleepResponse))

    // Engine healthy -> namespace -> our runner listed -> an action routes (on a throwaway store).
    yield* waitForRunner(TARGET, {
      since: bootClock,
      probe: Effect.suspend(() => {
        const storeId = `hib-ready-${nanoid(8)}`
        return Effect.tryPromise(() =>
          rawClient.getOrCreate(ACTOR_NAME, [storeId]).action({
            name: ACTION_TEST_INFO,
            args: [encodeTestInfoRequest({ storeId, clientId: HARNESS_CLIENT_ID })],
          }),
        )
      }),
    }).pipe(Effect.orDie)

    return { info, sleep }
  }),
).pipe(Layer.provide(EngineLayer))

const runtime = ManagedRuntime.make(HarnessLayer.pipe(Layer.orDie))
let harness: typeof Harness.Service

beforeAll(async () => {
  harness = Context.get(await runtime.context(), Harness)
}, HOOK_TIMEOUT_MS)

afterAll(async () => {
  await runtime.dispose()
}, HOOK_TIMEOUT_MS)

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

const storeIdFor = (label: string) => `hib-${label}-${nanoid(8)}`

const seqNumsOf = (item: SyncBackend.PullResItem<unknown>): ReadonlyArray<number> =>
  item.batch.map((b) => b.eventEncoded.seqNum as number)

const factoryFor = (clientId: string, options?: { readonly startSeq?: number; readonly parent?: number }) =>
  makeEventFactory({
    client: EventFactory.clientIdentity(clientId, `${clientId}-session`),
    startSeq: options?.startSeq ?? 1,
    initialParent: options?.parent ?? 'root',
  })

interface TestClient {
  readonly backend: SyncBackend.SyncBackend<any>
  /** Every `isConnected` value observed since construction (the initial `false` included). */
  readonly connectedLog: ReadonlyArray<{ readonly at: number; readonly connected: boolean }>
}

/**
 * One simulated LiveStore client over its own `KeyValueStore`. The liveness
 * ping is disabled: it is an action, so it would count as activity and
 * muddy every timing below (it makes no difference to the findings — an open
 * connection already keeps the actor awake).
 */
const makeClient = (args: {
  readonly storeId: string
  readonly clientId: string
}): Effect.Effect<TestClient, never, Scope.Scope | HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const kvContext = yield* Layer.build(Layer.fresh(KeyValueStore.layerMemory))
    const kv = Context.get(kvContext, KeyValueStore.KeyValueStore)

    const backend = yield* makeRivetSync({ ...clientOptions, ping: { enabled: false } })({
      storeId: args.storeId,
      clientId: args.clientId,
      payload: undefined,
    }).pipe(Effect.provideService(KeyValueStore.KeyValueStore, kv), Effect.orDie)

    const connectedLog: Array<{ at: number; connected: boolean }> = []
    yield* SubscriptionRef.changes(backend.isConnected).pipe(
      Stream.runForEach((connected) => Effect.sync(() => connectedLog.push({ at: Date.now(), connected }))),
      Effect.forkScoped,
    )

    return { backend, connectedLog }
  })

/** Number of `→ false` transitions in a client's `isConnected` log after the initial value. */
const disconnects = (client: TestClient): number =>
  client.connectedLog.filter((entry, index) => index > 0 && entry.connected === false).length

/** Forks a live pull from the start of the eventlog; the deferred resolves with the first item carrying `seqNum`. */
const awaitLiveSeqNum = (
  client: TestClient,
  seqNum: number,
): Effect.Effect<Deferred.Deferred<SyncBackend.PullResItem<unknown>, unknown>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const received = yield* Deferred.make<SyncBackend.PullResItem<unknown>, unknown>()
    yield* client.backend.pull(Option.none(), { live: true }).pipe(
      Stream.filter((item) => seqNumsOf(item).includes(seqNum)),
      Stream.runFirstUnsafe,
      (self) => Deferred.complete(received, self),
      Effect.forkScoped,
    )
    return received
  })

const connOf = (info: TestInfoResponse, clientId: string) => info.conns.find((conn) => conn.clientId === clientId)

const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope | HttpClient.HttpClient>): Promise<A> =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.provide(FetchHttpClient.layer)))

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

describe('LiveStoreSync actor sleep & hibernation (sleepTimeout 1.5 s)', { timeout: 90_000 }, () => {
  it('does not sleep on its own while an idle client stays connected (rivetkit 2.3.17; update the docs if this starts failing)', () =>
    run(
      Effect.gen(function* () {
        const storeId = storeIdFor('idle')
        const a = yield* makeClient({ storeId, clientId: 'client-a' })

        // A live pull (catch-up + event subscription) is the idle state of a LiveStore leader.
        yield* awaitLiveSeqNum(a, 1)
        yield* a.backend.connect

        const before = yield* harness.info(storeId)
        const aConn = connOf(before, 'client-a')
        // The connection *is* hibernatable — the runtime just never fires the idle sleep while it is open.
        expect(aConn?.hibernatable).toBe(true)

        yield* Effect.sleep(Duration.millis(IDLE_MS))

        const after = yield* harness.info(storeId)
        expect(after.wakeCount).toBe(before.wakeCount)
        expect(after.previousSleptAt).toBeNull()
        expect(connOf(after, 'client-a')?.id).toBe(aConn?.id)
        expect(disconnects(a)).toBe(0)
        expect(yield* SubscriptionRef.get(a.backend.isConnected)).toBe(true)
      }),
    ))

  it('sleeps after sleepTimeout once the last connection is gone; the next action wakes it with head and backendId from SQLite', () =>
    run(
      Effect.gen(function* () {
        const storeId = storeIdFor('last-conn')

        const seeded = yield* Effect.scoped(
          Effect.gen(function* () {
            const a = yield* makeClient({ storeId, clientId: 'client-a' })
            yield* a.backend.push([
              factoryFor('client-a').todoCreated.next({ id: 'a-1', text: 'before sleep', completed: false }),
            ])
            return yield* harness.info(storeId)
          }),
        )
        // The scope closed: client-a's connection (and the harness' transient one) are gone.
        const closedAt = Date.now()
        expect(seeded.head).toBe(1)

        yield* Effect.sleep(Duration.millis(IDLE_MS))

        const after = yield* harness.info(storeId)
        expect(after.wakeCount).toBe(seeded.wakeCount + 1)
        expect(after.previousSleptAt).not.toBeNull()
        const sleptAfterCloseMs = after.previousSleptAt! - closedAt
        // Slept roughly one sleepTimeout after the last connection closed (the timer had
        // been running since the last action, hence the lower bound of 0).
        expect(sleptAfterCloseMs).toBeGreaterThanOrEqual(0)
        expect(sleptAfterCloseMs).toBeLessThan(IDLE_MS)
        expect(after.wokeAt).toBeGreaterThanOrEqual(closedAt + IDLE_MS - 50)
        // Per-wake state was rebuilt from SQLite.
        expect(after.head).toBe(1)
        expect(after.backendId).toBe(seeded.backendId)
      }),
    ))

  it('forced sleep: the idle live-pull connection hibernates and receives the fan-out of a push that wakes the actor', () =>
    run(
      Effect.gen(function* () {
        const storeId = storeIdFor('hibernate')
        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        // B connects lazily, i.e. only when it pushes — after the actor is asleep.
        const b = yield* makeClient({ storeId, clientId: 'client-b' })

        const aGets1 = yield* awaitLiveSeqNum(a, 1)
        yield* a.backend.connect
        const before = yield* harness.info(storeId)
        const aConnBefore = connOf(before, 'client-a')
        expect(aConnBefore?.hibernatable).toBe(true)

        yield* Effect.sleep(SETTLE_BEFORE_SLEEP)
        const sleepRequestedAt = Date.now()
        const sleepAck = yield* harness.sleep(storeId)
        expect(sleepAck.conns).toBeGreaterThanOrEqual(1)
        yield* Effect.sleep(Duration.seconds(2))

        // The client never noticed: no `isConnected` transition while the actor was asleep.
        expect(disconnects(a)).toBe(0)
        expect(yield* SubscriptionRef.get(a.backend.isConnected)).toBe(true)

        const pushedAt = Date.now()
        yield* b.backend.push([factoryFor('client-b').todoCreated.next({ id: 'b-1', text: 'after sleep', completed: false })])
        const received = yield* Deferred.await(aGets1).pipe(Effect.timeout(Duration.seconds(20)))

        const after = yield* harness.info(storeId)
        expect(after.wakeCount).toBe(before.wakeCount + 1)
        expect(after.previousSleptAt).not.toBeNull()
        expect(after.previousSleptAt!).toBeGreaterThanOrEqual(sleepRequestedAt - 5)
        expect(after.previousSleptAt!).toBeLessThan(pushedAt)
        expect(after.wokeAt).toBeGreaterThanOrEqual(pushedAt - 5)
        expect(after.head).toBe(1)

        // Same connection object on the new actor instance: same id, `conn.params` still decode.
        expect(connOf(after, 'client-a')?.id).toBe(aConnBefore?.id)
        expect(connOf(after, 'client-a')?.hibernatable).toBe(true)
        expect(connOf(after, 'client-b')).toBeDefined()

        // Delivered over the hibernated socket — not via a reconnect + re-catch-up.
        expect(seqNumsOf(received)).toEqual([1])
        expect(received.batch[0]!.eventEncoded.clientId).toBe('client-b')
        expect(disconnects(a)).toBe(0)
      }),
    ))

  it("forced sleep: the hibernated client's own push after the wake lands against the head reloaded from SQLite (directly, or after the one reconnect rivetkit 2.3.17's ws.message_index_skip can cause)", () =>
    run(
      Effect.gen(function* () {
        const storeId = storeIdFor('own-push')
        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        const factory = factoryFor('client-a')

        const aGets1 = yield* awaitLiveSeqNum(a, 1)
        const aGets2 = yield* awaitLiveSeqNum(a, 2)
        yield* a.backend.connect
        yield* a.backend.push([factory.todoCreated.next({ id: 'a-1', text: 'before sleep', completed: false })])
        yield* Deferred.await(aGets1).pipe(Effect.timeout(Duration.seconds(20)))
        const awake = yield* harness.info(storeId)
        expect(awake.head).toBe(1)

        yield* Effect.sleep(SETTLE_BEFORE_SLEEP)
        yield* harness.sleep(storeId)
        yield* Effect.sleep(Duration.seconds(2))
        expect(disconnects(a)).toBe(0)

        // Upstream (rivetkit 2.3.17) sometimes refuses the first message on a
        // hibernated socket whose persisted message index lags by one
        // (`1008 ws.message_index_skip`, see docs/spike-results.md C3). Then the
        // action fails as a transport blip (`IsOfflineError`, which LiveStore's
        // leader retries), rivetkit reconnects at once and the retry lands.
        // Either way the push must be admitted against the head reloaded from
        // SQLite, and the client must end up connected with at most one dip.
        const e2 = factory.todoCreated.next({ id: 'a-2', text: 'after sleep', completed: false })
        const firstAttempt = yield* a.backend.push([e2]).pipe(Effect.result)
        if (firstAttempt._tag === 'Failure') {
          expect(firstAttempt.failure._tag).toBe('IsOfflineError')
          yield* a.backend
            .push([e2])
            .pipe(Effect.retry({ while: (error) => error._tag === 'IsOfflineError', schedule: Schedule.spaced('250 millis') }))
        }
        // The live pull sees it too (over the hibernated socket, or via the re-catch-up after the reconnect).
        const received = yield* Deferred.await(aGets2).pipe(Effect.timeout(Duration.seconds(20)))
        expect(seqNumsOf(received)).toContain(2)

        const after = yield* harness.info(storeId)
        expect(after.wakeCount).toBe(awake.wakeCount + 1)
        expect(after.head).toBe(2)
        expect(after.backendId).toBe(awake.backendId)
        expect(connOf(after, 'client-a')).toBeDefined()
        if (firstAttempt._tag === 'Success') {
          // Same hibernated socket throughout.
          expect(disconnects(a)).toBe(0)
          expect(connOf(after, 'client-a')?.id).toBe(connOf(awake, 'client-a')?.id)
        } else {
          // rivetkit reconnected (occasionally more than once while the retry was in flight).
          expect(disconnects(a)).toBeGreaterThanOrEqual(1)
          expect(connOf(after, 'client-a')?.id).not.toBe(connOf(awake, 'client-a')?.id)
        }
        expect(yield* SubscriptionRef.get(a.backend.isConnected)).toBe(true)

        // A stale push (parent 0) is still rejected: the reloaded head is enforced.
        const stale = factoryFor('client-stale').todoCreated.next({ id: 's-1', text: 'stale', completed: false })
        const failure = yield* a.backend.push([stale]).pipe(Effect.flip)
        expect(failure._tag).toBe('ServerAheadError')

        // Durable and in order for a fresh client.
        const reader = yield* makeClient({ storeId, clientId: 'client-reader' })
        const items = yield* reader.backend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
        expect(items.flatMap(seqNumsOf)).toEqual([1, 2])
      }),
    ))
})
