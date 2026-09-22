/**
 * Rivet-specific conformance cases — the behaviour LiveStore's ported suite
 * (`tests/sync-provider.test.ts`) does not cover because it only ever drives
 * a *single* backend instance with a single `KeyValueStore`.
 *
 * Everything here therefore builds **one backend per simulated client**, each
 * with its own `KeyValueStore.layerMemory` (built fresh, so two clients can
 * never share the backing `Map`), and asserts on how those clients see each
 * other through the actor:
 *
 * 1. two clients on one store, both directions;
 * 2. the `ServerAheadError` guarantee the LiveStore leader relies on;
 * 3. `backendId` is persisted in the KV and enforced by the server;
 * 4. a cursor without a persisted `backendId` is accepted;
 * 5. a push before any pull still learns the `backendId` (from the `PushAck`);
 * 6. `validatePayload` rejection (per-client, not per-store);
 * 7. reconnect after `TestDisconnectAll` re-catches up on missed events;
 * 8. a 500 KB event round-trips within the configured limits.
 *
 * Conventions are shared with `tests/sync-provider.test.ts` (the suite runtime
 * from `useProviderRuntime` in `tests/harness/runtime.ts`, `it.live`,
 * per-test `KeyValueStore.layerMemory`, `test-store-…-${nanoid}` ids,
 * `makeEventFactory`). Store ids must stay unique: the Rivet engine outlives
 * vitest and persists actor state.
 *
 * Needs a Rivet engine: run through `pnpm test:conformance`.
 */

import { SyncBackend } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import {
  Context,
  Duration,
  Effect,
  Fiber,
  KeyValueStore,
  Layer,
  Option,
  Result,
  type Schema,
  type Scope,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'
import { describe, expect, it } from '@effect/vitest'

import { makeEventFactory } from './harness/events.ts'
import { providerRegistry, selectedProviderKeys } from './harness/providers/registry.ts'
import { REJECTED_PAYLOAD } from './harness/providers/rivet.ts'
import { type RuntimeServices, useProviderRuntime } from './harness/runtime.ts'
import { SyncProviderImpl } from './harness/types.ts'

const providerLayers = selectedProviderKeys().map((key) => ({ key, ...providerRegistry[key] }))

/** Key `SyncBackend.makeBackendIdHelper` caches the backend id under. */
const BACKEND_ID_KEY = 'backendId'

const runFirstNonEmpty = <T, E, R>(stream: Stream.Stream<SyncBackend.PullResItem<T>, E, R>) =>
  stream.pipe(
    Stream.filter(({ batch }) => batch.length > 0),
    Stream.runFirstUnsafe,
  )

/** Resolves on the first pull item that carries `seqNum` (catch-up page or live fan-out). */
const runFirstWithSeqNum =
  (seqNum: number) =>
  <T, E, R>(stream: Stream.Stream<SyncBackend.PullResItem<T>, E, R>) =>
    stream.pipe(
      Stream.filter(({ batch }) => batch.some((item) => (item.eventEncoded.seqNum as number) === seqNum)),
      Stream.runFirstUnsafe,
    )

const seqNumsOf = (item: SyncBackend.PullResItem<unknown>): ReadonlyArray<number> =>
  item.batch.map((b) => b.eventEncoded.seqNum as number)

/** Narrows a `Result` to its failure, or fails the test with what came back instead. */
const expectFailure = <A, E>(result: Result.Result<A, E>): E => {
  if (Result.isFailure(result) === false) {
    throw new Error(`expected a failure, got a success: ${JSON.stringify(result.success)}`)
  }
  return result.failure
}

/** Give a forked live pull time to finish its catch-up and subscribe before the fan-out under test. */
const SETTLE = Duration.millis(750)

for (const { key, layer, name } of providerLayers) {
  describe.skipIf(key !== 'rivet')(`${name} sync provider (Rivet specifics)`, { timeout: 90_000 }, () => {
    const { withTestCtx, storeIdFor } = useProviderRuntime(layer)

    interface TestClient {
      readonly backend: SyncBackend.SyncBackend<any>
      readonly kv: KeyValueStore.KeyValueStore
    }

    /**
     * One simulated LiveStore client: its own backend instance over its own
     * freshly built `KeyValueStore.layerMemory` (`Layer.fresh` so the build
     * can never be memoized onto the per-test store above).
     *
     * `seedBackendId` is written *before* the backend is constructed:
     * `SyncBackend.makeBackendIdHelper` reads the key once, at construction.
     */
    const makeClient = ({
      storeId,
      clientId,
      payload,
      seedBackendId,
    }: {
      readonly storeId: string
      readonly clientId: string
      readonly payload?: Schema.Json
      readonly seedBackendId?: string
    }): Effect.Effect<
      TestClient,
      any,
      RuntimeServices | Scope.Scope
    > =>
      Effect.gen(function* () {
        const kvContext = yield* Layer.build(Layer.fresh(KeyValueStore.layerMemory))
        const kv = Context.get(kvContext, KeyValueStore.KeyValueStore)

        if (seedBackendId !== undefined) yield* kv.set(BACKEND_ID_KEY, seedBackendId)

        const backend = yield* Effect.andThen(SyncProviderImpl, (_) =>
          _.makeProvider({ storeId, clientId, payload }),
        ).pipe(Effect.provideService(KeyValueStore.KeyValueStore, kv))

        return { backend, kv }
      })

    const factoryFor = (clientId: string, options?: { readonly startSeq?: number; readonly parent?: number }) =>
      makeEventFactory({
        client: EventFactory.clientIdentity(clientId, `${clientId}-session`),
        startSeq: options?.startSeq ?? 1,
        initialParent: options?.parent ?? 'root',
      })

    // ------------------------------------------------------------------
    // 1. Two clients, same store, both directions
    // ------------------------------------------------------------------

    it.live('syncs two clients on the same store in both directions', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        const b = yield* makeClient({ storeId, clientId: 'client-b' })

        const aFactory = factoryFor('client-a')
        // B's event chains onto A's: seqNum 2, parentSeqNum 1.
        const bFactory = factoryFor('client-b', { startSeq: 2, parent: 1 })

        // A waits for B's event, B waits for A's (A also sees its own fan-out).
        const aWaitsFor2 = yield* a.backend
          .pull(Option.none(), { live: true })
          .pipe(runFirstWithSeqNum(2), Effect.forkChild)
        const bWaitsFor1 = yield* b.backend
          .pull(Option.none(), { live: true })
          .pipe(runFirstWithSeqNum(1), Effect.forkChild)

        yield* a.backend.connect
        yield* b.backend.connect
        yield* Effect.sleep(SETTLE)

        // A → B
        const e1 = aFactory.todoCreated.next({ id: 'a-1', text: 'from a', completed: false })
        expect(e1.seqNum as number).toBe(1)
        yield* a.backend.push([e1])

        const seenByB = yield* Fiber.join(bWaitsFor1)
        expect(seqNumsOf(seenByB)).toEqual([1])
        expect(seenByB.batch[0]!.eventEncoded.clientId).toBe('client-a')

        // B → A
        const e2 = bFactory.todoCreated.next({ id: 'b-1', text: 'from b', completed: false })
        expect(e2.seqNum as number).toBe(2)
        expect(e2.parentSeqNum as number).toBe(1)
        yield* b.backend.push([e2])

        const seenByA = yield* Fiber.join(aWaitsFor2)
        expect(seqNumsOf(seenByA)).toEqual([2])
        expect(seenByA.batch[0]!.eventEncoded.clientId).toBe('client-b')

        // Both clients persisted the same (server-assigned) backend id.
        const backendIdA = yield* a.kv.get(BACKEND_ID_KEY)
        const backendIdB = yield* b.kv.get(BACKEND_ID_KEY)
        expect(backendIdA).toBeDefined()
        expect(backendIdB).toBe(backendIdA)
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 2. ServerAhead guarantee (the leader's anti-deadlock invariant)
    // ------------------------------------------------------------------

    it.live('rejects a stale push with ServerAheadError after delivering the newer event', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        const b = yield* makeClient({ storeId, clientId: 'client-b' })

        // B is already live-pulling when its push is rejected — this is exactly
        // the situation the LiveStore leader is in when it rebases.
        const bLive = yield* b.backend.pull(Option.none(), { live: true }).pipe(runFirstNonEmpty, Effect.forkChild)

        yield* b.backend.connect
        yield* Effect.sleep(SETTLE)

        const e1 = factoryFor('client-a').todoCreated.next({ id: 'a-1', text: 'wins', completed: false })
        yield* a.backend.push([e1])

        // B still believes the store is empty: seqNum 1, parentSeqNum 0 (root).
        const stale = factoryFor('client-b').todoCreated.next({ id: 'b-1', text: 'loses', completed: false })
        expect(stale.parentSeqNum as number).toBe(0)

        const failure: any = expectFailure(yield* b.backend.push([stale]).pipe(Effect.result))
        expect(failure._tag).toBe('ServerAheadError')
        expect(failure.minimumExpectedNum as number).toBe(1)
        expect(failure.providedNum as number).toBe(0)

        // …and the rejected pusher has already been handed the event it is
        // behind on, so it can rebase instead of deadlocking.
        const delivered = yield* Fiber.join(bLive).pipe(Effect.timeout(Duration.seconds(20)))
        expect(seqNumsOf(delivered)).toEqual([1])
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 3. backendId persisted in the KV + enforced by the server
    // ------------------------------------------------------------------

    it.live('persists the backendId and rejects a cursor carrying a foreign one', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const a = yield* makeClient({ storeId, clientId: 'client-a' })

        const firstPull = yield* a.backend.pull(Option.none()).pipe(Stream.runFirstUnsafe)
        expect(firstPull).toEqual(SyncBackend.pullResItemEmpty())

        const realBackendId = yield* a.kv.get(BACKEND_ID_KEY)
        expect(typeof realBackendId).toBe('string')
        expect(realBackendId).not.toBe('')

        // A different client on the same store, whose KV claims a foreign backend.
        const bogus = yield* makeClient({ storeId, clientId: 'client-bogus', seedBackendId: 'bogus' })

        const cursor = Option.some({
          eventSequenceNumber: EventSequenceNumber.Global.make(0),
          metadata: Option.none<never>(),
        })

        const failure: any = expectFailure(
          yield* bogus.backend.pull(cursor).pipe(Stream.runCollectReadonlyArray, Effect.result),
        )
        expect(failure._tag).toBe('BackendIdMismatchError')
        expect(failure.expected).toBe(realBackendId)
        expect(failure.received).toBe('bogus')
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 4. Cursor without a persisted backendId
    // ------------------------------------------------------------------

    it.live('accepts a cursor from a client that never persisted a backendId', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const writer = yield* makeClient({ storeId, clientId: 'client-writer' })
        yield* writer.backend.push([
          factoryFor('client-writer').todoCreated.next({ id: 'w-1', text: 'seed', completed: false }),
        ])

        // Fresh client: holds a cursor (from an earlier session) but no backendId.
        const reader = yield* makeClient({ storeId, clientId: 'client-reader' })
        expect(yield* reader.kv.get(BACKEND_ID_KEY)).toBeUndefined()

        const cursor = Option.some({
          eventSequenceNumber: EventSequenceNumber.Global.make(1),
          metadata: Option.none<never>(),
        })

        const items = yield* reader.backend.pull(cursor).pipe(Stream.runCollectReadonlyArray)
        expect(items).toEqual([SyncBackend.pullResItemEmpty()])

        // The pull response taught the reader the store's backend id.
        const learned = yield* reader.kv.get(BACKEND_ID_KEY)
        expect(learned).toBeDefined()
        expect(learned).toBe(yield* writer.kv.get(BACKEND_ID_KEY))
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 5. Push before any pull
    // ------------------------------------------------------------------

    it.live('learns the backendId from a push that happens before any pull', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        expect(yield* a.kv.get(BACKEND_ID_KEY)).toBeUndefined()

        yield* a.backend.push([factoryFor('client-a').todoCreated.next({ id: 'a-1', text: 'first', completed: false })])

        const backendId = yield* a.kv.get(BACKEND_ID_KEY)
        expect(typeof backendId).toBe('string')
        expect(backendId).not.toBe('')

        // The event really landed, and the learned id matches what a pull reports.
        const items = yield* a.backend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
        expect(items.flatMap(seqNumsOf)).toEqual([1])
        expect(yield* a.kv.get(BACKEND_ID_KEY)).toBe(backendId)
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 6. validatePayload rejection
    // ------------------------------------------------------------------

    it.live('rejects a client whose syncPayload fails validatePayload', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const rejected = yield* makeClient({ storeId, clientId: 'client-rejected', payload: REJECTED_PAYLOAD })
        const allowed = yield* makeClient({ storeId, clientId: 'client-allowed' })

        // `push` → InvalidPayloadError on the wire → UnknownError for LiveStore.
        const pushFailure: any = expectFailure(
          yield* rejected.backend
            .push([factoryFor('client-rejected').todoCreated.next({ id: 'r-1', text: 'nope', completed: false })])
            .pipe(Effect.result),
        )
        expect(pushFailure._tag).toBe('UnknownError')
        expect(pushFailure.note).toContain('validatePayload')

        // The same for its live pull: it never yields an item.
        const pullFailure: any = expectFailure(
          yield* rejected.backend.pull(Option.none(), { live: true }).pipe(Stream.runFirstUnsafe, Effect.result),
        )
        expect(pullFailure._tag).toBe('UnknownError')
        expect(pullFailure.note).toContain('validatePayload')

        // Its connection is open right now; the server disconnects it on the
        // next fan-out (connection auth is lazy, see `src/server/connections.ts`).
        yield* rejected.backend.connect
        expect(yield* SubscriptionRef.get(rejected.backend.isConnected)).toBe(true)

        const wasDisconnected = yield* SubscriptionRef.changes(rejected.backend.isConnected).pipe(
          Stream.filter((connected) => connected === false),
          Stream.runFirstUnsafe,
          Effect.forkChild,
        )

        const allowedLive = yield* allowed.backend
          .pull(Option.none(), { live: true })
          .pipe(runFirstNonEmpty, Effect.forkChild)

        yield* allowed.backend.connect
        yield* Effect.sleep(SETTLE)

        // An authorized client on the same store is unaffected.
        yield* allowed.backend.push([
          factoryFor('client-allowed').todoCreated.next({ id: 'ok-1', text: 'allowed', completed: false }),
        ])
        const received = yield* Fiber.join(allowedLive)
        expect(seqNumsOf(received)).toEqual([1])

        yield* Fiber.join(wasDisconnected).pipe(Effect.timeout(Duration.seconds(20)))

        // rivetkit reconnects on its own; the client is back up but still
        // rejected on every request (asserted above).
        yield* rejected.backend.connect
        expect(yield* SubscriptionRef.get(rejected.backend.isConnected)).toBe(true)
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 7. Reconnect re-catches up on events missed while offline
    // ------------------------------------------------------------------

    it.live('re-catches up on events pushed while the live pull was disconnected', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)

        const a = yield* makeClient({ storeId, clientId: 'client-a' })
        // B is deliberately untouched so far: its connection is created lazily
        // by the push below, i.e. *after* the outage has hit A.
        const b = yield* makeClient({ storeId, clientId: 'client-b' })

        const aLive = yield* a.backend.pull(Option.none(), { live: true }).pipe(runFirstNonEmpty, Effect.forkChild)

        yield* a.backend.connect
        yield* Effect.sleep(SETTLE)

        const syncProvider = yield* SyncProviderImpl
        yield* syncProvider.turnBackendOffline

        // No pause: B's push (and therefore the fan-out) happens while A is
        // still reconnecting, so A can only learn about it by re-catching up.
        yield* b.backend.push([
          factoryFor('client-b').todoCreated.next({ id: 'b-1', text: 'while offline', completed: false }),
        ])

        yield* syncProvider.turnBackendOnline

        const received = yield* Fiber.join(aLive).pipe(Effect.timeout(Duration.seconds(30)))
        expect(seqNumsOf(received)).toEqual([1])
        expect(received.batch[0]!.eventEncoded.clientId).toBe('client-b')
      }).pipe(withTestCtx()),
    )

    // ------------------------------------------------------------------
    // 8. Large (but in-budget) event
    // ------------------------------------------------------------------

    it.live('round-trips a 500 KB event through push and live pull', (test) =>
      Effect.gen(function* () {
        const storeId = storeIdFor(test.task.name)
        const PAYLOAD_BYTES = 500_000

        const a = yield* makeClient({ storeId, clientId: 'client-a' })

        const aLive = yield* a.backend.pull(Option.none(), { live: true }).pipe(runFirstNonEmpty, Effect.forkChild)

        yield* a.backend.connect
        yield* Effect.sleep(SETTLE)

        const text = 'x'.repeat(PAYLOAD_BYTES)
        yield* a.backend.push([factoryFor('client-a').todoCreated.next({ id: 'big-1', text, completed: false })])

        const received = yield* Fiber.join(aLive).pipe(Effect.timeout(Duration.seconds(30)))
        expect(seqNumsOf(received)).toEqual([1])
        expect((received.batch[0]!.eventEncoded.args as { text: string }).text.length).toBe(PAYLOAD_BYTES)

        // …and it is durable, not just broadcast.
        const items = yield* a.backend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
        expect(items.flatMap(seqNumsOf)).toEqual([1])
      }).pipe(withTestCtx()),
    )
  })
}
