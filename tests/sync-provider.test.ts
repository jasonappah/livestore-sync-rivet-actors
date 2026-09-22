/**
 * Conformance suite for the Rivet-backed `SyncBackend`, ported from
 * LiveStore's `tests/sync-provider/src/sync-provider.test.ts`.
 *
 * Differences from upstream (kept minimal so the file stays diffable):
 * - Light harness: `@effect/vitest`'s `it.live` + a per-test
 *   `KeyValueStore.layerMemory`/`Logger` layer instead of
 *   `@livestore/utils-dev`'s `Vitest.makeWithTestCtx` (which drags in OTel and
 *   `adapter-web`). The suite runtime lives in `tests/harness/runtime.ts`.
 * - Events come from `tests/harness/events.ts` (`Events.synced` + `EventFactory`).
 * - Property-based large-batch cases live in `tests/sync-provider-properties.test.ts`.
 * - Store ids carry a `nanoid()` suffix: the Rivet engine outlives vitest and
 *   persists actor state across runs.
 *
 * Needs a Rivet engine: run through `pnpm test:conformance`.
 */

import { BackendIdMismatchError, SyncBackend } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import {
  Deferred,
  Duration,
  Effect,
  Fiber,
  Option,
  Queue,
  Result,
  Schema,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { describe, expect, it } from '@effect/vitest'

import { makeEventFactory } from './harness/events.ts'
import { providerRegistry, selectedProviderKeys } from './harness/providers/registry.ts'
import { defaultClient, useProviderRuntime } from './harness/runtime.ts'
import { SyncProviderImpl } from './harness/types.ts'

// NOTE: These specs should mirror LeaderSyncProcessor semantics: pushes never bypass the
// queueing/rebase rules, and live pulls represent the long-lived stream the leader relies on.
// Keep scenarios aligned with those invariants so we only test protocol-compliant usage.

const providerLayers = selectedProviderKeys().map((key) => ({ key, ...providerRegistry[key] }))

const runFirstNonEmpty = <T, E, R>(stream: Stream.Stream<SyncBackend.PullResItem<T>, E, R>) =>
  stream.pipe(
    Stream.filter(({ batch }) => batch.length > 0),
    Stream.runFirstUnsafe,
  )

const seqNumsOf = (items: ReadonlyArray<SyncBackend.PullResItem<unknown>>): ReadonlyArray<number> =>
  items.flatMap((item) => item.batch.map((b) => b.eventEncoded.seqNum as number))

/** Verifies: LS.SYS.SYNC-R02, LS.SYS.SYNC-R03, LS.SYS.SYNC-R04, LS.SYS.SYNC-R05, LS.SYS.VER.CONF-R01 */
for (const { key, layer, name } of providerLayers) {
  describe(`${name} sync provider`, { timeout: 60_000 }, () => {
    const { withTestCtx, makeProvider } = useProviderRuntime(layer)

    // Simple test to verify the setup works
    it.live('can create sync backend', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)

        // Just verify we can create the backend. `SyncBackend.isSyncBackend` is
        // stale in 0.5.0-dev.0 (expects `connect`/`ping` to be functions, but the
        // type declares them as `Effect` values), so check the fields directly.
        expect(syncBackend).toBeDefined()
        expect(typeof syncBackend.connect).toBe('object')
        expect(typeof syncBackend.pull).toBe('function')
        expect(typeof syncBackend.push).toBe('function')
        expect(typeof syncBackend.ping).toBe('object')
        expect(typeof syncBackend.isConnected).toBe('object')
        expect(syncBackend.metadata.name).toBe('livestore-sync-rivet-actors')
        expect(syncBackend.supports).toEqual({ pullPageInfoKnown: true, pullLive: true })
      }).pipe(withTestCtx()),
    )

    it.live('can ping sync backend', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)

        yield* syncBackend.ping
      }).pipe(withTestCtx()),
    )

    it.live('can connect to sync backend', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)

        // Check initial state
        const initialConnected = yield* SubscriptionRef.get(syncBackend.isConnected)
        expect(initialConnected).toBe(false)

        // Connect
        yield* syncBackend.connect

        // Check connected state
        const connected = yield* SubscriptionRef.get(syncBackend.isConnected)
        expect(connected).toBe(true)
      }).pipe(withTestCtx()),
    )

    it.live('can pull events from sync backend', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)

        // Pull without cursor (initial sync)
        const firstPull = yield* syncBackend.pull(Option.none()).pipe(Stream.runFirstUnsafe)

        // Verify we got a valid response
        expect(firstPull).toEqual(SyncBackend.pullResItemEmpty())
      }).pipe(withTestCtx()),
    )

    describe('live pull', () => {
      it.live('needs to return a no-more page info', (test) =>
        Effect.gen(function* () {
          const syncBackend = yield* makeProvider(test.task.name)

          const firstPull = yield* syncBackend.pull(Option.none(), { live: true }).pipe(Stream.runFirstUnsafe)

          expect(firstPull.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
        }).pipe(withTestCtx()),
      )

      it.live('survives idle and receives later event', (test) =>
        Effect.gen(function* () {
          const syncBackend = yield* makeProvider(test.task.name)
          const eventFactory = makeEventFactory({ client: defaultClient, startSeq: 1, initialParent: 'root' })

          // Start live pull and wait for the first non-empty batch in a fiber
          const fiber = yield* syncBackend.pull(Option.none(), { live: true }).pipe(runFirstNonEmpty, Effect.forkChild)

          // Let the live pull idle for a bit (covers long-poll/SSE)
          yield* Effect.sleep(800)

          // Push an event; live stream should emit it
          yield* syncBackend.push([
            eventFactory.todoCreated.next({ id: 'idle-1', text: 'Late event', completed: false }),
          ])

          const result = yield* Fiber.join(fiber)
          expect(result.batch.length).toBe(1)
        }).pipe(withTestCtx()),
      )
    })

    it.live('can pull with cursor', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)

        const eventFactory = makeEventFactory({ client: defaultClient })

        yield* syncBackend.push([eventFactory.todoCreated.next({ id: '1', text: 'Test event 1.', completed: false })])

        // First pull without cursor
        const firstPull = yield* syncBackend.pull(Option.none()).pipe(runFirstNonEmpty)
        expect(firstPull.batch.length).toBe(1)

        // Pull with cursor from a specific position
        const secondPull = yield* syncBackend
          .pull(SyncBackend.cursorFromPullResItem(firstPull))
          .pipe(Stream.runFirstUnsafe)

        expect(secondPull).toEqual(SyncBackend.pullResItemEmpty())
      }).pipe(withTestCtx()),
    )

    describe.skipIf(key !== 'rivet')('serialized admission', () => {
      it.live('serializes concurrent pushes that share the same parent', (test) =>
        Effect.gen(function* () {
          const syncBackend = yield* makeProvider(test.task.name)
          const contenders = Array.from({ length: 8 }, (_, index) =>
            makeEventFactory({
              client: EventFactory.clientIdentity(`contender-${index}`, `session-${index}`),
              startSeq: 1,
              initialParent: 'root',
            }).todoCreated.next({ id: `contender-${index}`, text: `Contender ${index}`, completed: false }),
          )
          const ready = yield* Queue.unbounded<void>()
          const release = yield* Deferred.make<void>()

          const fibers = yield* Effect.forEach(contenders, (event) =>
            Effect.gen(function* () {
              yield* Queue.offer(ready, undefined)
              yield* Deferred.await(release)
              return yield* syncBackend.push([event]).pipe(Effect.result)
            }).pipe(Effect.forkChild),
          )

          // All contenders cross the same explicit start barrier before backend admission.
          yield* Effect.forEach(contenders, () => Queue.take(ready))
          yield* Deferred.succeed(release, undefined)

          const results = yield* Effect.forEach(fibers, Fiber.join)
          const successes = results.filter(Result.isSuccess)
          const failures = results.filter(Result.isFailure)

          expect(successes).toHaveLength(1)
          expect(failures).toHaveLength(contenders.length - 1)
          expect(failures.every((result) => result.failure._tag === 'ServerAheadError')).toBe(true)

          const pulled = yield* syncBackend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
          expect(pulled.flatMap((item) => item.batch)).toHaveLength(1)
        }).pipe(withTestCtx()),
      )
    })

    describe('large batches handling', () => {
      const MIN_BATCH_PAYLOAD_BYTES = 1_000_000

      type LargeBatchScenario = {
        variant: 'fewLarge' | 'manySmall'
        eventCount: number
        payloadSize: number
        pushBatchSize: number
      }

      const deterministicBatchCases: ReadonlyArray<{
        label: string
        scenario: LargeBatchScenario
      }> = [
        {
          label: 'streams dozens of extremely large batches',
          scenario: { variant: 'fewLarge', eventCount: 60, payloadSize: 120_000, pushBatchSize: 6 },
        },
        {
          label: 'streams thousands of small batches',
          scenario: { variant: 'manySmall', eventCount: 1_800, payloadSize: 1_024, pushBatchSize: 90 },
        },
      ]

      const approxBatchPayloadBytes = (scenario: LargeBatchScenario) => scenario.eventCount * scenario.payloadSize

      const makeBatchEvents = (
        scenario: LargeBatchScenario,
        { baseId }: { baseId: string },
      ): ReadonlyArray<LiveStoreEvent.Global.Encoded> => {
        const payload = 'x'.repeat(scenario.payloadSize)
        const batchClient = EventFactory.clientIdentity(`${baseId}-client`, `${baseId}-session`)
        const eventFactory = makeEventFactory({ client: batchClient, startSeq: 1, initialParent: 'root' })

        return Array.from({ length: scenario.eventCount }, (_, index) =>
          eventFactory.todoCreated.next({
            id: `${baseId}-${index}`,
            text: payload,
            completed: false,
          }),
        )
      }

      const pushBatchEvents = (
        syncBackend: SyncBackend.SyncBackend<any>,
        batches: ReadonlyArray<LiveStoreEvent.Global.Encoded>,
        pushBatchSize: number,
      ) =>
        Effect.gen(function* () {
          const batchSize = Math.max(1, pushBatchSize)

          for (let index = 0; index < batches.length; index += batchSize) {
            const batch = batches.slice(index, index + batchSize)
            if (batch.length === 0) continue

            yield* syncBackend.push(batch)
          }
        })

      type BatchPullStats = {
        totalEvents: number
        nonEmptyBatches: number
        maxBatchSize: number
      }

      const collectBatchPullStats = (syncBackend: SyncBackend.SyncBackend<any>) =>
        syncBackend.pull(Option.none()).pipe(
          Stream.runFold(
            (): BatchPullStats => ({ totalEvents: 0, nonEmptyBatches: 0, maxBatchSize: 0 }),
            (acc, { batch }) => ({
              totalEvents: acc.totalEvents + batch.length,
              nonEmptyBatches: acc.nonEmptyBatches + (batch.length > 0 ? 1 : 0),
              maxBatchSize: Math.max(acc.maxBatchSize, batch.length),
            }),
          ),
        )

      // Per-scenario timeout (all providers)
      const scenarioTimeoutMs = Duration.toMillis(Duration.minutes(6))

      // Deterministic scenarios only: upstream's property-based variants need fast-check.
      for (const { label, scenario } of deterministicBatchCases) {
        it.live(
          label,
          (test) =>
            Effect.gen(function* () {
              const scenarioId = nanoid()
              const approxBytes = approxBatchPayloadBytes(scenario)

              expect(approxBytes).toBeGreaterThanOrEqual(MIN_BATCH_PAYLOAD_BYTES)

              const syncBackend = yield* makeProvider(`${test.task.name}-${scenario.variant}`)

              const batchEvents = makeBatchEvents(scenario, {
                baseId: `${scenario.variant}-${scenarioId}`,
              })

              yield* pushBatchEvents(syncBackend, batchEvents, scenario.pushBatchSize)

              const stats = yield* collectBatchPullStats(syncBackend)

              expect(stats.totalEvents).toBe(scenario.eventCount)
              expect(stats.nonEmptyBatches).toBeGreaterThan(0)

              // Rivet pages catch-up pulls (100 events / 900 KB per page), so 1 800
              // events can never arrive in a single page.
              if (scenario.variant === 'manySmall') {
                expect(stats.nonEmptyBatches).toBeGreaterThan(1)
                expect(stats.maxBatchSize).toBeLessThanOrEqual(100)
              }
            }).pipe(withTestCtx()),
          scenarioTimeoutMs,
        )
      }
    })

    it.live('non-live pull returns multiple events', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)
        const eventFactory = makeEventFactory({ client: defaultClient, startSeq: 1, initialParent: 'root' })

        // Push at least two events
        for (let i = 0; i < 2; i++) {
          yield* syncBackend.push([
            eventFactory.todoCreated.next({
              id: `multi-${i}`,
              text: `Event ${i}`,
              completed: i % 2 === 0,
            }),
          ])
        }

        // Non-live pull should return both events across its pages
        const results = yield* syncBackend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
        const pulled = results.flatMap((r) => r.batch.map((b) => b.eventEncoded))
        expect(pulled.length).toBeGreaterThanOrEqual(2)
      }).pipe(withTestCtx()),
    )

    it.live('non-live pull pages with known remaining counts', (test) =>
      Effect.gen(function* () {
        const syncBackend = yield* makeProvider(test.task.name)
        const eventFactory = makeEventFactory({ client: defaultClient, startSeq: 1, initialParent: 'root' })

        // 250 events in one push (the client splits it into ≤100-event actions).
        const TOTAL_EVENTS = 250
        const batch = Array.from({ length: TOTAL_EVENTS }, (_, i) =>
          eventFactory.todoCreated.next({ id: `page-${i}`, text: `Event ${i}`, completed: false }),
        )
        yield* syncBackend.push(batch)

        const results = yield* syncBackend.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)

        // pullPageSize 100 → 100 / 100 / 50, with an exact remaining count on every page.
        expect(results.map((r) => r.pageInfo)).toEqual([
          SyncBackend.pageInfoMoreKnown(150),
          SyncBackend.pageInfoMoreKnown(50),
          SyncBackend.pageInfoNoMore,
        ])
        expect(results.map((r) => r.batch.length)).toEqual([100, 100, 50])

        const seqNums = seqNumsOf(results)
        expect(seqNums).toEqual(Array.from({ length: TOTAL_EVENTS }, (_, i) => EventSequenceNumber.Global.make(i + 1)))
      }).pipe(withTestCtx()),
    )

    describe('connection management', () => {
      it.live('can reconnect to sync backend', (test) =>
        Effect.gen(function* () {
          const syncBackend = yield* makeProvider(test.task.name)

          const fiber = yield* syncBackend.pull(Option.none(), { live: true }).pipe(runFirstNonEmpty, Effect.forkChild)

          // Make sure the live pull is actually connected before the outage.
          yield* syncBackend.connect

          const syncProvider = yield* SyncProviderImpl

          yield* syncProvider.turnBackendOffline
          yield* Effect.sleep(1000)
          yield* syncProvider.turnBackendOnline

          const eventFactory = makeEventFactory({ client: defaultClient })

          eventFactory.todoCreated.advanceTo(1, 'root')
          yield* syncBackend.push([eventFactory.todoCreated.next({ id: '1', text: 'Test event 1.', completed: false })])

          const result = yield* Fiber.join(fiber)
          expect(result.batch.length).toBe(1)
        }).pipe(withTestCtx()),
      )
    })

    /**
     * Tests that BackendIdMismatchError is properly serialized and deserialized
     * over the RPC boundary.
     *
     * This test creates the error, encodes it to JSON, and verifies all fields
     * are preserved - which was broken before the fix for issue #981 where
     * Schema.Defect() lost the structured error fields during serialization.
     *
     * @see https://github.com/livestorejs/livestore/issues/981
     */
    it.live('BackendIdMismatchError serializes correctly', () =>
      Effect.gen(function* () {
        const originalError = new BackendIdMismatchError({
          expected: 'expected-backend-id-123',
          received: 'received-backend-id-456',
        })

        // Verify the error structure before serialization
        expect(originalError._tag).toBe('BackendIdMismatchError')
        expect(originalError.expected).toBe('expected-backend-id-123')
        expect(originalError.received).toBe('received-backend-id-456')

        // Simulate what happens during RPC: encode to JSON and decode back
        const str = yield* Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown))(originalError)
        const encoded = (yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(str)) as {
          _tag: string
          expected: string
          received: string
        }

        // The encoded form should preserve the structure (this was broken before the fix)
        expect(encoded._tag).toBe('BackendIdMismatchError')
        expect(encoded.expected).toBe('expected-backend-id-123')
        expect(encoded.received).toBe('received-backend-id-456')
      }).pipe(withTestCtx()),
    )
  })
}
