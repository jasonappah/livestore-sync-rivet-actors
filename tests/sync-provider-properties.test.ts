/**
 * Property-based "large batches handling" cases, ported from LiveStore's
 * `tests/sync-provider/src/sync-provider.test.ts` (`Vitest.live.prop(
 * 'streams batch variations over provider payload limits', ...)`). The
 * deterministic cases of that block live in `tests/sync-provider.test.ts`.
 *
 * Generators: the same `LargeBatchScenarioSchema` upstream uses — a union of
 * `fewLarge` (20–28 events × 70–110 KB, pushed 6–12 at a time) and `manySmall`
 * (1 200–1 600 events × 0.9–1.2 KB, pushed 30–160 at a time), each ≥ 1 MB in
 * total. `@effect/vitest`'s `it.live.prop` turns the schema into a fast-check
 * arbitrary (`Schema.toArbitrary`, via the `fast-check` that `effect` bundles
 * as `effect/testing/FastCheck`). The largest event (~110 KB) stays well under
 * the harness client's `maxPushBytes` (900 000), so every scenario is
 * pushable; a 6–12-event push chunk of large events exceeds it, which
 * exercises the client's byte-based push splitting.
 *
 * Beyond upstream's "all events come back" check, each run asserts, against
 * the Rivet provider's paging contract (`pullPageSize: 100`,
 * `maxMessageBytes: 900_000`):
 * - a non-live pull returns every pushed event exactly once, in seqNum order,
 *   with its payload intact;
 * - no page exceeds 100 events (nor ~900 KB of payload);
 * - page infos are `MoreKnown(remaining)` with `remaining` = events not yet
 *   delivered (strictly descending), followed by exactly one `NoMore`;
 * - a second client's live pull, started before the pushes, receives all
 *   events in seqNum order.
 *
 * Runs: {@link NUM_RUNS} (default 8; upstream uses 1). Each run moves 1–3 MB
 * through a real engine, so the budget is kept small; shrinking is disabled
 * (`endOnFailure`) because every shrink step would be another multi-MB round
 * trip. Reproduce a failure with the printed seed: `FC_SEED=<seed> FC_NUM_RUNS=1
 * pnpm test:conformance tests/sync-provider-properties.test.ts`.
 *
 * Needs a Rivet engine: run through `pnpm test:conformance`.
 */

import { SyncBackend } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import { Duration, Effect, Fiber, Option, Schema, Stream } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { describe, expect, it } from '@effect/vitest'

import { makeEventFactory } from './harness/events.ts'
import { providerRegistry, selectedProviderKeys } from './harness/providers/registry.ts'
import { useProviderRuntime } from './harness/runtime.ts'

const providerLayers = selectedProviderKeys().map((key) => ({ key, ...providerRegistry[key] }))

const envInt = (name: string): number | undefined => {
  const raw = process.env[name]
  if (raw === undefined || raw === '') return undefined
  const value = Number.parseInt(raw, 10)
  return Number.isFinite(value) ? value : undefined
}

/** fast-check runs per provider (override with `FC_NUM_RUNS`). */
const NUM_RUNS = envInt('FC_NUM_RUNS') ?? 8
const SEED = envInt('FC_SEED')

/** Rivet harness paging limits (`tests/harness/providers/rivet.ts`). */
const MAX_PAGE_EVENTS = 100
const MAX_MESSAGE_BYTES = 900_000

const MIN_BATCH_PAYLOAD_BYTES = 1_000_000

// Verbatim from upstream.
const fewLargeScenarioSchema = Schema.Struct({
  variant: Schema.Literal('fewLarge'),
  eventCount: Schema.Int.check(Schema.isBetween({ minimum: 20, maximum: 28 })),
  payloadSize: Schema.Int.check(Schema.isBetween({ minimum: 70_000, maximum: 110_000 })),
  pushBatchSize: Schema.Int.check(Schema.isBetween({ minimum: 6, maximum: 12 })),
}).pipe(
  Schema.check(
    Schema.makeFilter((scenario) => scenario.eventCount * scenario.payloadSize >= MIN_BATCH_PAYLOAD_BYTES, {
      message: 'Large batch scenarios should exceed provider payload limits',
    }),
  ),
)

const manySmallScenarioSchema = Schema.Struct({
  variant: Schema.Literal('manySmall'),
  eventCount: Schema.Int.check(Schema.isBetween({ minimum: 1_200, maximum: 1_600 })),
  payloadSize: Schema.Int.check(Schema.isBetween({ minimum: 900, maximum: 1_200 })),
  pushBatchSize: Schema.Int.check(Schema.isBetween({ minimum: 30, maximum: 160 })),
}).pipe(
  Schema.check(
    Schema.makeFilter((scenario) => scenario.eventCount * scenario.payloadSize >= MIN_BATCH_PAYLOAD_BYTES, {
      message: 'Small batch scenarios should exceed provider payload limits',
    }),
  ),
)

const LargeBatchScenarioSchema = Schema.Union([fewLargeScenarioSchema, manySmallScenarioSchema])

type LargeBatchScenario = (typeof LargeBatchScenarioSchema)['Type']

const batchScenarioSummary = (scenario: LargeBatchScenario) =>
  `${scenario.variant}-${scenario.eventCount}x${scenario.payloadSize}-by${scenario.pushBatchSize}`

const makeBatchEvents = (
  scenario: LargeBatchScenario,
  { baseId }: { baseId: string },
): ReadonlyArray<LiveStoreEvent.Global.Encoded> => {
  const payload = 'x'.repeat(scenario.payloadSize)
  const batchClient = EventFactory.clientIdentity(`${baseId}-client`, `${baseId}-session`)
  const eventFactory = makeEventFactory({ client: batchClient, startSeq: 1, initialParent: 'root' })

  return Array.from({ length: scenario.eventCount }, (_, index) =>
    eventFactory.todoCreated.next({ id: `${baseId}-${index}`, text: payload, completed: false }),
  )
}

const pushBatchEvents = (
  syncBackend: SyncBackend.SyncBackend<any>,
  events: ReadonlyArray<LiveStoreEvent.Global.Encoded>,
  pushBatchSize: number,
) =>
  Effect.gen(function* () {
    const batchSize = Math.max(1, pushBatchSize)
    for (let index = 0; index < events.length; index += batchSize) {
      yield* syncBackend.push(events.slice(index, index + batchSize))
    }
  })

const expectedSeqNums = (count: number) =>
  Array.from({ length: count }, (_, i) => EventSequenceNumber.Global.make(i + 1))

/** Per-run budget; the vitest case gets `NUM_RUNS` of these plus slack. */
const RUN_TIMEOUT = Duration.minutes(2)
const vitestTimeoutMs = NUM_RUNS * Duration.toMillis(RUN_TIMEOUT) + 30_000

for (const { layer, name } of providerLayers) {
  describe(`${name} sync provider (large-batch properties)`, () => {
    const { withTestCtx, storeIdFor, makeProviderFor } = useProviderRuntime(layer)

    it.live.prop(
      'streams batch variations over provider payload limits',
      [LargeBatchScenarioSchema],
      ([scenario], test) =>
        Effect.gen(function* () {
          const summary = batchScenarioSummary(scenario)
          const { eventCount, payloadSize } = scenario
          expect(eventCount * payloadSize).toBeGreaterThanOrEqual(MIN_BATCH_PAYLOAD_BYTES)

          const storeId = storeIdFor(`${test.task.name}-${summary}`)
          const writer = yield* makeProviderFor({ storeId, clientId: 'writer' })
          const reader = yield* makeProviderFor({ storeId, clientId: 'reader' })

          const baseId = `${scenario.variant}-${nanoid(8)}`
          const events = makeBatchEvents(scenario, { baseId })

          // Second client: live pull from scratch, started before any push, so
          // events arrive through catch-up and/or live fan-out.
          const liveFiber = yield* reader.pull(Option.none(), { live: true }).pipe(
            Stream.flatMap((item) => Stream.fromIterable(item.batch)),
            Stream.map((item) => item.eventEncoded.seqNum),
            Stream.take(eventCount),
            Stream.runCollect,
            Effect.timeout(RUN_TIMEOUT),
            Effect.forkChild,
          )

          yield* pushBatchEvents(writer, events, scenario.pushBatchSize)

          // Non-live pull: full catch-up, page by page.
          const pages = yield* writer.pull(Option.none()).pipe(Stream.runCollectReadonlyArray)
          const pulled = pages.flatMap((page) => page.batch.map((item) => item.eventEncoded))

          expect(pulled.length, summary).toBe(eventCount)
          expect(
            pulled.map((event) => event.seqNum),
            summary,
          ).toEqual(expectedSeqNums(eventCount))
          pulled.forEach((event, index) => {
            const args = event.args as { id: string; text: string }
            expect(args.id).toBe(`${baseId}-${index}`)
            expect(args.text.length).toBe(payloadSize)
          })

          // Page sizes: bounded by pullPageSize and (roughly) by maxMessageBytes.
          for (const page of pages) {
            expect(page.batch.length, summary).toBeGreaterThan(0)
            expect(page.batch.length, summary).toBeLessThanOrEqual(MAX_PAGE_EVENTS)
            expect(page.batch.length * payloadSize, summary).toBeLessThanOrEqual(MAX_MESSAGE_BYTES)
          }

          // Page infos: MoreKnown(remaining-after-this-page)… then exactly one NoMore.
          let delivered = 0
          const expectedPageInfos = pages.map((page, index) => {
            delivered += page.batch.length
            return index === pages.length - 1
              ? SyncBackend.pageInfoNoMore
              : SyncBackend.pageInfoMoreKnown(eventCount - delivered)
          })
          expect(
            pages.map((page) => page.pageInfo),
            summary,
          ).toEqual(expectedPageInfos)
          expect(delivered).toBe(eventCount)

          // Live pull on the second client: everything, in order, no duplicates.
          const liveSeqNums = yield* Fiber.join(liveFiber)
          expect([...liveSeqNums], summary).toEqual(expectedSeqNums(eventCount))
        }).pipe(withTestCtx()),
      {
        timeout: vitestTimeoutMs,
        fastCheck: {
          numRuns: NUM_RUNS,
          endOnFailure: true,
          ...(SEED === undefined ? {} : { seed: SEED }),
        },
      },
    )
  })
}
