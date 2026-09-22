import { BackendIdMismatchError, IsOfflineError, SyncBackend } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Fiber, Option, Queue, Stream, TestClock } from '@livestore/utils/effect'
import { describe, expect, it } from '@effect/vitest'

import { makeActionClient } from '../../src/client/action-client.ts'
import { resolveRivetSyncOptions, type RivetSyncOptions } from '../../src/client/options.ts'
import { type BackendIdHelper, makePull, type PullDeps } from '../../src/client/pull.ts'
import type { ConnStatus } from '../../src/client/types.ts'
import {
  ACTION_PULL,
  decodePullRequest,
  emptyPullResponse,
  encodePullResponse,
  PullResponse,
  type PullRequest,
  pullResponseToItem,
  type SyncMetadata,
} from '../../src/common/mod.ts'
import { makeEventFactory } from '../harness/events.ts'
import { type FakeConnection, makeFakeConnection } from '../harness/fake-connection.ts'

const STORE_ID = 'store-1'
const CLIENT_ID = 'client-1'
const BACKEND_ID = 'backend-1'

type Item = SyncBackend.PullResItem<SyncMetadata>
type Event = LiveStoreEvent.Global.Encoded

/** Lets forked stream fibers process what was just published (real macrotask; the TestClock stays put). */
const settle = Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)))

/** Simulates the CBOR transport. */
const overTheWire = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

const seqNums = (item: Item) => item.batch.map((b) => b.eventEncoded.seqNum as number)

/** `n` chained events starting at seqNum 1. */
const makeEvents = (n: number): Array<Event> => {
  const factory = makeEventFactory()
  return Array.from({ length: n }, (_, i) => factory.todoCreated.next({ id: `t${i + 1}`, text: `todo ${i + 1}`, completed: false }))
}

const response = (batch: ReadonlyArray<Event>, pageInfo: SyncBackend.PullResPageInfo, backendId = BACKEND_ID) =>
  PullResponse.make({
    batch: batch.map((eventEncoded) => ({ eventEncoded, metadata: Option.none() })),
    pageInfo,
    backendId,
  })

const makeFakeBackendIdHelper = (initial: Option.Option<string>) => {
  let current = initial
  const helper: BackendIdHelper = {
    get: () => current,
    lazySet: (id) =>
      Effect.sync(() => {
        current = Option.some(id)
      }),
  }
  return helper
}

/**
 * In-memory server: answers `Pull` from a mutable log with server-side paging
 * semantics (`MoreKnown(remaining)` / `NoMore`, one `NoMore` page for an
 * empty history). `failNext` injects transient failures; `beforeRespond`
 * lets a test act (e.g. broadcast) while a request is in flight.
 */
const serveLog = (fake: FakeConnection, log: Array<Event>, backendId = BACKEND_ID) => {
  const state = {
    failures: [] as Array<IsOfflineError>,
    beforeRespond: undefined as ((req: PullRequest) => Effect.Effect<void>) | undefined,
  }
  fake.onAction(ACTION_PULL, (payload) =>
    Effect.gen(function* () {
      const req = decodePullRequest(payload)
      const failure = state.failures.shift()
      if (failure !== undefined) return yield* Effect.fail(failure)
      if (state.beforeRespond !== undefined) yield* state.beforeRespond(req)
      const from = Option.match(req.cursor, { onNone: () => 0, onSome: (c) => c.eventSequenceNumber as number })
      const pending = log.filter((e) => e.seqNum > from)
      const limit = req.limit ?? 100
      const page = pending.slice(0, limit)
      const remaining = pending.length - page.length
      const res =
        page.length === 0
          ? emptyPullResponse(backendId)
          : response(page, remaining > 0 ? SyncBackend.pageInfoMoreKnown(remaining) : SyncBackend.pageInfoNoMore, backendId)
      return overTheWire(encodePullResponse(res))
    }),
  )
  return {
    failNext: (n: number) => {
      for (let i = 0; i < n; i++) state.failures.push(new IsOfflineError({ cause: new Error('simulated offline') }))
    },
    setBeforeRespond: (f: (req: PullRequest) => Effect.Effect<void>) => {
      state.beforeRespond = f
    },
  }
}

const pullCalls = (fake: FakeConnection) => fake.calls.filter((c) => c.name === ACTION_PULL).map((c) => decodePullRequest(c.payload))

const setup = (config: { readonly initialStatus?: ConnStatus; readonly backendId?: Option.Option<string>; readonly options?: Partial<RivetSyncOptions>; readonly payload?: PullDeps['payload'] } = {}) =>
  Effect.gen(function* () {
    const fake = yield* makeFakeConnection({ initialStatus: config.initialStatus ?? 'connected' })
    const helper = makeFakeBackendIdHelper(config.backendId ?? Option.none())
    const options = resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420', pullPageSize: 2, ...config.options })
    const pull = makePull({
      conn: fake.conn,
      actions: makeActionClient(fake.conn),
      backendIdHelper: helper,
      storeId: STORE_ID,
      clientId: CLIENT_ID,
      payload: config.payload,
      options,
    })
    return { fake, helper, options, pull }
  })

const emit = (fake: FakeConnection, batch: ReadonlyArray<Event>, backendId = BACKEND_ID) =>
  fake.emitPullEvent(overTheWire(encodePullResponse(response(batch, SyncBackend.pageInfoNoMore, backendId))))

/** Runs a live stream into a queue in the background; `takeN` awaits items deterministically. */
const runLive = (stream: Stream.Stream<Item, unknown>) =>
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<Item>()
    const fiber = yield* Effect.forkChild(Stream.runForEach(stream, (item) => Queue.offer(queue, item)))
    const takeN = (n: number) => Effect.forEach(Array.from({ length: n }), () => Queue.take(queue))
    const assertNoMore = Effect.gen(function* () {
      yield* settle
      expect(yield* Queue.size(queue)).toBe(0)
    })
    return { queue, fiber, takeN, assertNoMore }
  })

describe('makePull', () => {
  it.effect('non-live: forwards catch-up pages verbatim with the right cursors and ends after NoMore', () =>
    Effect.gen(function* () {
      const { fake, pull, helper } = yield* setup()
      const log = makeEvents(3)
      serveLog(fake, log)

      const items = yield* Stream.runCollect(pull(Option.none()))

      expect(items).toEqual([
        pullResponseToItem(response([log[0]!, log[1]!], SyncBackend.pageInfoMoreKnown(1))),
        pullResponseToItem(response([log[2]!], SyncBackend.pageInfoNoMore)),
      ])
      expect(items.map(seqNums)).toEqual([[1, 2], [3]])
      expect(helper.get()).toEqual(Option.some(BACKEND_ID))

      const calls = pullCalls(fake)
      expect(calls).toHaveLength(2)
      expect(calls[0]!.cursor).toEqual(Option.none())
      expect(calls[1]!.cursor).toEqual(
        Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(2), backendId: Option.some(BACKEND_ID) }),
      )
      for (const call of calls) {
        expect(call.limit).toBe(2)
        expect(call.storeId).toBe(STORE_ID)
        expect(call.clientId).toBe(CLIENT_ID)
      }
      for (const call of fake.calls) expect('payload' in (call.payload as object)).toBe(false)
    }),
  )

  it.effect('sends the sync payload when configured and a Some cursor from the given cursor', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup({ payload: { token: 'abc' }, backendId: Option.some(BACKEND_ID) })
      serveLog(fake, makeEvents(3))

      const items = yield* Stream.runCollect(
        pull(Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(2), metadata: Option.none() })),
      )
      expect(items.map(seqNums)).toEqual([[3]])
      const [call] = pullCalls(fake)
      expect(call!.payload).toEqual({ token: 'abc' })
      expect(call!.cursor).toEqual(
        Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(2), backendId: Option.some(BACKEND_ID) }),
      )
    }),
  )

  it.effect('live: empty history yields exactly one empty NoMore item and stays open', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      serveLog(fake, [])

      const live = yield* runLive(pull(Option.none(), { live: true }))
      const [first] = yield* live.takeN(1)
      expect(first).toEqual(SyncBackend.pullResItemEmpty())
      yield* live.assertNoMore
      expect(live.fiber.pollUnsafe()).toBeUndefined()
      expect(pullCalls(fake)).toHaveLength(1)
    }),
  )

  it.effect('live: dedupes events already seen and emits only the fresh tail', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const log = makeEvents(5)
      serveLog(fake, log.slice(0, 4))

      const live = yield* runLive(pull(Option.none(), { live: true }))
      const initial = yield* live.takeN(2)
      expect(initial.map(seqNums)).toEqual([[1, 2], [3, 4]])

      yield* emit(fake, [log[2]!, log[3]!, log[4]!])
      const [item] = yield* live.takeN(1)
      expect(seqNums(item!)).toEqual([5])
      expect(item!.pageInfo).toEqual(SyncBackend.pageInfoNoMore)

      // A pure duplicate is dropped entirely.
      yield* emit(fake, [log[4]!])
      yield* live.assertNoMore
      expect(pullCalls(fake)).toHaveLength(2)
    }),
  )

  it.effect('live: re-catches-up from lastSeen exactly once per reconnect and filters overlapping events', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const log = makeEvents(6)
      const served = log.slice(0, 4)
      serveLog(fake, served)

      const live = yield* runLive(pull(Option.none(), { live: true }))
      yield* live.takeN(2)
      expect(pullCalls(fake)).toHaveLength(2)

      // Event 5 lands while we are away.
      served.push(log[4]!)
      yield* fake.setStatus('disconnected')
      yield* fake.setStatus('connected')

      const [recovered] = yield* live.takeN(1)
      expect(seqNums(recovered!)).toEqual([5])
      const calls = pullCalls(fake)
      expect(calls).toHaveLength(3)
      expect(calls[2]!.cursor).toEqual(
        Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(4), backendId: Option.some(BACKEND_ID) }),
      )

      // The broadcast of 5 arriving late is filtered; 6 flows through.
      yield* emit(fake, [log[4]!])
      yield* emit(fake, [log[4]!, log[5]!])
      const [next] = yield* live.takeN(1)
      expect(seqNums(next!)).toEqual([6])
      yield* live.assertNoMore
      expect(pullCalls(fake)).toHaveLength(3)
    }),
  )

  it.effect('live: a status change that is not `connected` does not trigger a catch-up', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      serveLog(fake, makeEvents(1))
      const live = yield* runLive(pull(Option.none(), { live: true }))
      yield* live.takeN(1)
      yield* fake.setStatus('disconnected')
      yield* fake.setStatus('connecting')
      yield* live.assertNoMore
      expect(pullCalls(fake)).toHaveLength(1)
    }),
  )

  it.effect('live: repairs a gap (missed broadcast) by catching up in order without duplicates', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const log = makeEvents(4)
      const served = log.slice(0, 2)
      serveLog(fake, served)

      const live = yield* runLive(pull(Option.none(), { live: true }))
      const initial = yield* live.takeN(1)
      expect(initial.map(seqNums)).toEqual([[1, 2]])

      // Server appended 3 and 4 but we only see the broadcast of 4.
      served.push(log[2]!, log[3]!)
      yield* emit(fake, [log[3]!])

      const [repaired] = yield* live.takeN(1)
      expect(seqNums(repaired!)).toEqual([3, 4])
      expect(repaired!.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
      const calls = pullCalls(fake)
      expect(calls).toHaveLength(2)
      expect(calls[1]!.cursor).toEqual(
        Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(2), backendId: Option.some(BACKEND_ID) }),
      )

      // The late broadcast of 3 is now a duplicate.
      yield* emit(fake, [log[2]!])
      yield* live.assertNoMore
    }),
  )

  it.effect('live: a foreign backendId on a live event fails the stream with BackendIdMismatchError', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const log = makeEvents(2)
      serveLog(fake, log)

      const fiber = yield* Effect.forkChild(Effect.flip(Stream.runDrain(pull(Option.none(), { live: true }))))
      yield* settle
      yield* emit(fake, [log[1]!], 'backend-2')

      const error = yield* Fiber.join(fiber)
      expect(error).toBeInstanceOf(BackendIdMismatchError)
      expect(error).toMatchObject({ expected: BACKEND_ID, received: 'backend-2' })
    }),
  )

  it.effect('catch-up: a page from a foreign backendId fails with BackendIdMismatchError', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup({ backendId: Option.some('backend-old') })
      serveLog(fake, makeEvents(1), 'backend-new')

      const error = yield* Effect.flip(Stream.runCollect(pull(Option.none())))
      expect(error).toBeInstanceOf(BackendIdMismatchError)
      expect(error).toMatchObject({ expected: 'backend-old', received: 'backend-new' })
    }),
  )

  it.effect('live: a transiently failing re-catch-up is retried with backoff and the stream survives', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup({ options: { reconnect: { baseDelay: '1 second', maxDelay: '30 seconds' } } })
      const log = makeEvents(3)
      const served = log.slice(0, 2)
      const server = serveLog(fake, served)

      const live = yield* runLive(pull(Option.none(), { live: true }))
      yield* live.takeN(1)
      expect(pullCalls(fake)).toHaveLength(1)

      served.push(log[2]!)
      server.failNext(2)
      yield* fake.setStatus('disconnected')
      yield* fake.setStatus('connected')
      yield* settle
      // First attempt failed; retry is sleeping (~1 s jittered).
      expect(pullCalls(fake)).toHaveLength(2)
      yield* TestClock.adjust('1300 millis')
      yield* settle
      // Second attempt failed; retry is sleeping (~2 s jittered).
      expect(pullCalls(fake)).toHaveLength(3)
      yield* TestClock.adjust('2600 millis')

      const [recovered] = yield* live.takeN(1)
      expect(seqNums(recovered!)).toEqual([3])
      expect(pullCalls(fake)).toHaveLength(4)
      expect(live.fiber.pollUnsafe()).toBeUndefined()
    }),
  )

  it.effect('live: the initial catch-up is not retried internally (IsOfflineError propagates)', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const server = serveLog(fake, makeEvents(1))
      server.failNext(1)
      const error = yield* Effect.flip(Stream.runDrain(pull(Option.none(), { live: true })))
      expect(error).toBeInstanceOf(IsOfflineError)
    }),
  )

  it.effect('live: events published during the initial catch-up are buffered, ordered and deduped', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      const log = makeEvents(4)
      const server = serveLog(fake, log.slice(0, 3))
      // While page 2 is in flight the server broadcasts 3 (which page 2 also contains) and 4.
      server.setBeforeRespond((req) =>
        Option.isSome(req.cursor) ? Effect.andThen(emit(fake, [log[2]!]), emit(fake, [log[3]!])) : Effect.void,
      )

      const live = yield* runLive(pull(Option.none(), { live: true }))
      const items = yield* live.takeN(3)
      expect(items.map(seqNums)).toEqual([[1, 2], [3], [4]])
      yield* live.assertNoMore
      expect(pullCalls(fake)).toHaveLength(2)
    }),
  )

  it.effect('waits for the connection before issuing the first Pull', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup({ initialStatus: 'connecting' })
      serveLog(fake, makeEvents(1))

      const live = yield* runLive(pull(Option.none(), { live: true }))
      yield* settle
      expect(fake.calls).toHaveLength(0)

      yield* fake.setStatus('connected')
      const [first] = yield* live.takeN(1)
      expect(seqNums(first!)).toEqual([1])
      expect(pullCalls(fake)).toHaveLength(1)
    }),
  )

  it.effect('non-live: a status change to connected after completion has no effect', () =>
    Effect.gen(function* () {
      const { fake, pull } = yield* setup()
      serveLog(fake, makeEvents(1))
      yield* Stream.runCollect(pull(Option.none()))
      yield* fake.setStatus('disconnected')
      yield* fake.setStatus('connected')
      yield* settle
      expect(pullCalls(fake)).toHaveLength(1)
    }),
  )
})
