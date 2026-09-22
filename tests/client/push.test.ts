/**
 * Unit tests for `makePush` (T13) against the fake connection + the real
 * action client, so request encoding, chunking and error mapping are all
 * exercised end-to-end minus the rivetkit transport.
 */

import { ServerAheadError, UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Deferred, Effect, Fiber, Option, Result, Semaphore } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import { ACTION_PUSH, encodePushAck, encodePushError, PushAck } from '../../src/common/mod.ts'
import { makeActionClient } from '../../src/client/action-client.ts'
import { ACTION_ERROR_ENVELOPE_TAG, ACTION_ERROR_ENVELOPE_VERSION } from '../../src/client/errors.ts'
import { resolveRivetSyncOptions, type RivetSyncOptions } from '../../src/client/options.ts'
import { type BackendIdHelper, makePush } from '../../src/client/push.ts'
import { RawActionFailure } from '../../src/client/types.ts'
import { makeEventFactory } from '../harness/events.ts'
import { type FakeConnection, makeFakeConnection } from '../harness/fake-connection.ts'

const seq = EventSequenceNumber.Global.make

/** Simulates the CBOR transport for values the server sends back. */
const overTheWire = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

const envelope = (error: unknown) => ({
  _tag: ACTION_ERROR_ENVELOPE_TAG,
  version: ACTION_ERROR_ENVELOPE_VERSION,
  error,
})

/**
 * Stand-in for `SyncBackend.makeBackendIdHelper` (which needs a
 * `KeyValueStore`). Same semantics: `get` is synchronous, `lazySet` only
 * writes on change.
 */
const makeFakeBackendIdHelper = (
  initial: Option.Option<string> = Option.none(),
): { readonly helper: BackendIdHelper; readonly writes: Array<string> } => {
  const ref = { current: initial }
  const writes: Array<string> = []
  return {
    helper: {
      get: () => ref.current,
      lazySet: (backendId: string) =>
        Effect.sync(() => {
          if (Option.getOrUndefined(ref.current) !== backendId) {
            ref.current = Option.some(backendId)
            writes.push(backendId)
          }
        }),
    },
    writes,
  }
}

const options = (overrides: Partial<RivetSyncOptions> = {}) =>
  resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420', ...overrides })

interface PushCtx {
  readonly fake: FakeConnection
  readonly push: ReturnType<typeof makePush>
  readonly backendIdHelper: BackendIdHelper
  readonly writes: Array<string>
}

const withPush = <A, E>(
  use: (ctx: PushCtx) => Effect.Effect<A, E>,
  config: {
    readonly options?: Partial<RivetSyncOptions>
    readonly initialBackendId?: Option.Option<string>
  } = {},
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fake = yield* makeFakeConnection()
      const { helper, writes } = makeFakeBackendIdHelper(config.initialBackendId ?? Option.none())
      const semaphore = yield* Semaphore.make(1)
      const push = makePush({
        actions: makeActionClient(fake.conn),
        backendIdHelper: helper,
        storeId: 's1',
        clientId: 'c1',
        payload: undefined,
        options: options(config.options ?? {}),
        semaphore,
      })
      return yield* use({ fake, push, backendIdHelper: helper, writes })
    }),
  )

/** Registers a handler that acks every push with `backendId`. */
const ackWith = (fake: FakeConnection, backendId: string): void => {
  fake.onAction(ACTION_PUSH, () => Effect.succeed(overTheWire(encodePushAck(PushAck.make({ backendId })))))
}

const manyEvents = (count: number): ReadonlyArray<LiveStoreEvent.Global.Encoded> => {
  const factory = makeEventFactory()
  return Array.from({ length: count }, (_, index) =>
    factory.todoCreated.next({ id: `t${index}`, text: `todo ${index}`, completed: false }),
  )
}

/** One event whose `args.text` is roughly `bytes` bytes long. */
const bigEvent = (index: number, bytes: number): LiveStoreEvent.Global.Encoded => ({
  name: 'todo.created',
  args: { id: `t${index}`, text: 'x'.repeat(bytes), completed: false },
  seqNum: seq(index + 1),
  parentSeqNum: seq(index),
  clientId: 'c1',
  sessionId: 'c1-session',
})

const byteSize = (value: unknown): number => new TextEncoder().encode(JSON.stringify(value)).byteLength

describe('makePush', () => {
  it('splits 150 events into 100 + 50 sequential Push calls carrying the helper backendId', async () => {
    const batch = manyEvents(150)

    const { calls, writes } = await withPush(
      ({ fake, push, writes }) =>
        Effect.gen(function* () {
          ackWith(fake, 'backend-1')
          yield* push(batch)
          return { calls: fake.calls, writes }
        }),
      { initialBackendId: Option.some('backend-1') },
    )

    expect(calls).toHaveLength(2)
    expect(calls.map((call) => call.name)).toEqual([ACTION_PUSH, ACTION_PUSH])

    const payloads = calls.map((call) => call.payload as Record<string, unknown>)
    const batches = payloads.map((payload) => payload.batch as ReadonlyArray<{ seqNum: number }>)
    expect(batches.map((chunk) => chunk.length)).toEqual([100, 50])
    // Chunks keep the original order and never overlap.
    expect(batches[0]![0]!.seqNum).toBe(1)
    expect(batches[0]!.at(-1)!.seqNum).toBe(100)
    expect(batches[1]![0]!.seqNum).toBe(101)
    expect(batches[1]!.at(-1)!.seqNum).toBe(150)

    for (const payload of payloads) {
      expect(payload.backendId).toEqual({ _tag: 'Some', value: 'backend-1' })
      expect(payload.storeId).toBe('s1')
      expect(payload.clientId).toBe('c1')
      // `payload` is an `optionalKey`: an absent sync payload must be an absent key.
      expect('payload' in payload).toBe(false)
    }

    // The ack repeated the id we already had, so nothing was written.
    expect(writes).toEqual([])
  })

  it('sends the backendId learned from the first ack on the next chunk', async () => {
    const { payloads, writes } = await withPush(({ fake, push, writes }) =>
      Effect.gen(function* () {
        ackWith(fake, 'backend-1')
        yield* push(manyEvents(150))
        return { payloads: fake.calls.map((call) => call.payload as Record<string, unknown>), writes }
      }),
    )

    expect(payloads[0]!.backendId).toEqual({ _tag: 'None' })
    expect(payloads[1]!.backendId).toEqual({ _tag: 'Some', value: 'backend-1' })
    expect(writes).toEqual(['backend-1'])
  })

  it('keeps every chunk within maxPushBytes', async () => {
    const maxPushBytes = 500_000
    const batch = Array.from({ length: 6 }, (_, index) => bigEvent(index, 200_000))

    const { calls } = await withPush(
      ({ fake, push }) =>
        Effect.gen(function* () {
          ackWith(fake, 'backend-1')
          yield* push(batch)
          return { calls: fake.calls }
        }),
      { options: { maxPushBytes } },
    )

    // 6 × ~200 KB against a 500 KB budget → 2 events per request.
    expect(calls).toHaveLength(3)
    for (const call of calls) {
      expect(byteSize(call.payload)).toBeLessThanOrEqual(maxPushBytes)
      expect((call.payload as { batch: ReadonlyArray<unknown> }).batch).toHaveLength(2)
    }
  })

  it('fails with UnknownError (and sends nothing) when a single event exceeds maxPushBytes', async () => {
    const { error, calls } = await withPush(
      ({ fake, push }) =>
        Effect.gen(function* () {
          ackWith(fake, 'backend-1')
          const result = yield* Effect.result(push([bigEvent(0, 200_000)]))
          if (Result.isSuccess(result)) throw new Error('expected the push to fail')
          return { error: result.failure, calls: fake.calls }
        }),
      { options: { maxPushBytes: 1000 } },
    )

    expect(error).toBeInstanceOf(UnknownError)
    expect((error as UnknownError).note).toBe('single event exceeds maxPushBytes')
    expect((error as UnknownError).cause).toMatchObject({ _tag: 'OversizeChunkItemError', maxBytes: 1000 })
    expect(calls).toHaveLength(0)
  })

  it('is a no-op for an empty batch', async () => {
    const calls = await withPush(({ fake, push }) =>
      Effect.gen(function* () {
        ackWith(fake, 'backend-1')
        yield* push([])
        return fake.calls
      }),
    )

    expect(calls).toHaveLength(0)
  })

  it('passes a declared ServerAheadError through unchanged and stops pushing', async () => {
    const { error, calls } = await withPush(({ fake, push }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PUSH, () =>
          Effect.fail(
            new RawActionFailure({
              cause: {
                group: 'user',
                code: 'ServerAheadError',
                message: 'server ahead',
                metadata: overTheWire(
                  envelope(encodePushError(new ServerAheadError({ minimumExpectedNum: seq(5), providedNum: seq(1) }))),
                ),
              },
              statusAtFailure: 'connected',
            }),
          ),
        )
        const result = yield* Effect.result(push(manyEvents(150)))
        if (Result.isSuccess(result)) throw new Error('expected the push to fail')
        return { error: result.failure, calls: fake.calls }
      }),
    )

    expect(error).toBeInstanceOf(ServerAheadError)
    expect(error).toMatchObject({ minimumExpectedNum: 5, providedNum: 1 })
    // The second chunk must not be attempted after the first one was rejected.
    expect(calls).toHaveLength(1)
  })

  it('serialises concurrent pushes through the semaphore', async () => {
    const order = await Effect.runPromise(
      Effect.gen(function* () {
        const fake = yield* makeFakeConnection()
        const { helper } = makeFakeBackendIdHelper()
        const semaphore = yield* Semaphore.make(1)
        const push = makePush({
          actions: makeActionClient(fake.conn),
          backendIdHelper: helper,
          storeId: 's1',
          clientId: 'c1',
          payload: undefined,
          options: options(),
          semaphore,
        })

        // The first push blocks inside its action until the gate opens; the
        // second one must not touch the wire before that.
        const gate = yield* Deferred.make<void>()
        const order: Array<string> = []
        fake.onAction(ACTION_PUSH, (payload) =>
          Effect.gen(function* () {
            const id = ((payload as { batch: ReadonlyArray<{ args: { id: string } }> }).batch[0]!.args as { id: string })
              .id
            order.push(id)
            if (id === 'first') yield* Deferred.await(gate)
            return overTheWire(encodePushAck(PushAck.make({ backendId: 'backend-1' })))
          }),
        )

        const event = (id: string): LiveStoreEvent.Global.Encoded => ({
          name: 'todo.created',
          args: { id, text: id, completed: false },
          seqNum: seq(1),
          parentSeqNum: seq(0),
          clientId: 'c1',
          sessionId: 'c1-session',
        })

        const firstFiber = yield* Effect.forkChild(push([event('first')]))
        // Give the second push every chance to overtake the first.
        const secondFiber = yield* Effect.forkChild(Effect.yieldNow.pipe(Effect.andThen(push([event('second')]))))
        yield* Effect.yieldNow
        yield* Effect.yieldNow
        const midFlight = [...order]

        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(firstFiber)
        yield* Fiber.join(secondFiber)

        return { midFlight, final: order, calls: fake.calls.length }
      }),
    )

    expect(order.midFlight).toEqual(['first'])
    expect(order.final).toEqual(['first', 'second'])
    expect(order.calls).toBe(2)
  })
})
