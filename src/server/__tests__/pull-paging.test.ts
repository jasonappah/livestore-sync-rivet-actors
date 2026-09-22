/**
 * Unit coverage for the `Pull` handler (`src/server/pull.ts`): paging,
 * `remaining` accounting, cursor/backendId semantics, the byte guard and the
 * `onPull` / `onPullRes` hooks.
 *
 * No Rivet engine is involved — `makeFakeDb()` backs storage with
 * `node:sqlite`. Store ids are still unique per test because the Rivet engine
 * used by the conformance suite persists actor state across runs.
 */

import { SyncBackend } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Cause, Effect, Exit, Option } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { describe, expect, it } from 'vitest'

import { makeEventFactory } from '../../../tests/harness/events.ts'
import {
  emptyPullResponse,
  type PullResponse,
  PullRequest,
  pullResponseToItem,
  type SyncMetadata,
} from '../../common/mod.ts'
import type { LiveStoreSyncActorOptions } from '../options.ts'
import { makePull } from '../pull.ts'
import { makeSyncStorage, migrate, type SyncStorage } from '../sqlite.ts'
import { makeStoreCtx, type RawConn, type StoreCtx } from '../store-ctx.ts'
import { makeFakeDb } from './fake-raw-access.ts'

const CLIENT_ID = 'client-1'
const CREATED_AT = '2026-01-01T00:00:00.000Z'

const noConns = (): Iterable<RawConn> => []

type Harness = {
  readonly storeId: string
  readonly ctx: StoreCtx<any>
  readonly storage: SyncStorage
  readonly pull: (req: PullRequest) => Effect.Effect<PullResponse, unknown>
  readonly seed: (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>) => Promise<void>
}

const setup = async (options: LiveStoreSyncActorOptions<any> = {}): Promise<Harness> => {
  const fake = makeFakeDb()
  await migrate(fake.db)
  const storage = makeSyncStorage(fake.db)
  // Unique per test: the conformance engine persists actor state across runs.
  const storeId = `store-${nanoid()}`

  const ctx = await Effect.runPromise(
    makeStoreCtx({ key: [storeId], db: fake.db, conns: noConns, log: () => {}, options }),
  )

  const seed = (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>) =>
    Effect.runPromise(
      storage.appendEventsAndUpdateHead(batch, CREATED_AT, {
        storeId,
        backendId: ctx.backendId,
        newHead: batch.at(-1)!.seqNum,
      }),
    )

  return { storeId, ctx, storage, pull: makePull(ctx), seed }
}

const makeEvents = (count: number, text: (index: number) => string = (index) => `t${index}`) => {
  const factory = makeEventFactory()
  return Array.from({ length: count }, (_, index) =>
    factory.todoCreated.next({ id: `t${index}`, text: text(index), completed: false }),
  )
}

const req = (
  h: Harness,
  overrides: {
    readonly storeId?: string
    readonly cursor?: PullRequest['cursor']
    readonly limit?: number
    readonly payload?: PullRequest['payload']
  } = {},
): PullRequest =>
  PullRequest.make({
    storeId: overrides.storeId ?? h.storeId,
    clientId: CLIENT_ID,
    cursor: overrides.cursor ?? Option.none(),
    ...(overrides.limit !== undefined ? { limit: overrides.limit } : {}),
    ...(overrides.payload !== undefined ? { payload: overrides.payload } : {}),
  })

const cursorAt = (seqNum: number, backendId: Option.Option<string>) =>
  Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(seqNum), backendId })

/** Next cursor the client would derive, exactly as `SyncBackend` does. */
const nextCursor = (res: PullResponse) =>
  Option.map(SyncBackend.cursorFromPullResItem(pullResponseToItem(res)), (cursor) => ({
    eventSequenceNumber: cursor.eventSequenceNumber,
    backendId: Option.some(res.backendId),
  }))

const expectFailure = async (effect: Effect.Effect<unknown, unknown>): Promise<any> => {
  const exit = await Effect.runPromiseExit(effect)
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit) === false) throw new Error('expected a failure')
  return Cause.squash(exit.cause)
}

/** Drains every page a client would fetch, following the returned cursors. */
const drain = async (h: Harness, first: PullRequest) => {
  const pages: PullResponse[] = []
  let request = first

  for (let guard = 0; guard < 100; guard += 1) {
    const res = await Effect.runPromise(h.pull(request))
    pages.push(res)
    if (res.pageInfo._tag === 'NoMore') return pages

    const cursor = nextCursor(res)
    request = PullRequest.make({ ...request, cursor })
  }

  throw new Error('pull did not terminate')
}

// -----------------------------------------------------------------------------

describe('makePull', () => {
  it('returns exactly one empty NoMore page for an empty store', async () => {
    const h = await setup()

    const res = await Effect.runPromise(h.pull(req(h)))

    expect(res).toEqual(emptyPullResponse(h.ctx.backendId))
    expect(res.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
  })

  it('pages 250 events at pullPageSize 100 with MoreKnown remaining counts', async () => {
    const h = await setup({ pullPageSize: 100 })
    await h.seed(makeEvents(250))

    const pages = await drain(h, req(h))

    expect(pages.map((page) => page.batch.length)).toEqual([100, 100, 50])
    expect(pages.map((page) => page.pageInfo)).toEqual([
      SyncBackend.pageInfoMoreKnown(150),
      SyncBackend.pageInfoMoreKnown(50),
      SyncBackend.pageInfoNoMore,
    ])

    const seqNums = pages.flatMap((page) => page.batch.map((item) => item.eventEncoded.seqNum))
    expect(seqNums).toEqual(Array.from({ length: 250 }, (_, index) => index + 1))
  })

  it('every page carries the backendId and a pageInfo', async () => {
    const h = await setup({ pullPageSize: 100 })
    await h.seed(makeEvents(250))

    const pages = await drain(h, req(h))

    for (const page of pages) {
      expect(page.backendId).toBe(h.ctx.backendId)
      expect(['NoMore', 'MoreKnown', 'MoreUnknown']).toContain(page.pageInfo._tag)
    }
  })

  it('honours a request limit below the page size', async () => {
    const h = await setup({ pullPageSize: 100 })
    await h.seed(makeEvents(250))

    const res = await Effect.runPromise(h.pull(req(h, { limit: 10 })))

    expect(res.batch.length).toBe(10)
    expect(res.pageInfo).toEqual(SyncBackend.pageInfoMoreKnown(240))
  })

  it('clamps a request limit above the server page size', async () => {
    const h = await setup({ pullPageSize: 100 })
    await h.seed(makeEvents(250))

    const res = await Effect.runPromise(h.pull(req(h, { limit: 1000 })))

    expect(res.batch.length).toBe(100)
    expect(res.pageInfo).toEqual(SyncBackend.pageInfoMoreKnown(150))
  })

  it('returns an empty NoMore page for a cursor at the head', async () => {
    const h = await setup()
    await h.seed(makeEvents(5))

    const res = await Effect.runPromise(h.pull(req(h, { cursor: cursorAt(5, Option.some(h.ctx.backendId)) })))

    expect(res).toEqual(emptyPullResponse(h.ctx.backendId))
  })

  it('attaches Some(SyncMetadata) with the stored createdAt to every row', async () => {
    const h = await setup()
    await h.seed(makeEvents(3))

    const res = await Effect.runPromise(h.pull(req(h)))

    for (const item of res.batch) {
      expect(Option.isSome(item.metadata)).toBe(true)
      const metadata = Option.getOrThrow(item.metadata) as SyncMetadata
      expect(metadata._tag).toBe('SyncMessage.SyncMetadata')
      expect(metadata.createdAt).toBe(CREATED_AT)
    }
  })

  // ---------------------------------------------------------------------------
  // backendId
  // ---------------------------------------------------------------------------

  it('fails with BackendIdMismatchError for a foreign cursor backendId', async () => {
    const h = await setup()
    await h.seed(makeEvents(3))

    const error = await expectFailure(h.pull(req(h, { cursor: cursorAt(1, Option.some('bogus')) })))

    expect(error._tag).toBe('BackendIdMismatchError')
    expect(error.expected).toBe(h.ctx.backendId)
    expect(error.received).toBe('bogus')
  })

  it('accepts a cursor without a backendId', async () => {
    const h = await setup()
    await h.seed(makeEvents(3))

    const res = await Effect.runPromise(h.pull(req(h, { cursor: cursorAt(1, Option.none()) })))

    expect(res.batch.map((item) => item.eventEncoded.seqNum)).toEqual([2, 3])
    expect(res.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
  })

  // ---------------------------------------------------------------------------
  // Byte guard
  // ---------------------------------------------------------------------------

  it('shrinks a page to fit maxMessageBytes and keeps remaining consistent', async () => {
    const h = await setup({ pullPageSize: 100, maxMessageBytes: 350_000 })
    const big = 'x'.repeat(100_000)
    await h.seed(makeEvents(20, () => big))

    const first = await Effect.runPromise(h.pull(req(h)))

    expect(first.batch.length).toBeGreaterThan(0)
    expect(first.batch.length).toBeLessThan(20)
    expect(first.pageInfo).toEqual(SyncBackend.pageInfoMoreKnown(20 - first.batch.length))

    const pages = await drain(h, req(h))
    const seqNums = pages.flatMap((page) => page.batch.map((item) => item.eventEncoded.seqNum))

    expect(pages.at(-1)!.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
    expect(seqNums).toEqual(Array.from({ length: 20 }, (_, index) => index + 1))
  })

  it('fails with UnknownError naming the seqNum when a single event exceeds maxMessageBytes', async () => {
    const h = await setup({ maxMessageBytes: 900_000 })
    await h.seed(makeEvents(1, () => 'x'.repeat(1_000_000)))

    const error = await expectFailure(h.pull(req(h)))

    expect(error._tag).toBe('UnknownError')
    expect(error.note).toContain('exceeds maxMessageBytes')
    expect(error.note).toContain('event 1')
    expect(error.cause._tag).toBe('OversizeChunkItemError')
  })

  it('reports the oversize failure to onPullRes as an UnknownError', async () => {
    const seen: unknown[] = []
    const h = await setup({
      maxMessageBytes: 900_000,
      onPullRes: (message) => {
        seen.push(message)
      },
    })
    await h.seed(makeEvents(1, () => 'x'.repeat(1_000_000)))

    await expectFailure(h.pull(req(h)))

    expect(seen.length).toBe(1)
    expect((seen[0] as { _tag: string })._tag).toBe('UnknownError')
  })

  // ---------------------------------------------------------------------------
  // Validation
  // ---------------------------------------------------------------------------

  it('rejects a request whose storeId does not match the actor key', async () => {
    const h = await setup()

    const error = await expectFailure(h.pull(req(h, { storeId: 'someone-elses-store' })))

    expect(error._tag).toBe('InvalidPayloadError')
    expect(error.storeId).toBe('someone-elses-store')
  })

  it('rejects a request whose payload validatePayload refuses', async () => {
    const h = await setup({
      validatePayload: (payload: any) => {
        if (payload?.token !== 'good') throw new Error('nope')
      },
    })
    await h.seed(makeEvents(1))

    const error = await expectFailure(h.pull(req(h, { payload: { token: 'bad' } })))
    expect(error._tag).toBe('InvalidPayloadError')

    const res = await Effect.runPromise(h.pull(req(h, { payload: { token: 'good' } })))
    expect(res.batch.length).toBe(1)
  })

  // ---------------------------------------------------------------------------
  // Hooks
  // ---------------------------------------------------------------------------

  it('calls onPull before onPullRes with the request, caller context and response', async () => {
    const order: string[] = []
    const onPullArgs: unknown[][] = []
    const onPullResArgs: unknown[] = []

    const h = await setup({
      onPull: (message, callbackCtx) => {
        order.push('onPull')
        onPullArgs.push([message, callbackCtx])
      },
      onPullRes: (message) => {
        order.push('onPullRes')
        onPullResArgs.push(message)
      },
    })
    await h.seed(makeEvents(2))

    const request = req(h, { payload: { token: 'abc' } })
    const res = await Effect.runPromise(h.pull(request))

    expect(order).toEqual(['onPull', 'onPullRes'])
    expect(onPullArgs[0]![0]).toEqual(request)
    expect(onPullArgs[0]![1]).toEqual({ storeId: h.storeId, clientId: CLIENT_ID, payload: { token: 'abc' } })
    expect(onPullResArgs[0]).toBe(res)
  })

  it('omits the payload key from the callback context when the caller sent none', async () => {
    let callbackCtx: unknown
    const h = await setup({
      onPull: (_message, ctx) => {
        callbackCtx = ctx
      },
    })

    await Effect.runPromise(h.pull(req(h)))

    expect(callbackCtx).toEqual({ storeId: h.storeId, clientId: CLIENT_ID })
    expect(Object.hasOwn(callbackCtx as object, 'payload')).toBe(false)
  })

  it('does not run onPull when validation already rejected the caller', async () => {
    let called = 0
    const h = await setup({
      onPull: () => {
        called += 1
      },
    })

    await expectFailure(h.pull(req(h, { storeId: 'other' })))

    expect(called).toBe(0)
  })
})
