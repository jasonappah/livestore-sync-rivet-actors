/**
 * Unit coverage for the `Push` handler's admission logic: request checks
 * that run before the gate, the serialized head check + append under the
 * gate, and the hook sequence around it. Fan-out behaviour lives in
 * `connections.test.ts`.
 */

import type { BackendIdMismatchError, ServerAheadError, UnknownError } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import { Deferred, Effect, Exit, Fiber, Option } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import type { InvalidPayloadError, PushAck } from '../../common/mod.ts'
import { makePush, PUSH_BATCH_NOT_CHAINED_NOTE, PUSH_BATCH_TOO_LARGE_NOTE } from '../push.ts'
import { STORE_ID_MISMATCH_REASON } from '../validate-payload.ts'
import { chainedEvents, expectFailure, expectSuccess, makeTestCtx, pushReq, runExit } from './push-test-utils.ts'

/** Follows `.cause` links to the innermost error and returns its message. */
const rootMessage = (error: unknown): string => {
  let current: unknown = error
  while (typeof current === 'object' && current !== null && 'cause' in current && current.cause !== undefined) {
    current = current.cause
  }
  return current instanceof Error ? current.message : String(current)
}

describe('makePush: admission', () => {
  it('rejects a batch whose first parent is not the head with ServerAheadError', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const error = expectFailure(await runExit(push(pushReq(t.storeId, chainedEvents(2, 5))))) as ServerAheadError

    expect(error._tag).toBe('ServerAheadError')
    expect(error.minimumExpectedNum).toBe(0)
    expect(error.providedNum).toBe(5)
    expect(t.rowCount()).toBe(0)
    expect(t.ctx.headRef.current).toBe(EventSequenceNumber.Client.ROOT.global)
    expect(t.persistedHead()).toBe(0)
  })

  it('admits exactly one of 8 concurrent pushes sharing the same parent', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const contenders = Array.from({ length: 8 }, (_, index) => chainedEvents(1, 'root', `contender-${index}`))

    const results = await Effect.runPromise(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>()

        const racing = yield* Effect.forkChild(
          Effect.forEach(
            contenders,
            (batch) =>
              Deferred.await(release).pipe(
                Effect.andThen(push(pushReq(t.storeId, batch, { clientId: batch[0]!.clientId }))),
                Effect.exit,
              ),
            { concurrency: 'unbounded' },
          ),
        )

        yield* Deferred.succeed(release, undefined)
        return yield* Fiber.join(racing)
      }),
    )

    const successes = results.filter(Exit.isSuccess)
    const failures = results.filter(Exit.isFailure)

    expect(successes).toHaveLength(1)
    expect(failures).toHaveLength(7)
    for (const failure of failures) {
      const error = expectFailure(failure) as ServerAheadError
      expect(error._tag).toBe('ServerAheadError')
      expect(error.minimumExpectedNum).toBe(1)
      expect(error.providedNum).toBe(0)
    }

    expect(t.rowCount()).toBe(1)
    expect(t.ctx.headRef.current).toBe(1)
    expect(t.persistedHead()).toBe(1)
  })

  it('persists a chained batch, advances the head and accepts a follow-up push', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const ack = expectSuccess(await runExit(push(pushReq(t.storeId, chainedEvents(3)))))

    expect(ack).toEqual({ backendId: t.ctx.backendId })
    expect(t.rowCount()).toBe(3)
    expect(t.ctx.headRef.current).toBe(3)
    expect(t.persistedHead()).toBe(3)

    const rows = t.raw.prepare('SELECT seqNum, parentSeqNum, name, clientId FROM eventlog_v1 ORDER BY seqNum').all()
    expect(rows.map((row) => ({ ...row }))).toEqual([
      { seqNum: 1, parentSeqNum: 0, name: 'todo.created', clientId: 'test-client' },
      { seqNum: 2, parentSeqNum: 1, name: 'todo.created', clientId: 'test-client' },
      { seqNum: 3, parentSeqNum: 2, name: 'todo.created', clientId: 'test-client' },
    ])

    expectSuccess(await runExit(push(pushReq(t.storeId, chainedEvents(2, 3)))))

    expect(t.rowCount()).toBe(5)
    expect(t.ctx.headRef.current).toBe(5)
    expect(t.persistedHead()).toBe(5)
  })
})

describe('makePush: request checks', () => {
  it('rejects a batch whose events do not form a contiguous chain', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const [a] = chainedEvents(1, 'root', 'a')
    const [b] = chainedEvents(1, 'root', 'b')

    const error = expectFailure(await runExit(push(pushReq(t.storeId, [a!, b!])))) as UnknownError

    expect(error._tag).toBe('UnknownError')
    expect(error.note).toBe(PUSH_BATCH_NOT_CHAINED_NOTE)
    expect(t.rowCount()).toBe(0)
  })

  it('rejects an event whose seqNum does not advance past its parent', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const [event] = chainedEvents(1)
    const stuck = { ...event!, seqNum: EventSequenceNumber.Global.make(0) }

    const error = expectFailure(await runExit(push(pushReq(t.storeId, [stuck])))) as UnknownError

    expect(error._tag).toBe('UnknownError')
    expect(error.note).toBe(PUSH_BATCH_NOT_CHAINED_NOTE)
  })

  it('rejects a batch larger than maxPushEventsPerRequest', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const error = expectFailure(await runExit(push(pushReq(t.storeId, chainedEvents(101))))) as UnknownError

    expect(error._tag).toBe('UnknownError')
    expect(error.note).toBe(PUSH_BATCH_TOO_LARGE_NOTE)
    expect(t.rowCount()).toBe(0)
    expect(t.ctx.headRef.current).toBe(0)
  })

  it('acks an empty batch without touching validation or hooks', async () => {
    const calls: string[] = []
    const t = await makeTestCtx({
      validatePayload: () => {
        calls.push('validatePayload')
      },
      onPush: () => {
        calls.push('onPush')
      },
      onPushRes: () => {
        calls.push('onPushRes')
      },
    })
    const push = makePush(t.ctx)

    // Even a mismatching storeId is accepted: nothing about the request is inspected.
    const ack = expectSuccess(await runExit(push(pushReq('some-other-store', []))))

    expect(ack).toEqual({ backendId: t.ctx.backendId })
    expect(calls).toEqual([])
  })

  it('rejects a foreign backendId with BackendIdMismatchError', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const error = expectFailure(
      await runExit(push(pushReq(t.storeId, chainedEvents(1), { backendId: Option.some('other') }))),
    ) as BackendIdMismatchError

    expect(error._tag).toBe('BackendIdMismatchError')
    expect(error.expected).toBe(t.ctx.backendId)
    expect(error.received).toBe('other')
    expect(t.rowCount()).toBe(0)
  })

  it('accepts a matching backendId', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    expectSuccess(
      await runExit(push(pushReq(t.storeId, chainedEvents(1), { backendId: Option.some(t.ctx.backendId) }))),
    )
    expect(t.rowCount()).toBe(1)
  })

  it('rejects a storeId that does not match the actor key with InvalidPayloadError', async () => {
    const t = await makeTestCtx()
    const push = makePush(t.ctx)

    const error = expectFailure(await runExit(push(pushReq('other-store', chainedEvents(1))))) as InvalidPayloadError

    expect(error._tag).toBe('InvalidPayloadError')
    expect(error.reason).toBe(STORE_ID_MISMATCH_REASON)
    expect(t.rowCount()).toBe(0)
  })

  it('rejects when validatePayload rejects, before onPush runs', async () => {
    const calls: string[] = []
    const t = await makeTestCtx({
      validatePayload: () => {
        throw new Error('nope')
      },
      onPush: () => {
        calls.push('onPush')
      },
    })
    const push = makePush(t.ctx)

    const error = expectFailure(await runExit(push(pushReq(t.storeId, chainedEvents(1))))) as InvalidPayloadError

    expect(error._tag).toBe('InvalidPayloadError')
    expect(calls).toEqual([])
  })
})

describe('makePush: hooks', () => {
  it('runs onPush → onPullRes (per chunk) → onPushRes(ack) on success', async () => {
    const calls: Array<[string, unknown]> = []
    const t = await makeTestCtx({
      onPush: (message, ctx) => {
        calls.push(['onPush', { batchLength: message.batch.length, ctx }])
      },
      onPullRes: (message) => {
        calls.push(['onPullRes', message])
      },
      onPushRes: (message) => {
        calls.push(['onPushRes', message])
      },
    })
    const push = makePush(t.ctx)

    const batch = chainedEvents(2)
    const ack = expectSuccess(
      await runExit(push(pushReq(t.storeId, batch, { payload: { token: 'abc' } }))),
    ) as PushAck

    expect(calls.map(([name]) => name)).toEqual(['onPush', 'onPullRes', 'onPushRes'])
    expect(calls[0]![1]).toEqual({
      batchLength: 2,
      ctx: { storeId: t.storeId, clientId: 'client-1', payload: { token: 'abc' } },
    })

    const pullRes = calls[1]![1] as { batch: Array<{ eventEncoded: unknown; metadata: Option.Option<unknown> }> }
    expect(pullRes.batch.map((item) => item.eventEncoded)).toEqual(batch)
    expect(pullRes.batch.every((item) => Option.isSome(item.metadata))).toBe(true)

    expect(calls[2]![1]).toBe(ack)
  })

  it('omits the payload key from the onPush context when the request has none', async () => {
    const seen: unknown[] = []
    const t = await makeTestCtx({
      onPush: (_message, ctx) => {
        seen.push(ctx)
      },
    })

    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1)))))

    expect(seen).toEqual([{ storeId: t.storeId, clientId: 'client-1' }])
    expect(Object.hasOwn(seen[0] as object, 'payload')).toBe(false)
  })

  it('hands an UnknownError to onPushRes', async () => {
    const calls: Array<[string, unknown]> = []
    const t = await makeTestCtx({
      onPush: () => {
        calls.push(['onPush', undefined])
      },
      onPushRes: (message) => {
        calls.push(['onPushRes', message])
      },
    })
    const push = makePush(t.ctx)

    const [a] = chainedEvents(1, 'root', 'a')
    const [b] = chainedEvents(1, 'root', 'b')
    const error = expectFailure(await runExit(push(pushReq(t.storeId, [a!, b!])))) as UnknownError

    expect(calls.map(([name]) => name)).toEqual(['onPush', 'onPushRes'])
    expect(calls[1]![1]).toBe(error)
  })

  it('does not call onPushRes for ServerAheadError', async () => {
    const calls: string[] = []
    const t = await makeTestCtx({
      onPushRes: () => {
        calls.push('onPushRes')
      },
    })

    expectFailure(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1, 7)))))

    expect(calls).toEqual([])
  })

  it('surfaces a throwing onPush as UnknownError and still reports it to onPushRes', async () => {
    const seen: unknown[] = []
    const t = await makeTestCtx({
      onPush: () => {
        throw new Error('hook boom')
      },
      onPushRes: (message) => {
        seen.push(message)
      },
    })

    const error = expectFailure(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1))))) as UnknownError

    expect(error._tag).toBe('UnknownError')
    // `trySyncOrPromiseOrEffect` wraps the throw in effect's own `Cause.UnknownError`
    // before `mapToUnknownError` wraps it in LiveStore's — walk to the root.
    expect(rootMessage(error)).toContain('hook boom')
    expect(seen).toEqual([error])
    expect(t.rowCount()).toBe(0)
  })
})
