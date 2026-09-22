/**
 * Unit coverage for the `AdminInfo` / `AdminReset` handlers against the
 * in-memory SQLite fake and fake connections — no engine.
 */

import { EventSequenceNumber } from '@livestore/common/schema'
import { Deferred, Effect, Exit, Fiber, Option } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import { AdminUnauthorizedError, PERSISTENCE_FORMAT_VERSION } from '../../common/mod.ts'
import { ADMIN_DISABLED_NOTE, makeAdminInfo, makeAdminReset, secretsMatch, STORE_RESET_DISCONNECT_REASON } from '../admin.ts'
import { makePull } from '../pull.ts'
import { makePush } from '../push.ts'
import { makeStoreCtx } from '../store-ctx.ts'
import { STORE_ID_MISMATCH_REASON, VALIDATE_REJECTED_REASON } from '../validate-payload.ts'
import {
  chainedEvents,
  CLIENT_ID,
  expectFailure,
  expectSuccess,
  makeTestCtx,
  makeValidConn,
  pushReq,
  runExit,
} from './push-test-utils.ts'

const SECRET = 's3cret'
const ADMIN = { admin: { secret: SECRET } } as const

const adminReq = (storeId: string, adminSecret = SECRET) => ({ storeId, clientId: CLIENT_ID, adminSecret })

describe('secretsMatch', () => {
  it('compares in constant time and rejects length mismatches', () => {
    expect(secretsMatch('abc', 'abc')).toBe(true)
    expect(secretsMatch('abc', 'abd')).toBe(false)
    expect(secretsMatch('abc', 'abcd')).toBe(false)
    expect(secretsMatch('abc', '')).toBe(false)
    // Multi-byte characters are compared by bytes, not UTF-16 units.
    expect(secretsMatch('é', 'e')).toBe(false)
    expect(secretsMatch('é', 'é')).toBe(true)
  })
})

describe.each([
  ['AdminInfo', makeAdminInfo],
  ['AdminReset', makeAdminReset],
] as const)('%s gate', (_name, make) => {
  it('is refused when admin is not configured', async () => {
    const t = await makeTestCtx({})
    const error = expectFailure(await runExit(make(t.ctx)(adminReq(t.storeId))))
    expect(error._tag).toBe('UnknownError')
    expect((error as { note?: string }).note).toBe(ADMIN_DISABLED_NOTE)
  })

  it('treats an empty secret as disabled', async () => {
    const t = await makeTestCtx({ admin: { secret: '' } })
    const error = expectFailure(await runExit(make(t.ctx)(adminReq(t.storeId, ''))))
    expect((error as { note?: string }).note).toBe(ADMIN_DISABLED_NOTE)
  })

  it('rejects a wrong secret with AdminUnauthorizedError', async () => {
    const t = await makeTestCtx(ADMIN)
    const error = expectFailure(await runExit(make(t.ctx)(adminReq(t.storeId, 'nope'))))
    expect(error).toBeInstanceOf(AdminUnauthorizedError)
    expect(error).toMatchObject({ _tag: 'AdminUnauthorizedError', storeId: t.storeId })
    expect(t.logs.some((entry) => entry.level === 'warn' && entry.msg.includes('adminSecret'))).toBe(true)
  })

  it('still applies caller validation (storeId, validatePayload) before the secret', async () => {
    const t = await makeTestCtx({ ...ADMIN, validatePayload: () => Promise.reject(new Error('no')) })

    const mismatch = expectFailure(await runExit(make(t.ctx)(adminReq('other-store'))))
    expect(mismatch).toMatchObject({ _tag: 'InvalidPayloadError', reason: STORE_ID_MISMATCH_REASON })

    const rejected = expectFailure(await runExit(make(t.ctx)(adminReq(t.storeId))))
    expect(rejected).toMatchObject({ _tag: 'InvalidPayloadError', reason: VALIDATE_REJECTED_REASON })
  })
})

describe('AdminInfo', () => {
  it('reports head, backendId, event and connection counts', async () => {
    const t = await makeTestCtx(ADMIN)
    t.conns.push(makeValidConn('a', t.storeId), makeValidConn('b', t.storeId))

    const empty = expectSuccess(await runExit(makeAdminInfo(t.ctx)(adminReq(t.storeId))))
    expect(empty).toEqual({
      storeId: t.storeId,
      backendId: t.ctx.backendId,
      currentHead: 0,
      eventCount: 0,
      connectionCount: 2,
      persistenceFormatVersion: PERSISTENCE_FORMAT_VERSION,
    })

    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(4)))))
    const after = expectSuccess(await runExit(makeAdminInfo(t.ctx)(adminReq(t.storeId))))
    expect(after).toMatchObject({ currentHead: 4, eventCount: 4, connectionCount: 2 })
  })
})

describe('AdminReset', () => {
  it('wipes the store, mints and persists a new backendId, resets the head and disconnects every connection', async () => {
    const t = await makeTestCtx(ADMIN)
    const a = makeValidConn('a', t.storeId)
    const b = makeValidConn('b', t.storeId)
    t.conns.push(a, b)

    const push = makePush(t.ctx)
    const pull = makePull(t.ctx)
    expectSuccess(await runExit(push(pushReq(t.storeId, chainedEvents(3)))))
    const oldBackendId = t.ctx.backendId
    expect(t.rowCount()).toBe(3)
    expect(t.ctx.connAuth.size).toBe(2)

    const res = expectSuccess(await runExit(makeAdminReset(t.ctx)(adminReq(t.storeId))))

    // New id, in memory and persisted.
    expect(res.backendId).not.toBe(oldBackendId)
    expect(t.ctx.backendId).toBe(res.backendId)
    expect(t.ctx.backendIdRef.current).toBe(res.backendId)
    const row = t.raw.prepare('SELECT currentHead, backendId FROM context_v1 WHERE storeId = ?').get(t.storeId) as {
      currentHead: number
      backendId: string
    }
    expect({ ...row }).toEqual({ currentHead: 0, backendId: res.backendId })

    // Storage and head are back at root; the auth cache is gone.
    expect(t.rowCount()).toBe(0)
    expect(t.ctx.headRef.current).toBe(EventSequenceNumber.Client.ROOT.global)
    expect(t.ctx.connAuth.size).toBe(0)

    // Every connection was told why.
    expect(a.disconnectReasons).toEqual([STORE_RESET_DISCONNECT_REASON])
    expect(b.disconnectReasons).toEqual([STORE_RESET_DISCONNECT_REASON])

    // A client still holding the old id is rejected on pull and push.
    const stalePull = expectFailure(
      await runExit(
        pull({
          storeId: t.storeId,
          clientId: CLIENT_ID,
          cursor: Option.some({
            eventSequenceNumber: EventSequenceNumber.Global.make(3),
            backendId: Option.some(oldBackendId),
          }),
        }),
      ),
    )
    expect(stalePull).toMatchObject({ _tag: 'BackendIdMismatchError', expected: res.backendId, received: oldBackendId })

    const stalePush = expectFailure(
      await runExit(push(pushReq(t.storeId, chainedEvents(1, 3), { backendId: Option.some(oldBackendId) }))),
    )
    expect(stalePush._tag).toBe('BackendIdMismatchError')

    // A fresh history chained from root is accepted and acked with the new id.
    const ack = expectSuccess(
      await runExit(push(pushReq(t.storeId, chainedEvents(2), { backendId: Option.some(res.backendId) }))),
    )
    expect(ack.backendId).toBe(res.backendId)
    expect(t.persistedHead()).toBe(2)

    const fresh = expectSuccess(await runExit(pull({ storeId: t.storeId, clientId: CLIENT_ID, cursor: Option.none() })))
    expect(fresh.backendId).toBe(res.backendId)
    expect(fresh.batch.map((item) => item.eventEncoded.seqNum)).toEqual([1, 2])
  })

  it('survives a wake: the next makeStoreCtx loads the new backendId and root head', async () => {
    const t = await makeTestCtx(ADMIN)
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(2)))))
    const res = expectSuccess(await runExit(makeAdminReset(t.ctx)(adminReq(t.storeId))))

    // Same database, new wake.
    const db = {
      execute: async (sql: string, ...params: unknown[]) => {
        const statement = t.raw.prepare(sql)
        if (sql.trim().toUpperCase().startsWith('SELECT')) {
          return statement.all(...(params as never[])).map((r) => ({ ...r }))
        }
        statement.run(...(params as never[]))
        return []
      },
      transaction: () => Promise.reject(new Error('unused')),
    } as never
    const woke = await Effect.runPromise(
      makeStoreCtx({ key: [t.storeId], db, conns: () => [], log: () => {}, options: ADMIN }),
    )
    expect(woke.backendId).toBe(res.backendId)
    expect(woke.headRef.current).toBe(0)
  })

  it('does not reset on a wrong secret', async () => {
    const t = await makeTestCtx(ADMIN)
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(2)))))
    const before = t.ctx.backendId

    expectFailure(await runExit(makeAdminReset(t.ctx)(adminReq(t.storeId, 'wrong'))))

    expect(t.ctx.backendId).toBe(before)
    expect(t.ctx.headRef.current).toBe(2)
    expect(t.rowCount()).toBe(2)
    expect(conn.disconnected).toBe(false)
  })

  it('waits for the push gate before touching storage', async () => {
    const t = await makeTestCtx(ADMIN)
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(2)))))

    const observed = await Effect.runPromise(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const holder = yield* Effect.forkChild(t.ctx.pushSemaphore.withPermits(1)(Deferred.await(gate)))
        yield* Effect.yieldNow
        const reset = yield* Effect.forkChild(makeAdminReset(t.ctx)(adminReq(t.storeId)))
        yield* Effect.sleep('20 millis')
        const rowsWhileHeld = t.rowCount()
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(holder)
        const exit = yield* Fiber.await(reset)
        return { rowsWhileHeld, exit }
      }),
    )
    expect(observed.rowsWhileHeld).toBe(2)
    expect(Exit.isSuccess(observed.exit)).toBe(true)
    expect(t.rowCount()).toBe(0)
  })

  it('a push that passed the pre-gate backendId check is re-checked under the gate', async () => {
    const t = await makeTestCtx(ADMIN)
    const oldBackendId = t.ctx.backendId

    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const gate = yield* Deferred.make<void>()
        const holder = yield* Effect.forkChild(t.ctx.pushSemaphore.withPermits(1)(Deferred.await(gate)))
        yield* Effect.yieldNow
        // Parent = root, so only the backend id can reject it once admitted.
        const push = yield* Effect.forkChild(
          makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1), { backendId: Option.some(oldBackendId) })),
        )
        yield* Effect.sleep('20 millis')
        // What `AdminReset` does to the in-memory id while holding the gate.
        t.ctx.backendIdRef.current = 'replaced-by-reset'
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(holder)
        return yield* Fiber.await(push)
      }),
    )
    const error = expectFailure(exit)
    expect(error).toMatchObject({ _tag: 'BackendIdMismatchError', expected: 'replaced-by-reset', received: oldBackendId })
    expect(t.rowCount()).toBe(0)
  })
})
