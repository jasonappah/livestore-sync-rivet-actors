/**
 * Unit coverage for the test-only `TestInfo` / `TestSleep` handlers and the
 * wake bookkeeping they report — against fake connections, no engine.
 * (`TestDisconnectAll` is covered in `connections.test.ts`.)
 */

import { Effect } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import { firstWake, makeStoreCtx, type WakeInfo } from '../store-ctx.ts'
import { makeTestInfo, makeTestSleep, TEST_INFO_DISABLED_NOTE, TEST_SLEEP_DISABLED_NOTE } from '../test-actions.ts'
import { STORE_ID_MISMATCH_REASON } from '../validate-payload.ts'
import { makeFakeDb } from './fake-raw-access.ts'
import { chainedEvents, CLIENT_ID, expectFailure, expectSuccess, makeFakeConn, makeTestCtx, makeValidConn, pushReq, runExit } from './push-test-utils.ts'
import { makePush } from '../push.ts'
import { migrate } from '../sqlite.ts'

describe('TestInfo', () => {
  it('is refused unless testing is enabled', async () => {
    const t = await makeTestCtx({})
    const error = expectFailure(await runExit(makeTestInfo(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(error._tag).toBe('UnknownError')
    expect((error as { note?: string }).note).toBe(TEST_INFO_DISABLED_NOTE)
  })

  it('validates the caller like every other action', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    const error = expectFailure(await runExit(makeTestInfo(t.ctx)({ storeId: 'other-store', clientId: CLIENT_ID })))
    expect(error._tag).toBe('InvalidPayloadError')
    expect((error as { reason?: string }).reason).toBe(STORE_ID_MISMATCH_REASON)
  })

  it('reports the wake info, the head, the backendId and the connections with their decoded clientId', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    t.conns.push(makeValidConn('a', t.storeId), makeFakeConn('garbage', { nope: true }))

    const initial = expectSuccess(await runExit(makeTestInfo(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(initial.wakeCount).toBe(1)
    expect(initial.previousSleptAt).toBeNull()
    expect(initial.wokeAt).toBe(t.ctx.wake.at)
    expect(initial.head).toBe(0)
    expect(initial.backendId).toBe(t.ctx.backendId)
    expect(initial.conns).toEqual([
      { id: 'a', clientId: 'a-client', hibernatable: null },
      { id: 'garbage', clientId: null, hibernatable: null },
    ])

    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(3)))))
    const afterPush = expectSuccess(await runExit(makeTestInfo(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(afterPush.head).toBe(3)
  })

  it('echoes the wake info handed to makeStoreCtx', async () => {
    const fake = makeFakeDb()
    await migrate(fake.db)
    const wake: WakeInfo = { count: 3, at: 1_000, previousSleptAt: 900 }
    const ctx = await Effect.runPromise(
      makeStoreCtx({ key: ['store'], db: fake.db, conns: () => [], log: () => {}, options: { testing: { enabled: true } }, wake }),
    )
    expect(ctx.wake).toEqual(wake)
    const info = expectSuccess(await runExit(makeTestInfo(ctx)({ storeId: 'store', clientId: CLIENT_ID })))
    expect(info).toMatchObject({ wakeCount: 3, wokeAt: 1_000, previousSleptAt: 900 })
  })

  it('firstWake() is the default', async () => {
    const before = Date.now()
    const t = await makeTestCtx({})
    expect(t.ctx.wake.count).toBe(1)
    expect(t.ctx.wake.previousSleptAt).toBeNull()
    expect(t.ctx.wake.at).toBeGreaterThanOrEqual(before)
    expect(firstWake().count).toBe(1)
  })
})

describe('TestSleep', () => {
  it('is refused unless testing is enabled, and does not sleep', async () => {
    const t = await makeTestCtx({})
    let slept = 0
    const sleep = Effect.sync(() => {
      slept += 1
    })
    const error = expectFailure(await runExit(makeTestSleep(t.ctx, sleep)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(error._tag).toBe('UnknownError')
    expect((error as { note?: string }).note).toBe(TEST_SLEEP_DISABLED_NOTE)
    expect(slept).toBe(0)
  })

  it('does not sleep for a caller that fails validation', async () => {
    const t = await makeTestCtx({ testing: { enabled: true }, validatePayload: () => Promise.reject(new Error('no')) })
    let slept = 0
    const sleep = Effect.sync(() => {
      slept += 1
    })
    const error = expectFailure(await runExit(makeTestSleep(t.ctx, sleep)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(error._tag).toBe('InvalidPayloadError')
    expect(slept).toBe(0)
  })

  it('runs the sleep effect once and reports the connection count', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    t.conns.push(makeValidConn('a', t.storeId), makeValidConn('b', t.storeId))
    let slept = 0
    const sleep = Effect.sync(() => {
      slept += 1
    })
    const res = expectSuccess(await runExit(makeTestSleep(t.ctx, sleep)({ storeId: t.storeId, clientId: CLIENT_ID })))
    expect(res.conns).toBe(2)
    expect(slept).toBe(1)
  })
})
