/**
 * Unit coverage for live-pull fan-out (`connections.ts`), the `Ping` handler
 * and the test-only `TestDisconnectAll` handler — all against fake
 * connections, no Rivet engine involved.
 */

import type { UnknownError } from '@livestore/common'
import { Effect, Option } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import {
  decodePullResponse,
  type InvalidPayloadError,
  LIVE_PULL_EVENT,
  type PullResponse,
} from '../../common/mod.ts'
import { authorizedConns, fanOut, UNAUTHORIZED_DISCONNECT_REASON } from '../connections.ts'
import { makePing } from '../ping.ts'
import { makePush } from '../push.ts'
import { makeTestDisconnectAll, TEST_ACTIONS_DISABLED_NOTE, TEST_DISCONNECT_REASON } from '../test-actions.ts'
import { CONN_PARAMS_FAILURE_REASON, STORE_ID_MISMATCH_REASON } from '../validate-payload.ts'
import {
  chainedEvents,
  CLIENT_ID,
  expectFailure,
  expectSuccess,
  type FakeConn,
  makeFakeConn,
  makeTestCtx,
  makeValidConn,
  pushReq,
  runExit,
} from './push-test-utils.ts'

/** Decodes every `pull` event a fake connection received, in order. */
const receivedPulls = (conn: FakeConn): PullResponse[] =>
  conn.sent.map(([name, payload]) => {
    expect(name).toBe(LIVE_PULL_EVENT)
    return decodePullResponse(payload)
  })

describe('fan-out on push', () => {
  it('sends one pull event per push to every valid connection and disconnects invalid ones', async () => {
    let validations = 0
    const t = await makeTestCtx({
      validatePayload: () => {
        validations += 1
      },
    })
    const a = makeValidConn('a', t.storeId)
    const b = makeValidConn('b', t.storeId)
    const garbage = makeFakeConn('garbage', { nope: true })
    t.conns.push(a, b, garbage)

    const push = makePush(t.ctx)
    const batch = chainedEvents(2)
    const ack = expectSuccess(await runExit(push(pushReq(t.storeId, batch))))

    for (const conn of [a, b]) {
      const pulls = receivedPulls(conn)
      expect(pulls).toHaveLength(1)
      const [res] = pulls
      expect(res!.backendId).toBe(ack.backendId)
      expect(res!.pageInfo).toEqual({ _tag: 'NoMore' })
      expect(res!.batch.map((item) => item.eventEncoded)).toEqual(batch)
      for (const item of res!.batch) {
        expect(Option.isSome(item.metadata)).toBe(true)
        if (Option.isSome(item.metadata)) {
          expect(item.metadata.value._tag).toBe('SyncMessage.SyncMetadata')
          expect(Number.isNaN(Date.parse(item.metadata.value.createdAt))).toBe(false)
        }
      }
      expect(conn.disconnected).toBe(false)
    }

    expect(garbage.sent).toEqual([])
    expect(garbage.disconnected).toBe(true)
    expect(garbage.disconnectReasons).toEqual([UNAUTHORIZED_DISCONNECT_REASON])
    expect(t.ctx.connAuth.get('a')).toBe('ok')
    expect(t.ctx.connAuth.get('b')).toBe('ok')
    expect(t.ctx.connAuth.get('garbage')).toBe('rejected')

    const warning = t.logs.find((entry) => entry.msg === 'disconnecting unauthorized connection')
    expect(warning?.level).toBe('warn')
    expect(warning?.data?.connId).toBe('garbage')
    expect(String(warning?.data?.reason)).toContain(CONN_PARAMS_FAILURE_REASON)

    // 1 for the push request itself + 1 per valid connection. The garbage
    // connection fails ConnParams decoding before validatePayload runs.
    expect(validations).toBe(3)

    // Second push: cached verdicts, no re-validation, no second disconnect.
    const batch2 = chainedEvents(1, 2)
    expectSuccess(await runExit(push(pushReq(t.storeId, batch2))))

    expect(validations).toBe(4)
    expect(receivedPulls(a)).toHaveLength(2)
    expect(receivedPulls(b)).toHaveLength(2)
    expect(receivedPulls(a)[1]!.batch.map((item) => item.eventEncoded)).toEqual(batch2)
    expect(garbage.sent).toEqual([])
    expect(garbage.disconnectReasons).toHaveLength(1)
  })

  it('disconnects a connection whose params fail validatePayload', async () => {
    const t = await makeTestCtx({
      validatePayload: (payload) => {
        if ((payload as { token?: string } | undefined)?.token !== 'good') throw new Error('bad token')
      },
    })
    const good = makeValidConn('good', t.storeId, { token: 'good' })
    const bad = makeValidConn('bad', t.storeId, { token: 'bad' })
    const wrongStore = makeValidConn('wrong-store', 'another-store', { token: 'good' })
    t.conns.push(good, bad, wrongStore)

    expectSuccess(
      await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1), { payload: { token: 'good' } }))),
    )

    expect(receivedPulls(good)).toHaveLength(1)
    expect(bad.disconnected).toBe(true)
    expect(bad.sent).toEqual([])
    expect(wrongStore.disconnected).toBe(true)
    expect(wrongStore.sent).toEqual([])
    expect(t.ctx.connAuth.get('bad')).toBe('rejected')
    expect(t.ctx.connAuth.get('wrong-store')).toBe('rejected')
  })

  it('prunes verdicts for connections that went away', async () => {
    const t = await makeTestCtx()
    const a = makeValidConn('a', t.storeId)
    const b = makeValidConn('b', t.storeId)
    const garbage = makeFakeConn('garbage', 'not-an-object')
    t.conns.push(a, b, garbage)

    const push = makePush(t.ctx)
    expectSuccess(await runExit(push(pushReq(t.storeId, chainedEvents(1)))))
    expect([...t.ctx.connAuth.keys()].sort()).toEqual(['a', 'b', 'garbage'])

    // `a` disconnects; rivetkit drops it from `c.conns`, and so does our fake.
    t.conns.splice(t.conns.indexOf(a), 1)
    t.conns.splice(t.conns.indexOf(garbage), 1)

    expectSuccess(await runExit(push(pushReq(t.storeId, chainedEvents(1, 1)))))

    expect([...t.ctx.connAuth.keys()]).toEqual(['b'])
    expect(receivedPulls(a)).toHaveLength(1)
    expect(receivedPulls(b)).toHaveLength(2)
  })

  it('does not fail the push when a connection throws on send', async () => {
    const t = await makeTestCtx()
    const broken = makeFakeConn('broken', a2params(t.storeId), { throwOnSend: true })
    const healthy = makeValidConn('healthy', t.storeId)
    t.conns.push(broken, healthy)

    const ack = expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1)))))

    expect(ack.backendId).toBe(t.ctx.backendId)
    expect(receivedPulls(healthy)).toHaveLength(1)
    expect(t.rowCount()).toBe(1)
    expect(t.logs.some((entry) => entry.msg === 'failed to send live pull event to connection')).toBe(true)
    // A throwing socket is still an authorized connection; nothing disconnects it.
    expect(broken.disconnected).toBe(false)
  })

  it('splits a large batch into several pull events that cover every event in order', async () => {
    const t = await makeTestCtx({ maxMessageBytes: 600_000 })
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)

    const bigText = 'x'.repeat(250_000)
    const batch = chainedEvents(5, 'root', 'big', () => bigText)
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, batch))))

    const pulls = receivedPulls(conn)
    expect(pulls.length).toBeGreaterThan(1)
    expect(pulls.every((res) => res.pageInfo._tag === 'NoMore')).toBe(true)
    expect(pulls.every((res) => res.backendId === t.ctx.backendId)).toBe(true)
    expect(pulls.flatMap((res) => res.batch.map((item) => item.eventEncoded))).toEqual(batch)
    for (const [name, payload] of conn.sent) {
      expect(name).toBe(LIVE_PULL_EVENT)
      expect(new TextEncoder().encode(JSON.stringify(payload)).byteLength).toBeLessThanOrEqual(600_000)
    }
  })

  it('runs onPullRes once per chunk, before the ack', async () => {
    const calls: string[] = []
    const t = await makeTestCtx({
      maxMessageBytes: 600_000,
      onPullRes: (message) => {
        calls.push(`onPullRes:${(message as PullResponse).batch.length}`)
      },
      onPushRes: () => {
        calls.push('onPushRes')
      },
    })
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)

    const batch = chainedEvents(5, 'root', 'big', () => 'x'.repeat(250_000))
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, batch))))

    const chunkSizes = receivedPulls(conn).map((res) => res.batch.length)
    expect(calls).toEqual([...chunkSizes.map((size) => `onPullRes:${size}`), 'onPushRes'])
  })

  it('acks (and persists) a push whose single event exceeds maxMessageBytes, but skips fan-out', async () => {
    const t = await makeTestCtx({ maxMessageBytes: 1024 })
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)

    const batch = chainedEvents(1, 'root', 'huge', () => 'x'.repeat(4096))
    const ack = expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, batch))))

    expect(ack.backendId).toBe(t.ctx.backendId)
    expect(t.rowCount()).toBe(1)
    expect(t.ctx.headRef.current).toBe(1)
    expect(conn.sent).toEqual([])
    const entry = t.logs.find((log) => log.msg.includes('exceeds maxMessageBytes'))
    expect(entry?.level).toBe('error')
    expect(entry?.data?.maxBytes).toBe(1024)

    // The store is still consistent: the next chained push succeeds.
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1, 1)))))
    expect(receivedPulls(conn)).toHaveLength(1)
  })

  it('delivers the pull event to the pushing connection as well', async () => {
    const t = await makeTestCtx()
    const pusher = makeFakeConn('pusher', a2params(t.storeId))
    t.conns.push(pusher)

    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1)))))

    expect(receivedPulls(pusher)).toHaveLength(1)
  })
})

/** `ConnParams` for the test's default `clientId` (the pusher's identity). */
const a2params = (storeId: string) => ({ storeId, clientId: CLIENT_ID })

describe('authorizedConns / fanOut directly', () => {
  it('returns only connections with an ok verdict and caches every verdict', async () => {
    const t = await makeTestCtx()
    const ok = makeValidConn('ok', t.storeId)
    const bad = makeFakeConn('bad', 42)
    t.conns.push(ok, bad)

    const first = await Effect.runPromise(authorizedConns(t.ctx))
    expect(first.map((conn) => conn.id)).toEqual(['ok'])
    expect(bad.disconnected).toBe(true)

    const second = await Effect.runPromise(authorizedConns(t.ctx))
    expect(second.map((conn) => conn.id)).toEqual(['ok'])
    expect(bad.disconnectReasons).toHaveLength(1)
  })

  it('fanOut sends every encoded response in order to each authorized connection', async () => {
    const t = await makeTestCtx()
    const a = makeValidConn('a', t.storeId)
    const rejected = makeFakeConn('rejected', null)
    t.conns.push(a, rejected)

    await Effect.runPromise(fanOut(t.ctx, ['one', 'two', 'three']))

    expect(a.sent).toEqual([
      [LIVE_PULL_EVENT, 'one'],
      [LIVE_PULL_EVENT, 'two'],
      [LIVE_PULL_EVENT, 'three'],
    ])
    expect(rejected.sent).toEqual([])
  })

  it('fanOut with no connections is a no-op', async () => {
    const t = await makeTestCtx()
    await Effect.runPromise(fanOut(t.ctx, ['one']))
    expect(t.ctx.connAuth.size).toBe(0)
  })
})

describe('makePing', () => {
  it('returns a Pong for a valid caller', async () => {
    const t = await makeTestCtx()

    const pong = expectSuccess(await runExit(makePing(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })))

    expect(pong).toEqual({ _tag: 'SyncMessage.Pong' })
  })

  it('rejects an invalid caller with InvalidPayloadError', async () => {
    const t = await makeTestCtx()

    const error = expectFailure(
      await runExit(makePing(t.ctx)({ storeId: 'other-store', clientId: CLIENT_ID })),
    ) as InvalidPayloadError

    expect(error._tag).toBe('InvalidPayloadError')
    expect(error.reason).toBe(STORE_ID_MISMATCH_REASON)
  })

  it('maps a failing validatePayload to InvalidPayloadError', async () => {
    const t = await makeTestCtx({
      validatePayload: () => Promise.reject(new Error('expired')),
    })

    const error = expectFailure(
      await runExit(makePing(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })),
    ) as InvalidPayloadError

    expect(error._tag).toBe('InvalidPayloadError')
  })
})

describe('makeTestDisconnectAll', () => {
  it('is refused with UnknownError unless testing.enabled is set', async () => {
    const t = await makeTestCtx()
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)

    const error = expectFailure(
      await runExit(makeTestDisconnectAll(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })),
    ) as UnknownError

    expect(error._tag).toBe('UnknownError')
    expect(error.note).toBe(TEST_ACTIONS_DISABLED_NOTE)
    expect(conn.disconnected).toBe(false)
  })

  it('still validates the caller when enabled', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    const conn = makeValidConn('a', t.storeId)
    t.conns.push(conn)

    const error = expectFailure(
      await runExit(makeTestDisconnectAll(t.ctx)({ storeId: 'other-store', clientId: CLIENT_ID })),
    ) as InvalidPayloadError

    expect(error._tag).toBe('InvalidPayloadError')
    expect(conn.disconnected).toBe(false)
  })

  it('disconnects every connection and clears the verdict cache when enabled', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    const a = makeValidConn('a', t.storeId)
    const b = makeValidConn('b', t.storeId)
    const garbage = makeFakeConn('garbage', {})
    t.conns.push(a, b, garbage)

    // Populate the cache first.
    expectSuccess(await runExit(makePush(t.ctx)(pushReq(t.storeId, chainedEvents(1)))))
    expect(t.ctx.connAuth.size).toBe(3)

    const res = expectSuccess(
      await runExit(makeTestDisconnectAll(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })),
    )

    expect(res).toEqual({ disconnected: 3 })
    expect(a.disconnectReasons).toEqual([TEST_DISCONNECT_REASON])
    expect(b.disconnectReasons).toEqual([TEST_DISCONNECT_REASON])
    // Already disconnected as unauthorized; disconnected again here.
    expect(garbage.disconnectReasons).toEqual([UNAUTHORIZED_DISCONNECT_REASON, TEST_DISCONNECT_REASON])
    expect(t.ctx.connAuth.size).toBe(0)
  })

  it('survives a connection whose disconnect rejects', async () => {
    const t = await makeTestCtx({ testing: { enabled: true } })
    const flaky = makeValidConn('flaky', t.storeId)
    flaky.disconnect = async () => {
      throw new Error('already closed')
    }
    t.conns.push(flaky, makeValidConn('ok', t.storeId))

    const res = expectSuccess(
      await runExit(makeTestDisconnectAll(t.ctx)({ storeId: t.storeId, clientId: CLIENT_ID })),
    )

    expect(res).toEqual({ disconnected: 2 })
  })
})
