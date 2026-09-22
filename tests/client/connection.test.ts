import { IsOfflineError } from '@livestore/common'
import { Duration, Effect, Fiber, PubSub, SubscriptionRef, TestClock } from '@livestore/utils/effect'
import { describe, expect, it } from '@effect/vitest'

import { backoffDelay, makeConnection, type MakeConnectionArgs } from '../../src/client/connection.ts'
import { resolveRivetSyncOptions, type RivetSyncOptions } from '../../src/client/options.ts'
import { type Connection, type ConnStatus, RawActionFailure } from '../../src/client/types.ts'
import { type FakeConn, type FakeRivetClient, makeFakeRivetClient } from '../harness/fake-rivet-client.ts'

const STORE_ID = 'store-1'
const CONN_PARAMS = { storeId: STORE_ID, clientId: 'client-1', token: 'secret' }

/** Lets rivetkit-callback → Effect bridging and stream subscriptions settle (real macrotask; the TestClock stays put). */
const settle = Effect.promise(() => new Promise<void>((resolve) => setTimeout(resolve, 5)))

const setup = (overrides: Partial<RivetSyncOptions> = {}, args: Partial<MakeConnectionArgs> = {}) =>
  Effect.gen(function* () {
    const fake = makeFakeRivetClient()
    const options = resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420', ...overrides })
    const conn = yield* makeConnection({ options, storeId: STORE_ID, connParams: CONN_PARAMS, createClient: () => fake.client, ...args })
    return { fake, conn, options }
  })

/** Creates the lazy conn and flips the fake to `connected`. */
const connect = (fake: FakeRivetClient, conn: Connection) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(conn.awaitConnected)
    yield* settle
    const raw = fake.conns[0]!
    raw.setStatus('connected')
    yield* Fiber.join(fiber)
    return raw
  })

const statusOf = (fake: FakeRivetClient): ConnStatus | undefined => fake.conns[0]?.connStatus

describe('makeConnection', () => {
  it.effect('creates the client eagerly but the conn lazily', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      expect(fake.handles).toEqual([{ name: 'LiveStoreSync', key: [STORE_ID] }])
      expect(fake.conns).toHaveLength(0)
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(false)
      expect(yield* SubscriptionRef.get(conn.status)).toBe('idle')
    }),
  )

  it.effect('uses the configured actorName', () =>
    Effect.gen(function* () {
      const { fake } = yield* setup({ actorName: 'EchoActor' })
      expect(fake.handles[0]!.name).toBe('EchoActor')
    }),
  )

  it.effect('awaitConnected connects on first use, passes connParams verbatim and resolves on `connected`', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const fiber = yield* Effect.forkChild(conn.awaitConnected)
      yield* settle
      expect(fake.conns).toHaveLength(1)
      expect(fake.conns[0]!.params).toBe(CONN_PARAMS)
      expect(yield* SubscriptionRef.get(conn.status)).toBe('connecting')
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(false)

      fake.conns[0]!.setStatus('connected')
      yield* Fiber.join(fiber)
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(true)
      expect(yield* SubscriptionRef.get(conn.status)).toBe('connected')
    }),
  )

  it.effect('concurrent first uses create a single conn', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const a = yield* Effect.forkChild(conn.awaitConnected)
      const b = yield* Effect.forkChild(conn.awaitConnected)
      yield* settle
      expect(fake.conns).toHaveLength(1)
      fake.conns[0]!.setStatus('connected')
      yield* Fiber.join(a)
      yield* Fiber.join(b)
    }),
  )

  it.effect('awaitConnected fails with IsOfflineError after connectTimeout', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup({ connectTimeout: '3 seconds' })
      const fiber = yield* Effect.forkChild(Effect.flip(conn.awaitConnected))
      yield* settle
      expect(statusOf(fake)).toBe('connecting')
      yield* TestClock.adjust('2999 millis')
      yield* settle
      expect(fiber.pollUnsafe()).toBeUndefined()
      yield* TestClock.adjust('1 millis')
      const error = yield* Fiber.join(fiber)
      expect(error).toBeInstanceOf(IsOfflineError)
      // Still connecting underneath; a later `connected` is picked up normally.
      fake.conns[0]!.setStatus('connected')
      yield* conn.awaitConnected
    }),
  )

  it.effect('mirrors every status into `status` and `isConnected`', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      const transitions: Array<[ConnStatus, boolean]> = [
        ['disconnected', false],
        ['connecting', false],
        ['connected', true],
        ['idle', false],
      ]
      for (const [next, expected] of transitions) {
        raw.setStatus(next)
        expect(yield* SubscriptionRef.get(conn.status)).toBe(next)
        expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(expected)
      }
    }),
  )

  it.effect('subscribes to `pull` at conn creation and publishes args[0] of each event', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const subscription = yield* PubSub.subscribe(conn.pullEvents)
      const raw = yield* connect(fake, conn)
      expect(raw.subscribedEvents()).toEqual(['pull'])
      expect(raw.calls).toHaveLength(0)

      raw.emit('pull', { page: 1 }, 'ignored-second-arg')
      raw.emit('pull', { page: 2 })
      raw.emit('other', { page: 3 })
      expect(yield* PubSub.take(subscription)).toEqual({ page: 1 })
      expect(yield* PubSub.take(subscription)).toEqual({ page: 2 })
    }),
  )

  it.effect('onError callbacks are swallowed (debug log only)', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      raw.fireError(new Error('socket hiccup'))
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(true)
    }),
  )

  it.effect('action sends { name, args: [payload] } and returns the resolved value', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      const fiber = yield* Effect.forkChild(conn.action('Pull', { cursor: 0 }))
      yield* settle
      expect(raw.calls).toEqual([{ name: 'Pull', args: [{ cursor: 0 }] }])
      expect(raw.pendingActions).toHaveLength(1)
      raw.pendingActions[0]!.resolve({ events: [] })
      expect(yield* Fiber.join(fiber)).toEqual({ events: [] })
    }),
  )

  it.effect('action waits for `connected` before sending (rivetkit would only queue)', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const fiber = yield* Effect.forkChild(conn.action('Ping', {}))
      yield* settle
      const raw = fake.conns[0]!
      expect(raw.calls).toHaveLength(0)
      raw.setStatus('connected')
      yield* settle
      expect(raw.calls).toEqual([{ name: 'Ping', args: [{}] }])
      raw.pendingActions[0]!.resolve('pong')
      expect(yield* Fiber.join(fiber)).toBe('pong')
    }),
  )

  it.effect('action fails with RawActionFailure({ cause, statusAtFailure }) when the promise rejects while connected', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      const fiber = yield* Effect.forkChild(Effect.flip(conn.action('Push', { batch: [] })))
      yield* settle
      const boom = { group: 'user', code: 'ServerAheadError', metadata: { x: 1 } }
      raw.pendingActions[0]!.reject(boom)
      const error = yield* Fiber.join(fiber)
      expect(error).toBeInstanceOf(RawActionFailure)
      expect((error as RawActionFailure).cause).toBe(boom)
      expect((error as RawActionFailure).statusAtFailure).toBe('connected')
    }),
  )

  it.effect('action fails with IsOfflineError when the connection drops while the promise is pending', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      const fiber = yield* Effect.forkChild(Effect.flip(conn.action('Push', { batch: [] })))
      yield* settle
      const pending = raw.pendingActions[0]!
      raw.setStatus('disconnected')
      const error = yield* Fiber.join(fiber)
      expect(error).toBeInstanceOf(IsOfflineError)
      // The losing `tryPromise` fiber was interrupted → its AbortSignal fired.
      expect(pending.signal?.aborted).toBe(true)
    }),
  )

  it.effect('action captures the status observed at rejection time', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup()
      const raw = yield* connect(fake, conn)
      // Reject synchronously from inside the handler; status is still `connected`.
      raw.onAction = () => Promise.reject(new Error('Connection closed (code: 1000, reason: bye)'))
      const error = yield* Effect.flip(conn.action('Ping', {}))
      expect(error).toBeInstanceOf(RawActionFailure)
      expect((error as RawActionFailure).statusAtFailure).toBe('connected')
      expect(((error as RawActionFailure).cause as Error).message).toMatch(/Connection closed/)
    }),
  )

  it.effect('idle while not disposing → recreates the conn after backoff with the same params', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup({ reconnect: { baseDelay: '1 second', maxDelay: '1 second' } })
      const first = yield* connect(fake, conn)

      // rivetkit gave up (e.g. connection_open_failed) → idle, and we did not dispose.
      first.setStatus('idle')
      yield* settle
      expect(fake.conns).toHaveLength(1)
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(false)

      yield* TestClock.adjust('1 second')
      yield* settle
      expect(fake.conns).toHaveLength(2)
      expect(first.disposed).toBe(true)
      const second = fake.conns[1]!
      expect(second.params).toBe(CONN_PARAMS)
      expect(yield* SubscriptionRef.get(conn.status)).toBe('connecting')

      // The old conn's listeners were detached: its late events are ignored.
      first.emit('pull', 'stale')
      first.setStatus('connected')
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(false)

      second.setStatus('connected')
      yield* conn.awaitConnected
      expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(true)

      // Actions now go to the new conn.
      second.onAction = async (name) => `${name}:ok`
      expect(yield* conn.action('Ping', {})).toBe('Ping:ok')
    }),
  )

  it.effect('backoff grows exponentially up to maxDelay and resets on `connected`', () =>
    Effect.gen(function* () {
      const { fake, conn } = yield* setup({ reconnect: { baseDelay: '1 second', maxDelay: '3 seconds' } })
      const first = yield* connect(fake, conn)

      // attempt 0: 1 s · [0.8, 1.2] → ≤ 1.2 s
      first.setStatus('idle')
      yield* settle
      yield* TestClock.adjust('700 millis')
      yield* settle
      expect(fake.conns).toHaveLength(1)
      yield* TestClock.adjust('500 millis')
      yield* settle
      expect(fake.conns).toHaveLength(2)

      // attempt 1 (no `connected` in between): 2 s · [0.8, 1.2] → > 1.5 s, ≤ 2.4 s
      fake.conns[1]!.setStatus('idle')
      yield* settle
      yield* TestClock.adjust('1500 millis')
      yield* settle
      expect(fake.conns).toHaveLength(2)
      yield* TestClock.adjust('1000 millis')
      yield* settle
      expect(fake.conns).toHaveLength(3)

      // `connected` resets: next idle recreates after ≤ 1.2 s again (attempt 1 would need > 1.5 s)
      fake.conns[2]!.setStatus('connected')
      fake.conns[2]!.setStatus('idle')
      yield* settle
      yield* TestClock.adjust('1200 millis')
      yield* settle
      expect(fake.conns).toHaveLength(4)
      expect(yield* SubscriptionRef.get(conn.status)).toBe('connecting')
    }),
  )

  it.effect('idle before the conn was ever created is ignored', () =>
    Effect.gen(function* () {
      const { fake } = yield* setup({ reconnect: { baseDelay: '1 second', maxDelay: '1 second' } })
      yield* TestClock.adjust('10 seconds')
      yield* settle
      expect(fake.conns).toHaveLength(0)
    }),
  )

  it.effect('scope close disposes the conn before the client and never recreates', () =>
    Effect.gen(function* () {
      const fake = makeFakeRivetClient()
      const options = resolveRivetSyncOptions({
        endpoint: 'http://127.0.0.1:6420',
        reconnect: { baseDelay: '1 millis', maxDelay: '1 millis' },
      })
      let raw: FakeConn | undefined
      yield* Effect.scoped(
        Effect.gen(function* () {
          const conn = yield* makeConnection({ options, storeId: STORE_ID, connParams: CONN_PARAMS, createClient: () => fake.client })
          raw = yield* connect(fake, conn)
        }),
      )
      expect(fake.disposeOrder).toEqual(['conn:0', 'client'])
      expect(raw!.disposed).toBe(true)
      expect(fake.disposed).toBe(true)
      expect(raw!.subscribedEvents()).toEqual([])

      yield* TestClock.adjust('10 seconds')
      yield* settle
      expect(fake.conns).toHaveLength(1)
    }),
  )

  it.effect('scope close with no conn created only disposes the client', () =>
    Effect.gen(function* () {
      const fake = makeFakeRivetClient()
      const options = resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420' })
      yield* Effect.scoped(makeConnection({ options, storeId: STORE_ID, connParams: CONN_PARAMS, createClient: () => fake.client }))
      expect(fake.disposeOrder).toEqual(['client'])
      expect(fake.conns).toHaveLength(0)
    }),
  )
})

describe('backoffDelay', () => {
  const reconnect = { baseDelay: Duration.seconds(1), maxDelay: Duration.seconds(30) }
  const ms = (attempt: number, random: number) => Duration.toMillis(backoffDelay(attempt, reconnect, () => random))

  it('doubles per attempt with ±20% jitter', () => {
    expect(ms(0, 0)).toBe(800)
    expect(ms(0, 0.5)).toBe(1000)
    expect(ms(0, 1)).toBe(1200)
    expect(ms(1, 0.5)).toBe(2000)
    expect(ms(3, 0.5)).toBe(8000)
  })

  it('never exceeds maxDelay, even after jitter, and survives huge attempt counts', () => {
    expect(ms(4, 1)).toBe(19_200)
    expect(ms(5, 1)).toBe(30_000)
    expect(ms(60, 1)).toBe(30_000)
    expect(ms(1000, 0)).toBe(24_000)
  })
})
