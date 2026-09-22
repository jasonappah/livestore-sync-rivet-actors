/**
 * Unit tests for `makePing` (T13): the success path asserts liveness, the
 * timeout path marks the connection offline and fails with
 * `Cause.TimeoutError` (the third member of `SyncBackend['ping']`'s error
 * type).
 */

import { Cause, Deferred, Effect, Fiber, SubscriptionRef, TestClock } from '@livestore/utils/effect'
import { describe, expect, it } from '@effect/vitest'

import { ACTION_PING, encodePong, Pong } from '../../src/common/mod.ts'
import { makeActionClient } from '../../src/client/action-client.ts'
import { resolveRivetSyncOptions, type RivetSyncOptions } from '../../src/client/options.ts'
import { makePing } from '../../src/client/ping.ts'
import { type FakeConnection, makeFakeConnection } from '../harness/fake-connection.ts'

/** Simulates the CBOR transport for values the server sends back. */
const overTheWire = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

const setup = (overrides: Partial<RivetSyncOptions> = {}) =>
  Effect.gen(function* () {
    const fake: FakeConnection = yield* makeFakeConnection({ initialStatus: 'connected' })
    const options = resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420', ...overrides })
    const ping = makePing({
      actions: makeActionClient(fake.conn),
      conn: fake.conn,
      storeId: 's1',
      clientId: 'c1',
      payload: undefined,
      options,
    })
    return { fake, ping }
  })

describe('makePing', () => {
  it.effect('sends a Ping carrying the sync context and marks the connection connected', () =>
    Effect.gen(function* () {
      const { fake, ping } = yield* setup()
      // A stale offline flag that only a successful ping can clear.
      yield* SubscriptionRef.set(fake.conn.isConnected, false)
      fake.onAction(ACTION_PING, () => Effect.succeed(overTheWire(encodePong(Pong.make({})))))

      yield* ping

      expect(fake.calls).toHaveLength(1)
      expect(fake.calls[0]!.name).toBe(ACTION_PING)
      const payload = fake.calls[0]!.payload as Record<string, unknown>
      expect(payload).toEqual({ storeId: 's1', clientId: 'c1' })
      // `payload` is an `optionalKey`: an absent sync payload must be an absent key.
      expect('payload' in payload).toBe(false)
      expect(yield* SubscriptionRef.get(fake.conn.isConnected)).toBe(true)
    }),
  )

  it.effect('fails with Cause.TimeoutError and marks the connection offline when the Ping never answers', () =>
    Effect.gen(function* () {
      const { fake, ping } = yield* setup({ ping: { requestTimeout: '5 seconds' } })
      const never = yield* Deferred.make<unknown>()
      fake.onAction(ACTION_PING, () => Deferred.await(never))

      const fiber = yield* Effect.forkChild(Effect.flip(ping))
      yield* TestClock.adjust('4999 millis')
      expect(fiber.pollUnsafe()).toBeUndefined()
      expect(yield* SubscriptionRef.get(fake.conn.isConnected)).toBe(true)

      yield* TestClock.adjust('1 millis')
      const error = yield* Fiber.join(fiber)

      expect(Cause.isTimeoutError(error)).toBe(true)
      expect(yield* SubscriptionRef.get(fake.conn.isConnected)).toBe(false)
    }),
  )
})
