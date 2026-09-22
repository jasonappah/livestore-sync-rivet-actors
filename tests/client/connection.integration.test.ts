/**
 * Drives `makeConnection` against a real rivetkit actor on the local engine
 * (auto-spawned by `setupTest` with `startEngine = true`; see
 * `docs/spike-results.md` §B1). Runs in the `conformance` vitest project:
 *
 *   pnpm vitest run --project conformance tests/client/connection.integration.test.ts
 */

import { IsOfflineError } from '@livestore/common'
import { type Duration, Effect, PubSub, Stream, SubscriptionRef } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { actor, setup } from 'rivetkit'
import { setupTest } from 'rivetkit/test'
import { describe, expect, it } from 'vitest'

import { makeConnection } from '../../src/client/connection.ts'
import { resolveRivetSyncOptions } from '../../src/client/options.ts'
import { type ConnStatus, RawActionFailure } from '../../src/client/types.ts'

const EchoActor = actor({
  actions: {
    echo: (_c, x: unknown) => x,
    kick: async (c) => {
      for (const conn of c.conns.values()) await conn.disconnect('bye')
    },
    emitTo: (c, payload: unknown) => {
      for (const conn of c.conns.values()) conn.send('pull', payload)
    },
  },
})

const registry = setup({ use: { EchoActor } })
// `setupTest` only spawns the engine with RIVET_RUN_ENGINE=1 or this flag.
registry.config.startEngine = true

const timeout = (label: string, duration: Duration.Input) =>
  Effect.timeoutOrElse({
    duration,
    orElse: () => Effect.die(new Error(`timed out: ${label}`)),
  })

describe('makeConnection (real engine)', () => {
  it('connects lazily, echoes, receives pull events, survives a server kick', async (ctx) => {
    await setupTest(ctx, registry)
    const endpoint = registry.parseConfig().endpoint ?? 'http://127.0.0.1:6420'
    const storeId = `conn-it-${nanoid()}`
    const connParams = { storeId, clientId: `client-${nanoid()}`, token: 'test-token' }

    const t0 = Date.now()
    const timeline: Array<{ readonly ms: number; readonly status: ConnStatus }> = []

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const conn = yield* makeConnection({
            options: resolveRivetSyncOptions({ endpoint, actorName: 'EchoActor', connectTimeout: '20 seconds' }),
            storeId,
            connParams,
          })
          yield* Effect.forkScoped(
            Stream.runForEach(SubscriptionRef.changes(conn.status), (status) =>
              Effect.sync(() => {
                timeline.push({ ms: Date.now() - t0, status })
              }),
            ),
          )
          // Subscribe to live events before the first action (subscriptions register at conn creation).
          const pulls = yield* PubSub.subscribe(conn.pullEvents)

          expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(false)
          expect(yield* SubscriptionRef.get(conn.status)).toBe('idle')

          yield* conn.awaitConnected
          const connectedAt = Date.now() - t0
          expect(yield* SubscriptionRef.get(conn.isConnected)).toBe(true)

          expect(yield* conn.action('echo', { a: 1 }).pipe(timeout('echo', '10 seconds'))).toEqual({ a: 1 })

          yield* conn.action('emitTo', { seq: 7, events: [] }).pipe(timeout('emitTo', '10 seconds'))
          expect(yield* PubSub.take(pulls).pipe(timeout('pull event', '10 seconds'))).toEqual({ seq: 7, events: [] })

          // Server-side disconnect: the in-flight action fails (plain "Connection closed" Error → RawActionFailure,
          // or IsOfflineError if the status flip wins the race) and rivetkit reconnects on its own.
          const kickedAt = Date.now() - t0
          const kickError = yield* Effect.flip(conn.action('kick', null)).pipe(timeout('kick', '10 seconds'))
          expect(kickError instanceof RawActionFailure || kickError instanceof IsOfflineError).toBe(true)
          if (kickError instanceof RawActionFailure) {
            expect(String((kickError.cause as Error).message)).toMatch(/Connection (closed|lost)/)
          }

          yield* conn.awaitConnected.pipe(timeout('reconnect', '20 seconds'))
          const reconnectedAt = Date.now() - t0

          expect(yield* conn.action('echo', 'after-kick').pipe(timeout('echo after kick', '10 seconds'))).toBe('after-kick')

          console.log('[connection.integration] connected after', connectedAt, 'ms; kicked at', kickedAt, 'ms; reconnected at', reconnectedAt, 'ms; kick failed with', kickError._tag, kickError instanceof RawActionFailure ? `(${String((kickError.cause as Error).message)}, status ${kickError.statusAtFailure})` : '')
          console.log('[connection.integration] status timeline:', JSON.stringify(timeline))
        }),
      ),
    )

    // The recorder subscribes asynchronously, so the replayed first value is `idle` or already `connecting`.
    const statuses = timeline.map((entry) => entry.status).filter((status, index) => !(index === 0 && status === 'idle'))
    expect(statuses.slice(0, 2)).toEqual(['connecting', 'connected'])
    const afterFirstConnected = statuses.slice(2)
    expect(afterFirstConnected).toContain('disconnected')
    expect(afterFirstConnected.indexOf('disconnected')).toBeLessThan(afterFirstConnected.lastIndexOf('connected'))
    // The recorder fiber is interrupted before the conn is disposed, so no disposal `idle` is recorded;
    // rivetkit never went idle on its own (it reconnects forever).
    expect(afterFirstConnected).not.toContain('idle')
  }, 60_000)
})
