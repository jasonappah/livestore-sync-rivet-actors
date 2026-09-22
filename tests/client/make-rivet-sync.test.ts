/**
 * Assembly tests (T13): the constructor must produce a `SyncBackend` that
 * matches LiveStore's contract, report itself correctly to devtools, and —
 * crucially for LiveStore's conformance suite — must not touch the network
 * before the backend is actually used.
 */

import { SyncBackend } from '@livestore/common'
import { makeMockSyncBackend } from '@livestore/common/sync'
import { Effect, FetchHttpClient, KeyValueStore, Layer, SubscriptionRef } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import type { CreateRivetClient, RivetClientLike, RivetConnLike } from '../../src/client/connection.ts'
import { makeRivetSync, makeRivetSyncWith } from '../../src/client/make-rivet-sync.ts'
import type { RivetSyncOptions } from '../../src/client/options.ts'

/** An endpoint nothing listens on: reaching the network at all would be a bug. */
const ENDPOINT = 'http://127.0.0.1:1'

const layer = Layer.mergeAll(KeyValueStore.layerMemory, FetchHttpClient.layer)

interface Spy {
  readonly createClient: CreateRivetClient
  readonly clients: Array<{ readonly endpoint: string }>
  readonly handles: Array<{ readonly name: string; readonly key: string | ReadonlyArray<string> }>
  readonly conns: Array<unknown>
}

/** Records every rivetkit interaction without ever opening a connection. */
const makeSpy = (): Spy => {
  const clients: Array<{ readonly endpoint: string }> = []
  const handles: Array<{ readonly name: string; readonly key: string | ReadonlyArray<string> }> = []
  const conns: Array<unknown> = []

  const conn: RivetConnLike = {
    connStatus: 'connecting',
    on: () => () => {},
    onStatusChange: () => () => {},
    onError: () => () => {},
    action: () => Promise.reject(new Error('spy: no action should be issued')),
    dispose: () => Promise.resolve(),
  }

  const createClient: CreateRivetClient = (options) => {
    clients.push({ endpoint: options.endpoint })
    const client: RivetClientLike = {
      getOrCreate: (name, key) => {
        handles.push({ name, key })
        return {
          connect: () => {
            conns.push(conn)
            return conn
          },
        }
      },
      dispose: () => Promise.resolve(),
    }
    return client
  }

  return { createClient, clients, handles, conns }
}

const withBackend = <A>(
  use: (ctx: {
    readonly backend: SyncBackend.SyncBackend<unknown>
    readonly spy: Spy
  }) => Effect.Effect<A, never, never>,
  options: Partial<RivetSyncOptions> = {},
): Promise<A> => {
  const spy = makeSpy()
  return Effect.runPromise(
    Effect.gen(function* () {
      const backend = yield* makeRivetSyncWith({ endpoint: ENDPOINT, ...options }, { createClient: spy.createClient })({
        storeId: 'store-1',
        clientId: 'client-1',
        payload: undefined,
      })
      return yield* use({ backend: backend as SyncBackend.SyncBackend<unknown>, spy })
    }).pipe(Effect.scoped, Effect.provide(layer), Effect.orDie),
  )
}

/**
 * `SyncBackend.isSyncBackend` is stale in `@livestore/common@0.5.0-dev.0`: it
 * requires `connect` and `ping` to be *functions*, while the `SyncBackend`
 * type declares both as `Effect` values (objects in Effect 4). LiveStore's own
 * `makeMockSyncBackend` fails the guard for the same reason, so this test
 * pins parity with it instead of pretending the guard can pass.
 */
const mockBackendGuardResult = Effect.runPromise(
  Effect.gen(function* () {
    const mock = yield* makeMockSyncBackend()
    const backend = yield* mock.makeSyncBackend
    return SyncBackend.isSyncBackend(backend)
  }).pipe(Effect.scoped, Effect.withSpan('mock'), Effect.orDie),
)

describe('makeRivetSync', () => {
  it('matches the SyncBackend contract field by field', async () => {
    const shape = await withBackend(({ backend }) =>
      Effect.succeed({
        guard: SyncBackend.isSyncBackend(backend),
        pull: typeof backend.pull,
        push: typeof backend.push,
        // `connect`/`ping` are `Effect` values per the `SyncBackend` type.
        connect: typeof backend.connect,
        ping: typeof backend.ping,
        isConnected: typeof backend.isConnected,
      }),
    )

    expect(shape.pull).toBe('function')
    expect(shape.push).toBe('function')
    expect(shape.connect).toBe('object')
    expect(shape.ping).toBe('object')
    expect(shape.isConnected).toBe('object')
    // Same verdict as LiveStore's own mock backend (currently `false`; see above).
    expect(shape.guard).toBe(await mockBackendGuardResult)
  })

  it('reports its metadata and capabilities', async () => {
    const { metadata, supports } = await withBackend(
      ({ backend }) => Effect.succeed({ metadata: backend.metadata, supports: backend.supports }),
      { actorName: 'CustomSync' },
    )

    expect(metadata).toEqual({
      name: 'livestore-sync-rivet-actors',
      description: 'LiveStore sync backend implementation using Rivet Actors',
      protocol: 'rivet-actor-ws',
      endpoint: ENDPOINT,
      actorName: 'CustomSync',
    })
    expect(supports).toEqual({ pullPageInfoKnown: true, pullLive: true })
  })

  it('starts disconnected and does not connect to rivetkit during construction', async () => {
    const { isConnected, spy } = await withBackend(({ backend, spy }) =>
      Effect.map(SubscriptionRef.get(backend.isConnected), (isConnected) => ({ isConnected, spy })),
    )

    expect(isConnected).toBe(false)
    // The client + handle are created eagerly (cheap, no I/O); the `ActorConn` is not.
    expect(spy.clients).toEqual([{ endpoint: ENDPOINT }])
    expect(spy.handles).toEqual([{ name: 'LiveStoreSync', key: ['store-1'] }])
    expect(spy.conns).toEqual([])
  })

  it('does not connect when the ping fiber is disabled either', async () => {
    const spyConns = await withBackend(({ spy }) => Effect.succeed(spy.conns), { ping: { enabled: false } })
    expect(spyConns).toEqual([])
  })

  it('never reaches the transport for an empty push', async () => {
    const conns = await withBackend(({ backend, spy }) =>
      Effect.gen(function* () {
        // The spy conn rejects every action, so a wire call would fail here.
        yield* Effect.orDie(backend.push([]))
        return spy.conns.length
      }),
    )

    expect(conns).toBe(0)
  })

  it('makeRivetSync delegates to makeRivetSyncWith', async () => {
    const backend = await Effect.runPromise(
      Effect.gen(function* () {
        return yield* makeRivetSync({ endpoint: ENDPOINT })({
          storeId: 'store-2',
          clientId: 'client-2',
          payload: undefined,
        })
      }).pipe(Effect.scoped, Effect.provide(layer), Effect.orDie),
    )

    expect(backend.metadata.name).toBe('livestore-sync-rivet-actors')
    expect(await Effect.runPromise(SubscriptionRef.get(backend.isConnected))).toBe(false)
  })
})
