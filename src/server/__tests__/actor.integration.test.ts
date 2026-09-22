/**
 * Integration test for the real `LiveStoreSync` actor: the layer built by
 * `makeLiveStoreSyncActor` is booted through `Registry.test` (which
 * auto-spawns / reuses the local Rivet engine) and driven with a raw
 * `rivetkit/client`, exactly as the LiveStore client will drive it.
 *
 * The engine outlives the test process and persists actor state, so every
 * test uses a fresh `storeId`.
 */

import { ServerAheadError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Layer, ManagedRuntime, Option, type Schema } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { Registry } from '@rivetkit/effect'
import { ActorError, type ActorConnRaw, createClient } from 'rivetkit/client'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import {
  ACTION_PING,
  ACTION_PULL,
  ACTION_PUSH,
  ACTION_TEST_DISCONNECT_ALL,
  ACTOR_NAME,
  decodePingError,
  decodePong,
  decodePullResponse,
  decodePushAck,
  decodePushError,
  decodeTestDisconnectAllResponse,
  encodeConnParams,
  encodePingRequest,
  encodePullRequest,
  encodePushRequest,
  encodeTestDisconnectAllRequest,
  InvalidPayloadError,
  LIVE_PULL_EVENT,
} from '../../common/mod.ts'
import { makeLiveStoreSyncActor, registryOptions } from '../actor.ts'
import { VALIDATE_REJECTED_REASON } from '../validate-payload.ts'
import { chainedEvents } from './push-test-utils.ts'

// -----------------------------------------------------------------------------
// Boot
// -----------------------------------------------------------------------------

const ENDPOINT = process.env.RIVET_ENDPOINT ?? 'http://127.0.0.1:6420'
const CLIENT_ID = 'it-client'
const OK_PAYLOAD = { token: 'ok' } as const
const BAD_PAYLOAD = { token: 'bad' } as const

const ADMIN_SECRET = 's3cret'

const ActorsLayer = makeLiveStoreSyncActor({
  testing: { enabled: true },
  admin: { secret: ADMIN_SECRET },
  validatePayload: (payload) => {
    const token = (payload as { token?: unknown } | undefined)?.token
    if (token !== 'ok') throw new Error(`rejected token ${String(token)}`)
  },
})

const TestLayer = Registry.test.pipe(
  Layer.provide(ActorsLayer),
  Layer.provideMerge(Registry.layer(registryOptions({ noWelcome: true, endpoint: process.env.RIVET_ENDPOINT }))),
)
const runtime = ManagedRuntime.make(TestLayer)

let client: ReturnType<typeof createClient>

beforeAll(async () => {
  await runtime.runPromise(Effect.void)
  client = createClient({ endpoint: ENDPOINT })
  await waitForRunnerRoutable()
})

/**
 * `Registry.test` returns as soon as the runner process is up, but on a cold
 * engine the namespace's runner config can lag by a few seconds
 * (`actor.no_runner_config_configured`). Retry a trivial action on a
 * throwaway store until it succeeds so the first real test never eats that
 * window. Mirrors the readiness wait the conformance harness does in
 * `tests/harness/engine.ts`.
 */
const waitForRunnerRoutable = async (timeoutMs = 45_000): Promise<void> => {
  const started = Date.now()
  const storeId = `it-ready-${nanoid()}`
  let lastError: unknown
  while (Date.now() - started < timeoutMs) {
    try {
      await client.getOrCreate(ACTOR_NAME, [storeId]).action({
        name: ACTION_PING,
        args: [encodePingRequest({ storeId, clientId: CLIENT_ID, payload: OK_PAYLOAD })],
      })
      return
    } catch (error) {
      lastError = error
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
  throw new Error(`runner did not become routable within ${timeoutMs}ms: ${String(lastError)}`)
}

afterAll(async () => {
  await client.dispose()
  await runtime.dispose()
})

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const waitFor = async (predicate: () => boolean, label: string, timeoutMs = 20_000): Promise<void> => {
  const started = Date.now()
  while (predicate() === false) {
    if (Date.now() - started > timeoutMs) throw new Error(`timed out waiting for ${label}`)
    await sleep(25)
  }
}

const freshStoreId = () => `it-${nanoid()}`

type TestConn = {
  readonly conn: ActorConnRaw
  /** Raw `pull` event payloads in arrival order. */
  readonly events: unknown[]
  readonly statuses: string[]
}

/** Connects to `storeId` with `payload` in the conn params and subscribes to live pulls before anything else. */
const connect = (storeId: string, payload: Schema.Json | undefined): TestConn => {
  const handle = client.getOrCreate(ACTOR_NAME, [storeId])
  // Untyped `createClient()` yields `ActorConn<AnyActorDefinition>` without `on`; it lives on the raw class.
  const conn = handle.connect(
    encodeConnParams({ storeId, clientId: CLIENT_ID, ...(payload !== undefined ? { payload } : {}) }),
  ) as unknown as ActorConnRaw
  const events: unknown[] = []
  const statuses: string[] = []
  conn.onStatusChange((status) => statuses.push(status))
  conn.on(LIVE_PULL_EVENT, (payload) => events.push(payload))
  return { conn, events, statuses }
}

const push = (
  conn: ActorConnRaw,
  storeId: string,
  batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>,
  payload: Schema.Json = OK_PAYLOAD,
) =>
  conn.action({
    name: ACTION_PUSH,
    args: [encodePushRequest({ storeId, clientId: CLIENT_ID, payload, batch, backendId: Option.none() })],
  })

const pull = (
  conn: ActorConnRaw,
  storeId: string,
  cursor: Option.Option<{ eventSequenceNumber: EventSequenceNumber.Global.Type; backendId: Option.Option<string> }>,
) =>
  conn
    .action({
      name: ACTION_PULL,
      args: [encodePullRequest({ storeId, clientId: CLIENT_ID, payload: OK_PAYLOAD, cursor })],
    })
    .then(decodePullResponse)

const seqNums = (res: ReturnType<typeof decodePullResponse>) => res.batch.map((item) => item.eventEncoded.seqNum)

const expectActorError = async (promise: Promise<unknown>, code: string): Promise<ActorError> => {
  const error = await promise.then(
    () => undefined,
    (cause: unknown) => cause,
  )
  expect(error).toBeInstanceOf(ActorError)
  const actorError = error as ActorError
  expect(actorError.code).toBe(code)
  expect((actorError.metadata as { _tag?: unknown })?._tag).toBe('EffectActionError')
  return actorError
}

const envelopeError = (error: ActorError): unknown => (error.metadata as { error: unknown }).error

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

describe('LiveStoreSync actor (in-process engine, raw rivetkit client)', () => {
  it('push acks with the backendId and fans the batch out to every authorized connection of that store only', async () => {
    const storeA = freshStoreId()
    const storeB = freshStoreId()
    const pusher = connect(storeA, OK_PAYLOAD)
    const observer = connect(storeA, OK_PAYLOAD)
    const otherStore = connect(storeB, OK_PAYLOAD)
    try {
      await Promise.all([pusher.conn.ready, observer.conn.ready, otherStore.conn.ready])

      const batch = chainedEvents(2)
      const ack = decodePushAck(await push(pusher.conn, storeA, batch))
      expect(typeof ack.backendId).toBe('string')
      expect(ack.backendId.length).toBeGreaterThan(0)

      await waitFor(() => pusher.events.length >= 1 && observer.events.length >= 1, 'live pull on both conns')

      for (const received of [pusher.events[0], observer.events[0]]) {
        const res = decodePullResponse(received)
        expect(seqNums(res)).toEqual([1, 2])
        expect(res.pageInfo._tag).toBe('NoMore')
        expect(res.backendId).toBe(ack.backendId)
        expect(res.batch.every((item) => Option.isSome(item.metadata))).toBe(true)
      }

      // Catch-up pull sees the same events; a cursor after the first event yields the rest.
      const full = await pull(pusher.conn, storeA, Option.none())
      expect(seqNums(full)).toEqual([1, 2])
      expect(full.pageInfo._tag).toBe('NoMore')
      expect(full.backendId).toBe(ack.backendId)

      const tail = await pull(
        pusher.conn,
        storeA,
        Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(1), backendId: Option.some(ack.backendId) }),
      )
      expect(seqNums(tail)).toEqual([2])

      await sleep(300)
      expect(otherStore.events).toEqual([])
      expect(pusher.events).toHaveLength(1)
      expect(observer.events).toHaveLength(1)
    } finally {
      await Promise.all([pusher.conn.dispose(), observer.conn.dispose(), otherStore.conn.dispose()])
    }
  })

  it('rejects a push whose parent is not the head with a decodable ServerAheadError envelope', async () => {
    const storeId = freshStoreId()
    const { conn } = connect(storeId, OK_PAYLOAD)
    try {
      await conn.ready
      await push(conn, storeId, chainedEvents(1)) // head → 1

      const error = await expectActorError(push(conn, storeId, chainedEvents(1, 5)), 'ServerAheadError')
      const decoded = decodePushError(envelopeError(error))
      expect(decoded).toBeInstanceOf(ServerAheadError)
      expect(decoded).toMatchObject({ minimumExpectedNum: 1, providedNum: 5 })
    } finally {
      await conn.dispose()
    }
  })

  it('surfaces validatePayload rejections as InvalidPayloadError on Push and Ping', async () => {
    const storeId = freshStoreId()
    const { conn } = connect(storeId, OK_PAYLOAD)
    try {
      await conn.ready

      const pushError = await expectActorError(push(conn, storeId, chainedEvents(1), BAD_PAYLOAD), 'InvalidPayloadError')
      const decodedPush = decodePushError(envelopeError(pushError))
      expect(decodedPush).toBeInstanceOf(InvalidPayloadError)
      expect(decodedPush).toMatchObject({ storeId, reason: VALIDATE_REJECTED_REASON })

      const pingError = await expectActorError(
        conn.action({
          name: ACTION_PING,
          args: [encodePingRequest({ storeId, clientId: CLIENT_ID, payload: BAD_PAYLOAD })],
        }),
        'InvalidPayloadError',
      )
      expect(decodePingError(envelopeError(pingError))).toBeInstanceOf(InvalidPayloadError)

      // Nothing was persisted by the rejected push.
      const res = await pull(conn, storeId, Option.none())
      expect(res.batch).toEqual([])
    } finally {
      await conn.dispose()
    }
  })

  it('disconnects a connection whose params fail validatePayload on the next fan-out and never sends it events', async () => {
    const storeId = freshStoreId()
    const good = connect(storeId, OK_PAYLOAD)
    const bad = connect(storeId, BAD_PAYLOAD)
    try {
      await Promise.all([good.conn.ready, bad.conn.ready])
      // Connecting alone must not trigger validation. The engine may drop the very
      // first attempt while the actor wakes (`guard.actor_wake_retries_exceeded`,
      // `actor.destroyed_during_creation` — seen when a previous test file's runner
      // is still being torn down) and rivetkit reconnects at once, so only count
      // disconnects that happen after the connection is up.
      expect(bad.statuses.at(-1)).toBe('connected')
      const disconnectsBeforePush = bad.statuses.filter((status) => status === 'disconnected').length

      await push(good.conn, storeId, chainedEvents(1))

      await waitFor(
        () => bad.statuses.filter((status) => status === 'disconnected').length > disconnectsBeforePush,
        'bad connection to be disconnected',
      )
      await waitFor(() => good.events.length >= 1, 'good connection to receive the live pull')
      expect(bad.events).toEqual([])
      expect(seqNums(decodePullResponse(good.events[0]))).toEqual([1])
    } finally {
      await Promise.all([good.conn.dispose(), bad.conn.dispose()])
    }
  })

  it('answers Ping with Pong', async () => {
    const storeId = freshStoreId()
    const { conn } = connect(storeId, OK_PAYLOAD)
    try {
      await conn.ready
      const pong = decodePong(
        await conn.action({
          name: ACTION_PING,
          args: [encodePingRequest({ storeId, clientId: CLIENT_ID, payload: OK_PAYLOAD })],
        }),
      )
      expect(pong._tag).toBe('SyncMessage.Pong')
    } finally {
      await conn.dispose()
    }
  })

  it('TestDisconnectAll closes every connection; the client reconnects and keeps receiving live pulls', async () => {
    const storeId = freshStoreId()
    const { conn, events, statuses } = connect(storeId, OK_PAYLOAD)
    try {
      await conn.ready

      // Via the stateless handle: an in-flight action *on the connection being
      // closed* would reject with "Connection closed" before the response arrives.
      const handle = client.getOrCreate(ACTOR_NAME, [storeId])
      const res = decodeTestDisconnectAllResponse(
        await handle.action({
          name: ACTION_TEST_DISCONNECT_ALL,
          args: [encodeTestDisconnectAllRequest({ storeId, clientId: CLIENT_ID, payload: OK_PAYLOAD })],
        }),
      )
      // The WS connection plus the transient connection rivetkit opens for the
      // stateless `handle.action` call itself (`c.conns` includes it).
      expect(res.disconnected).toBeGreaterThanOrEqual(1)

      await waitFor(() => statuses.includes('disconnected'), 'disconnect to be observed')
      await waitFor(
        () => statuses.lastIndexOf('connected') > statuses.indexOf('disconnected'),
        'client to reconnect',
      )
      expect(statuses.slice(0, 3)).toEqual(['connected', 'disconnected', 'connecting'])

      // Live pulls resume on the reconnected socket (rivetkit re-subscribes).
      await push(conn, storeId, chainedEvents(1))
      await waitFor(() => events.length >= 1, 'live pull after reconnect')
      expect(seqNums(decodePullResponse(events[0]))).toEqual([1])
    } finally {
      await conn.dispose()
    }
  })

  it('refuses TestDisconnectAll for a caller that fails validatePayload', async () => {
    const storeId = freshStoreId()
    const handle = client.getOrCreate(ACTOR_NAME, [storeId])
    await expectActorError(
      handle.action({
        name: ACTION_TEST_DISCONNECT_ALL,
        args: [encodeTestDisconnectAllRequest({ storeId, clientId: CLIENT_ID, payload: BAD_PAYLOAD })],
      }),
      'InvalidPayloadError',
    )
  })
})
