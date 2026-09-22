/**
 * Integration test for the admin actions, driven end to end through the
 * public client entry: `makeRivetSyncAdmin` (ops helper over a plain HTTP
 * handle) and `makeRivetSync` (a real LiveStore SyncBackend with a live pull).
 *
 * It boots its own `LiveStoreSync` actor with an admin secret through
 * `Registry.test`, like `src/server/__tests__/actor.integration.test.ts`, and
 * lives in the conformance layer because it needs both the server and the
 * client entry points.
 */

import { BackendIdMismatchError, type SyncBackend } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import {
  Cause,
  Effect,
  Exit,
  FetchHttpClient,
  Fiber,
  KeyValueStore,
  Layer,
  ManagedRuntime,
  Option,
  Stream,
} from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { Registry } from '@rivetkit/effect'
import { createClient } from 'rivetkit/client'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'

import { makeRivetSync, makeRivetSyncAdmin } from '../src/client/mod.ts'
import { ACTION_PING, ACTOR_NAME, AdminUnauthorizedError, encodePingRequest } from '../src/common/mod.ts'
import { chainedEvents } from '../src/server/__tests__/push-test-utils.ts'
import { makeLiveStoreSyncActor, registryOptions } from '../src/server/mod.ts'
import { VALIDATE_REJECTED_REASON } from '../src/server/validate-payload.ts'

// -----------------------------------------------------------------------------
// Boot
// -----------------------------------------------------------------------------

const ENDPOINT = process.env.RIVET_ENDPOINT ?? 'http://127.0.0.1:6420'
const CLIENT_ID = 'admin-it-client'
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

afterAll(async () => {
  await client.dispose()
  await runtime.dispose()
})

/** Same cold-start guard as the actor integration test: retry a Ping until the runner is routable. */
const waitForRunnerRoutable = async (timeoutMs = 45_000): Promise<void> => {
  const started = Date.now()
  const storeId = `admin-ready-${nanoid()}`
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
      await sleep(500)
    }
  }
  throw new Error(`runner did not become routable within ${timeoutMs}ms: ${String(lastError)}`)
}

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

const freshStoreId = () => `admin-it-${nanoid()}`

describe('admin actions (in-process engine, makeRivetSyncAdmin + makeRivetSync)', () => {
  const withBackend = <A, E>(
    storeId: string,
    clientId: string,
    body: (backend: SyncBackend.SyncBackend<any>) => Effect.Effect<A, E>,
  ): Promise<A> =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* makeRivetSync({ endpoint: ENDPOINT, ping: { enabled: false } })({
            storeId,
            clientId,
            payload: OK_PAYLOAD,
          })
          return yield* body(backend)
        }),
      ).pipe(Effect.provide(Layer.merge(KeyValueStore.layerMemory, FetchHttpClient.layer))),
    )

  it('info reports the store; reset wipes it, fails live pulls with BackendIdMismatchError and allows a fresh history', async () => {
    const storeId = freshStoreId()
    const admin = makeRivetSyncAdmin({ endpoint: ENDPOINT, clientId: 'it-admin' })
    try {
      // A wrong secret (after a passing validatePayload) is a typed AdminUnauthorizedError.
      await expect(admin.info(storeId, 'wrong', OK_PAYLOAD)).rejects.toBeInstanceOf(AdminUnauthorizedError)
      // validatePayload still applies to admin requests.
      await expect(admin.info(storeId, ADMIN_SECRET, BAD_PAYLOAD)).rejects.toMatchObject({
        _tag: 'InvalidPayloadError',
        reason: VALIDATE_REJECTED_REASON,
      })

      const { oldBackendId, newBackendId } = await withBackend(storeId, 'client-a', (backend) =>
        Effect.gen(function* () {
          const seen: number[] = []
          const livePull = yield* backend.pull(Option.none(), { live: true }).pipe(
            Stream.runForEach((item) =>
              Effect.sync(() => {
                for (const event of item.batch) seen.push(event.eventEncoded.seqNum)
              }),
            ),
            Effect.forkChild,
          )

          yield* backend.push(chainedEvents(2))
          yield* Effect.promise(() => waitFor(() => seen.length >= 2, 'live pull to see both events'))

          const before = yield* Effect.promise(() => admin.info(storeId, ADMIN_SECRET, OK_PAYLOAD))
          expect(before).toMatchObject({ storeId, currentHead: 2, eventCount: 2 })
          // The client's WS connection plus the transient one of this HTTP action.
          expect(before.connectionCount).toBeGreaterThanOrEqual(1)

          const { backendId } = yield* Effect.promise(() => admin.reset(storeId, ADMIN_SECRET, OK_PAYLOAD))
          expect(backendId).not.toBe(before.backendId)

          // The reset disconnected the client; its reconnect catch-up carries the stale id.
          const exit = yield* Fiber.await(livePull).pipe(Effect.timeout('20 seconds'), Effect.orDie)
          expect(Exit.isFailure(exit)).toBe(true)
          if (Exit.isFailure(exit)) {
            const error = Cause.squash(exit.cause)
            expect(error).toBeInstanceOf(BackendIdMismatchError)
            expect(error).toMatchObject({ expected: backendId, received: before.backendId })
          }
          expect(seen).toEqual([1, 2])

          // So does a fresh (non-live) pull with the stale cursor.
          const stalePull = yield* backend
            .pull(Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(2), metadata: Option.none() }))
            .pipe(Stream.runCollect, Effect.flip)
          expect(stalePull).toBeInstanceOf(BackendIdMismatchError)

          return { oldBackendId: before.backendId, newBackendId: backendId }
        }),
      )

      const afterReset = await admin.info(storeId, ADMIN_SECRET, OK_PAYLOAD)
      expect(afterReset).toMatchObject({ backendId: newBackendId, currentHead: 0, eventCount: 0 })

      // A brand-new client (empty KeyValueStore) pushes a history from root.
      await withBackend(storeId, 'client-b', (backend) =>
        Effect.gen(function* () {
          yield* backend.push(chainedEvents(3, 'root', 'client-b'))
          const pulled = yield* backend.pull(Option.none()).pipe(Stream.runCollect)
          expect(pulled.flatMap((item) => item.batch.map((event) => event.eventEncoded.seqNum))).toEqual([1, 2, 3])
        }),
      )

      const final = await admin.info(storeId, ADMIN_SECRET, OK_PAYLOAD)
      expect(final).toMatchObject({ backendId: newBackendId, currentHead: 3, eventCount: 3 })
      expect(final.backendId).not.toBe(oldBackendId)
    } finally {
      await admin.dispose()
    }
  })
})
