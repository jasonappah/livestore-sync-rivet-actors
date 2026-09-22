import { BackendIdMismatchError, IsOfflineError, ServerAheadError, SyncBackend, UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Exit, Option, Result } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import {
  ACTION_PING,
  ACTION_PULL,
  ACTION_PUSH,
  ACTION_TEST_DISCONNECT_ALL,
  encodePong,
  encodePullResponse,
  encodePushAck,
  encodePushError,
  encodeTestDisconnectAllResponse,
  InvalidPayloadError,
  PingRequest,
  Pong,
  PullRequest,
  PullResponse,
  PushAck,
  PushRequest,
  SyncMetadata,
  TestDisconnectAllRequest,
} from '../../src/common/mod.ts'
import { type ActionClient, makeActionClient } from '../../src/client/action-client.ts'
import { ACTION_ERROR_ENVELOPE_TAG, ACTION_ERROR_ENVELOPE_VERSION } from '../../src/client/errors.ts'
import { type ConnStatus, RawActionFailure } from '../../src/client/types.ts'
import { type FakeConnection, makeFakeConnection } from '../harness/fake-connection.ts'

const seq = EventSequenceNumber.Global.make

const event = (seqNum: number, parentSeqNum: number): LiveStoreEvent.Global.Encoded => ({
  name: 'todoCreated-v1',
  args: { id: 't1' },
  seqNum: seq(seqNum),
  parentSeqNum: seq(parentSeqNum),
  clientId: 'c1',
  sessionId: 'session-1',
})

/** Simulates the CBOR transport for values the server sends back. */
const overTheWire = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

const envelope = (error: unknown) => ({
  _tag: ACTION_ERROR_ENVELOPE_TAG,
  version: ACTION_ERROR_ENVELOPE_VERSION,
  error,
})

const rawFailure = (cause: unknown, statusAtFailure: ConnStatus = 'connected') =>
  new RawActionFailure({ cause, statusAtFailure })

/** Boots the fake connection + action client inside one Effect. */
const withClient = <A, E>(
  use: (ctx: { readonly client: ActionClient; readonly fake: FakeConnection }) => Effect.Effect<A, E>,
): Promise<A> =>
  Effect.runPromise(
    Effect.flatMap(makeFakeConnection(), (fake) => use({ client: makeActionClient(fake.conn), fake })),
  )

const expectFailure = <A, E>(effect: Effect.Effect<A, E>): Effect.Effect<E> =>
  Effect.map(Effect.result(effect), (result) => {
    if (Result.isSuccess(result)) throw new Error('expected the action to fail')
    return result.failure
  })

describe('makeActionClient / pull', () => {
  it('encodes the request, sends it under the Pull action name and decodes the response', async () => {
    const response = PullResponse.make({
      batch: [{ eventEncoded: event(1, 0), metadata: Option.some(SyncMetadata.make({ createdAt: '2026-01-01T00:00:00.000Z' })) }],
      pageInfo: SyncBackend.pageInfoNoMore,
      backendId: 'backend-1',
    })

    const { decoded, calls } = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PULL, () => Effect.succeed(overTheWire(encodePullResponse(response))))
        const decoded = yield* client.pull(PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() }))
        return { decoded, calls: fake.calls }
      }),
    )

    expect(calls).toHaveLength(1)
    expect(calls[0]!.name).toBe(ACTION_PULL)
    const payload = calls[0]!.payload as Record<string, unknown>
    expect(payload).toEqual({ storeId: 's1', clientId: 'c1', cursor: { _tag: 'None' } })
    // `payload`/`limit` use `optionalKey`: an absent value must be an absent key.
    expect('payload' in payload).toBe(false)
    expect('limit' in payload).toBe(false)

    expect(decoded).toEqual(response)
    expect(decoded.batch[0]!.eventEncoded.seqNum).toBe(1)
    expect(decoded.pageInfo._tag).toBe('NoMore')
  })

  it('encodes a Some cursor, sync payload and limit', async () => {
    const calls = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PULL, () =>
          Effect.succeed(overTheWire(encodePullResponse(PullResponse.make({
            batch: [],
            pageInfo: SyncBackend.pageInfoNoMore,
            backendId: 'backend-1',
          })))),
        )
        yield* client.pull(
          PullRequest.make({
            storeId: 's1',
            clientId: 'c1',
            payload: { authToken: 'abc' },
            cursor: Option.some({ eventSequenceNumber: seq(7), backendId: Option.some('backend-1') }),
            limit: 25,
          }),
        )
        return fake.calls
      }),
    )

    expect(calls[0]!.payload).toEqual({
      storeId: 's1',
      clientId: 'c1',
      payload: { authToken: 'abc' },
      cursor: { _tag: 'Some', value: { eventSequenceNumber: 7, backendId: { _tag: 'Some', value: 'backend-1' } } },
      limit: 25,
    })
  })

  it('maps a declared BackendIdMismatchError from the envelope', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        const encoded = encodePushError(new BackendIdMismatchError({ expected: 'a', received: 'b' }))
        fake.onAction(ACTION_PULL, () =>
          Effect.fail(
            rawFailure({ group: 'user', code: 'BackendIdMismatchError', metadata: overTheWire(envelope(encoded)) }),
          ),
        )
        return yield* expectFailure(client.pull(PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() })))
      }),
    )

    expect(error).toBeInstanceOf(BackendIdMismatchError)
    expect(error).toMatchObject({ expected: 'a', received: 'b' })
  })

  it('maps a dropped in-flight action to IsOfflineError', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PULL, () =>
          Effect.fail(rawFailure(new Error('Connection closed (code: 1000, reason: unauthorized)'))),
        )
        return yield* expectFailure(client.pull(PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() })))
      }),
    )

    expect(error).toBeInstanceOf(IsOfflineError)
  })

  it('passes an IsOfflineError raised by the connection through', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PULL, () => Effect.fail(new IsOfflineError({ cause: 'connect timeout' })))
        return yield* expectFailure(client.pull(PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() })))
      }),
    )

    expect(error).toBeInstanceOf(IsOfflineError)
    expect((error as IsOfflineError).cause).toBe('connect timeout')
  })

  it('dies when the server returns an undecodable success value', async () => {
    const exit = await Effect.runPromiseExit(
      Effect.flatMap(makeFakeConnection(), (fake) => {
        const client = makeActionClient(fake.conn)
        fake.onAction(ACTION_PULL, () => Effect.succeed({ nope: true }))
        return client.pull(PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() }))
      }),
    )

    expect(Exit.isFailure(exit)).toBe(true)
  })
})

describe('makeActionClient / push', () => {
  it('encodes the batch and decodes the ack', async () => {
    const { ack, calls } = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PUSH, () =>
          Effect.succeed(overTheWire(encodePushAck(PushAck.make({ backendId: 'backend-1' })))),
        )
        const ack = yield* client.push(
          PushRequest.make({
            storeId: 's1',
            clientId: 'c1',
            batch: [event(1, 0), event(2, 1)],
            backendId: Option.none(),
          }),
        )
        return { ack, calls: fake.calls }
      }),
    )

    expect(calls[0]!.name).toBe(ACTION_PUSH)
    const payload = calls[0]!.payload as Record<string, unknown>
    expect(payload.backendId).toEqual({ _tag: 'None' })
    expect((payload.batch as ReadonlyArray<unknown>)).toHaveLength(2)
    expect('payload' in payload).toBe(false)
    expect(ack).toEqual(PushAck.make({ backendId: 'backend-1' }))
  })

  it('maps a declared ServerAheadError from the envelope', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        const encodedServerAheadError = encodePushError(
          new ServerAheadError({ minimumExpectedNum: seq(5), providedNum: seq(3) }),
        )
        fake.onAction(ACTION_PUSH, () =>
          Effect.fail(
            rawFailure({
              group: 'user',
              code: 'ServerAheadError',
              message: 'Fail failed',
              metadata: overTheWire(envelope(encodedServerAheadError)),
            }),
          ),
        )
        return yield* expectFailure(
          client.push(
            PushRequest.make({ storeId: 's1', clientId: 'c1', batch: [event(4, 3)], backendId: Option.some('backend-1') }),
          ),
        )
      }),
    )

    expect(error).toBeInstanceOf(ServerAheadError)
    expect(error).toMatchObject({ minimumExpectedNum: 5, providedNum: 3 })
  })

  it('re-maps a declared InvalidPayloadError to UnknownError', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        const encoded = encodePushError(new InvalidPayloadError({ storeId: 's1', reason: 'unauthorized' }))
        fake.onAction(ACTION_PUSH, () =>
          Effect.fail(rawFailure({ group: 'user', code: 'InvalidPayloadError', metadata: overTheWire(envelope(encoded)) })),
        )
        return yield* expectFailure(
          client.push(PushRequest.make({ storeId: 's1', clientId: 'c1', batch: [event(1, 0)], backendId: Option.none() })),
        )
      }),
    )

    expect(error).toBeInstanceOf(UnknownError)
    expect((error as UnknownError).note).toBe('validatePayload rejected')
  })

  it('maps a rivetkit internal error to UnknownError with group + code', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PUSH, () =>
          Effect.fail(rawFailure({ group: 'rivetkit', code: 'internal_error', metadata: null })),
        )
        return yield* expectFailure(
          client.push(PushRequest.make({ storeId: 's1', clientId: 'c1', batch: [event(1, 0)], backendId: Option.none() })),
        )
      }),
    )

    expect(error).toBeInstanceOf(UnknownError)
    expect((error as UnknownError).payload).toEqual({ group: 'rivetkit', code: 'internal_error' })
  })

  it('maps a failure observed while disconnected to IsOfflineError', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PUSH, () => Effect.fail(rawFailure({ group: 'request', code: 'invalid' }, 'disconnected')))
        return yield* expectFailure(
          client.push(PushRequest.make({ storeId: 's1', clientId: 'c1', batch: [event(1, 0)], backendId: Option.none() })),
        )
      }),
    )

    expect(error).toBeInstanceOf(IsOfflineError)
  })
})

describe('makeActionClient / ping + testDisconnectAll', () => {
  it('round-trips a Ping', async () => {
    const { pong, calls } = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PING, () => Effect.succeed(overTheWire(encodePong(Pong.make({})))))
        const pong = yield* client.ping(PingRequest.make({ storeId: 's1', clientId: 'c1' }))
        return { pong, calls: fake.calls }
      }),
    )

    expect(calls[0]!.name).toBe(ACTION_PING)
    expect(calls[0]!.payload).toEqual({ storeId: 's1', clientId: 'c1' })
    expect(pong._tag).toBe('SyncMessage.Pong')
  })

  it('maps a guard failure on Ping to IsOfflineError', async () => {
    const error = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_PING, () => Effect.fail(rawFailure({ group: 'guard', code: 'actor_ready_timeout' })))
        return yield* expectFailure(client.ping(PingRequest.make({ storeId: 's1', clientId: 'c1' })))
      }),
    )

    expect(error).toBeInstanceOf(IsOfflineError)
  })

  it('round-trips TestDisconnectAll', async () => {
    const { res, calls } = await withClient(({ client, fake }) =>
      Effect.gen(function* () {
        fake.onAction(ACTION_TEST_DISCONNECT_ALL, () =>
          Effect.succeed(overTheWire(encodeTestDisconnectAllResponse({ disconnected: 3 }))),
        )
        const res = yield* client.testDisconnectAll(TestDisconnectAllRequest.make({ storeId: 's1', clientId: 'c1' }))
        return { res, calls: fake.calls }
      }),
    )

    expect(calls[0]!.name).toBe(ACTION_TEST_DISCONNECT_ALL)
    expect(res.disconnected).toBe(3)
  })
})
