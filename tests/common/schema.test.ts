import { BackendIdMismatchError, ServerAheadError, SyncBackend, UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Option } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import {
  ACTION_PING,
  ACTION_PULL,
  ACTION_PUSH,
  ACTION_TEST_DISCONNECT_ALL,
  ACTOR_NAME,
  ConnParams,
  DEFAULT_PULL_PAGE_SIZE,
  decodeConnParams,
  decodePingRequest,
  decodePullError,
  decodePullRequest,
  decodePullResponse,
  decodePushAck,
  decodePushError,
  decodePushRequest,
  decodeTestDisconnectAllRequest,
  decodeTestDisconnectAllResponse,
  emptyPullResponse,
  encodeConnParams,
  encodePingRequest,
  encodePong,
  encodePullError,
  encodePullRequest,
  encodePullResponse,
  encodePushAck,
  encodePushError,
  encodePushRequest,
  encodeTestDisconnectAllRequest,
  encodeTestDisconnectAllResponse,
  decodePong,
  InvalidPayloadError,
  LIVE_PULL_EVENT,
  MAX_PULL_EVENTS_PER_MESSAGE,
  MAX_PUSH_EVENTS_PER_REQUEST,
  MAX_TRANSPORT_PAYLOAD_BYTES,
  PERSISTENCE_FORMAT_VERSION,
  PingRequest,
  Pong,
  PullRequest,
  PullResponse,
  pullResponseToItem,
  PushAck,
  PushRequest,
  SyncMetadata,
  TestDisconnectAllRequest,
  TestDisconnectAllResponse,
} from '../../src/common/mod.ts'

/**
 * Simulates the transport: the encoded value must survive a JSON round-trip
 * (rivetkit CBOR is JSON-compatible) before it is handed back to the decoder.
 */
const overTheWire = (encoded: unknown): unknown => JSON.parse(JSON.stringify(encoded))

const seq = EventSequenceNumber.Global.make

const event = (seqNum: number, parentSeqNum: number, args: unknown = {}): LiveStoreEvent.Global.Encoded => ({
  name: 'todoCreated-v1',
  args,
  seqNum: seq(seqNum),
  parentSeqNum: seq(parentSeqNum),
  clientId: 'client-xyz',
  sessionId: 'session-123',
})

describe('constants', () => {
  it('pins the wire-level names and limits', () => {
    expect(ACTOR_NAME).toBe('LiveStoreSync')
    expect(ACTION_PULL).toBe('Pull')
    expect(ACTION_PUSH).toBe('Push')
    expect(ACTION_PING).toBe('Ping')
    expect(ACTION_TEST_DISCONNECT_ALL).toBe('TestDisconnectAll')
    expect(LIVE_PULL_EVENT).toBe('pull')
    expect(MAX_PULL_EVENTS_PER_MESSAGE).toBe(100)
    expect(MAX_PUSH_EVENTS_PER_REQUEST).toBe(100)
    expect(MAX_TRANSPORT_PAYLOAD_BYTES).toBe(900_000)
    expect(DEFAULT_PULL_PAGE_SIZE).toBe(100)
    expect(PERSISTENCE_FORMAT_VERSION).toBe(1)
  })
})

describe('ConnParams', () => {
  it('round-trips with a payload', () => {
    const params = ConnParams.make({ storeId: 's1', clientId: 'c1', payload: { authToken: 'abc' } })
    const encoded = encodeConnParams(params)
    expect(encoded).toEqual({ storeId: 's1', clientId: 'c1', payload: { authToken: 'abc' } })
    expect(decodeConnParams(overTheWire(encoded))).toEqual(params)
  })

  it('omits the payload key entirely when there is no payload', () => {
    const params = ConnParams.make({ storeId: 's1', clientId: 'c1' })
    const encoded = encodeConnParams(params) as Record<string, unknown>
    expect('payload' in encoded).toBe(false)
    expect(decodeConnParams(overTheWire(encoded))).toEqual(params)
  })
})

describe('PullRequest', () => {
  it('round-trips a None cursor without payload/limit', () => {
    const req = PullRequest.make({ storeId: 's1', clientId: 'c1', cursor: Option.none() })
    const encoded = encodePullRequest(req) as Record<string, unknown>

    expect(encoded).toEqual({ storeId: 's1', clientId: 'c1', cursor: { _tag: 'None' } })
    expect('payload' in encoded).toBe(false)
    expect('limit' in encoded).toBe(false)
    expect(decodePullRequest(overTheWire(encoded))).toEqual(req)
  })

  it('round-trips a Some cursor with Some backendId, payload and limit', () => {
    const req = PullRequest.make({
      storeId: 's1',
      clientId: 'c1',
      payload: { authToken: 'abc', nested: [1, true, null] },
      cursor: Option.some({ eventSequenceNumber: seq(42), backendId: Option.some('backend-1') }),
      limit: 25,
    })
    const encoded = encodePullRequest(req)

    expect(encoded).toEqual({
      storeId: 's1',
      clientId: 'c1',
      payload: { authToken: 'abc', nested: [1, true, null] },
      cursor: { _tag: 'Some', value: { eventSequenceNumber: 42, backendId: { _tag: 'Some', value: 'backend-1' } } },
      limit: 25,
    })

    const decoded = decodePullRequest(overTheWire(encoded))
    expect(decoded).toEqual(req)
    expect(Option.getOrThrow(decoded.cursor).eventSequenceNumber).toBe(42)
  })

  it('round-trips a Some cursor with a None backendId', () => {
    const req = PullRequest.make({
      storeId: 's1',
      clientId: 'c1',
      cursor: Option.some({ eventSequenceNumber: seq(7), backendId: Option.none() }),
    })
    const encoded = encodePullRequest(req)

    expect(encoded).toEqual({
      storeId: 's1',
      clientId: 'c1',
      cursor: { _tag: 'Some', value: { eventSequenceNumber: 7, backendId: { _tag: 'None' } } },
    })

    const decoded = decodePullRequest(overTheWire(encoded))
    expect(decoded).toEqual(req)
    expect(Option.isNone(Option.getOrThrow(decoded.cursor).backendId)).toBe(true)
  })
})

describe('PullResponse', () => {
  const metadata = SyncMetadata.make({ createdAt: '2024-05-01T12:00:00.000Z' })

  const responseWith = (pageInfo: SyncBackend.PullResPageInfo, metadataOpt: Option.Option<SyncMetadata>) =>
    PullResponse.make({
      batch: [
        { eventEncoded: event(1, 0, { id: 'a', text: 'Buy milk', done: false, tags: ['x'] }), metadata: metadataOpt },
        { eventEncoded: event(2, 1, { id: 'b', text: null }), metadata: metadataOpt },
      ],
      pageInfo,
      backendId: 'backend-1',
    })

  it.each([
    ['NoMore', SyncBackend.pageInfoNoMore],
    ['MoreKnown', SyncBackend.pageInfoMoreKnown(150)],
    ['MoreUnknown', SyncBackend.pageInfoMoreUnknown],
  ] as const)('round-trips pageInfo %s with Some metadata', (_name, pageInfo) => {
    const res = responseWith(pageInfo, Option.some(metadata))
    const encoded = encodePullResponse(res) as { pageInfo: unknown }

    expect(encoded.pageInfo).toEqual(pageInfo)
    expect(decodePullResponse(overTheWire(encoded))).toEqual(res)
  })

  it('round-trips None metadata', () => {
    const res = responseWith(SyncBackend.pageInfoNoMore, Option.none())
    const encoded = encodePullResponse(res) as { batch: ReadonlyArray<{ metadata: unknown }> }

    expect(encoded.batch[0]!.metadata).toEqual({ _tag: 'None' })
    expect(decodePullResponse(overTheWire(encoded))).toEqual(res)
  })

  it('encodes the event fields verbatim and preserves arbitrary JSON args', () => {
    const res = responseWith(SyncBackend.pageInfoNoMore, Option.some(metadata))
    const encoded = encodePullResponse(res) as {
      batch: ReadonlyArray<{ eventEncoded: Record<string, unknown>; metadata: Record<string, unknown> }>
      backendId: string
    }

    expect(encoded.batch[0]!.eventEncoded).toEqual({
      name: 'todoCreated-v1',
      args: { id: 'a', text: 'Buy milk', done: false, tags: ['x'] },
      seqNum: 1,
      parentSeqNum: 0,
      clientId: 'client-xyz',
      sessionId: 'session-123',
    })
    expect(encoded.batch[0]!.metadata).toEqual({
      _tag: 'Some',
      value: { _tag: 'SyncMessage.SyncMetadata', createdAt: '2024-05-01T12:00:00.000Z' },
    })
    expect(encoded.backendId).toBe('backend-1')
  })

  it('emptyPullResponse is a single NoMore page', () => {
    const res = emptyPullResponse('backend-1')
    expect(res.batch).toEqual([])
    expect(res.pageInfo).toEqual(SyncBackend.pageInfoNoMore)
    expect(res.backendId).toBe('backend-1')
    expect(decodePullResponse(overTheWire(encodePullResponse(res)))).toEqual(res)
  })

  it('pullResponseToItem strips backendId', () => {
    const res = responseWith(SyncBackend.pageInfoMoreKnown(3), Option.some(metadata))
    const item = pullResponseToItem(res)

    expect(Object.keys(item).sort()).toEqual(['batch', 'pageInfo'])
    expect(item.batch).toBe(res.batch)
    expect(item.pageInfo).toEqual(SyncBackend.pageInfoMoreKnown(3))
    expect('backendId' in item).toBe(false)
  })
})

describe('PushRequest / PushAck', () => {
  it('round-trips with a Some backendId and a payload', () => {
    const req = PushRequest.make({
      storeId: 's1',
      clientId: 'c1',
      payload: { authToken: 'abc' },
      batch: [event(1, 0, { id: 'a' }), event(2, 1, { id: 'b' })],
      backendId: Option.some('backend-1'),
    })
    const encoded = encodePushRequest(req) as { backendId: unknown; batch: ReadonlyArray<unknown> }

    expect(encoded.backendId).toEqual({ _tag: 'Some', value: 'backend-1' })
    expect(encoded.batch).toHaveLength(2)
    expect(decodePushRequest(overTheWire(encoded))).toEqual(req)
  })

  it('round-trips with a None backendId, no payload and an empty batch', () => {
    const req = PushRequest.make({ storeId: 's1', clientId: 'c1', batch: [], backendId: Option.none() })
    const encoded = encodePushRequest(req) as Record<string, unknown>

    expect('payload' in encoded).toBe(false)
    expect(encoded).toEqual({ storeId: 's1', clientId: 'c1', batch: [], backendId: { _tag: 'None' } })
    expect(decodePushRequest(overTheWire(encoded))).toEqual(req)
  })

  it('round-trips a PushAck', () => {
    const ack = PushAck.make({ backendId: 'backend-1' })
    const encoded = encodePushAck(ack)

    expect(encoded).toEqual({ backendId: 'backend-1' })
    expect(decodePushAck(overTheWire(encoded))).toEqual(ack)
  })
})

describe('Ping / Pong / test actions', () => {
  it('round-trips a PingRequest', () => {
    const req = PingRequest.make({ storeId: 's1', clientId: 'c1' })
    const encoded = encodePingRequest(req) as Record<string, unknown>

    expect('payload' in encoded).toBe(false)
    expect(decodePingRequest(overTheWire(encoded))).toEqual(req)
  })

  it('round-trips a Pong', () => {
    const pong = Pong.make({})
    const encoded = encodePong(pong)

    expect(encoded).toEqual({ _tag: 'SyncMessage.Pong' })
    expect(decodePong(overTheWire(encoded))).toEqual(pong)
  })

  it('round-trips the TestDisconnectAll pair', () => {
    const req = TestDisconnectAllRequest.make({ storeId: 's1', clientId: 'c1' })
    const res = TestDisconnectAllResponse.make({ disconnected: 3 })

    expect(decodeTestDisconnectAllRequest(overTheWire(encodeTestDisconnectAllRequest(req)))).toEqual(req)
    expect(encodeTestDisconnectAllResponse(res)).toEqual({ disconnected: 3 })
    expect(decodeTestDisconnectAllResponse(overTheWire(encodeTestDisconnectAllResponse(res)))).toEqual(res)
  })
})

describe('error unions', () => {
  it('round-trips UnknownError, keeping the Error cause as an Error', () => {
    const error = new UnknownError({ cause: new Error('x'), note: 'boom' })
    const encoded = encodePullError(error) as Record<string, unknown>

    expect(encoded._tag).toBe('UnknownError')
    // `Schema.Defect` serialises an `Error` as `{ name, message }` (no stack by default).
    expect(encoded.cause).toEqual({ name: 'Error', message: 'x' })
    expect(encoded.note).toBe('boom')

    const decoded = decodePullError(overTheWire(encoded))
    expect(decoded._tag).toBe('UnknownError')
    expect(decoded).toBeInstanceOf(UnknownError)
    // The defect is revived as an `Error` with the same message; the stack is NOT preserved.
    expect((decoded as UnknownError).cause).toBeInstanceOf(Error)
    expect(((decoded as UnknownError).cause as Error).message).toBe('x')
  })

  it('round-trips ServerAheadError', () => {
    const error = new ServerAheadError({ minimumExpectedNum: seq(5), providedNum: seq(3) })
    const encoded = encodePushError(error)

    expect(encoded).toEqual({ _tag: 'ServerAheadError', minimumExpectedNum: 5, providedNum: 3 })

    const decoded = decodePushError(overTheWire(encoded))
    expect(decoded._tag).toBe('ServerAheadError')
    expect(decoded).toBeInstanceOf(ServerAheadError)
    expect(decoded).toEqual(error)
  })

  it('round-trips BackendIdMismatchError', () => {
    const error = new BackendIdMismatchError({ expected: 'a', received: 'b' })
    const encoded = encodePushError(error)

    expect(encoded).toEqual({ _tag: 'BackendIdMismatchError', expected: 'a', received: 'b' })

    const decoded = decodePushError(overTheWire(encoded))
    expect(decoded._tag).toBe('BackendIdMismatchError')
    expect(decoded).toBeInstanceOf(BackendIdMismatchError)
    expect(decoded).toEqual(error)
  })

  it('round-trips InvalidPayloadError without a cause', () => {
    const error = new InvalidPayloadError({ storeId: 's1', reason: 'validatePayload rejected' })
    const encoded = encodePullError(error) as Record<string, unknown>

    expect(encoded).toEqual({ _tag: 'InvalidPayloadError', storeId: 's1', reason: 'validatePayload rejected' })
    expect('cause' in encoded).toBe(false)

    const decoded = decodePullError(overTheWire(encoded))
    expect(decoded._tag).toBe('InvalidPayloadError')
    expect(decoded).toBeInstanceOf(InvalidPayloadError)
  })

  it('round-trips InvalidPayloadError with an Error cause', () => {
    const error = new InvalidPayloadError({ storeId: 's1', reason: 'bad token', cause: new Error('boom') })
    const encoded = encodePullError(error) as Record<string, unknown>

    expect(encoded.cause).toEqual({ name: 'Error', message: 'boom' })

    const decoded = decodePullError(overTheWire(encoded)) as InvalidPayloadError
    expect(decoded._tag).toBe('InvalidPayloadError')
    expect(decoded.cause).toBeInstanceOf(Error)
    expect((decoded.cause as Error).message).toBe('boom')
  })

  it('discriminates every member of the push error union by _tag', () => {
    const members = [
      new UnknownError({ cause: new Error('x') }),
      new ServerAheadError({ minimumExpectedNum: seq(2), providedNum: seq(1) }),
      new BackendIdMismatchError({ expected: 'a', received: 'b' }),
      new InvalidPayloadError({ storeId: 's1', reason: 'nope' }),
    ] as const

    for (const member of members) {
      const decoded = decodePushError(overTheWire(encodePushError(member)))
      expect(decoded._tag).toBe(member._tag)
    }
  })
})
