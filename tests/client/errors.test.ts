import { BackendIdMismatchError, IsOfflineError, ServerAheadError, UnknownError } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import { Effect, Option, Result, Schema } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import { encodePushError, InvalidPayloadError, PullErrorJson, PushErrorJson } from '../../src/common/mod.ts'
import {
  ACTION_ERROR_ENVELOPE_TAG,
  ACTION_ERROR_ENVELOPE_VERSION,
  classifyActionFailure,
  decodeActionErrorEnvelope,
  isRivetErrorLike,
} from '../../src/client/errors.ts'
import { type ConnStatus, RawActionFailure } from '../../src/client/types.ts'

const seq = EventSequenceNumber.Global.make

const decodePushError = Schema.decodeUnknownEffect(PushErrorJson)
const decodePullError = Schema.decodeUnknownEffect(PullErrorJson)

const envelope = (error: unknown) => ({
  _tag: ACTION_ERROR_ENVELOPE_TAG,
  version: ACTION_ERROR_ENVELOPE_VERSION,
  error,
})

/** Simulates CBOR transport: metadata arrives verbatim as plain JSON. */
const overTheWire = (value: unknown): unknown => JSON.parse(JSON.stringify(value))

const failure = (cause: unknown, statusAtFailure: ConnStatus = 'connected') =>
  new RawActionFailure({ cause, statusAtFailure })

/** Runs a classifier and returns the (always present) mapped error. */
const classify = <E>(
  decodeError: (u: unknown) => Effect.Effect<E, unknown>,
  raw: RawActionFailure | IsOfflineError,
) => {
  const result = Effect.runSync(Effect.result(classifyActionFailure(decodeError)(raw)))
  if (Result.isSuccess(result)) throw new Error('classifier unexpectedly succeeded')
  return result.failure
}

describe('isRivetErrorLike', () => {
  it('accepts a structural rivet error', () => {
    expect(isRivetErrorLike({ group: 'actor', code: 'not_found' })).toBe(true)
    expect(isRivetErrorLike({ group: 'user', code: 'ServerAheadError', message: 'x', metadata: null })).toBe(true)
  })

  it('rejects everything else', () => {
    expect(isRivetErrorLike(new Error('Connection closed'))).toBe(false)
    expect(isRivetErrorLike(null)).toBe(false)
    expect(isRivetErrorLike(undefined)).toBe(false)
    expect(isRivetErrorLike('actor/not_found')).toBe(false)
    expect(isRivetErrorLike({ group: 'actor' })).toBe(false)
    expect(isRivetErrorLike({ group: 1, code: 2 })).toBe(false)
  })
})

describe('decodeActionErrorEnvelope', () => {
  it('extracts the inner error from a valid envelope', () => {
    const inner = { _tag: 'UnknownError', cause: { name: 'Error', message: 'boom' } }
    expect(decodeActionErrorEnvelope(envelope(inner))).toStrictEqual(Option.some(inner))
  })

  it('rejects a wrong tag, wrong version or non-object metadata', () => {
    expect(Option.isNone(decodeActionErrorEnvelope({ ...envelope({}), _tag: 'Other' }))).toBe(true)
    expect(Option.isNone(decodeActionErrorEnvelope({ ...envelope({}), version: 2 }))).toBe(true)
    expect(Option.isNone(decodeActionErrorEnvelope(null))).toBe(true)
    expect(Option.isNone(decodeActionErrorEnvelope(undefined))).toBe(true)
    expect(Option.isNone(decodeActionErrorEnvelope('nope'))).toBe(true)
  })
})

describe('classifyActionFailure', () => {
  it('passes an IsOfflineError through untouched', () => {
    const offline = new IsOfflineError({ cause: 'connect timeout' })
    expect(classify(decodePullError, offline)).toBe(offline)
  })

  it('maps any failure observed while not connected to IsOfflineError', () => {
    for (const status of ['idle', 'connecting', 'disconnected'] as const) {
      // Even a decodable user error becomes offline when the socket was down.
      const cause = { group: 'user', code: 'ServerAheadError', metadata: envelope({ _tag: 'ServerAheadError' }) }
      const mapped = classify(decodePushError, failure(cause, status))
      expect(mapped).toBeInstanceOf(IsOfflineError)
      expect((mapped as IsOfflineError).cause).toBe(cause)
    }
  })

  it('maps a plain "Connection closed" Error to IsOfflineError', () => {
    const cause = new Error('Connection closed (code: 1000, reason: unauthorized)')
    const mapped = classify(decodePullError, failure(cause))
    expect(mapped).toBeInstanceOf(IsOfflineError)
    expect((mapped as IsOfflineError).cause).toBe(cause)
  })

  it('maps a plain "Connection lost" Error to IsOfflineError', () => {
    const mapped = classify(decodePullError, failure(new Error('connection lost')))
    expect(mapped).toBeInstanceOf(IsOfflineError)
  })

  it('maps an unrelated plain Error to UnknownError', () => {
    const cause = new Error('something else entirely')
    const mapped = classify(decodePullError, failure(cause))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).cause).toBe(cause)
    expect((mapped as UnknownError).payload).toBeUndefined()
  })

  it('maps actor lifecycle codes to IsOfflineError', () => {
    for (const code of ['aborted', 'not_found', 'stopping', 'restarting']) {
      const mapped = classify(decodePullError, failure({ group: 'actor', code }))
      expect(mapped, code).toBeInstanceOf(IsOfflineError)
    }
  })

  it('maps any guard error to IsOfflineError', () => {
    const cause = { group: 'guard', code: 'actor_ready_timeout' }
    const mapped = classify(decodePullError, failure(cause))
    expect(mapped).toBeInstanceOf(IsOfflineError)
    expect((mapped as IsOfflineError).cause).toBe(cause)
  })

  it('maps a structured ws/* close (e.g. ws.message_index_skip after a hibernation wake) to IsOfflineError', () => {
    const cause = { group: 'ws', code: 'message_index_skip', message: 'Connection closed: ws.message_index_skip' }
    const mapped = classify(decodePullError, failure(cause))
    expect(mapped).toBeInstanceOf(IsOfflineError)
    expect((mapped as IsOfflineError).cause).toBe(cause)
  })

  it('maps other rivet errors to UnknownError carrying group + code', () => {
    const cause = { group: 'request', code: 'invalid' }
    const mapped = classify(decodePullError, failure(cause))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).cause).toBe(cause)
    expect((mapped as UnknownError).payload).toEqual({ group: 'request', code: 'invalid' })
  })

  it('maps a non-lifecycle actor code to UnknownError', () => {
    const mapped = classify(decodePullError, failure({ group: 'actor', code: 'action_not_found' }))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).payload).toEqual({ group: 'actor', code: 'action_not_found' })
  })

  it('maps a dropped-socket rivet error (message/incoming_too_long) to UnknownError', () => {
    const mapped = classify(decodePushError, failure({ group: 'message', code: 'incoming_too_long' }))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).payload).toEqual({ group: 'message', code: 'incoming_too_long' })
  })

  it('decodes a declared ServerAheadError out of the envelope', () => {
    const encodedServerAheadError = encodePushError(new ServerAheadError({ minimumExpectedNum: seq(5), providedNum: seq(3) }))
    const cause = {
      group: 'user',
      code: 'ServerAheadError',
      message: 'Fail failed',
      metadata: overTheWire(envelope(encodedServerAheadError)),
    }
    const mapped = classify(decodePushError, failure(cause))
    expect(mapped).toBeInstanceOf(ServerAheadError)
    expect(mapped).toMatchObject({ minimumExpectedNum: 5, providedNum: 3 })
  })

  it('decodes a declared BackendIdMismatchError out of the envelope', () => {
    const encoded = encodePushError(new BackendIdMismatchError({ expected: 'a', received: 'b' }))
    const cause = { group: 'user', code: 'BackendIdMismatchError', metadata: overTheWire(envelope(encoded)) }
    const mapped = classify(decodePushError, failure(cause))
    expect(mapped).toBeInstanceOf(BackendIdMismatchError)
    expect(mapped).toMatchObject({ expected: 'a', received: 'b' })
  })

  it('passes a declared UnknownError through', () => {
    const encoded = encodePushError(new UnknownError({ cause: new Error('inner boom'), note: 'server note' }))
    const cause = { group: 'user', code: 'UnknownError', metadata: overTheWire(envelope(encoded)) }
    const mapped = classify(decodePushError, failure(cause))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).note).toBe('server note')
  })

  it('maps an undecodable envelope payload to UnknownError', () => {
    const cause = { group: 'user', code: 'Whatever', metadata: envelope({ _tag: 'NotAnError', nope: true }) }
    const mapped = classify(decodePushError, failure(cause))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).note).toBe('undecodable action error')
    expect((mapped as UnknownError).cause).toBe(cause)
  })

  it('re-maps a declared InvalidPayloadError to UnknownError', () => {
    const encoded = encodePushError(new InvalidPayloadError({ storeId: 's1', reason: 'unauthorized' }))
    const cause = { group: 'user', code: 'InvalidPayloadError', metadata: overTheWire(envelope(encoded)) }
    const mapped = classify(decodePushError, failure(cause))
    expect(mapped).toBeInstanceOf(UnknownError)
    expect((mapped as UnknownError).note).toBe('validatePayload rejected')
    expect((mapped as UnknownError).cause).toBeInstanceOf(InvalidPayloadError)
  })
})
