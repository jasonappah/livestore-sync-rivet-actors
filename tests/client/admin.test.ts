/**
 * Unit coverage for `makeRivetSyncAdmin` against a fake rivetkit client:
 * request encoding, response decoding and typed error surfacing.
 */

import { UnknownError } from '@livestore/common'
import { describe, expect, it } from 'vitest'

import { DEFAULT_ADMIN_CLIENT_ID, makeRivetSyncAdminWith, toAdminError } from '../../src/client/admin.ts'
import {
  ACTION_ADMIN_INFO,
  ACTION_ADMIN_RESET,
  ACTOR_NAME,
  AdminUnauthorizedError,
  decodeAdminInfoRequest,
  decodeAdminResetRequest,
  encodeAdminError,
  encodeAdminInfoResponse,
  encodeAdminResetResponse,
  InvalidPayloadError,
  PERSISTENCE_FORMAT_VERSION,
} from '../../src/common/mod.ts'

type Call = { actorName: string; key: string | string[]; name: string; args: unknown[] }

const makeFake = (respond: (call: Call) => Promise<unknown>) => {
  const calls: Call[] = []
  const created: unknown[] = []
  let disposed = 0
  const createClient = (options: unknown) => {
    created.push(options)
    return {
      getOrCreate: (actorName: string, key: string | string[]) => ({
        action: (opts: { name: string; args: unknown[] }) => {
          const call = { actorName, key, name: opts.name, args: opts.args }
          calls.push(call)
          return respond(call)
        },
      }),
      dispose: async () => {
        disposed += 1
      },
    }
  }
  return { calls, created, createClient, disposed: () => disposed }
}

/** What `@rivetkit/effect` throws for a declared action error (duck-typed `ActorError`). */
const envelopeError = (error: Parameters<typeof encodeAdminError>[0]) =>
  Object.assign(new Error('action failed'), {
    group: 'user',
    code: error._tag,
    metadata: { _tag: 'EffectActionError', version: 1, error: encodeAdminError(error) },
  })

const INFO = {
  storeId: 'store-1',
  backendId: 'b1',
  currentHead: 3,
  eventCount: 3,
  connectionCount: 1,
  persistenceFormatVersion: PERSISTENCE_FORMAT_VERSION,
} as never

describe('makeRivetSyncAdmin', () => {
  it('encodes info/reset requests for the store actor and decodes the responses', async () => {
    const fake = makeFake(async (call) =>
      call.name === ACTION_ADMIN_INFO ? encodeAdminInfoResponse(INFO) : encodeAdminResetResponse({ backendId: 'b2' }),
    )
    const admin = makeRivetSyncAdminWith(
      { endpoint: 'http://engine', token: 't', namespace: 'ns' },
      { createClient: fake.createClient },
    )

    expect(await admin.info('store-1', 'sec')).toEqual(INFO)
    expect(await admin.reset('store-1', 'sec', { token: 'ok' })).toEqual({ backendId: 'b2' })
    await admin.dispose()

    expect(fake.created).toEqual([{ endpoint: 'http://engine', token: 't', namespace: 'ns' }])
    expect(fake.disposed()).toBe(1)
    expect(fake.calls.map(({ actorName, key, name }) => ({ actorName, key, name }))).toEqual([
      { actorName: ACTOR_NAME, key: ['store-1'], name: ACTION_ADMIN_INFO },
      { actorName: ACTOR_NAME, key: ['store-1'], name: ACTION_ADMIN_RESET },
    ])
    const infoReq = fake.calls[0]!.args[0] as Record<string, unknown>
    expect('payload' in infoReq).toBe(false)
    expect(decodeAdminInfoRequest(infoReq)).toEqual({
      storeId: 'store-1',
      clientId: DEFAULT_ADMIN_CLIENT_ID,
      adminSecret: 'sec',
    })
    expect(decodeAdminResetRequest(fake.calls[1]!.args[0])).toEqual({
      storeId: 'store-1',
      clientId: DEFAULT_ADMIN_CLIENT_ID,
      adminSecret: 'sec',
      payload: { token: 'ok' },
    })
  })

  it('honours actorName and clientId', async () => {
    const fake = makeFake(async () => encodeAdminResetResponse({ backendId: 'b2' }))
    const admin = makeRivetSyncAdminWith(
      { endpoint: 'http://engine', actorName: 'Custom', clientId: 'ops' },
      { createClient: fake.createClient },
    )
    await admin.reset('s', 'sec')
    expect(fake.calls[0]!.actorName).toBe('Custom')
    expect(decodeAdminResetRequest(fake.calls[0]!.args[0]).clientId).toBe('ops')
  })

  it.each([
    ['AdminUnauthorizedError', new AdminUnauthorizedError({ storeId: 's' }), AdminUnauthorizedError],
    ['InvalidPayloadError', new InvalidPayloadError({ storeId: 's', reason: 'nope' }), InvalidPayloadError],
    ['UnknownError', new UnknownError({ cause: 'disabled', note: 'admin actions are disabled (set admin.secret)' }), UnknownError],
  ] as const)('throws a decoded %s from the error envelope', async (_label, error, ctor) => {
    const fake = makeFake(() => Promise.reject(envelopeError(error)))
    const admin = makeRivetSyncAdminWith({ endpoint: 'http://engine' }, { createClient: fake.createClient })
    const thrown = await admin.info('s', 'sec').then(
      () => undefined,
      (cause: unknown) => cause,
    )
    expect(thrown).toBeInstanceOf(ctor)
    expect(thrown).toMatchObject({ _tag: error._tag })
  })

  it('wraps transport and non-envelope failures in UnknownError', async () => {
    const rivet = toAdminError({ group: 'actor', code: 'not_found', message: 'gone' }, ACTION_ADMIN_INFO)
    expect(rivet).toBeInstanceOf(UnknownError)
    expect(rivet).toMatchObject({ payload: { group: 'actor', code: 'not_found' } })

    const plain = toAdminError(new Error('fetch failed'), ACTION_ADMIN_RESET)
    expect(plain).toBeInstanceOf(UnknownError)

    const undecodable = toAdminError(
      { group: 'user', code: 'x', metadata: { _tag: 'EffectActionError', version: 1, error: { _tag: 'Nope' } } },
      ACTION_ADMIN_INFO,
    )
    expect(undecodable).toBeInstanceOf(UnknownError)
    expect((undecodable as UnknownError).note).toContain('undecodable')
  })

  it('throws UnknownError for an undecodable success value', async () => {
    const fake = makeFake(async () => ({ nope: true }))
    const admin = makeRivetSyncAdminWith({ endpoint: 'http://engine' }, { createClient: fake.createClient })
    await expect(admin.reset('s', 'sec')).rejects.toBeInstanceOf(UnknownError)
  })
})
