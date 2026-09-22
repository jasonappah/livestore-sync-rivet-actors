/**
 * Unit coverage for the per-wake actor context and everything the action
 * handlers lean on before they touch storage: option resolution, hook
 * dispatch, error narrowing and caller validation.
 *
 * No Rivet engine is involved — `makeFakeDb()` backs the storage with
 * `node:sqlite` and the actor key is passed in directly.
 */

import { UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Cause, Effect, Exit, Schema } from '@livestore/utils/effect'
import { beforeEach, describe, expect, it } from 'vitest'

import { makeEventFactory } from '../../../tests/harness/events.ts'
import {
  DEFAULT_PULL_PAGE_SIZE,
  InvalidPayloadError,
  MAX_PULL_EVENTS_PER_MESSAGE,
  MAX_PUSH_EVENTS_PER_REQUEST,
  MAX_TRANSPORT_PAYLOAD_BYTES,
} from '../../common/mod.ts'
import { AdminInfo, AdminReset, LiveStoreSync, Ping, Pull, Push, TestDisconnectAll, TestInfo, TestSleep } from '../actions.ts'
import { mapToDeclaredErrors, runHook } from '../hooks.ts'
import { type LiveStoreSyncActorOptions, resolveOptions } from '../options.ts'
import { makeSyncStorage, migrate, type SyncStorage } from '../sqlite.ts'
import { makeStoreCtx, type RawConn } from '../store-ctx.ts'
import {
  CONN_PARAMS_FAILURE_REASON,
  SCHEMA_FAILURE_REASON,
  STORE_ID_MISMATCH_REASON,
  VALIDATE_REJECTED_REASON,
  validateConnParams,
  validateSyncPayload,
} from '../validate-payload.ts'
import { makeFakeDb } from './fake-raw-access.ts'

const STORE_ID = 'store-1'
const CLIENT_ID = 'client-1'

const noConns = (): Iterable<RawConn> => []

let fake: ReturnType<typeof makeFakeDb>
let storage: SyncStorage

beforeEach(async () => {
  fake = makeFakeDb()
  await migrate(fake.db)
  storage = makeSyncStorage(fake.db)
})

const wake = (
  overrides: {
    key?: ReadonlyArray<string> | string
    options?: LiveStoreSyncActorOptions<any>
  } = {},
) =>
  makeStoreCtx({
    key: overrides.key ?? [STORE_ID],
    db: fake.db,
    conns: noConns,
    // Keep the console quiet: the stale-head path logs a warning by default.
    log: () => {},
    options: overrides.options ?? {},
  })

const appendEvents = (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>) =>
  Effect.runPromise(
    storage.appendEventsAndUpdateHead(batch, '2026-01-01T00:00:00.000Z', {
      storeId: STORE_ID,
      backendId: 'placeholder',
      newHead: batch.at(-1)!.seqNum,
    }),
  )

const newEvents = (count: number) => {
  const factory = makeEventFactory()
  return Array.from({ length: count }, (_, index) =>
    factory.todoCreated.next({ id: `t${index}`, text: `t${index}`, completed: false }),
  )
}

// -----------------------------------------------------------------------------

describe('makeStoreCtx', () => {
  it('mints a backendId on first wake and persists head + backendId', async () => {
    const ctx = await Effect.runPromise(wake())

    expect(ctx.storeId).toBe(STORE_ID)
    expect(ctx.backendId).toMatch(/^[\w-]{10,}$/)
    expect(ctx.headRef.current).toBe(EventSequenceNumber.Client.ROOT.global)
    expect(await Effect.runPromise(storage.loadContext(STORE_ID))).toEqual({
      currentHead: 0,
      backendId: ctx.backendId,
    })
  })

  it('keeps the same backendId and reloads the head across wakes', async () => {
    const first = await Effect.runPromise(wake())
    await appendEvents(newEvents(3))
    await Effect.runPromise(storage.saveContext({ storeId: STORE_ID, currentHead: 3, backendId: first.backendId }))

    const second = await Effect.runPromise(wake())

    expect(second.backendId).toBe(first.backendId)
    expect(second.headRef.current).toBe(3)
  })

  it('reconciles (and persists) a persisted head that lags the eventlog', async () => {
    const first = await Effect.runPromise(wake())
    await appendEvents(newEvents(5))
    // Simulate a crash between the eventlog insert and the context upsert.
    await Effect.runPromise(storage.saveContext({ storeId: STORE_ID, currentHead: 2, backendId: first.backendId }))

    const second = await Effect.runPromise(wake())

    expect(second.headRef.current).toBe(5)
    expect(await Effect.runPromise(storage.loadContext(STORE_ID))).toEqual({
      currentHead: 5,
      backendId: first.backendId,
    })
  })

  it('never moves the head backwards when the persisted head is ahead', async () => {
    const first = await Effect.runPromise(wake())
    await Effect.runPromise(storage.saveContext({ storeId: STORE_ID, currentHead: 9, backendId: first.backendId }))

    const second = await Effect.runPromise(wake())

    expect(second.headRef.current).toBe(9)
  })

  it('accepts a plain string key', async () => {
    const ctx = await Effect.runPromise(wake({ key: STORE_ID }))
    expect(ctx.storeId).toBe(STORE_ID)
  })

  it.each([
    ['an empty key', [] as ReadonlyArray<string>],
    ['a multi-element key', ['a', 'b'] as ReadonlyArray<string>],
  ])('dies on %s', async (_label, key) => {
    const exit = await Effect.runPromiseExit(wake({ key }))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.hasDies(exit.cause)).toBe(true)
      expect(String(Cause.squash(exit.cause))).toContain('single-element array')
    }
  })

  it('exposes resolved options, a semaphore and an empty conn-auth cache', async () => {
    const ctx = await Effect.runPromise(wake({ options: { pullPageSize: 1000 } }))

    expect(ctx.options.pullPageSize).toBe(MAX_PULL_EVENTS_PER_MESSAGE)
    expect(ctx.options.testingEnabled).toBe(false)
    expect(ctx.connAuth.size).toBe(0)
    expect(typeof ctx.pushSemaphore.withPermits).toBe('function')
    expect([...ctx.conns()]).toEqual([])
  })
})

// -----------------------------------------------------------------------------

describe('resolveOptions', () => {
  it('applies defaults', () => {
    const resolved = resolveOptions({})

    expect(resolved.pullPageSize).toBe(DEFAULT_PULL_PAGE_SIZE)
    expect(resolved.maxPushEventsPerRequest).toBe(MAX_PUSH_EVENTS_PER_REQUEST)
    expect(resolved.maxMessageBytes).toBe(MAX_TRANSPORT_PAYLOAD_BYTES)
    expect(resolved.testingEnabled).toBe(false)
    expect(resolved.adminSecret).toBeUndefined()
  })

  it('resolves admin.secret, treating an empty secret as disabled', () => {
    expect(resolveOptions({ admin: { secret: 's3cret' } }).adminSecret).toBe('s3cret')
    expect(resolveOptions({ admin: { secret: '' } }).adminSecret).toBeUndefined()
  })

  it('clamps pullPageSize into [1, MAX_PULL_EVENTS_PER_MESSAGE]', () => {
    expect(resolveOptions({ pullPageSize: 0 }).pullPageSize).toBe(1)
    expect(resolveOptions({ pullPageSize: -5 }).pullPageSize).toBe(1)
    expect(resolveOptions({ pullPageSize: 7.9 }).pullPageSize).toBe(7)
    expect(resolveOptions({ pullPageSize: 10_000 }).pullPageSize).toBe(MAX_PULL_EVENTS_PER_MESSAGE)
    expect(resolveOptions({ pullPageSize: Number.NaN }).pullPageSize).toBe(DEFAULT_PULL_PAGE_SIZE)
  })

  it('clamps maxPushEventsPerRequest and maxMessageBytes', () => {
    expect(resolveOptions({ maxPushEventsPerRequest: 0 }).maxPushEventsPerRequest).toBe(1)
    expect(resolveOptions({ maxPushEventsPerRequest: 10_000 }).maxPushEventsPerRequest).toBe(
      MAX_PUSH_EVENTS_PER_REQUEST,
    )
    expect(resolveOptions({ maxMessageBytes: 1 }).maxMessageBytes).toBe(1024)
    expect(resolveOptions({ maxMessageBytes: 2_000_000 }).maxMessageBytes).toBe(2_000_000)
  })

  it('mirrors testing.enabled and preserves the raw callbacks', () => {
    const onPush = () => {}
    const resolved = resolveOptions({ testing: { enabled: true }, onPush })

    expect(resolved.testingEnabled).toBe(true)
    expect(resolved.onPush).toBe(onPush)
  })
})

// -----------------------------------------------------------------------------

describe('runHook', () => {
  it('is a no-op when the hook is undefined', async () => {
    expect(await Effect.runPromise(runHook(undefined, 1))).toBeUndefined()
  })

  it('runs sync, promise and Effect hooks', async () => {
    const seen: string[] = []

    await Effect.runPromise(
      runHook((value: string) => {
        seen.push(`sync:${value}`)
      }, 'a'),
    )
    await Effect.runPromise(
      runHook(async (value: string) => {
        seen.push(`promise:${value}`)
      }, 'b'),
    )
    await Effect.runPromise(
      runHook(
        (value: string) =>
          Effect.sync(() => {
            seen.push(`effect:${value}`)
          }),
        'c',
      ),
    )

    expect(seen).toEqual(['sync:a', 'promise:b', 'effect:c'])
  })

  it.each([
    [
      'a sync throw',
      () => {
        throw new Error('boom-sync')
      },
    ],
    ['a rejected promise', () => Promise.reject(new Error('boom-promise'))],
    ['a failing Effect', () => Effect.fail(new Error('boom-effect'))],
  ])('maps %s to UnknownError', async (_label, hook) => {
    const exit = await Effect.runPromiseExit(runHook(hook as never))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = Cause.findErrorOption(exit.cause)
      expect(error._tag).toBe('Some')
      expect((Cause.squash(exit.cause) as UnknownError)._tag).toBe('UnknownError')
    }
  })
})

// -----------------------------------------------------------------------------

describe('mapToDeclaredErrors', () => {
  const declared = mapToDeclaredErrors(['InvalidPayloadError'])

  const invalid = new InvalidPayloadError({ storeId: STORE_ID, reason: 'nope' })

  it('passes a declared failure through unchanged', async () => {
    const exit = await Effect.runPromiseExit(Effect.fail(invalid).pipe(declared))

    expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toBe(invalid)
  })

  it('leaves success untouched', async () => {
    expect(await Effect.runPromise(Effect.succeed(42).pipe(declared))).toBe(42)
  })

  it('converts an undeclared failure to UnknownError', async () => {
    const exit = await Effect.runPromiseExit(Effect.fail(new Error('undeclared')).pipe(declared))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = Cause.squash(exit.cause) as UnknownError
      expect(error._tag).toBe('UnknownError')
      expect(String(error.cause)).toContain('undeclared')
    }
  })

  it('converts a defect to UnknownError (as a failure, not a defect)', async () => {
    const exit = await Effect.runPromiseExit(Effect.die(new Error('defect')).pipe(declared))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(Cause.hasDies(exit.cause)).toBe(false)
      const error = Cause.squash(exit.cause) as UnknownError
      expect(error._tag).toBe('UnknownError')
      expect(String(error.cause)).toContain('defect')
    }
  })

  it('re-raises interruption untouched', async () => {
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        yield* Effect.interrupt
      }).pipe(declared),
    )

    expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true)
  })
})

// -----------------------------------------------------------------------------

describe('validateSyncPayload', () => {
  const ctxWith = (options: LiveStoreSyncActorOptions<any>) => ({
    storeId: STORE_ID,
    options: resolveOptions(options),
  })

  const req = (overrides: Partial<{ storeId: string; clientId: string; payload: Schema.Json }> = {}) => ({
    storeId: STORE_ID,
    clientId: CLIENT_ID,
    ...overrides,
  })

  const expectInvalid = async (effect: Effect.Effect<unknown, unknown>, reason: string) => {
    const exit = await Effect.runPromiseExit(effect)
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = Cause.squash(exit.cause) as InvalidPayloadError
      expect(error._tag).toBe('InvalidPayloadError')
      expect(error.reason).toBe(reason)
    }
  }

  it('passes the raw payload through when nothing is configured', async () => {
    expect(await Effect.runPromise(validateSyncPayload(ctxWith({}), req({ payload: { token: 'abc' } })))).toEqual({
      token: 'abc',
    })
    expect(await Effect.runPromise(validateSyncPayload(ctxWith({}), req()))).toBeUndefined()
  })

  it('rejects a storeId that does not match the actor key', async () => {
    await expectInvalid(validateSyncPayload(ctxWith({}), req({ storeId: 'other' })), STORE_ID_MISMATCH_REASON)
  })

  it('decodes the payload with syncPayloadSchema', async () => {
    const syncPayloadSchema = Schema.Struct({ token: Schema.String })

    expect(
      await Effect.runPromise(validateSyncPayload(ctxWith({ syncPayloadSchema }), req({ payload: { token: 'abc' } }))),
    ).toEqual({ token: 'abc' })
  })

  it('rejects a payload that fails syncPayloadSchema', async () => {
    const syncPayloadSchema = Schema.Struct({ token: Schema.String })

    await expectInvalid(
      validateSyncPayload(ctxWith({ syncPayloadSchema }), req({ payload: { token: 42 } })),
      SCHEMA_FAILURE_REASON,
    )
    // Decoding runs even for an absent payload, so a schema can require one.
    await expectInvalid(validateSyncPayload(ctxWith({ syncPayloadSchema }), req()), SCHEMA_FAILURE_REASON)
  })

  it('hands the decoded payload to validatePayload together with the context', async () => {
    const seen: unknown[] = []
    const syncPayloadSchema = Schema.Struct({ token: Schema.String })

    const decoded = await Effect.runPromise(
      validateSyncPayload(
        ctxWith({
          syncPayloadSchema,
          validatePayload: (payload, validateCtx) => {
            seen.push([payload, validateCtx])
          },
        }),
        req({ payload: { token: 'abc' } }),
      ),
    )

    expect(decoded).toEqual({ token: 'abc' })
    expect(seen).toEqual([[{ token: 'abc' }, { storeId: STORE_ID, clientId: CLIENT_ID }]])
  })

  it.each([
    [
      'throws',
      () => {
        throw new Error('nope')
      },
    ],
    ['returns a rejected promise', async () => Promise.reject(new Error('nope'))],
    // A failing Effect is outside the declared `SyncOrPromiseOrEffect<void>`
    // return type, but the dispatcher handles it, so cover it here too.
    ['returns a failing Effect', (() => Effect.fail(new Error('nope'))) as never],
  ])('rejects when validatePayload %s', async (_label, validatePayload) => {
    await expectInvalid(
      validateSyncPayload(ctxWith({ validatePayload: validatePayload as never }), req()),
      VALIDATE_REJECTED_REASON,
    )
  })

  it('rejects when validatePayload dies', async () => {
    await expectInvalid(
      validateSyncPayload(ctxWith({ validatePayload: () => Effect.die(new Error('nope')) as never }), req()),
      VALIDATE_REJECTED_REASON,
    )
  })
})

// -----------------------------------------------------------------------------

describe('validateConnParams', () => {
  const ctx = { storeId: STORE_ID, options: resolveOptions<Schema.Json>({}) }

  it('decodes params and delegates to validateSyncPayload', async () => {
    expect(
      await Effect.runPromise(
        validateConnParams(ctx, { storeId: STORE_ID, clientId: CLIENT_ID, payload: { token: 'abc' } }),
      ),
    ).toEqual({ token: 'abc' })
  })

  it('accepts params without a payload', async () => {
    expect(
      await Effect.runPromise(validateConnParams(ctx, { storeId: STORE_ID, clientId: CLIENT_ID })),
    ).toBeUndefined()
  })

  it('rejects malformed params', async () => {
    const exit = await Effect.runPromiseExit(validateConnParams(ctx, { storeId: STORE_ID }))

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      const error = Cause.squash(exit.cause) as InvalidPayloadError
      expect(error._tag).toBe('InvalidPayloadError')
      expect(error.reason).toBe(CONN_PARAMS_FAILURE_REASON)
    }
  })

  it('rejects params addressing another store', async () => {
    const exit = await Effect.runPromiseExit(
      validateConnParams(ctx, { storeId: 'other', clientId: CLIENT_ID }),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect((Cause.squash(exit.cause) as InvalidPayloadError).reason).toBe(STORE_ID_MISMATCH_REASON)
    }
  })
})

// -----------------------------------------------------------------------------

describe('actions contract', () => {
  it('registers the eight actions on the actor in order', () => {
    expect(LiveStoreSync.name).toBe('LiveStoreSync')
    expect(LiveStoreSync.actions.map((action) => action._tag)).toEqual([
      'Pull',
      'Push',
      'Ping',
      'AdminInfo',
      'AdminReset',
      'TestDisconnectAll',
      'TestInfo',
      'TestSleep',
    ])
  })

  it('carries a payload schema for every action', () => {
    for (const action of [Pull, Push, Ping, AdminInfo, AdminReset, TestDisconnectAll, TestInfo, TestSleep]) {
      expect(action.hasPayload).toBe(true)
    }
  })
})
