/**
 * Shared fixtures for the push / connection handler tests: fake connections
 * implementing `RawConn`, a wake context over `makeFakeDb()`, request
 * builders and Exit helpers.
 */

import type { LiveStoreEvent } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import { Cause, Effect, Exit, Option, type Schema } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'
import { expect } from 'vitest'

import { makeEventFactory } from '../../../tests/harness/events.ts'
import { encodeConnParams, type PushRequest } from '../../common/mod.ts'
import type { LiveStoreSyncActorOptions } from '../options.ts'
import { migrate } from '../sqlite.ts'
import { type LogLevel, makeStoreCtx, type RawConn, type StoreCtx } from '../store-ctx.ts'
import { makeFakeDb } from './fake-raw-access.ts'

export const CLIENT_ID = 'client-1'

// -----------------------------------------------------------------------------
// Fake connections
// -----------------------------------------------------------------------------

export type FakeConn = RawConn & {
  readonly sent: Array<[name: string, payload: unknown]>
  disconnected: boolean
  readonly disconnectReasons: Array<string | undefined>
}

export const makeFakeConn = (
  id: string,
  params: unknown,
  options: { readonly throwOnSend?: boolean } = {},
): FakeConn => {
  const conn: FakeConn = {
    id,
    params,
    sent: [],
    disconnected: false,
    disconnectReasons: [],
    send: (name, ...args) => {
      if (options.throwOnSend === true) throw new Error(`socket closed (${id})`)
      conn.sent.push([name, args[0]])
    },
    disconnect: async (reason) => {
      conn.disconnected = true
      conn.disconnectReasons.push(reason)
    },
  }
  return conn
}

/** A connection whose params are a well-formed `ConnParams` for `storeId`. */
export const makeValidConn = (id: string, storeId: string, payload?: Schema.Json): FakeConn =>
  makeFakeConn(id, encodeConnParams({ storeId, clientId: `${id}-client`, ...(payload !== undefined ? { payload } : {}) }))

// -----------------------------------------------------------------------------
// Wake context
// -----------------------------------------------------------------------------

export type LogEntry = { level: LogLevel; msg: string; data: Record<string, unknown> | undefined }

export type TestCtx = {
  readonly storeId: string
  readonly ctx: StoreCtx<any>
  readonly conns: FakeConn[]
  readonly logs: LogEntry[]
  readonly raw: ReturnType<typeof makeFakeDb>['raw']
  readonly rowCount: () => number
  readonly persistedHead: () => number | undefined
}

/** Wakes a fresh actor over an in-memory database with a unique `storeId`. */
export const makeTestCtx = async (
  options: LiveStoreSyncActorOptions<any> = {},
  initialConns: FakeConn[] = [],
): Promise<TestCtx> => {
  const storeId = `store-${nanoid()}`
  const fake = makeFakeDb()
  await migrate(fake.db)

  const conns = initialConns
  const logs: LogEntry[] = []

  const ctx = await Effect.runPromise(
    makeStoreCtx({
      key: [storeId],
      db: fake.db,
      conns: () => conns,
      log: (level, msg, data) => {
        logs.push({ level, msg, data })
      },
      options,
    }),
  )

  return {
    storeId,
    ctx,
    conns,
    logs,
    raw: fake.raw,
    rowCount: () => Number((fake.raw.prepare('SELECT COUNT(*) AS n FROM eventlog_v1').get() as { n: number }).n),
    persistedHead: () => {
      const row = fake.raw.prepare('SELECT currentHead FROM context_v1 WHERE storeId = ?').get(storeId) as
        | { currentHead: number }
        | undefined
      return row?.currentHead
    },
  }
}

// -----------------------------------------------------------------------------
// Requests & events
// -----------------------------------------------------------------------------

export const pushReq = (
  storeId: string,
  batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>,
  overrides: Partial<Omit<PushRequest, 'batch'>> = {},
): PushRequest => ({
  storeId,
  clientId: CLIENT_ID,
  batch,
  backendId: Option.none(),
  ...overrides,
})

/**
 * `count` chained events from a fresh factory whose first event's parent is
 * `parent` (`'root'` → 0) and whose first `seqNum` is `parent + 1`.
 */
export const chainedEvents = (
  count: number,
  parent: number | 'root' = 'root',
  client = 'test-client',
  text = (index: number) => `t${index}`,
): LiveStoreEvent.Global.Encoded[] => {
  const factory = makeEventFactory({
    client: EventFactory.clientIdentity(client),
    startSeq: parent === 'root' ? 1 : parent + 1,
    initialParent: parent,
  })
  return Array.from({ length: count }, (_, index) =>
    factory.todoCreated.next({ id: `${client}-${index}`, text: text(index), completed: false }),
  )
}

// -----------------------------------------------------------------------------
// Exit helpers
// -----------------------------------------------------------------------------

/** Asserts `exit` is a failure and returns its (squashed) error. */
export const expectFailure = <A, E>(exit: Exit.Exit<A, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true)
  if (Exit.isFailure(exit)) return Cause.squash(exit.cause) as E
  throw new Error('unreachable')
}

/** Asserts `exit` is a success and returns its value. */
export const expectSuccess = <A, E>(exit: Exit.Exit<A, E>): A => {
  if (Exit.isSuccess(exit)) return exit.value
  throw new Error(`expected success, got failure: ${String(Cause.squash(exit.cause))}`)
}

export const runExit = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromiseExit(effect)
