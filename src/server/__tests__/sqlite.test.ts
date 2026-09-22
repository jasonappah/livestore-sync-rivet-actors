import { UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Option } from '@livestore/utils/effect'
import { beforeEach, describe, expect, it } from 'vitest'

import { makeEventFactory } from '../../../tests/harness/events.ts'
import {
  CONTEXT_TABLE,
  EVENTLOG_TABLE,
  type EventlogRow,
  makeSyncStorage,
  migrate,
  migrationSql,
  rowToBatchItem,
  type SyncStorage,
} from '../sqlite.ts'
import { failOnNthTransactionStatement, makeFakeDb } from './fake-raw-access.ts'

const run = <A>(effect: Effect.Effect<A, UnknownError>) => Effect.runPromise(effect)

const seq = EventSequenceNumber.Global.make

let fake: ReturnType<typeof makeFakeDb>
let storage: SyncStorage

beforeEach(async () => {
  fake = makeFakeDb()
  await migrate(fake.db)
  storage = makeSyncStorage(fake.db)
})

const appendAll = (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>, storeId = 's1', backendId = 'b1') =>
  run(
    storage.appendEventsAndUpdateHead(batch, '2026-01-01T00:00:00.000Z', {
      storeId,
      backendId,
      newHead: batch.at(-1)!.seqNum,
    }),
  )

const countEventRows = () => (fake.raw.prepare(`SELECT COUNT(*) AS n FROM ${EVENTLOG_TABLE}`).get() as { n: number }).n

describe('migration', () => {
  it('names tables after the persistence format version', () => {
    expect(EVENTLOG_TABLE).toBe('eventlog_v1')
    expect(CONTEXT_TABLE).toBe('context_v1')
    expect(migrationSql).toHaveLength(2)
    expect(migrationSql.every((sql) => sql.includes('STRICT'))).toBe(true)
  })

  it('is idempotent', async () => {
    // `beforeEach` already migrated once.
    await expect(migrate(fake.db)).resolves.toBeUndefined()

    const tables = fake.raw
      .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
      .all()
      .map((row) => (row as { name: string }).name)

    expect(tables).toEqual([CONTEXT_TABLE, EVENTLOG_TABLE])
  })
})

describe('context', () => {
  it('is undefined before anything is saved', async () => {
    expect(await run(storage.loadContext('s1'))).toBeUndefined()
  })

  it('round-trips and upserts', async () => {
    await run(storage.saveContext({ storeId: 's1', currentHead: 7, backendId: 'b1' }))
    expect(await run(storage.loadContext('s1'))).toEqual({ currentHead: 7, backendId: 'b1' })

    await run(storage.saveContext({ storeId: 's1', currentHead: 12, backendId: 'b2' }))
    expect(await run(storage.loadContext('s1'))).toEqual({ currentHead: 12, backendId: 'b2' })

    // Other stores are untouched by the upsert.
    expect(await run(storage.loadContext('s2'))).toBeUndefined()
  })
})

describe('reads', () => {
  it('maxSeqNum is undefined on an empty eventlog', async () => {
    expect(await run(storage.maxSeqNum)).toBeUndefined()
  })

  it('maxSeqNum reflects inserted events', async () => {
    const factory = makeEventFactory()
    await appendAll([1, 2, 3].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false })))

    expect(await run(storage.maxSeqNum)).toBe(3)
  })

  it('countAfter counts all events for None and only later ones for Some', async () => {
    const factory = makeEventFactory()
    await appendAll([1, 2, 3, 4].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false })))

    expect(await run(storage.countAfter(Option.none()))).toBe(4)
    expect(await run(storage.countAfter(Option.some(seq(0))))).toBe(4)
    expect(await run(storage.countAfter(Option.some(seq(2))))).toBe(2)
    expect(await run(storage.countAfter(Option.some(seq(4))))).toBe(0)
  })

  it('selectPage orders ascending, honours the limit and excludes the cursor', async () => {
    const factory = makeEventFactory()
    await appendAll([1, 2, 3, 4, 5].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false })))

    const firstPage = await run(storage.selectPage(Option.none(), 2))
    expect(firstPage.map((row) => row.seqNum)).toEqual([1, 2])

    const secondPage = await run(storage.selectPage(Option.some(seq(2)), 2))
    expect(secondPage.map((row) => row.seqNum)).toEqual([3, 4])

    const lastPage = await run(storage.selectPage(Option.some(seq(4)), 10))
    expect(lastPage.map((row) => row.seqNum)).toEqual([5])

    expect(await run(storage.selectPage(Option.some(seq(5)), 10))).toEqual([])
  })

  it('selectPage returns the stored column values', async () => {
    const factory = makeEventFactory()
    const event = factory.todoCreated.next({ id: 't1', text: 'hello', completed: true })
    await appendAll([event])

    const [row] = await run(storage.selectPage(Option.none(), 10))
    expect(row).toEqual({
      seqNum: 1,
      parentSeqNum: 0,
      name: 'todo.created',
      args: JSON.stringify({ id: 't1', text: 'hello', completed: true }),
      createdAt: '2026-01-01T00:00:00.000Z',
      clientId: 'test-client',
      sessionId: 'test-client-session',
    })
  })
})

describe('rowToBatchItem', () => {
  const baseRow: EventlogRow = {
    seqNum: 4,
    parentSeqNum: 3,
    name: 'todo.created',
    args: JSON.stringify({ id: 't1', text: 'hi', completed: false, nested: { a: [1, null] } }),
    createdAt: '2026-01-01T00:00:00.000Z',
    clientId: 'c1',
    sessionId: 'sess1',
  }

  it('parses JSON args and attaches metadata', () => {
    const item = rowToBatchItem(baseRow)

    expect(item.eventEncoded).toEqual({
      name: 'todo.created',
      args: { id: 't1', text: 'hi', completed: false, nested: { a: [1, null] } },
      seqNum: 4,
      parentSeqNum: 3,
      clientId: 'c1',
      sessionId: 'sess1',
    })
    expect(Option.getOrThrow(item.metadata)).toEqual({
      _tag: 'SyncMessage.SyncMetadata',
      createdAt: '2026-01-01T00:00:00.000Z',
    })
  })

  it('maps NULL args to undefined', () => {
    const item = rowToBatchItem({ ...baseRow, args: null })

    expect(item.eventEncoded.args).toBeUndefined()
  })

  it('round-trips events through append → selectPage → rowToBatchItem', async () => {
    const factory = makeEventFactory()
    const created = factory.todoCreated.next({ id: 't1', text: 'hi', completed: false })
    const completed = factory.todoCompleted.next({ id: 't1' })
    await appendAll([created, completed])

    const rows = await run(storage.selectPage(Option.none(), 10))
    expect(rows.map(rowToBatchItem).map((item) => item.eventEncoded)).toEqual([created, completed])
  })
})

describe('appendEventsAndUpdateHead', () => {
  it('writes 120 events in 3 statements and advances the head', async () => {
    const factory = makeEventFactory()
    const batch = Array.from({ length: 120 }, (_, i) =>
      factory.todoCreated.next({ id: `t${i}`, text: `t${i}`, completed: false }),
    )

    const statements: string[] = []
    const spy = {
      execute: fake.db.execute.bind(fake.db),
      transaction: <T>(cb: (tx: typeof fake.db) => Promise<T>, opts: { name: string }) => {
        expect(opts.name).toBe('livestore-sync:append')
        return fake.db.transaction((tx) => {
          const recorded = {
            execute: async (sql: string, ...params: unknown[]) => {
              statements.push(sql)
              return tx.execute(sql, ...params)
            },
            transaction: tx.transaction.bind(tx),
          }
          return cb(recorded as typeof fake.db)
        }, opts)
      },
    } as typeof fake.db

    await run(
      makeSyncStorage(spy).appendEventsAndUpdateHead(batch, '2026-01-01T00:00:00.000Z', {
        storeId: 's1',
        backendId: 'b1',
        newHead: 120,
      }),
    )

    const inserts = statements.filter((sql) => sql.startsWith(`INSERT INTO ${EVENTLOG_TABLE}`))
    expect(inserts).toHaveLength(3)
    expect(inserts[0]!.split('(?, ?, ?, ?, ?, ?, ?)').length - 1).toBe(50)
    expect(inserts[2]!.split('(?, ?, ?, ?, ?, ?, ?)').length - 1).toBe(20)
    expect(statements.at(-1)!.startsWith(`INSERT INTO ${CONTEXT_TABLE}`)).toBe(true)

    expect(countEventRows()).toBe(120)
    expect(await run(storage.maxSeqNum)).toBe(120)
    expect(await run(storage.loadContext('s1'))).toEqual({ currentHead: 120, backendId: 'b1' })
  })

  it('stores undefined args as SQL NULL', async () => {
    const batch: ReadonlyArray<LiveStoreEvent.Global.Encoded> = [
      {
        name: 'no.args',
        args: undefined,
        seqNum: seq(1),
        parentSeqNum: seq(0),
        clientId: 'c1',
        sessionId: 'sess1',
      },
    ]
    await appendAll(batch)

    const [row] = await run(storage.selectPage(Option.none(), 10))
    expect(row!.args).toBeNull()
  })

  it('rolls back the whole append when the context upsert fails', async () => {
    const factory = makeEventFactory()
    const seed = [1, 2].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false }))
    await appendAll(seed)
    expect(countEventRows()).toBe(2)

    const nextBatch = [3, 4].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false }))
    // Statement #2 inside the transaction is the context upsert (one INSERT chunk precedes it).
    const failing = makeSyncStorage(failOnNthTransactionStatement(fake.db, 2))

    const exit = await Effect.runPromiseExit(
      failing.appendEventsAndUpdateHead(nextBatch, '2026-01-02T00:00:00.000Z', {
        storeId: 's1',
        backendId: 'b1',
        newHead: 4,
      }),
    )

    expect(exit._tag).toBe('Failure')
    // Neither the events nor the head moved.
    expect(countEventRows()).toBe(2)
    expect(await run(storage.maxSeqNum)).toBe(2)
    expect(await run(storage.loadContext('s1'))).toEqual({ currentHead: 2, backendId: 'b1' })
  })

  it('surfaces SQL failures as UnknownError carrying the statement', async () => {
    const broken = makeSyncStorage({
      execute: () => Promise.reject(new Error('boom')),
      transaction: () => Promise.reject(new Error('boom')),
    })

    const failure = await Effect.runPromise(Effect.flip(broken.countAfter(Option.none())))
    expect(failure).toBeInstanceOf(UnknownError)
    expect((failure.payload as { sql: string }).sql).toContain(`FROM ${EVENTLOG_TABLE}`)
  })
})

describe('resetStore', () => {
  it('drops every event and the store context row', async () => {
    const factory = makeEventFactory()
    await appendAll([1, 2, 3].map((n) => factory.todoCreated.next({ id: `t${n}`, text: `t${n}`, completed: false })))
    await run(storage.saveContext({ storeId: 'other', currentHead: 9, backendId: 'b9' }))

    await run(storage.resetStore('s1'))

    expect(countEventRows()).toBe(0)
    expect(await run(storage.maxSeqNum)).toBeUndefined()
    expect(await run(storage.countAfter(Option.none()))).toBe(0)
    expect(await run(storage.loadContext('s1'))).toBeUndefined()
    // Only the requested store's context row is removed.
    expect(await run(storage.loadContext('other'))).toEqual({ currentHead: 9, backendId: 'b9' })
  })
})
