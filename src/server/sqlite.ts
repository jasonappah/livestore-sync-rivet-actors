/**
 * SQLite persistence for the Rivet-backed LiveStore sync actor.
 *
 * The actor gets its database from `db({ onMigrate })` (`rivetkit/db`), whose
 * client is a `RawAccess`. This module only depends on the structural subset
 * it actually uses (`RawAccessLike`) so tests can substitute a plain
 * `node:sqlite` fake without pulling in rivetkit; a type-level check below
 * pins `RawAccess` as assignable to it.
 *
 * Verified Rivet SQLite behaviour (see `docs/spike-results.md` §B5):
 * - `execute` resolves to an array of row objects; non-SELECT statements
 *   resolve to `[]` (no `changes` / `lastInsertRowId`).
 * - `COUNT(*)` comes back as a `number`, `MAX()` over an empty table as `null`.
 * - `STRICT` tables are accepted and 350 bound params per statement work.
 * - `transaction(cb, { name })` rolls back when the callback throws; always
 *   pass a `name` (unnamed transactions log a warning).
 */

import { UnknownError } from '@livestore/common'
import { EventSequenceNumber, type LiveStoreEvent } from '@livestore/common/schema'
import { Effect, Option } from '@livestore/utils/effect'

import { PERSISTENCE_FORMAT_VERSION, type PullResponseBatchItem, SyncMetadata } from '../common/mod.ts'

// -----------------------------------------------------------------------------
// Database handle (structural subset of rivetkit's `RawAccess`)
// -----------------------------------------------------------------------------

/**
 * The part of `rivetkit/db`'s `RawAccess` this module needs. Declared
 * structurally so unit tests can back it with `node:sqlite`.
 */
export interface RawAccessLike {
  execute<Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    ...params: unknown[]
  ): Promise<Row[]>
  transaction<T>(callback: (tx: RawAccessLike) => Promise<T>, options: { name: string }): Promise<T>
}

/** Type-level proof that the real rivetkit client satisfies `RawAccessLike`. */
const _rawAccessIsAssignable: RawAccessLike = null as unknown as import('rivetkit/db').RawAccess
void _rawAccessIsAssignable

// -----------------------------------------------------------------------------
// Schema
// -----------------------------------------------------------------------------

/** Append-only global eventlog. One row per synced event. */
export const EVENTLOG_TABLE = `eventlog_v${PERSISTENCE_FORMAT_VERSION}` as const

/** Single-row-per-store table holding the persisted head and backend id. */
export const CONTEXT_TABLE = `context_v${PERSISTENCE_FORMAT_VERSION}` as const

/** Idempotent DDL, run by `db({ onMigrate: migrate })` before the actor wakes. */
export const migrationSql: ReadonlyArray<string> = [
  `CREATE TABLE IF NOT EXISTS ${EVENTLOG_TABLE} (seqNum INTEGER PRIMARY KEY, parentSeqNum INTEGER NOT NULL, name TEXT NOT NULL, args TEXT, createdAt TEXT NOT NULL, clientId TEXT NOT NULL, sessionId TEXT NOT NULL) STRICT`,
  `CREATE TABLE IF NOT EXISTS ${CONTEXT_TABLE} (storeId TEXT PRIMARY KEY, currentHead INTEGER NOT NULL, backendId TEXT NOT NULL) STRICT`,
]

/** Usable directly as `db({ onMigrate: migrate })`. Safe to run repeatedly. */
export const migrate = async (db: RawAccessLike): Promise<void> => {
  for (const sql of migrationSql) {
    await db.execute(sql)
  }
}

// -----------------------------------------------------------------------------
// Rows
// -----------------------------------------------------------------------------

/** A raw `eventlog_v*` row as returned by `execute`. */
export interface EventlogRow {
  seqNum: number
  parentSeqNum: number
  name: string
  /** JSON text, or `null` when the event carried no args. */
  args: string | null
  createdAt: string
  clientId: string
  sessionId: string
}

/**
 * A raw `context_v*` row. Declared as a type alias (not an interface) so it
 * carries an implicit index signature and satisfies `Record<string, unknown>`.
 */
export type ContextRow = {
  storeId: string
  currentHead: number
  backendId: string
}

/** Turns a stored row back into the wire shape a pull response carries. */
export const rowToBatchItem = (row: EventlogRow): PullResponseBatchItem => ({
  eventEncoded: {
    name: row.name,
    args: row.args === null ? undefined : JSON.parse(row.args),
    seqNum: EventSequenceNumber.Global.make(Number(row.seqNum)),
    parentSeqNum: EventSequenceNumber.Global.make(Number(row.parentSeqNum)),
    clientId: row.clientId,
    sessionId: row.sessionId,
  },
  metadata: Option.some(SyncMetadata.make({ createdAt: row.createdAt })),
})

// -----------------------------------------------------------------------------
// Storage API
// -----------------------------------------------------------------------------

export interface SyncStorage {
  /** `undefined` when the store has never been written to. */
  loadContext: (storeId: string) => Effect.Effect<{ currentHead: number; backendId: string } | undefined, UnknownError>
  /** Upserts the context row (head + backend id) for `storeId`. */
  saveContext: (row: { storeId: string; currentHead: number; backendId: string }) => Effect.Effect<void, UnknownError>
  /** Highest `seqNum` in the eventlog, or `undefined` when it is empty. */
  maxSeqNum: Effect.Effect<EventSequenceNumber.Global.Type | undefined, UnknownError>
  /** Number of events strictly after `cursor` (all events when `None`). */
  countAfter: (cursor: Option.Option<EventSequenceNumber.Global.Type>) => Effect.Effect<number, UnknownError>
  /** Up to `limit` events strictly after `cursor`, ascending by `seqNum`. */
  selectPage: (
    cursor: Option.Option<EventSequenceNumber.Global.Type>,
    limit: number,
  ) => Effect.Effect<ReadonlyArray<EventlogRow>, UnknownError>
  /**
   * Appends `batch` and advances the persisted head in a single transaction:
   * a mid-flight failure leaves both the eventlog and the context untouched.
   */
  appendEventsAndUpdateHead: (
    batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>,
    createdAt: string,
    ctx: { storeId: string; backendId: string; newHead: number },
  ) => Effect.Effect<void, UnknownError>
  /**
   * Drops every event and the context row for `storeId`, in one transaction.
   * The next wake (or the caller) mints a fresh `backendId`.
   */
  resetStore: (storeId: string) => Effect.Effect<void, UnknownError>
}

/**
 * Bound params per INSERT statement stay well inside the 350 verified as
 * working (50 rows × 7 columns).
 */
const INSERT_CHUNK_SIZE = 50

const EVENT_COLUMNS = 'seqNum, parentSeqNum, name, args, createdAt, clientId, sessionId'

export const makeSyncStorage = (db: RawAccessLike): SyncStorage => {
  const exec = <Row extends Record<string, unknown>>(
    sql: string,
    ...params: unknown[]
  ): Effect.Effect<Row[], UnknownError> =>
    Effect.tryPromise(() => db.execute<Row>(sql, ...params)).pipe(
      Effect.mapError((cause) => new UnknownError({ cause, payload: { sql } })),
    )

  const loadContext: SyncStorage['loadContext'] = (storeId) =>
    exec<ContextRow>(`SELECT storeId, currentHead, backendId FROM ${CONTEXT_TABLE} WHERE storeId = ?`, storeId).pipe(
      Effect.map((rows) => {
        const row = rows[0]
        return row === undefined
          ? undefined
          : { currentHead: Number(row.currentHead), backendId: String(row.backendId) }
      }),
    )

  const saveContext: SyncStorage['saveContext'] = ({ storeId, currentHead, backendId }) =>
    exec(
      `INSERT INTO ${CONTEXT_TABLE} (storeId, currentHead, backendId) VALUES (?, ?, ?) ON CONFLICT(storeId) DO UPDATE SET currentHead = excluded.currentHead, backendId = excluded.backendId`,
      storeId,
      currentHead,
      backendId,
    ).pipe(Effect.asVoid)

  const maxSeqNum: SyncStorage['maxSeqNum'] = exec<{ maxSeqNum: number | null }>(
    `SELECT MAX(seqNum) AS maxSeqNum FROM ${EVENTLOG_TABLE}`,
  ).pipe(
    Effect.map((rows) => {
      const value = rows[0]?.maxSeqNum
      return value === null || value === undefined ? undefined : EventSequenceNumber.Global.make(Number(value))
    }),
  )

  const countAfter: SyncStorage['countAfter'] = (cursor) =>
    (Option.isSome(cursor)
      ? exec<{ total: number }>(`SELECT COUNT(*) AS total FROM ${EVENTLOG_TABLE} WHERE seqNum > ?`, cursor.value)
      : exec<{ total: number }>(`SELECT COUNT(*) AS total FROM ${EVENTLOG_TABLE}`)
    ).pipe(Effect.map((rows) => Number(rows[0]?.total ?? 0)))

  const selectPage: SyncStorage['selectPage'] = (cursor, limit) =>
    (Option.isSome(cursor)
      ? exec<Record<string, unknown>>(
          `SELECT ${EVENT_COLUMNS} FROM ${EVENTLOG_TABLE} WHERE seqNum > ? ORDER BY seqNum ASC LIMIT ?`,
          cursor.value,
          limit,
        )
      : exec<Record<string, unknown>>(
          `SELECT ${EVENT_COLUMNS} FROM ${EVENTLOG_TABLE} ORDER BY seqNum ASC LIMIT ?`,
          limit,
        )
    ).pipe(
      Effect.map((rows) =>
        rows.map(
          (row): EventlogRow => ({
            seqNum: Number(row.seqNum),
            parentSeqNum: Number(row.parentSeqNum),
            name: String(row.name),
            args: row.args === null || row.args === undefined ? null : String(row.args),
            createdAt: String(row.createdAt),
            clientId: String(row.clientId),
            sessionId: String(row.sessionId),
          }),
        ),
      ),
    )

  const appendEventsAndUpdateHead: SyncStorage['appendEventsAndUpdateHead'] = (batch, createdAt, ctx) => {
    const upsertSql = `INSERT INTO ${CONTEXT_TABLE} (storeId, currentHead, backendId) VALUES (?, ?, ?) ON CONFLICT(storeId) DO UPDATE SET currentHead = excluded.currentHead, backendId = excluded.backendId`

    return Effect.tryPromise(() =>
      db.transaction(async (tx) => {
        for (let offset = 0; offset < batch.length; offset += INSERT_CHUNK_SIZE) {
          const chunk = batch.slice(offset, offset + INSERT_CHUNK_SIZE)
          const placeholders = chunk.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ')
          const params = chunk.flatMap((event) => [
            event.seqNum,
            event.parentSeqNum,
            event.name,
            event.args === undefined ? null : JSON.stringify(event.args),
            createdAt,
            event.clientId,
            event.sessionId,
          ])

          await tx.execute(`INSERT INTO ${EVENTLOG_TABLE} (${EVENT_COLUMNS}) VALUES ${placeholders}`, ...params)
        }

        await tx.execute(upsertSql, ctx.storeId, ctx.newHead, ctx.backendId)
      }, { name: 'livestore-sync:append' }),
    ).pipe(
      Effect.mapError(
        (cause) =>
          new UnknownError({
            cause,
            payload: { sql: upsertSql, table: EVENTLOG_TABLE, batchLength: batch.length },
          }),
      ),
      Effect.asVoid,
    )
  }

  // One transaction: a failure between the two DELETEs must not leave an
  // empty eventlog behind a context row that still carries the old head.
  const resetStore: SyncStorage['resetStore'] = (storeId) =>
    Effect.tryPromise(() =>
      db.transaction(async (tx) => {
        await tx.execute(`DELETE FROM ${EVENTLOG_TABLE}`)
        await tx.execute(`DELETE FROM ${CONTEXT_TABLE} WHERE storeId = ?`, storeId)
      }, { name: 'livestore-sync:reset' }),
    ).pipe(
      Effect.mapError((cause) => new UnknownError({ cause, payload: { op: 'resetStore', storeId } })),
      Effect.asVoid,
    )

  return { loadContext, saveContext, maxSeqNum, countAfter, selectPage, appendEventsAndUpdateHead, resetStore }
}
