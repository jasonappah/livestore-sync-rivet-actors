/**
 * An in-memory `RawAccessLike` backed by Node's built-in SQLite, used to test
 * `src/server/sqlite.ts` without booting a Rivet engine.
 *
 * It mirrors the runtime behaviour verified against Rivet in
 * `docs/spike-results.md` §B5: `execute` resolves to an array of row objects
 * and to `[]` for non-SELECT statements (no `changes`), and `transaction`
 * rolls back when its callback throws.
 *
 * `node:sqlite` is stable enough on Node 24 to need no flag, but it does emit
 * an `ExperimentalWarning` on first import.
 */

import { DatabaseSync } from 'node:sqlite'

import type { RawAccessLike } from '../sqlite.ts'

const returnsRows = (sql: string) => {
  const head = sql.trim().toUpperCase()
  return head.startsWith('SELECT') || head.startsWith('WITH') || head.startsWith('PRAGMA')
}

const makeExecute =
  (raw: DatabaseSync) =>
  async <Row extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    ...params: unknown[]
  ): Promise<Row[]> => {
    const statement = raw.prepare(sql)
    if (returnsRows(sql)) {
      // Spread into plain objects: `node:sqlite` hands back null-prototype rows.
      return statement.all(...(params as never[])).map((row) => ({ ...row }) as Row)
    }
    statement.run(...(params as never[]))
    return []
  }

/**
 * Creates a fresh in-memory database plus a `RawAccessLike` facade over it.
 * `raw` is exposed so tests can assert on state the facade does not surface.
 */
export const makeFakeDb = (): { db: RawAccessLike; raw: DatabaseSync } => {
  const raw = new DatabaseSync(':memory:')
  const execute = makeExecute(raw)

  const db: RawAccessLike = {
    execute,
    transaction: async <T>(callback: (tx: RawAccessLike) => Promise<T>, _options: { name: string }): Promise<T> => {
      raw.exec('BEGIN')
      try {
        const result = await callback({ execute, transaction: db.transaction })
        raw.exec('COMMIT')
        return result
      } catch (error) {
        raw.exec('ROLLBACK')
        throw error
      }
    },
  }

  return { db, raw }
}

/**
 * Wraps a `RawAccessLike` so the `nth` (1-based) `execute` call made *inside a
 * transaction* throws instead of running. Used to prove append + head update
 * are atomic.
 */
export const failOnNthTransactionStatement = (db: RawAccessLike, nth: number): RawAccessLike => ({
  execute: db.execute.bind(db),
  transaction: (callback, options) => {
    let seen = 0
    return db.transaction((tx) => {
      const guarded: RawAccessLike = {
        execute: async (sql, ...params) => {
          seen += 1
          if (seen === nth) throw new Error(`fake failure on statement #${nth}`)
          return tx.execute(sql, ...params)
        },
        transaction: tx.transaction.bind(tx),
      }
      return callback(guarded)
    }, options)
  },
})
