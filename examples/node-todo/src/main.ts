/**
 * End-to-end demo: two `@livestore/adapter-node` stores (`client-a` /
 * `client-b`) on the same `storeId`, syncing through the Rivet sync actor
 * served by `src/server.ts`.
 *
 *   Terminal 1: pnpm --filter example-node-todo server
 *   Terminal 2: pnpm --filter example-node-todo start
 *
 * What it checks:
 *   1. A commits three `todo.created` events → B converges to three rows.
 *   2. B commits a `todo.completed` event → A sees the row flipped.
 *
 * Exits 0 on convergence, 1 (with diagnostics) on timeout.
 *
 * Note: the Rivet engine persists actor state across runs (`~/.rivetkit`), so
 * every run uses a fresh `storeId`.
 */

import { makeAdapter } from '@livestore/adapter-node'
import { createStorePromise, type Store } from '@livestore/livestore'
import { makeRivetSync } from 'livestore-sync-rivet-actors/client'

import { events, schema, SyncPayload, tables } from './schema.ts'

const ENDPOINT = process.env.RIVET_ENDPOINT ?? 'http://127.0.0.1:6420'
const TIMEOUT_MS = 15_000
const POLL_INTERVAL_MS = 100

/** The Rivet engine persists actor state across runs → never reuse a storeId. */
const storeId = `node-todo-${Date.now()}`

const todosQuery = tables.todos.orderBy('id', 'asc')

type TodoStore = Store<typeof schema>

const makeClient = (clientId: string): Promise<TodoStore> =>
  createStorePromise({
    schema,
    storeId,
    // `makeAdapter` defaults `clientId` to the machine hostname, which would
    // make both stores in this process the *same* LiveStore client.
    adapter: makeAdapter({
      storage: { type: 'in-memory' },
      clientId,
      sync: {
        backend: makeRivetSync({ endpoint: ENDPOINT }),
        onSyncError: 'shutdown',
      },
    }),
    syncPayloadSchema: SyncPayload,
    syncPayload: { authToken: 'demo' },
  })

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

/** Polls `read` every 100 ms until `predicate` holds or the timeout elapses. */
const waitFor = async <T>(
  label: string,
  read: () => T,
  predicate: (value: T) => boolean,
): Promise<{ ok: boolean; value: T; elapsedMs: number }> => {
  const startedAt = Date.now()
  let value = read()
  while (predicate(value) === false) {
    if (Date.now() - startedAt > TIMEOUT_MS) {
      console.error(`✗ timed out after ${TIMEOUT_MS} ms waiting for ${label}; last value:`, value)
      return { ok: false, value, elapsedMs: Date.now() - startedAt }
    }
    await sleep(POLL_INTERVAL_MS)
    value = read()
  }
  return { ok: true, value, elapsedMs: Date.now() - startedAt }
}

const main = async (): Promise<number> => {
  console.log(`storeId: ${storeId}`)
  console.log(`endpoint: ${ENDPOINT}`)

  const [storeA, storeB] = await Promise.all([makeClient('client-a'), makeClient('client-b')])

  try {
    // --- 1. A → B -----------------------------------------------------------
    const todoIds = ['todo-1', 'todo-2', 'todo-3']
    storeA.commit(
      ...todoIds.map((id, index) => events.todoCreated({ id, text: `todo number ${index + 1}` })),
    )
    console.log(`client-a committed ${todoIds.length} todo.created events`)

    const seenByB = await waitFor(
      'client-b to see 3 todos',
      () => storeB.query(todosQuery),
      (rows) => rows.length === todoIds.length,
    )
    if (seenByB.ok === false) {
      console.error('client-a rows:', storeA.query(todosQuery))
      return 1
    }
    console.log(`✓ client-b converged in ${seenByB.elapsedMs} ms:`)
    console.table(seenByB.value)

    // --- 2. B → A -----------------------------------------------------------
    storeB.commit(events.todoCompleted({ id: 'todo-1' }))
    console.log(`client-b committed todo.completed for 'todo-1'`)

    const seenByA = await waitFor(
      `client-a to see 'todo-1' completed`,
      () => storeA.query(todosQuery),
      (rows) => rows.find((row) => row.id === 'todo-1')?.completed === true,
    )
    if (seenByA.ok === false) {
      console.error('client-b rows:', storeB.query(todosQuery))
      return 1
    }
    console.log(`✓ client-a converged in ${seenByA.elapsedMs} ms:`)
    console.table(seenByA.value)

    console.log('✓ bidirectional sync through the Rivet actor works')
    return 0
  } finally {
    await Promise.all([storeA.shutdownPromise(), storeB.shutdownPromise()])
  }
}

main().then(
  (code) => process.exit(code),
  (error: unknown) => {
    console.error('✗ example failed:', error)
    process.exit(1)
  },
)
