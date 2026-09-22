/**
 * Minimal LiveStore schema shared by both clients in this example.
 *
 * Shape copied from LiveStore's own todo fixture
 * (`packages/@livestore/livestore/src/utils/tests/fixture.ts`), reduced to the
 * two synced events the demo needs.
 */

import { Events, makeSchema, Schema, State } from '@livestore/livestore'

export const todos = State.SQLite.table({
  name: 'todos',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    text: State.SQLite.text({ default: '', nullable: false }),
    completed: State.SQLite.boolean({ default: false, nullable: false }),
  },
})

export const tables = { todos }

export const events = {
  todoCreated: Events.synced({
    name: 'todo.created',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String }),
  }),
  todoCompleted: Events.synced({
    name: 'todo.completed',
    schema: Schema.Struct({ id: Schema.String }),
  }),
}

const materializers = State.SQLite.materializers(events, {
  'todo.created': ({ id, text }) => tables.todos.insert({ id, text, completed: false }),
  'todo.completed': ({ id }) => tables.todos.update({ completed: true }).where({ id }),
})

export const state = State.SQLite.makeState({ tables, materializers })

export const schema = makeSchema({ events, state })

/** Sync payload both clients send; the server only accepts `authToken: 'demo'`. */
export const SyncPayload = Schema.Struct({ authToken: Schema.String })
