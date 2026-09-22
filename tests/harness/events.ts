/**
 * Shared event definitions for tests. Mirrors the shape LiveStore's own
 * fixtures use (`Events.synced` + `EventFactory`), so every test in this repo
 * builds `LiveStoreEvent.Global.Encoded` values the same way.
 */

import { Events } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import { Schema } from '@livestore/utils/effect'

export const events = {
  todoCreated: Events.synced({
    name: 'todo.created',
    schema: Schema.Struct({
      id: Schema.String,
      text: Schema.String,
      completed: Schema.Boolean,
    }),
  }),
  todoCompleted: Events.synced({
    name: 'todo.completed',
    schema: Schema.Struct({
      id: Schema.String,
    }),
  }),
}

/**
 * Sequence-tracking factory over {@link events}.
 *
 * `startSeq` defaults to 1 and the first event's `parentSeqNum` to the root
 * (`0`), matching what a fresh client would push.
 */
export const makeEventFactory = (
  config: Partial<EventFactory.EventFactoriesConfig> = {},
): ReturnType<ReturnType<typeof EventFactory.makeFactory<typeof events>>> =>
  EventFactory.makeFactory(events)({
    client: EventFactory.clientIdentity('test-client'),
    ...config,
  })
