/**
 * The sync server for this example: a Rivet actor host running the LiveStore
 * sync actor.
 *
 * Run it with `pnpm --filter example-node-todo run server` (which sets
 * `RIVET_RUN_ENGINE=1`, making rivetkit spawn a local Rivet engine at
 * http://127.0.0.1:6420). Point `RIVET_ENDPOINT` at a hosted engine instead
 * for a real deployment.
 *
 * Auth: clients must send `syncPayload: { authToken: 'demo' }`.
 */

import { NodeRuntime } from '@effect/platform-node'
import { Registry } from '@rivetkit/effect'
import { Layer } from 'effect'
import {
  makeLiveStoreSyncActor,
  RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
  registryOptions,
} from 'livestore-sync-rivet-actors/server'

const ActorsLayer = makeLiveStoreSyncActor({
  validatePayload: (payload) => {
    if ((payload as { authToken?: unknown } | undefined)?.authToken !== 'demo') {
      throw new Error('unauthorized')
    }
  },
})

const MainLayer = Registry.serve(ActorsLayer).pipe(
  Layer.provide(
    Registry.layer(
      registryOptions({
        endpoint: process.env.RIVET_ENDPOINT,
        maxIncomingMessageSize: RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE,
      }),
    ),
  ),
)

Layer.launch(MainLayer).pipe(NodeRuntime.runMain)
