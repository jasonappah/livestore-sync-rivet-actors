/**
 * Long-running Rivet actor server hosting the LiveStore sync actor.
 *
 * Run from the repo root (builds the library first so `dist/` exists):
 *
 *   pnpm build && pnpm example:server
 *
 * `RIVET_RUN_ENGINE=1` (set by the package scripts) makes rivetkit spawn a
 * local Rivet engine at http://127.0.0.1:6420 when `RIVET_ENDPOINT` is not
 * set. Point `RIVET_ENDPOINT` (and `RIVET_TOKEN`) at a hosted engine instead
 * for a real deployment.
 *
 * Auth: when `SYNC_AUTH_TOKEN` is set, every client must send
 * `syncPayload: { authToken: <SYNC_AUTH_TOKEN> }`; otherwise every caller is
 * accepted.
 */

import { NodeRuntime } from '@effect/platform-node'
import { Registry } from '@rivetkit/effect'
import { Layer } from 'effect'
import { makeLiveStoreSyncActor, registryOptions } from 'livestore-sync-rivet-actors/server'

const authTokenOf = (payload: unknown): string | undefined => {
  if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) return undefined
  const { authToken } = payload as { authToken?: unknown }
  return typeof authToken === 'string' ? authToken : undefined
}

const ActorsLayer = makeLiveStoreSyncActor({
  validatePayload: (payload, { storeId, clientId }) => {
    const expected = process.env.SYNC_AUTH_TOKEN
    if (expected === undefined || expected === '') return
    if (authTokenOf(payload) !== expected) {
      throw new Error(`invalid authToken for store '${storeId}' (client '${clientId}')`)
    }
  },
})

const MainLayer = Registry.serve(ActorsLayer).pipe(
  Layer.provide(
    Registry.layer(
      registryOptions({
        endpoint: process.env.RIVET_ENDPOINT,
        token: process.env.RIVET_TOKEN,
        namespace: process.env.RIVET_NAMESPACE,
      }),
    ),
  ),
)

Layer.launch(MainLayer).pipe(NodeRuntime.runMain)
