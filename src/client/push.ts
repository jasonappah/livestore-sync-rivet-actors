/**
 * `SyncBackend['push']` for the Rivet transport.
 *
 * The LiveStore leader may hand us more events than one Rivet action can
 * carry, so the batch is split by both item count
 * (`MAX_PUSH_EVENTS_PER_REQUEST`) and encoded byte size
 * (`options.maxPushBytes`, default 60 000 — rivetkit's default
 * `maxIncomingMessageSize` is 64 KB and an oversize frame makes the *server*
 * close the socket, which would otherwise loop forever).
 *
 * Chunks are pushed strictly sequentially (the server admits a batch only if
 * it chains onto its head) and the whole push runs under a client-side
 * semaphore so two concurrent `push` calls can never interleave their chunks.
 *
 * Browser-safe: no rivetkit imports.
 */

import type { SyncBackend } from '@livestore/common'
import { UnknownError } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { splitArrayBySize } from '@livestore/common/sync'
import { Effect, ReadonlyArray as EffectArray, type Schema, type Semaphore } from '@livestore/utils/effect'

import { encodePushRequest, MAX_PUSH_EVENTS_PER_REQUEST, PushRequest, type SyncMetadata } from '../common/mod.ts'
import type { ActionClient } from './action-client.ts'
import type { ResolvedRivetSyncOptions } from './options.ts'
import type { BackendIdHelper } from './pull.ts'

/** The KV-backed backend-id cache (`SyncBackend.makeBackendIdHelper`), shared with `pull`. */
export type { BackendIdHelper } from './pull.ts'

export interface PushDeps {
  readonly actions: ActionClient
  readonly backendIdHelper: BackendIdHelper
  readonly storeId: string
  readonly clientId: string
  readonly payload: Schema.Json | undefined
  readonly options: ResolvedRivetSyncOptions
  /** Serialises concurrent `push` calls. Shared with nothing else. */
  readonly semaphore: Semaphore.Semaphore
}

export const makePush = (deps: PushDeps): SyncBackend.SyncBackend<SyncMetadata>['push'] => {
  const { actions, backendIdHelper, clientId, options, payload, semaphore, storeId } = deps

  /**
   * Built fresh per chunk so a `backendId` learned from the previous ack is
   * carried by the next request. Also used as the chunker's `encode` so the
   * measured bytes include the request framing, not just the events.
   */
  const makeRequest = (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>): PushRequest =>
    PushRequest.make({
      storeId,
      clientId,
      // `payload` is an `optionalKey`: absent must mean an absent key.
      ...(payload !== undefined ? { payload } : {}),
      batch,
      backendId: backendIdHelper.get(),
    })

  const chunk = splitArrayBySize<LiveStoreEvent.Global.Encoded>({
    maxItems: MAX_PUSH_EVENTS_PER_REQUEST,
    maxBytes: options.maxPushBytes,
    encode: (items) => encodePushRequest(makeRequest(items)),
  })

  return (batch) => {
    if (EffectArray.isReadonlyArrayNonEmpty(batch) === false) return Effect.void

    return Effect.gen(function* () {
      const chunks = yield* chunk(batch).pipe(
        Effect.mapError((cause) => new UnknownError({ cause, note: 'single event exceeds maxPushBytes' })),
      )

      for (const batchChunk of chunks) {
        const ack = yield* actions.push(makeRequest(batchChunk))
        yield* backendIdHelper.lazySet(ack.backendId)
      }
    }).pipe(
      semaphore.withPermits(1),
      Effect.withSpan('rivet-sync:push', { attributes: { batchSize: batch.length } }),
    )
  }
}
