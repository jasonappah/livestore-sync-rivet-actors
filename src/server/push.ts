/**
 * `Push` action handler.
 *
 * Mirrors `@livestore/sync-cf`'s `cf-worker/do/push.ts`: a push is one
 * ordered admit → persist → publish operation. Everything that can be
 * decided from the request alone (payload, hooks, backend id, batch shape)
 * runs before the gate; the head check, the durable append, the in-memory
 * head update and the fan-out run under `pushSemaphore` so a later push can
 * neither interleave with the head transition nor overtake this push's
 * live-pull events. The gated section is uninterruptible: a committed head
 * must always be followed by its fan-out, otherwise a pusher that was told
 * `ServerAheadError` could wait forever for the event that beat it.
 *
 * The `PushAck` is returned only after fan-out, so the pusher's own live
 * pull (it is one of the connections) has already seen the events by the
 * time the ack arrives.
 */

import { BackendIdMismatchError, ServerAheadError, SyncBackend, UnknownError } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { splitArrayBySize } from '@livestore/common/sync'
import { Effect, Option, type ReadonlyArray as EffectArray } from '@livestore/utils/effect'

import {
  encodePullResponse,
  type InvalidPayloadError,
  MAX_PUSH_EVENTS_PER_REQUEST,
  PullResponse,
  PushAck,
  type PushRequest,
  SyncMetadata,
} from '../common/mod.ts'
import { fanOut } from './connections.ts'
import { mapToDeclaredErrors, runHook } from './hooks.ts'
import type { CallbackContext } from './options.ts'
import type { StoreCtx } from './store-ctx.ts'
import { validateSyncPayload } from './validate-payload.ts'

export type PushHandlerError = UnknownError | ServerAheadError | BackendIdMismatchError | InvalidPayloadError

export const PUSH_BATCH_TOO_LARGE_NOTE = 'push batch too large'
export const PUSH_BATCH_NOT_CHAINED_NOTE = 'push batch is not a contiguous chain'

const PUSH_ERROR_TAGS = ['ServerAheadError', 'BackendIdMismatchError', 'InvalidPayloadError', 'UnknownError'] as const

/** Builds the `onPush` callback context without ever materialising `payload: undefined`. */
const toCallbackContext = <TSyncPayload>(
  req: PushRequest,
  payload: TSyncPayload | undefined,
): CallbackContext<TSyncPayload> => ({
  storeId: req.storeId,
  clientId: req.clientId,
  ...(payload !== undefined ? { payload } : {}),
})

/**
 * `true` when every event's parent is the previous event and every event
 * moves the sequence forward. The first event's parent is checked against
 * the head later, under the gate.
 */
const isContiguousChain = (batch: ReadonlyArray<LiveStoreEvent.Global.Encoded>): boolean => {
  for (let index = 0; index < batch.length; index++) {
    const event = batch[index]!
    if (event.seqNum <= event.parentSeqNum) return false
    if (index > 0 && event.parentSeqNum !== batch[index - 1]!.seqNum) return false
  }
  return true
}

export const makePush =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: PushRequest): Effect.Effect<PushAck, PushHandlerError> => {
    // An empty push is a no-op: acked immediately, no validation, no hooks
    // (not even `onPushRes`), no span.
    if (req.batch.length === 0) return Effect.sync(() => PushAck.make({ backendId: ctx.backendId }))

    return pushNonEmpty(ctx, req)
  }

const pushNonEmpty = <TSyncPayload>(
  ctx: StoreCtx<TSyncPayload>,
  req: PushRequest,
): Effect.Effect<PushAck, PushHandlerError> =>
  Effect.gen(function* () {
    const { batch } = req

    const payload = yield* validateSyncPayload(ctx, req)
    yield* runHook(ctx.options.onPush, req, toCallbackContext(req, payload))

    if (Option.isSome(req.backendId) && req.backendId.value !== ctx.backendId) {
      return yield* new BackendIdMismatchError({ expected: ctx.backendId, received: req.backendId.value })
    }

    if (batch.length > ctx.options.maxPushEventsPerRequest) {
      return yield* new UnknownError({
        cause: new Error(`received ${batch.length} events, max ${ctx.options.maxPushEventsPerRequest}`),
        note: PUSH_BATCH_TOO_LARGE_NOTE,
        payload: { batchLength: batch.length, max: ctx.options.maxPushEventsPerRequest },
      })
    }

    if (isContiguousChain(batch) === false) {
      return yield* new UnknownError({
        cause: new Error('every event must have seqNum > parentSeqNum and parent the previous event'),
        note: PUSH_BATCH_NOT_CHAINED_NOTE,
        payload: { seqNums: batch.map((event) => [event.parentSeqNum, event.seqNum]) },
      })
    }

    // `batch` is non-empty here; keep the refinement for `splitArrayBySize`.
    const nonEmptyBatch = batch as EffectArray.NonEmptyReadonlyArray<LiveStoreEvent.Global.Encoded>

    const admission = Effect.gen(function* () {
      // Re-checked under the gate: an `AdminReset` may have replaced the
      // backend id while this push was waiting for the permit.
      if (Option.isSome(req.backendId) && req.backendId.value !== ctx.backendId) {
        return yield* new BackendIdMismatchError({ expected: ctx.backendId, received: req.backendId.value })
      }

      const head = ctx.headRef.current
      const firstParent = nonEmptyBatch[0].parentSeqNum
      if (firstParent !== head) {
        return yield* new ServerAheadError({ minimumExpectedNum: head, providedNum: firstParent })
      }

      const createdAt = new Date().toISOString()
      const newHead = nonEmptyBatch[nonEmptyBatch.length - 1]!.seqNum

      yield* ctx.storage.appendEventsAndUpdateHead(nonEmptyBatch, createdAt, {
        storeId: ctx.storeId,
        backendId: ctx.backendId,
        newHead,
      })
      // Only after the transaction committed: a failed append leaves the
      // in-memory head where the persisted one is.
      ctx.headRef.current = newHead

      const toPullResponse = (events: ReadonlyArray<LiveStoreEvent.Global.Encoded>): PullResponse =>
        PullResponse.make({
          batch: events.map((eventEncoded) => ({
            eventEncoded,
            metadata: Option.some(SyncMetadata.make({ createdAt })),
          })),
          pageInfo: SyncBackend.pageInfoNoMore,
          backendId: ctx.backendId,
        })

      // The events are durable and the head has moved, so from here on the
      // push *has* succeeded. If a single event cannot fit into one live
      // message (only possible with a misconfigured `maxMessageBytes`, since
      // rivetkit's incoming limit already bounds the whole push), we still
      // ack: failing would make the client retry into `ServerAheadError`
      // and never learn that its events landed. Live subscribers miss this
      // batch and catch up on their next pull instead.
      const chunks = yield* splitArrayBySize({
        maxItems: MAX_PUSH_EVENTS_PER_REQUEST,
        maxBytes: ctx.options.maxMessageBytes,
        encode: (events: ReadonlyArray<LiveStoreEvent.Global.Encoded>) => encodePullResponse(toPullResponse(events)),
      })(nonEmptyBatch).pipe(
        Effect.catchTag('OversizeChunkItemError', (error) =>
          Effect.sync(() => {
            ctx.log('error', 'persisted push exceeds maxMessageBytes; skipping live fan-out', {
              storeId: ctx.storeId,
              size: error.size,
              maxBytes: error.maxBytes,
              head: newHead,
            })
            return [] as ReadonlyArray<ReadonlyArray<LiveStoreEvent.Global.Encoded>>
          }),
        ),
      )

      const responses = chunks.map((events) => {
        const response = toPullResponse(events)
        return { response, encoded: encodePullResponse(response) }
      })

      for (const { response } of responses) {
        yield* runHook(ctx.options.onPullRes, response)
      }

      yield* fanOut(
        ctx,
        responses.map(({ encoded }) => encoded),
      )

      return PushAck.make({ backendId: ctx.backendId })
    })

    return yield* ctx.pushSemaphore.withPermits(1)(admission.pipe(Effect.uninterruptible))
  }).pipe(
    Effect.tap((ack) => runHook(ctx.options.onPushRes, ack)),
    Effect.tapError((error) => (error._tag === 'UnknownError' ? runHook(ctx.options.onPushRes, error) : Effect.void)),
    mapToDeclaredErrors(PUSH_ERROR_TAGS),
    Effect.withSpan('livestore-sync-rivet:push', {
      attributes: { storeId: ctx.storeId, batchSize: req.batch.length },
    }),
  )
