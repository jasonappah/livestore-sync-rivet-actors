/**
 * The `Pull` action handler: one catch-up page per call.
 *
 * Mirrors `@livestore/sync-cf`'s `cf-worker/do/pull.ts`, but returns a single
 * page instead of a stream — a Rivet action resolves to one value, so
 * pagination is client-driven (the client feeds the last event's `seqNum`
 * back as the next cursor) and live updates arrive out-of-band as
 * `LIVE_PULL_EVENT` connection events pushed by `push.ts`.
 *
 * `remaining` comes from a `COUNT(*)` taken with the same cursor as the page
 * itself, so a client can show real progress while catching up and the last
 * page is always `NoMore`.
 *
 * Like sync-cf (whose `emitIfEmpty` sits after the `onPullRes` tap), the
 * empty response produced for an already-caught-up cursor does not invoke
 * `onPullRes`.
 */

import { BackendIdMismatchError, SyncBackend, UnknownError } from '@livestore/common'
import type { EventSequenceNumber } from '@livestore/common/schema'
import { splitArrayBySize } from '@livestore/common/sync'
import { Effect, Option } from '@livestore/utils/effect'

import type { InvalidPayloadError, PullRequest, PullResponseBatchItem } from '../common/mod.ts'
import { emptyPullResponse, encodePullResponse, PullResponse } from '../common/mod.ts'
import { mapToDeclaredErrors, runHook } from './hooks.ts'
import { rowToBatchItem } from './sqlite.ts'
import type { StoreCtx } from './store-ctx.ts'
import { validateSyncPayload } from './validate-payload.ts'

/** Everything the `Pull` action declares on the wire (see `PullError`). */
export type PullHandlerError = UnknownError | BackendIdMismatchError | InvalidPayloadError

const clamp = (value: number, min: number, max: number): number => Math.min(Math.max(value, min), max)

const textEncoder = new TextEncoder()

/**
 * `OversizeChunkItemError` does not say *which* event blew the budget, so
 * re-measure to name it. Falls back to the first event of the page (the only
 * candidate `splitArrayBySize` can reject before it has looked at the rest).
 */
const oversizeSeqNum = (
  items: ReadonlyArray<PullResponseBatchItem>,
  backendId: string,
  maxBytes: number,
): EventSequenceNumber.Global.Type | undefined => {
  const measure = (item: PullResponseBatchItem) =>
    textEncoder.encode(
      JSON.stringify(
        encodePullResponse(
          PullResponse.make({ batch: [item], pageInfo: SyncBackend.pageInfoNoMore, backendId }),
        ),
      ),
    ).byteLength

  return (items.find((item) => measure(item) > maxBytes) ?? items[0])?.eventEncoded.seqNum
}

export const makePull =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: PullRequest): Effect.Effect<PullResponse, PullHandlerError> =>
    Effect.gen(function* () {
      const { backendId, storeId, storage, options } = ctx

      const payload = yield* validateSyncPayload({ storeId, options }, req)

      yield* runHook(options.onPull, req, {
        storeId,
        clientId: req.clientId,
        ...(payload !== undefined ? { payload } : {}),
      })

      // A cursor carrying no backend id is accepted: a client may hold a
      // cursor from an earlier session without having persisted the id.
      if (Option.isSome(req.cursor) === true && Option.isSome(req.cursor.value.backendId) === true) {
        const received = req.cursor.value.backendId.value
        if (received !== backendId) {
          return yield* new BackendIdMismatchError({ expected: backendId, received })
        }
      }

      const cursorSeq: Option.Option<EventSequenceNumber.Global.Type> = Option.map(
        req.cursor,
        (cursor) => cursor.eventSequenceNumber,
      )

      const total = yield* storage.countAfter(cursorSeq)
      if (total === 0) return emptyPullResponse(backendId)

      // The client may ask for fewer events than the page size, never more.
      const limit = clamp(req.limit ?? options.pullPageSize, 1, options.pullPageSize)

      const rows = yield* storage.selectPage(cursorSeq, limit)
      const items = rows.map(rowToBatchItem)

      // `total > 0` guarantees a non-empty page; stay defensive rather than
      // handing `[]` to `splitArrayBySize`, which requires a non-empty array.
      const [firstItem, ...restItems] = items
      if (firstItem === undefined) return emptyPullResponse(backendId)

      // Byte guard: keep only the first chunk that fits the transport budget.
      const chunks = yield* splitArrayBySize<PullResponseBatchItem>({
        maxItems: limit,
        maxBytes: options.maxMessageBytes,
        encode: (batch) =>
          encodePullResponse(PullResponse.make({ batch, pageInfo: SyncBackend.pageInfoNoMore, backendId })),
      })([firstItem, ...restItems]).pipe(
        Effect.mapError(
          (cause) =>
            new UnknownError({
              cause,
              note: `event ${oversizeSeqNum(items, backendId, options.maxMessageBytes)} exceeds maxMessageBytes`,
            }),
        ),
      )

      const page = chunks[0]
      const remaining = total - page.length

      const res = PullResponse.make({
        batch: page,
        pageInfo: remaining > 0 ? SyncBackend.pageInfoMoreKnown(remaining) : SyncBackend.pageInfoNoMore,
        backendId,
      })

      yield* runHook(options.onPullRes, res)

      return res
    }).pipe(
      // sync-cf hands `onPullRes` the `UnknownError` as well, so a host
      // observing pull responses also observes the failures.
      Effect.tapError((error) =>
        error._tag === 'UnknownError' ? runHook(ctx.options.onPullRes, error) : Effect.void,
      ),
      mapToDeclaredErrors(['BackendIdMismatchError', 'InvalidPayloadError', 'UnknownError']),
      Effect.withSpan('livestore-sync-rivet:pull', { attributes: { storeId: ctx.storeId } }),
    )
