/**
 * `SyncBackend.pull` over the Rivet connection.
 *
 * Catch-up is client-driven pagination over the `Pull` action (pages are
 * forwarded verbatim so `MoreKnown(remaining)` reaches LiveStore). In live
 * mode the stream then stays open and is fed by the actor's `pull` connection
 * events; it survives disconnects by re-catching-up from `lastSeen` whenever
 * the connection comes back, and repairs gaps (missed broadcasts) the same
 * way. Only `BackendIdMismatchError` (or a failure of the initial catch-up,
 * which the leader retries) ends a live stream.
 *
 * Invariants (see the plan, "Client design → Pull"):
 * - the event subscription is established BEFORE the first request;
 * - the last catch-up page is `NoMore` (server guarantee, forwarded as-is);
 * - no emitted item ever carries `seqNum <= lastSeen`.
 *
 * Browser-safe: no rivetkit imports.
 */

import { BackendIdMismatchError, type IsOfflineError, SyncBackend, type UnknownError } from '@livestore/common'
import { EventSequenceNumber } from '@livestore/common/schema'
import {
  Duration,
  Effect,
  Option,
  PubSub,
  Schedule,
  type Schema,
  Stream,
} from '@livestore/utils/effect'

import {
  decodePullResponseEffect,
  type PullRequest,
  type PullResponse,
  type PullResponseBatchItem,
  pullResponseToItem,
  type SyncMetadata,
} from '../common/mod.ts'
import type { ActionClient } from './action-client.ts'
import type { ResolvedRivetSyncOptions } from './options.ts'
import type { Connection, ConnStatus } from './types.ts'

/** The value yielded by `SyncBackend.makeBackendIdHelper`. */
export type BackendIdHelper = Effect.Success<typeof SyncBackend.makeBackendIdHelper>

export interface PullDeps {
  readonly conn: Connection
  readonly actions: ActionClient
  readonly backendIdHelper: BackendIdHelper
  readonly storeId: string
  readonly clientId: string
  readonly payload: Schema.Json | undefined
  readonly options: ResolvedRivetSyncOptions
}

type PullError = IsOfflineError | BackendIdMismatchError | UnknownError
type PullItem = SyncBackend.PullResItem<SyncMetadata>
type Seq = EventSequenceNumber.Global.Type

const ROOT = EventSequenceNumber.Client.ROOT.global

/** Inputs of the sequential live loop. */
type LiveInput = { readonly _tag: 'Event'; readonly raw: unknown } | { readonly _tag: 'Reconnected' }

const lastSeqNum = (batch: ReadonlyArray<PullResponseBatchItem>): Seq | undefined => batch.at(-1)?.eventEncoded.seqNum

export const makePull =
  (deps: PullDeps): SyncBackend.SyncBackend<SyncMetadata>['pull'] =>
  (cursor, pullOptions) => {
    const live = pullOptions?.live === true
    const hadCursor = Option.isSome(cursor)
    const initialSeq: Seq = Option.match(cursor, {
      onNone: () => ROOT,
      onSome: (c) => c.eventSequenceNumber,
    })

    /** Transient failures of a live-phase catch-up are retried; a backend reset is not. */
    const retrySchedule = Schedule.exponential(deps.options.reconnect.baseDelay).pipe(
      Schedule.modifyDelay(({ duration }) => Effect.succeed(Duration.min(duration, deps.options.reconnect.maxDelay))),
      Schedule.jittered,
    )
    const isTransient = (error: PullError) => error._tag !== 'BackendIdMismatchError'

    const stream = Effect.gen(function* () {
      // Avoid the leader's zero-delay retry loop while the socket is still
      // coming up; fails with IsOfflineError only after `connectTimeout`.
      yield* deps.conn.awaitConnected

      // Per-run state: re-running the returned stream starts over from the cursor.
      let lastSeen: Seq = initialSeq

      /** `None` only for a genuinely fresh client; otherwise carries whatever backendId we know. */
      const reqCursor = (seq: Seq): PullRequest['cursor'] =>
        seq === ROOT && !hadCursor
          ? Option.none()
          : Option.some({ eventSequenceNumber: seq, backendId: deps.backendIdHelper.get() })

      const fetchPage = (seq: Seq): Effect.Effect<PullResponse, PullError> =>
        deps.actions
          .pull({
            storeId: deps.storeId,
            clientId: deps.clientId,
            // `optionalKey`: an absent payload must be an absent key, never `undefined`.
            ...(deps.payload === undefined ? {} : { payload: deps.payload }),
            cursor: reqCursor(seq),
            limit: deps.options.pullPageSize,
          })
          .pipe(Effect.tap((res) => checkBackendId(res.backendId)))

      /**
       * Defensive: the server already rejects a `Some` cursor with a foreign id,
       * but a `None` cursor (or a reset between requests) must not silently
       * switch identities.
       */
      const checkBackendId = (received: string): Effect.Effect<void, BackendIdMismatchError | UnknownError> => {
        const stored = deps.backendIdHelper.get()
        if (Option.isSome(stored) && stored.value !== received) {
          return Effect.fail(new BackendIdMismatchError({ expected: stored.value, received }))
        }
        return deps.backendIdHelper.lazySet(received)
      }

      const advanceLastSeen = (batch: ReadonlyArray<PullResponseBatchItem>) => {
        const last = lastSeqNum(batch)
        if (last !== undefined && last > lastSeen) lastSeen = last
      }

      /**
       * Client-driven pagination from `from`. Pages are emitted verbatim; paging
       * stops on `NoMore` or on an empty page (defensive against a server that
       * reports more but returns nothing).
       */
      const catchUp = (from: Seq, options: { readonly retry: boolean }): Stream.Stream<PullResponse, PullError> =>
        Stream.paginate(from, (seq) =>
          fetchPage(seq).pipe(
            options.retry ? Effect.retry({ while: isTransient, schedule: retrySchedule }) : (self) => self,
            Effect.map((res) => {
              advanceLastSeen(res.batch)
              const last = lastSeqNum(res.batch)
              const next = res.pageInfo._tag !== 'NoMore' && last !== undefined ? Option.some(last) : Option.none()
              return [[res], next] as const
            }),
          ),
        )

      const toItems = (self: Stream.Stream<PullResponse, PullError>): Stream.Stream<PullItem, PullError> =>
        Stream.map(self, pullResponseToItem)

      if (!live) return toItems(catchUp(lastSeen, { retry: false }))

      // Subscribe BEFORE the first request so nothing broadcast during the
      // catch-up is lost (both subscriptions live in the stream's scope).
      const eventSub = yield* PubSub.subscribe(deps.conn.pullEvents)
      const statusSub = yield* PubSub.subscribe(deps.conn.status.pubsub)

      const events: Stream.Stream<LiveInput> = Stream.fromSubscription(eventSub).pipe(
        Stream.map((raw) => ({ _tag: 'Event', raw }) as const),
      )
      const reconnects: Stream.Stream<LiveInput> = Stream.fromSubscription(statusSub).pipe(
        // The SubscriptionRef pubsub replays the current value; we only want transitions.
        Stream.drop(1),
        Stream.filter((status: ConnStatus) => status === 'connected'),
        Stream.map(() => ({ _tag: 'Reconnected' }) as const),
      )

      /** Re-fetch everything after `lastSeen`; retried across transient failures. */
      const reCatchUp = (): Stream.Stream<PullItem, PullError> =>
        toItems(Stream.suspend(() => catchUp(lastSeen, { retry: true })))

      const handleEvent = (raw: unknown): Stream.Stream<PullItem, PullError> =>
        decodePullResponseEffect(raw).pipe(
          Effect.map(Option.some),
          Effect.catch((cause) =>
            Effect.logWarning('rivet-sync: ignoring undecodable live pull event', { storeId: deps.storeId, cause }).pipe(
              Effect.as(Option.none<PullResponse>()),
            ),
          ),
          Effect.map(
            Option.match({
              onNone: () => Stream.empty,
              onSome: (res): Stream.Stream<PullItem, PullError> => {
                const stored = deps.backendIdHelper.get()
                if (Option.isSome(stored) && stored.value !== res.backendId) {
                  return Stream.fail(new BackendIdMismatchError({ expected: stored.value, received: res.backendId }))
                }
                // Drop everything we already have (catch-up overlap, duplicate broadcast).
                const fresh = res.batch.filter((item) => item.eventEncoded.seqNum > lastSeen)
                const first = fresh[0]
                if (first === undefined) return Stream.empty
                // A missed broadcast: the server has more than this event; fetch it all in order.
                if (first.eventEncoded.parentSeqNum !== lastSeen) return reCatchUp()
                advanceLastSeen(fresh)
                return Stream.make<[PullItem]>({ batch: fresh, pageInfo: SyncBackend.pageInfoNoMore })
              },
            }),
          ),
          Stream.unwrap,
        )

      const liveStream = Stream.merge(events, reconnects).pipe(
        // Sequential on purpose: `lastSeen` is read and advanced in input order.
        Stream.flatMap((input) => (input._tag === 'Event' ? handleEvent(input.raw) : reCatchUp())),
      )

      return Stream.concat(toItems(catchUp(lastSeen, { retry: false })), liveStream)
    })

    return Stream.unwrap(stream).pipe(
      Stream.withSpan('rivet-sync:pull', {
        attributes: { storeId: deps.storeId, clientId: deps.clientId, live, cursor: Option.getOrUndefined(cursor)?.eventSequenceNumber },
      }),
    )
  }
