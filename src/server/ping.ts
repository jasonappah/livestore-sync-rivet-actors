/**
 * `Ping` action handler: a liveness probe that doubles as a re-check of the
 * caller's sync payload (a revoked token surfaces as `InvalidPayloadError`
 * on the next ping rather than only on the next push).
 */

import type { UnknownError } from '@livestore/common'
import { Effect } from '@livestore/utils/effect'

import { type InvalidPayloadError, type PingRequest, Pong } from '../common/mod.ts'
import { mapToDeclaredErrors } from './hooks.ts'
import type { StoreCtx } from './store-ctx.ts'
import { validateSyncPayload } from './validate-payload.ts'

export type PingHandlerError = UnknownError | InvalidPayloadError

const PING_ERROR_TAGS = ['InvalidPayloadError', 'UnknownError'] as const

export const makePing =
  <TSyncPayload>(ctx: StoreCtx<TSyncPayload>) =>
  (req: PingRequest): Effect.Effect<Pong, PingHandlerError> =>
    validateSyncPayload(ctx, req).pipe(
      Effect.as(Pong.make({})),
      mapToDeclaredErrors(PING_ERROR_TAGS),
      Effect.withSpan('livestore-sync-rivet:ping', { attributes: { storeId: ctx.storeId } }),
    )
