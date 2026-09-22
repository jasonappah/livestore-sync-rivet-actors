import { Schema } from '@livestore/utils/effect'

/**
 * Raised by the server when a request cannot be attributed to a valid caller:
 * the sync payload fails `syncPayloadSchema` / `validatePayload`, or the
 * request's `storeId` does not match the actor key.
 *
 * Distinguishable on the wire; the client maps it to `UnknownError` at the
 * `SyncBackend` boundary (LiveStore has no auth-specific error channel).
 */
export class InvalidPayloadError extends Schema.TaggedError<InvalidPayloadError>(
  '~livestore-sync-rivet-actors/InvalidPayloadError',
)('InvalidPayloadError', {
  storeId: Schema.String,
  reason: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}

/**
 * Raised by the admin actions (`AdminInfo`, `AdminReset`) when the request's
 * `adminSecret` does not match the server's `admin.secret`.
 *
 * Only the admin helper (`makeRivetSyncAdmin`) ever sees it; LiveStore's
 * `SyncBackend` never calls the admin actions.
 */
export class AdminUnauthorizedError extends Schema.TaggedError<AdminUnauthorizedError>(
  '~livestore-sync-rivet-actors/AdminUnauthorizedError',
)('AdminUnauthorizedError', {
  storeId: Schema.String,
}) {}
