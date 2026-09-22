import { BackendId, BackendIdMismatchError, ServerAheadError, SyncBackend, UnknownError } from '@livestore/common'
import { EventSequenceNumber, LiveStoreEvent } from '@livestore/common/schema'
import { Schema } from '@livestore/utils/effect'

import { AdminUnauthorizedError, InvalidPayloadError } from './errors.ts'

/**
 * Wire contract shared by the Rivet actor and the LiveStore `SyncBackend`
 * client. Mirrors `@livestore/sync-cf`'s `common/sync-message-types.ts` and
 * reuses `@livestore/common` schemas wherever possible.
 *
 * Every action payload / success / error value travels as
 * `Schema.toCodecJson(schema)` — the same canonical JSON encoding
 * `@rivetkit/effect` applies server-side, which keeps the encoding unambiguous
 * over rivetkit's CBOR transport.
 *
 * No `rivetkit` / `@rivetkit/effect` imports may appear in this module.
 */

const title = (name: string) => ({ title: `livestore-sync-rivet-actors:${name}` }) as const

// -----------------------------------------------------------------------------
// Metadata
// -----------------------------------------------------------------------------

/** Per-event sync metadata assigned by the backend. */
export const SyncMetadata = Schema.TaggedStruct('SyncMessage.SyncMetadata', {
  /** ISO date string */
  createdAt: Schema.String,
}).annotate(title('SyncMetadata'))

export type SyncMetadata = typeof SyncMetadata.Type
export type SyncMetadataEncoded = typeof SyncMetadata.Encoded

// -----------------------------------------------------------------------------
// Shared context fields
// -----------------------------------------------------------------------------

/**
 * Fields carried by every request. Rivet action handlers get no connection
 * context, so the calling identity travels in the payload itself.
 *
 * `payload` uses `optionalKey` (not `optional`): an absent payload must be an
 * absent key, never an explicit `undefined` (which JSON/CBOR cannot carry).
 */
export const SyncContextFields = {
  storeId: Schema.String,
  clientId: Schema.String,
  payload: Schema.optionalKey(Schema.Json),
}

/** Params passed to `handle.connect(params)`; validated once per connection. */
export const ConnParams = Schema.Struct(SyncContextFields).annotate(title('ConnParams'))

export type ConnParams = typeof ConnParams.Type
export type ConnParamsEncoded = typeof ConnParams.Encoded

// -----------------------------------------------------------------------------
// Pull
// -----------------------------------------------------------------------------

/**
 * `backendId` is an `Option` so a client holding a cursor but no persisted
 * backend id can still pull: `Some` mismatch → `BackendIdMismatchError`,
 * `None` → accepted.
 */
export const PullCursor = Schema.Struct({
  eventSequenceNumber: EventSequenceNumber.Global.Schema,
  backendId: Schema.Option(BackendId),
}).annotate(title('PullCursor'))

export type PullCursor = typeof PullCursor.Type

export const PullRequest = Schema.Struct({
  ...SyncContextFields,
  /** `None` starts from the beginning of the eventlog. */
  cursor: Schema.Option(PullCursor),
  /** Server clamps to `MAX_PULL_EVENTS_PER_MESSAGE`. */
  limit: Schema.optional(Schema.Int),
}).annotate(title('PullRequest'))

export type PullRequest = typeof PullRequest.Type
export type PullRequestEncoded = typeof PullRequest.Encoded

export const PullResponseBatchItem = Schema.Struct({
  eventEncoded: LiveStoreEvent.Global.Encoded,
  metadata: Schema.Option(SyncMetadata),
}).annotate(title('PullResponseBatchItem'))

export type PullResponseBatchItem = typeof PullResponseBatchItem.Type

export const PullResponse = Schema.Struct({
  batch: Schema.Array(PullResponseBatchItem),
  pageInfo: SyncBackend.PullResPageInfo,
  backendId: BackendId,
}).annotate(title('PullResponse'))

export type PullResponse = typeof PullResponse.Type
export type PullResponseEncoded = typeof PullResponse.Encoded

/** Exactly one `NoMore` page, as an empty history must still yield one item. */
export const emptyPullResponse = (backendId: string): PullResponse =>
  PullResponse.make({
    batch: [],
    pageInfo: SyncBackend.pageInfoNoMore,
    backendId,
  })

/** Strips the transport-only `backendId`, yielding LiveStore's `PullResItem`. */
export const pullResponseToItem = (res: PullResponse): SyncBackend.PullResItem<SyncMetadata> => ({
  batch: res.batch,
  pageInfo: res.pageInfo,
})

// -----------------------------------------------------------------------------
// Push
// -----------------------------------------------------------------------------

export const PushRequest = Schema.Struct({
  ...SyncContextFields,
  batch: Schema.Array(LiveStoreEvent.Global.Encoded),
  backendId: Schema.Option(BackendId),
}).annotate(title('PushRequest'))

export type PushRequest = typeof PushRequest.Type
export type PushRequestEncoded = typeof PushRequest.Encoded

export const PushAck = Schema.Struct({
  backendId: BackendId,
}).annotate(title('PushAck'))

export type PushAck = typeof PushAck.Type
export type PushAckEncoded = typeof PushAck.Encoded

// -----------------------------------------------------------------------------
// Ping
// -----------------------------------------------------------------------------

export const PingRequest = Schema.Struct(SyncContextFields).annotate(title('PingRequest'))

export type PingRequest = typeof PingRequest.Type
export type PingRequestEncoded = typeof PingRequest.Encoded

export const Pong = Schema.TaggedStruct('SyncMessage.Pong', {}).annotate(title('Pong'))

export type Pong = typeof Pong.Type
export type PongEncoded = typeof Pong.Encoded

// -----------------------------------------------------------------------------
// Admin
// -----------------------------------------------------------------------------

/**
 * Admin requests carry the usual sync context (so `validatePayload` still
 * applies) plus the shared `adminSecret`, mirroring sync-cf's
 * `AdminInfoRequest` / `AdminResetRoomRequest`.
 */
export const AdminInfoRequest = Schema.Struct({
  ...SyncContextFields,
  adminSecret: Schema.String,
}).annotate(title('AdminInfoRequest'))

export type AdminInfoRequest = typeof AdminInfoRequest.Type
export type AdminInfoRequestEncoded = typeof AdminInfoRequest.Encoded

export const AdminInfoResponse = Schema.Struct({
  storeId: Schema.String,
  backendId: BackendId,
  currentHead: EventSequenceNumber.Global.Schema,
  /** Rows in the eventlog. */
  eventCount: Schema.Int,
  /**
   * Connections the actor currently sees (`c.conns`). Includes the transient
   * connection rivetkit opens for the calling (stateless HTTP) action itself.
   */
  connectionCount: Schema.Int,
  /** `PERSISTENCE_FORMAT_VERSION` of the actor's SQLite layout. */
  persistenceFormatVersion: Schema.Int,
}).annotate(title('AdminInfoResponse'))

export type AdminInfoResponse = typeof AdminInfoResponse.Type
export type AdminInfoResponseEncoded = typeof AdminInfoResponse.Encoded

export const AdminResetRequest = Schema.Struct({
  ...SyncContextFields,
  adminSecret: Schema.String,
}).annotate(title('AdminResetRequest'))

export type AdminResetRequest = typeof AdminResetRequest.Type
export type AdminResetRequestEncoded = typeof AdminResetRequest.Encoded

export const AdminResetResponse = Schema.Struct({
  /** The freshly minted backend id; every client holding the old one gets `BackendIdMismatchError`. */
  backendId: BackendId,
}).annotate(title('AdminResetResponse'))

export type AdminResetResponse = typeof AdminResetResponse.Type
export type AdminResetResponseEncoded = typeof AdminResetResponse.Encoded

// -----------------------------------------------------------------------------
// Test-only
// -----------------------------------------------------------------------------

export const TestDisconnectAllRequest = Schema.Struct(SyncContextFields).annotate(title('TestDisconnectAllRequest'))

export type TestDisconnectAllRequest = typeof TestDisconnectAllRequest.Type

export const TestDisconnectAllResponse = Schema.Struct({
  disconnected: Schema.Int,
}).annotate(title('TestDisconnectAllResponse'))

export type TestDisconnectAllResponse = typeof TestDisconnectAllResponse.Type

export const TestInfoRequest = Schema.Struct(SyncContextFields).annotate(title('TestInfoRequest'))

export type TestInfoRequest = typeof TestInfoRequest.Type

/** One entry per connection the actor currently sees (`c.conns`). */
export const TestInfoConn = Schema.Struct({
  id: Schema.String,
  /** `clientId` decoded from `conn.params`, or `null` when the params are not valid `ConnParams`. */
  clientId: Schema.NullOr(Schema.String),
  /** rivetkit's `conn.isHibernatable`, or `null` when the runtime does not expose it. */
  hibernatable: Schema.NullOr(Schema.Boolean),
}).annotate(title('TestInfoConn'))

export type TestInfoConn = typeof TestInfoConn.Type

/**
 * Snapshot of the actor's per-wake state. `wakeCount` counts wakes of this
 * store's actor within the current runner process (the counter lives in
 * module scope, not in SQLite), so it observes sleep/wake cycles.
 */
export const TestInfoResponse = Schema.Struct({
  wakeCount: Schema.Int,
  /** Epoch ms when the current wake started. */
  wokeAt: Schema.Number,
  /** Epoch ms when the previous wake ended (sleep/destroy); `null` on the first wake. */
  previousSleptAt: Schema.NullOr(Schema.Number),
  head: EventSequenceNumber.Global.Schema,
  backendId: BackendId,
  conns: Schema.Array(TestInfoConn),
}).annotate(title('TestInfoResponse'))

export type TestInfoResponse = typeof TestInfoResponse.Type

export const TestSleepRequest = Schema.Struct(SyncContextFields).annotate(title('TestSleepRequest'))

export type TestSleepRequest = typeof TestSleepRequest.Type

/** Acknowledged before the actor actually stops; `conns` is the connection count at that moment. */
export const TestSleepResponse = Schema.Struct({
  conns: Schema.Int,
}).annotate(title('TestSleepResponse'))

export type TestSleepResponse = typeof TestSleepResponse.Type

// -----------------------------------------------------------------------------
// Error unions (declared per action, so the SDK error envelope can carry them)
// -----------------------------------------------------------------------------

export const PullError = Schema.Union([UnknownError, BackendIdMismatchError, InvalidPayloadError]).annotate(
  title('PullError'),
)

export type PullError = typeof PullError.Type

export const PushError = Schema.Union([
  UnknownError,
  ServerAheadError,
  BackendIdMismatchError,
  InvalidPayloadError,
]).annotate(title('PushError'))

export type PushError = typeof PushError.Type

export const PingError = Schema.Union([UnknownError, InvalidPayloadError]).annotate(title('PingError'))

export type PingError = typeof PingError.Type

export const AdminError = Schema.Union([UnknownError, InvalidPayloadError, AdminUnauthorizedError]).annotate(
  title('AdminError'),
)

export type AdminError = typeof AdminError.Type

// -----------------------------------------------------------------------------
// JSON codecs
// -----------------------------------------------------------------------------

/**
 * Derives the canonical JSON codec for a schema plus the four accessors both
 * sides need: sync (throwing) and `Effect` variants for each direction.
 *
 * Decoding always goes through `decodeUnknown*` because wire values arrive as
 * `unknown` (CBOR-decoded) rather than a statically known `Json`.
 */
const jsonCodec = <S extends Schema.Top & { readonly DecodingServices: never; readonly EncodingServices: never }>(
  schema: S,
) => {
  const codec = Schema.toCodecJson(schema)
  return {
    codec,
    encode: Schema.encodeSync(codec),
    decode: Schema.decodeUnknownSync(codec),
    encodeEffect: Schema.encodeEffect(codec),
    decodeEffect: Schema.decodeUnknownEffect(codec),
  }
}

const ConnParamsCodec = jsonCodec(ConnParams)
const PullRequestCodec = jsonCodec(PullRequest)
const PullResponseCodec = jsonCodec(PullResponse)
const PushRequestCodec = jsonCodec(PushRequest)
const PushAckCodec = jsonCodec(PushAck)
const PingRequestCodec = jsonCodec(PingRequest)
const PongCodec = jsonCodec(Pong)
const TestDisconnectAllRequestCodec = jsonCodec(TestDisconnectAllRequest)
const TestDisconnectAllResponseCodec = jsonCodec(TestDisconnectAllResponse)
const TestInfoRequestCodec = jsonCodec(TestInfoRequest)
const TestInfoResponseCodec = jsonCodec(TestInfoResponse)
const TestSleepRequestCodec = jsonCodec(TestSleepRequest)
const TestSleepResponseCodec = jsonCodec(TestSleepResponse)
const AdminInfoRequestCodec = jsonCodec(AdminInfoRequest)
const AdminInfoResponseCodec = jsonCodec(AdminInfoResponse)
const AdminResetRequestCodec = jsonCodec(AdminResetRequest)
const AdminResetResponseCodec = jsonCodec(AdminResetResponse)
const AdminErrorCodec = jsonCodec(AdminError)
const PullErrorCodec = jsonCodec(PullError)
const PushErrorCodec = jsonCodec(PushError)
const PingErrorCodec = jsonCodec(PingError)

export const ConnParamsJson = ConnParamsCodec.codec
export const encodeConnParams = ConnParamsCodec.encode
export const decodeConnParams = ConnParamsCodec.decode
export const encodeConnParamsEffect = ConnParamsCodec.encodeEffect
export const decodeConnParamsEffect = ConnParamsCodec.decodeEffect

export const PullRequestJson = PullRequestCodec.codec
export const encodePullRequest = PullRequestCodec.encode
export const decodePullRequest = PullRequestCodec.decode
export const encodePullRequestEffect = PullRequestCodec.encodeEffect
export const decodePullRequestEffect = PullRequestCodec.decodeEffect

export const PullResponseJson = PullResponseCodec.codec
export const encodePullResponse = PullResponseCodec.encode
export const decodePullResponse = PullResponseCodec.decode
export const encodePullResponseEffect = PullResponseCodec.encodeEffect
export const decodePullResponseEffect = PullResponseCodec.decodeEffect

export const PushRequestJson = PushRequestCodec.codec
export const encodePushRequest = PushRequestCodec.encode
export const decodePushRequest = PushRequestCodec.decode
export const encodePushRequestEffect = PushRequestCodec.encodeEffect
export const decodePushRequestEffect = PushRequestCodec.decodeEffect

export const PushAckJson = PushAckCodec.codec
export const encodePushAck = PushAckCodec.encode
export const decodePushAck = PushAckCodec.decode
export const encodePushAckEffect = PushAckCodec.encodeEffect
export const decodePushAckEffect = PushAckCodec.decodeEffect

export const PingRequestJson = PingRequestCodec.codec
export const encodePingRequest = PingRequestCodec.encode
export const decodePingRequest = PingRequestCodec.decode
export const encodePingRequestEffect = PingRequestCodec.encodeEffect
export const decodePingRequestEffect = PingRequestCodec.decodeEffect

export const PongJson = PongCodec.codec
export const encodePong = PongCodec.encode
export const decodePong = PongCodec.decode
export const encodePongEffect = PongCodec.encodeEffect
export const decodePongEffect = PongCodec.decodeEffect

export const TestDisconnectAllRequestJson = TestDisconnectAllRequestCodec.codec
export const encodeTestDisconnectAllRequest = TestDisconnectAllRequestCodec.encode
export const decodeTestDisconnectAllRequest = TestDisconnectAllRequestCodec.decode

export const TestDisconnectAllResponseJson = TestDisconnectAllResponseCodec.codec
export const encodeTestDisconnectAllResponse = TestDisconnectAllResponseCodec.encode
export const decodeTestDisconnectAllResponse = TestDisconnectAllResponseCodec.decode

export const TestInfoRequestJson = TestInfoRequestCodec.codec
export const encodeTestInfoRequest = TestInfoRequestCodec.encode
export const decodeTestInfoRequest = TestInfoRequestCodec.decode

export const TestInfoResponseJson = TestInfoResponseCodec.codec
export const encodeTestInfoResponse = TestInfoResponseCodec.encode
export const decodeTestInfoResponse = TestInfoResponseCodec.decode

export const TestSleepRequestJson = TestSleepRequestCodec.codec
export const encodeTestSleepRequest = TestSleepRequestCodec.encode
export const decodeTestSleepRequest = TestSleepRequestCodec.decode

export const TestSleepResponseJson = TestSleepResponseCodec.codec
export const encodeTestSleepResponse = TestSleepResponseCodec.encode
export const decodeTestSleepResponse = TestSleepResponseCodec.decode

export const AdminInfoRequestJson = AdminInfoRequestCodec.codec
export const encodeAdminInfoRequest = AdminInfoRequestCodec.encode
export const decodeAdminInfoRequest = AdminInfoRequestCodec.decode
export const encodeAdminInfoRequestEffect = AdminInfoRequestCodec.encodeEffect
export const decodeAdminInfoRequestEffect = AdminInfoRequestCodec.decodeEffect

export const AdminInfoResponseJson = AdminInfoResponseCodec.codec
export const encodeAdminInfoResponse = AdminInfoResponseCodec.encode
export const decodeAdminInfoResponse = AdminInfoResponseCodec.decode
export const encodeAdminInfoResponseEffect = AdminInfoResponseCodec.encodeEffect
export const decodeAdminInfoResponseEffect = AdminInfoResponseCodec.decodeEffect

export const AdminResetRequestJson = AdminResetRequestCodec.codec
export const encodeAdminResetRequest = AdminResetRequestCodec.encode
export const decodeAdminResetRequest = AdminResetRequestCodec.decode
export const encodeAdminResetRequestEffect = AdminResetRequestCodec.encodeEffect
export const decodeAdminResetRequestEffect = AdminResetRequestCodec.decodeEffect

export const AdminResetResponseJson = AdminResetResponseCodec.codec
export const encodeAdminResetResponse = AdminResetResponseCodec.encode
export const decodeAdminResetResponse = AdminResetResponseCodec.decode
export const encodeAdminResetResponseEffect = AdminResetResponseCodec.encodeEffect
export const decodeAdminResetResponseEffect = AdminResetResponseCodec.decodeEffect

export const AdminErrorJson = AdminErrorCodec.codec
export const encodeAdminError = AdminErrorCodec.encode
export const decodeAdminError = AdminErrorCodec.decode

export const PullErrorJson = PullErrorCodec.codec
export const encodePullError = PullErrorCodec.encode
export const decodePullError = PullErrorCodec.decode

export const PushErrorJson = PushErrorCodec.codec
export const encodePushError = PushErrorCodec.encode
export const decodePushError = PushErrorCodec.decode

export const PingErrorJson = PingErrorCodec.codec
export const encodePingError = PingErrorCodec.encode
export const decodePingError = PingErrorCodec.decode
