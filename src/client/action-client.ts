/**
 * Typed wrapper around `Connection.action`: encodes the request with the
 * shared JSON codec, classifies rivetkit failures into LiveStore errors and
 * decodes the success value.
 *
 * A success value that does not match the declared schema is a server
 * contract violation, so it dies rather than surfacing as a sync error.
 */

import type { BackendIdMismatchError, IsOfflineError, ServerAheadError } from '@livestore/common'
import { UnknownError } from '@livestore/common'
import { Effect, Schema } from '@livestore/utils/effect'

import {
  ACTION_PING,
  ACTION_PULL,
  ACTION_PUSH,
  ACTION_TEST_DISCONNECT_ALL,
  decodePongEffect,
  decodePullResponseEffect,
  decodePushAckEffect,
  encodePingRequestEffect,
  encodePullRequestEffect,
  encodePushRequestEffect,
  type PingRequest,
  PingErrorJson,
  type Pong,
  type PullRequest,
  PullErrorJson,
  type PullResponse,
  type PushAck,
  type PushRequest,
  PushErrorJson,
  type TestDisconnectAllRequest,
  TestDisconnectAllRequestJson,
  type TestDisconnectAllResponse,
  TestDisconnectAllResponseJson,
} from '../common/mod.ts'
import { classifyActionFailure } from './errors.ts'
import type { Connection, RawActionFailure } from './types.ts'

const decodePullError = Schema.decodeUnknownEffect(PullErrorJson)
const decodePushError = Schema.decodeUnknownEffect(PushErrorJson)
const decodePingError = Schema.decodeUnknownEffect(PingErrorJson)

const encodeTestDisconnectAllRequestEffect = Schema.encodeEffect(TestDisconnectAllRequestJson)
const decodeTestDisconnectAllResponseEffect = Schema.decodeUnknownEffect(TestDisconnectAllResponseJson)

/** Every action payload carries the calling identity (Rivet actions have no connection context). */
interface SyncContext {
  readonly storeId: string
  readonly clientId: string
}

/**
 * One request/response round trip:
 * encode → send → classify failure → decode success.
 */
const makeAction =
  <Req extends SyncContext, Res, EOut>(options: {
    readonly conn: Connection
    readonly name: string
    readonly span: string
    readonly encodeRequest: (req: Req) => Effect.Effect<unknown, unknown>
    readonly decodeSuccess: (raw: unknown) => Effect.Effect<Res, unknown>
    readonly classifyFailure: (failure: RawActionFailure | IsOfflineError) => Effect.Effect<never, EOut>
  }) =>
  (req: Req): Effect.Effect<Res, EOut | UnknownError> =>
    options.encodeRequest(req).pipe(
      Effect.mapError((cause) => new UnknownError({ cause, note: `failed to encode ${options.name} request` })),
      Effect.flatMap((encoded) =>
        options.conn.action(options.name, encoded).pipe(Effect.catch(options.classifyFailure)),
      ),
      Effect.flatMap((raw) => Effect.orDie(options.decodeSuccess(raw))),
      Effect.withSpan(`rivet-sync:${options.span}`, {
        attributes: { storeId: req.storeId, clientId: req.clientId },
      }),
    )

export interface ActionClient {
  readonly pull: (req: PullRequest) => Effect.Effect<PullResponse, IsOfflineError | UnknownError | BackendIdMismatchError>
  readonly push: (
    req: PushRequest,
  ) => Effect.Effect<PushAck, IsOfflineError | UnknownError | ServerAheadError | BackendIdMismatchError>
  readonly ping: (req: PingRequest) => Effect.Effect<Pong, IsOfflineError | UnknownError>
  readonly testDisconnectAll: (
    req: TestDisconnectAllRequest,
  ) => Effect.Effect<TestDisconnectAllResponse, IsOfflineError | UnknownError>
}

export const makeActionClient = (conn: Connection): ActionClient => ({
  pull: makeAction({
    conn,
    name: ACTION_PULL,
    span: 'pull',
    encodeRequest: encodePullRequestEffect,
    decodeSuccess: decodePullResponseEffect,
    classifyFailure: classifyActionFailure(decodePullError),
  }),
  push: makeAction({
    conn,
    name: ACTION_PUSH,
    span: 'push',
    encodeRequest: encodePushRequestEffect,
    decodeSuccess: decodePushAckEffect,
    classifyFailure: classifyActionFailure(decodePushError),
  }),
  ping: makeAction({
    conn,
    name: ACTION_PING,
    span: 'ping',
    encodeRequest: encodePingRequestEffect,
    decodeSuccess: decodePongEffect,
    classifyFailure: classifyActionFailure(decodePingError),
  }),
  testDisconnectAll: makeAction({
    conn,
    name: ACTION_TEST_DISCONNECT_ALL,
    span: 'test-disconnect-all',
    encodeRequest: encodeTestDisconnectAllRequestEffect,
    decodeSuccess: decodeTestDisconnectAllResponseEffect,
    // The test-only action declares no errors of its own.
    classifyFailure: classifyActionFailure(decodePingError),
  }),
})
