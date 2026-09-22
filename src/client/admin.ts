/**
 * Promise-based helper for ops scripts and tooling that calls the actor's
 * admin actions (`AdminInfo`, `AdminReset`).
 *
 * It uses stateless rivetkit *handles* (`handle.action(...)`, HTTP), never a
 * WebSocket connection: `AdminReset` disconnects every connection of the
 * store, and an in-flight action on a connection being closed would reject
 * before its response arrived.
 *
 * Failures are thrown as typed instances: `AdminUnauthorizedError` (wrong
 * `adminSecret`), `InvalidPayloadError` (`validatePayload` /
 * `syncPayloadSchema` / `storeId` rejection) or `UnknownError` (admin
 * disabled on the server, transport errors, anything else).
 *
 * Browser-safe: only `rivetkit/client` is imported.
 */

import { UnknownError } from '@livestore/common'
import { Option, type Schema } from '@livestore/utils/effect'
import { createClient as createRivetClient } from 'rivetkit/client'

import {
  ACTION_ADMIN_INFO,
  ACTION_ADMIN_RESET,
  ACTOR_NAME,
  type AdminError,
  type AdminInfoResponse,
  type AdminResetResponse,
  decodeAdminError,
  decodeAdminInfoResponse,
  decodeAdminResetResponse,
  encodeAdminInfoRequest,
  encodeAdminResetRequest,
} from '../common/mod.ts'
import { decodeActionErrorEnvelope, isRivetErrorLike } from './errors.ts'

export interface RivetSyncAdminOptions {
  /** Rivet endpoint, e.g. `https://api.rivet.dev` or `http://127.0.0.1:6420`. */
  readonly endpoint: string
  /** Rivet access token (omit for local engines). */
  readonly token?: string
  /** Rivet namespace (rivetkit defaults to `default`). */
  readonly namespace?: string
  /** Actor name the server registered the sync actor under. Default: `LiveStoreSync`. */
  readonly actorName?: string
  /** `clientId` sent with admin requests (visible to `validatePayload`). Default: `livestore-sync-rivet-admin`. */
  readonly clientId?: string
}

export interface RivetSyncAdmin {
  /** Head, backend id, event count and connection count of `storeId`'s actor. */
  readonly info: (storeId: string, adminSecret: string, payload?: Schema.Json) => Promise<AdminInfoResponse>
  /**
   * Wipes `storeId`'s eventlog, mints a new backend id and disconnects every
   * client. Connected LiveStore clients hit `BackendIdMismatchError` on
   * reconnect and apply `onBackendIdMismatch` (default: reset local state).
   */
  readonly reset: (storeId: string, adminSecret: string, payload?: Schema.Json) => Promise<AdminResetResponse>
  /** Disposes the underlying rivetkit client. */
  readonly dispose: () => Promise<void>
}

export const DEFAULT_ADMIN_CLIENT_ID = 'livestore-sync-rivet-admin'

/** Structural subset of rivetkit's client this helper uses (test seam). */
export interface AdminRivetClientLike {
  getOrCreate(
    name: string,
    key: string | string[],
  ): { action(opts: { readonly name: string; readonly args: unknown[] }): Promise<unknown> }
  dispose(): Promise<void>
}

/** Test seams. Not part of the public API. */
export interface RivetSyncAdminInternalOptions {
  readonly createClient?: (options: {
    readonly endpoint: string
    readonly token?: string
    readonly namespace?: string
  }) => AdminRivetClientLike
}

const defaultCreateClient: NonNullable<RivetSyncAdminInternalOptions['createClient']> = (options) =>
  createRivetClient({
    endpoint: options.endpoint,
    ...(options.token === undefined ? {} : { token: options.token }),
    ...(options.namespace === undefined ? {} : { namespace: options.namespace }),
  })

/**
 * Turns a rejected `handle.action` into a thrown typed error. A declared
 * action error comes back from `@rivetkit/effect` as an `EffectActionError`
 * envelope in `ActorError.metadata`; anything else is a transport / runtime
 * failure and becomes an `UnknownError`.
 */
export const toAdminError = (cause: unknown, action: string): AdminError => {
  if (isRivetErrorLike(cause)) {
    const envelope = decodeActionErrorEnvelope(cause.metadata)
    if (Option.isSome(envelope)) {
      try {
        return decodeAdminError(envelope.value)
      } catch (decodeCause) {
        return new UnknownError({ cause: decodeCause, note: `undecodable ${action} error` })
      }
    }
    return new UnknownError({ cause, note: `${action} failed`, payload: { group: cause.group, code: cause.code } })
  }
  return new UnknownError({ cause, note: `${action} failed` })
}

/** Like {@link makeRivetSyncAdmin}, with internal seams exposed (this package's tests only). */
export const makeRivetSyncAdminWith = (
  options: RivetSyncAdminOptions,
  internal: RivetSyncAdminInternalOptions = {},
): RivetSyncAdmin => {
  const createClient = internal.createClient ?? defaultCreateClient
  const client = createClient({
    endpoint: options.endpoint,
    ...(options.token === undefined ? {} : { token: options.token }),
    ...(options.namespace === undefined ? {} : { namespace: options.namespace }),
  })
  const actorName = options.actorName ?? ACTOR_NAME
  const clientId = options.clientId ?? DEFAULT_ADMIN_CLIENT_ID

  const call = async <Res>(
    action: string,
    storeId: string,
    encode: () => unknown,
    decode: (raw: unknown) => Res,
  ): Promise<Res> => {
    let encoded: unknown
    try {
      encoded = encode()
    } catch (cause) {
      throw new UnknownError({ cause, note: `failed to encode ${action} request` })
    }

    let raw: unknown
    try {
      raw = await client.getOrCreate(actorName, [storeId]).action({ name: action, args: [encoded] })
    } catch (cause) {
      throw toAdminError(cause, action)
    }

    try {
      return decode(raw)
    } catch (cause) {
      throw new UnknownError({ cause, note: `undecodable ${action} response` })
    }
  }

  // `payload` is an `optionalKey`: absent must mean an absent key.
  const context = (storeId: string, payload: Schema.Json | undefined) => ({
    storeId,
    clientId,
    ...(payload === undefined ? {} : { payload }),
  })

  return {
    info: (storeId, adminSecret, payload) =>
      call(
        ACTION_ADMIN_INFO,
        storeId,
        () => encodeAdminInfoRequest({ ...context(storeId, payload), adminSecret }),
        decodeAdminInfoResponse,
      ),
    reset: (storeId, adminSecret, payload) =>
      call(
        ACTION_ADMIN_RESET,
        storeId,
        () => encodeAdminResetRequest({ ...context(storeId, payload), adminSecret }),
        decodeAdminResetResponse,
      ),
    dispose: () => client.dispose(),
  }
}

/**
 * Creates an admin client for the `LiveStoreSync` actor. The server must be
 * started with `admin: { secret }`.
 *
 * @example
 * ```ts
 * import { makeRivetSyncAdmin } from 'livestore-sync-rivet-actors/client'
 *
 * const admin = makeRivetSyncAdmin({ endpoint: 'http://127.0.0.1:6420' })
 * try {
 *   console.log(await admin.info('my-store', process.env.SYNC_ADMIN_SECRET!))
 *   const { backendId } = await admin.reset('my-store', process.env.SYNC_ADMIN_SECRET!)
 * } finally {
 *   await admin.dispose()
 * }
 * ```
 */
export const makeRivetSyncAdmin = (options: RivetSyncAdminOptions): RivetSyncAdmin => makeRivetSyncAdminWith(options)
