# livestore-sync-rivet-actors

A [LiveStore](https://livestore.dev) **custom sync provider** backed by [Rivet Actors](https://rivet.dev).

- **Server**: a Rivet actor (`LiveStoreSync`, one instance per `storeId`) built with the Rivet Effect SDK
  (`@rivetkit/effect`), storing the eventlog in the actor's own SQLite database.
- **Client**: an implementation of LiveStore's `SyncBackend` interface speaking to that actor over a
  rivetkit WebSocket connection — catch-up pulls as actions, live pulls as connection events.
- Written in **Effect 4** throughout, against **LiveStore 0.5.0-dev**.

It is the Rivet analogue of `@livestore/sync-cf` (Cloudflare Durable Objects), and mirrors its options,
hooks and error semantics closely enough that swapping providers is a one-line change in your adapter.

---

## Status & compatibility

> **Pre-release.** Everything below is pinned to pre-release versions of both LiveStore and Effect.
> Expect breakage on every upstream bump.

| Dependency | Version | Note |
|---|---|---|
| `@livestore/common`, `@livestore/utils` | `0.5.0-dev.0` | npm dist-tag `dev` |
| `effect` and every `@effect/*` package | `4.0.0-rc.112` | **not** rc.113+ — see below |
| `rivetkit` | `2.3.17` | |
| `@rivetkit/effect` | `2.3.17` | **requires a pnpm patch**, see below |

**Why `effect@4.0.0-rc.112` exactly.** rc.113 removed `effect/testing/FastCheck`, which
`@livestore/utils@0.5.0-dev.0` still imports — installing rc.113 or later breaks LiveStore itself.
Pin `effect` *and* all `@effect/*` packages to `4.0.0-rc.112` until LiveStore moves.

### The `@rivetkit/effect` patch

`@rivetkit/effect@2.3.17` targets `effect@4.0.0-beta.66` and uses `Schema.TaggedErrorClass`, which the
Effect 4 release candidates removed (it is now `Schema.TaggedError`, with an identical call shape).
Without a patch, importing `livestore-sync-rivet-actors/server` produces hundreds of type errors.

This repo ships the patch at [`patches/@rivetkit__effect@2.3.17.patch`](./patches). It is small:

1. `Schema.TaggedErrorClass<X>(id)(…)` → `Schema.TaggedError<X>(id)(…)` (28 sites).
2. One `exactOptionalPropertyTypes` fix in `src/internal/ActionDispatcher.ts`
   (`code: cond ? tag : undefined` → `...(cond ? { code: tag } : {})`).
3. Two `readonly state?: X` → `readonly state?: X | undefined` fixes in `src/internal/*`.
4. `src/Actor.ts` (and `dist/Actor.{js,d.ts}`): `rivetkitActorOptionsKeys` is extended from
   `['name', 'icon']` to **every key of rivetkit's `ActorOptionsInput`**, so runtime tunables such as
   `sleepTimeout`, `sleepGracePeriod` and `actionTimeout` passed to `Actor.toLayer(…, options)` reach
   `Rivetkit.actor`. Upstream silently drops them; this is what makes the `actor` option below work.

**Consumers of the `./server` entry need the same patch** until upstream targets an Effect 4 rc.
Copy the patch file into your own repo and register it:

```yaml
# pnpm-workspace.yaml (pnpm 9+ / 10)
patchedDependencies:
  '@rivetkit/effect@2.3.17': patches/@rivetkit__effect@2.3.17.patch
```

```jsonc
// package.json — older pnpm, which reads patchedDependencies from here
{
  "pnpm": {
    "patchedDependencies": {
      "@rivetkit/effect@2.3.17": "patches/@rivetkit__effect@2.3.17.patch"
    }
  }
}
```

Then `pnpm install`. (With npm or yarn, apply the same diff with `patch-package` / `yarn patch`.)

The **client** entry (`livestore-sync-rivet-actors/client`) does not import `@rivetkit/effect` and
needs no patch.

---

## Install

```sh
pnpm add livestore-sync-rivet-actors
```

Peer dependencies of this package:

```sh
pnpm add effect@4.0.0-rc.112 rivetkit@2.3.17 \
  @livestore/common@0.5.0-dev.0 @livestore/utils@0.5.0-dev.0
# only for the ./server entry (patched, see above)
pnpm add -D @rivetkit/effect@2.3.17
```

`@rivetkit/effect` is an **optional** peer: applications that only use `./client` can skip it.

`@livestore/common` / `@livestore/utils` additionally declare these peers, so install them too
(they are what LiveStore itself requires, not this package):

```sh
pnpm add @effect/opentelemetry@4.0.0-rc.112 @effect/platform-browser@4.0.0-rc.112 \
  @effect/platform-bun@4.0.0-rc.112 @effect/platform-node@4.0.0-rc.112 \
  @effect/platform-node-shared@4.0.0-rc.112 @effect/vitest@4.0.0-rc.112 \
  @opentelemetry/api@^1.9.0 @opentelemetry/resources@^2.2.0 @standard-schema/spec@^1.1.0
```

### Entry points

| Import | Runs where | Pulls in |
|---|---|---|
| `livestore-sync-rivet-actors/client` | browser, worker, node | `rivetkit/client` only |
| `livestore-sync-rivet-actors/server` | node (Rivet actor host) | `@rivetkit/effect`, `rivetkit` |
| `livestore-sync-rivet-actors/common` | anywhere | nothing beyond Effect + LiveStore schemas |

---

## Server setup

Following the [Rivet Effect quickstart](https://rivet.dev/actors/docs/quickstart/effect/): build the
actor layer, serve it from a registry, and launch.

```ts
// server.ts
import { NodeRuntime } from '@effect/platform-node'
import { Registry } from '@rivetkit/effect'
import { Layer } from 'effect'
import { makeLiveStoreSyncActor, registryOptions } from 'livestore-sync-rivet-actors/server'

const ActorsLayer = makeLiveStoreSyncActor({
  validatePayload: (payload, { storeId, clientId }) => {
    if ((payload as { authToken?: string } | undefined)?.authToken !== process.env.SYNC_AUTH_TOKEN) {
      throw new Error(`unauthorized client '${clientId}' for store '${storeId}'`)
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
        // defaults to 4 MiB (RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE); rivetkit's own default is 64 KB
        maxIncomingMessageSize: 4 * 1024 * 1024,
      }),
    ),
  ),
)

Layer.launch(MainLayer).pipe(NodeRuntime.runMain)
```

Run it locally against an engine rivetkit spawns for you:

```sh
RIVET_RUN_ENGINE=1 tsx server.ts
```

`RIVET_RUN_ENGINE=1` starts a local Rivet engine at `http://127.0.0.1:6420` from the bundled
`@rivetkit/engine-cli` binary (nothing is downloaded at runtime). Note that the engine is spawned
**detached**: it outlives your server process and keeps its state under `~/.rivetkit/var/engine/db`.

| Env var | Meaning |
|---|---|
| `RIVET_RUN_ENGINE=1` | spawn a local engine when no endpoint is configured (dev/test only) |
| `RIVET_ENDPOINT` | engine endpoint, e.g. `http://127.0.0.1:6420` or your Rivet Cloud URL |
| `RIVET_TOKEN` | Rivet access token (hosted engines) |
| `RIVET_NAMESPACE` | Rivet namespace (rivetkit defaults to `default`) |

`registryOptions(...)` only builds the object; nothing forces you to read these from the environment.

**Deploying.** The actor host is an ordinary long-running Node process — deploy it anywhere you can run
one, pointed at a Rivet engine. For a managed engine use Rivet Cloud; for your own, self-host the
engine and set `RIVET_ENDPOINT`/`RIVET_TOKEN` accordingly. See the
[Rivet documentation](https://rivet.dev/docs) for both paths.

A complete, runnable version of the above is in
[`examples/node-server`](./examples/node-server) (`pnpm example:server`).

### Server options

`makeLiveStoreSyncActor(options: LiveStoreSyncActorOptions<TSyncPayload>)` — every field is optional.

| Option | Type | Default | Notes |
|---|---|---|---|
| `syncPayloadSchema` | `Schema.Decoder<TSyncPayload>` | — | Decodes the client's `syncPayload` before `validatePayload` sees it. Runs even when the payload is absent, so a schema can make it required. A decode failure is an `InvalidPayloadError`. |
| `validatePayload` | `(payload, { storeId, clientId }) => void \| Promise<void> \| Effect<void>` | — accept everything | See below. |
| `onPush` | `(message: PushRequest, ctx) => …` | — | Runs after validation, before admission. |
| `onPushRes` | `(message: PushAck \| UnknownError) => …` | — | |
| `onPull` | `(message: PullRequest, ctx) => …` | — | |
| `onPullRes` | `(message: PullResponse \| UnknownError) => …` | — | Also runs per chunk of a live fan-out. |
| `pullPageSize` | `number` | `100` | Events per catch-up page. Clamped to `[1, 100]` (`MAX_PULL_EVENTS_PER_MESSAGE`). Also clamps a client-supplied `limit`. |
| `maxPushEventsPerRequest` | `number` | `100` | Hard cap per `Push`. Clamped to `[1, 100]`. A larger batch fails with `UnknownError`. |
| `maxMessageBytes` | `number` | `900_000` | Byte budget for one pull page / live event (actor → client). Clamped to `>= 1024`. |
| `name` | `string` | `'LiveStore Sync'` | Display name forwarded to `Rivetkit.actor`. |
| `icon` | `string` | `'database'` | Display icon forwarded to `Rivetkit.actor`. |
| `actor` | `LiveStoreSyncRivetActorOptions` | rivetkit defaults | rivetkit actor runtime options forwarded verbatim to `Rivetkit.actor` (`sleepTimeout`, `sleepGracePeriod`, `actionTimeout`, `noSleep`, `connectionLiveness*`, `canHibernateWebSocket`, … — everything in rivetkit's `ActorOptionsInput` except `name`/`icon`). See [Sleep & hibernation](#sleep--hibernation). |
| `admin` | `{ secret: string }` | disabled | Enables the `AdminInfo` and `AdminReset` actions (see [Admin: inspecting & resetting a store](#admin-inspecting--resetting-a-store)). Requests must carry a matching `adminSecret` (constant-time compare) **and** pass `validatePayload`. An empty `secret` counts as disabled. |
| `testing` | `{ enabled: boolean }` | disabled | Enables the `TestDisconnectAll`, `TestInfo` and `TestSleep` actions. **Test-only — never enable in production**: any caller can drop every connection, read the actor's wake/connection state, or put the actor to sleep. |

`ctx` for the hooks is `{ storeId, clientId, payload? }`, with `payload` already decoded.
Hooks may be sync, return a promise, or be an Effect; a throw/rejection/failure becomes an
`UnknownError` for that request.

#### `validatePayload` semantics

- It runs **on every action** (`Pull`, `Push`, `Ping`, `AdminInfo`, `AdminReset`, the test-only
  actions), with the payload the action carried.
- It also runs **once per connection**, lazily: the first time the actor is about to send a live-pull
  event to a connection, that connection's `conn.params` are decoded as `ConnParams` and validated.
  The verdict is cached per connection id for the wake, so each connection is checked once.
- A connection that fails validation is **disconnected** (`conn.disconnect('unauthorized')`) and never
  receives events. rivetkit clients reconnect automatically, so a misconfigured client will be kicked
  again on each new connection rather than being permanently banned.
- A request that fails validation is rejected with `InvalidPayloadError`. The same error covers a
  `storeId` that does not match the actor key and a payload that fails `syncPayloadSchema`.
- `InvalidPayloadError` is distinguishable on the wire but is mapped to `UnknownError` on the client
  (LiveStore's `SyncBackend` has no auth-specific error channel).

#### Registry options

`registryOptions(input)` produces the object for `Registry.layer(...)`:

| Field | Default | Notes |
|---|---|---|
| `endpoint` | rivetkit's default | omitted entirely when `undefined` |
| `token`, `namespace`, `noWelcome` | — | omitted when `undefined` |
| `maxIncomingMessageSize` | `RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE` (4 MiB) | rivetkit's own default is 64 KiB |
| `maxOutgoingMessageSize` | rivetkit's default (1 MiB) | raise together with `maxMessageBytes` |

`Registry.Options` only *types* `endpoint | token | namespace | noWelcome | sqlite`, but
`@rivetkit/effect` spreads the whole object into `Rivetkit.setup(...)`, so the size knobs pass through
at runtime. `registryOptions` encapsulates the necessary cast.

---

## Client setup

`makeRivetSync(options)` returns the `SyncBackendConstructor` LiveStore adapters accept as
`sync.backend`.

```ts
import { makeAdapter } from '@livestore/adapter-node' // or adapter-web, …
import { createStorePromise } from '@livestore/livestore'
import { makeRivetSync } from 'livestore-sync-rivet-actors/client'

const adapter = makeAdapter({
  storage: { type: 'in-memory' },
  clientId: 'client-a',
  sync: {
    backend: makeRivetSync({ endpoint: process.env.RIVET_ENDPOINT ?? 'http://127.0.0.1:6420' }),
    onSyncError: 'shutdown',
  },
})

const store = await createStorePromise({
  schema,
  storeId: 'my-store',
  adapter,
  syncPayloadSchema: SyncPayload,  // optional; mirrors the server's syncPayloadSchema
  syncPayload: { authToken: 'demo' }, // reaches the server's validatePayload
})
```

`syncPayload` travels in two places: as the rivetkit connection params (validated once per connection)
and inside every action payload (validated per request). It must be JSON — never put `undefined` in it,
since an explicit `undefined` survives CBOR as a *present* key while an absent key stays absent.

The connection is created **lazily**: nothing touches the wire until LiveStore calls
`connect`/`pull`/`push`, so `isConnected` is `false` immediately after construction.

### Client options

| Option | Type | Default | Notes |
|---|---|---|---|
| `endpoint` | `string` | **required** | e.g. `http://127.0.0.1:6420`. Supports rivetkit's `https://namespace:token@host` URL auth syntax. |
| `token` | `string` | — | Rivet access token (omit for local engines). |
| `namespace` | `string` | — | rivetkit defaults to `default`. |
| `actorName` | `string` | `'LiveStoreSync'` | Must match the name the server registered the actor under (`ACTOR_NAME`). Change it only if you wrap the actor yourself. |
| `connectTimeout` | `Duration.Input` | `10 seconds` | How long an action waits for the socket to reach `connected` before failing with `IsOfflineError`. |
| `ping.enabled` | `boolean` | `true` | A background fiber pings on an interval; a successful ping re-asserts `isConnected`. The first ping is delayed one interval so it does not force the lazy connection open. |
| `ping.requestTimeout` | `Duration.Input` | `10 seconds` | On timeout: `isConnected` → `false` and the ping fails with `Cause.TimeoutError`. |
| `ping.requestInterval` | `Duration.Input` | `10 seconds` | |
| `pullPageSize` | `number` | `100` | Requested events per catch-up page; the server clamps it to its own `pullPageSize`. |
| `maxPushBytes` | `number` | `60_000` | Byte budget for one encoded `Push` payload. **Sized to fit rivetkit's default 64 KiB `maxIncomingMessageSize`** — an oversize frame makes the *server* close the socket, and a client that blindly retried the same frame would loop forever. Raise it (e.g. `900_000`) only when the server registry was started with a larger `maxIncomingMessageSize`. |
| `reconnect.baseDelay` | `Duration.Input` | `1 second` | Backoff used when rivetkit itself gives up (status `idle`) and the connection has to be recreated. Jittered. |
| `reconnect.maxDelay` | `Duration.Input` | `30 seconds` | |

Pushes are additionally split at `MAX_PUSH_EVENTS_PER_REQUEST` (100) events per chunk and sent strictly
sequentially under a client-side semaphore.

---

## Admin: inspecting & resetting a store

Start the server with an admin secret to enable two extra actions, the Rivet counterparts of
`@livestore/sync-cf`'s `AdminInfoRequest` / `AdminResetRoomRequest`:

```ts
makeLiveStoreSyncActor({ validatePayload, admin: { secret: process.env.SYNC_ADMIN_SECRET! } })
```

| Action | Returns | Does |
|---|---|---|
| `AdminInfo` | `{ storeId, backendId, currentHead, eventCount, connectionCount, persistenceFormatVersion }` | Read-only snapshot. `connectionCount` includes the transient connection rivetkit opens for the calling HTTP action. |
| `AdminReset` | `{ backendId }` (the **new** one) | Wipes the store (see below). |

Both actions declare `AdminError = UnknownError | InvalidPayloadError | AdminUnauthorizedError`:
`UnknownError { note: 'admin actions are disabled (set admin.secret)' }` when the server has no secret,
`InvalidPayloadError` when `validatePayload` / `syncPayloadSchema` / the `storeId` check rejects the
request (these run *before* the secret check), and `AdminUnauthorizedError { storeId }` for a wrong
`adminSecret`.

### Client admin helper

`makeRivetSyncAdmin` (from `livestore-sync-rivet-actors/client`, browser-safe) is a small
promise-based wrapper for scripts and ops tooling. It calls the actions over stateless rivetkit
handles (HTTP), never a WebSocket, and throws the typed errors above as instances:

```ts
import { UnknownError } from '@livestore/common'
import { AdminUnauthorizedError, makeRivetSyncAdmin } from 'livestore-sync-rivet-actors/client'

const admin = makeRivetSyncAdmin({ endpoint: process.env.RIVET_ENDPOINT!, token: process.env.RIVET_TOKEN })
try {
  // The optional third argument is the sync payload your validatePayload expects.
  const info = await admin.info('my-store', process.env.SYNC_ADMIN_SECRET!, { authToken: 'ops' })
  console.log(info.eventCount, info.currentHead, info.backendId)

  const { backendId } = await admin.reset('my-store', process.env.SYNC_ADMIN_SECRET!, { authToken: 'ops' })
} catch (error) {
  if (error instanceof AdminUnauthorizedError) console.error('wrong admin secret')
  else throw error // InvalidPayloadError, UnknownError (admin disabled, transport errors, …)
} finally {
  await admin.dispose()
}
```

| Option | Default | Notes |
|---|---|---|
| `endpoint` | **required** | as for `makeRivetSync` |
| `token`, `namespace` | — | as for `makeRivetSync` |
| `actorName` | `'LiveStoreSync'` | |
| `clientId` | `'livestore-sync-rivet-admin'` | the `clientId` `validatePayload` sees |

Treat the admin secret like any server credential: keep it out of browser bundles, even though the
helper itself is browser-safe.

### Resetting a store

`AdminReset` runs under the push gate (no push can interleave) and uninterruptibly:

1. deletes every eventlog row and the context row in one SQLite transaction,
2. mints a new `backendId`, persists `{ head: 0, backendId }` and updates the in-memory state,
3. disconnects every connection with reason `'store-reset'`.

For LiveStore clients this means their history no longer exists on the server. Each client reconnects
at once, its live pull re-catches-up with a cursor carrying the **old** `backendId`, and the actor
answers `BackendIdMismatchError` (so does any later pull or push that carries the old id). LiveStore's
leader then applies its `onBackendIdMismatch` sync option:

| `onBackendIdMismatch` | Effect |
|---|---|
| `'reset'` (**default**) | clears the client's local eventlog and state databases and shuts the store down (`IntentionalShutdownCause { reason: 'backend-id-mismatch' }`); the app re-creates the store, which then syncs from the (empty) server |
| `'shutdown'` | shuts the store down without clearing local data |
| `'ignore'` | keeps running with stale local data (sync stays broken) |

Local events a client had not pushed yet are **lost** under `'reset'`. Brand-new clients (no cached
`backendId`) simply start a fresh history from the root.

---

## Message size limits

rivetkit enforces two registry-level limits, and they are **asymmetric**:

| Direction | rivetkit default | What it bounds here |
|---|---|---|
| Incoming (client → actor) | **64 KiB** | `Push` payloads, and any action arguments |
| Outgoing (actor → client) | **1 MiB** | pull pages and live-pull events |

An oversize *incoming* message makes the engine close the WebSocket with
`message.incoming_too_long`; the client then reconnects. An oversize *outgoing* message fails the
action with `message.outgoing_too_long` but leaves the connection up.

To move larger batches you must raise **both** sides:

```ts
// server
Registry.layer(registryOptions({ maxIncomingMessageSize: 4 * 1024 * 1024 }))
makeLiveStoreSyncActor({ maxMessageBytes: 900_000 })   // ≤ maxOutgoingMessageSize

// client
makeRivetSync({ endpoint, maxPushBytes: 900_000 })     // ≤ server maxIncomingMessageSize
```

The defaults are deliberately safe in the other direction: with an unconfigured server
(64 KiB incoming), the default `maxPushBytes: 60_000` still works.

---

## Protocol & architecture

```
LiveStore leader (browser worker / node)              Rivet engine
┌───────────────────────────────────┐                ┌────────────────────────────────────────┐
│ makeRivetSync(opts)({storeId,…})   │  rivetkit WS   │ Actor "LiveStoreSync", key [storeId]    │
│  ├ lazy ActorConn, status→isConnected │◄───────────►│  ├ actions: Pull | Push | Ping          │
│  ├ pull: catch-up pages → live events │ actions +   │  ├ SQLite: eventlog_v1, context_v1      │
│  ├ push: chunked, serialized          │ 'pull' evts │  ├ push admission: semaphore + head chk │
│  └ ping / backendId (KeyValueStore)   │             │  └ fan-out 'pull' to authorized conns   │
└───────────────────────────────────┘                └────────────────────────────────────────┘
```

- **Actions.** `Pull`, `Push`, `Ping` (plus the admin actions `AdminInfo`, `AdminReset` and the
  test-only `TestDisconnectAll`, `TestInfo`, `TestSleep`) are rivetkit actions issued
  over a single WebSocket connection (`handle.connect(connParams)`). Every payload, success value and
  declared error is encoded with `Schema.toCodecJson(...)`, which is exactly what `@rivetkit/effect`
  does server-side, so the encoding is unambiguous over rivetkit's CBOR transport.
- **Live pull.** Actions return a single value, so live updates are *connection events*: after a
  successful push the actor calls `conn.send('pull', encodedPullResponse)` on every authorized
  connection — including the pusher's. The client subscribes to that event **before** its first `Pull`
  (unsubscribed events are dropped by rivetkit).
- **One actor per store.** The actor is addressed as `getOrCreate('LiveStoreSync', [storeId])`. A
  request whose `storeId` does not match the actor key is rejected.
- **Storage.** The actor's own SQLite database holds `eventlog_v1` (one row per global event) and
  `context_v1` (current head + `backendId`). An append and the head update happen in one transaction.
  On every wake the head is recomputed as `max(persisted head, MAX(seqNum))`, which closes the
  stale-head gap `@livestore/sync-cf` documents.
- **`backendId`.** A stable id generated on first wake and persisted in `context_v1` (replaced only by
  `AdminReset`). The client caches
  it in LiveStore's `KeyValueStore`, sends it with pulls and pushes, and checks it on live events. A
  mismatch means the server's eventlog was reset underneath the client →
  `BackendIdMismatchError`, which ends the live pull stream (LiveStore then resets the client).
  A cursor whose `backendId` is `None` (client has a cursor but no cached id) is accepted.
- **`ServerAheadError`.** Push admission is serialized: a batch is accepted only if
  `batch[0].parentSeqNum === head`. Losers of a race get
  `ServerAheadError { minimumExpectedNum, providedNum }` — but only *after* the winner's events have
  already been fanned out to them, so the rejected leader can rebase immediately and never deadlocks.
  Batches must also be internally chained.
- **`InvalidPayloadError`** is a distinct error on the wire but becomes
  `UnknownError { note: 'validatePayload rejected' }` at the `SyncBackend` boundary.

---

## Error semantics (client-facing)

`SyncBackend` only speaks four error types. This is how everything maps:

| Situation | Client sees |
|---|---|
| Push whose parent is behind the server's head | `ServerAheadError { minimumExpectedNum, providedNum }` |
| Cursor / push `backendId` mismatches the server's | `BackendIdMismatchError { expected, received }` |
| Live-pull event carrying a foreign `backendId` | `BackendIdMismatchError` (fails the pull stream) |
| Action attempted while the socket is not `connected`, or the socket drops mid-flight (plain `Error("Connection closed …")`) | `IsOfflineError` |
| Waiting for `connected` exceeds `connectTimeout` | `IsOfflineError` |
| Rivet error `actor/{aborted,not_found,stopping,restarting}` or any `guard/*` | `IsOfflineError` |
| `validatePayload` / `syncPayloadSchema` / `storeId` mismatch (`InvalidPayloadError`) | `UnknownError { note: 'validatePayload rejected' }` |
| Error envelope present but undecodable | `UnknownError { note: 'undecodable action error' }` |
| A single event larger than `maxPushBytes` | `UnknownError { note: 'single event exceeds maxPushBytes' }` |
| A single event larger than the server's `maxMessageBytes` | `UnknownError` (note names the sequence number) |
| Batch over `maxPushEventsPerRequest`, or not internally chained | `UnknownError` |
| Any other rivet error (`message/*`, `request/invalid`, `action/not_found`, `action/timed_out`, `rivetkit/internal_error`, …) | `UnknownError { payload: { group, code } }` |
| Ping does not answer within `ping.requestTimeout` | `isConnected` → `false`, fails with `Cause.TimeoutError` |

`IsOfflineError` is the retryable one — LiveStore's leader retries pulls and pushes on it.

---

## Reconnect behaviour

- rivetkit reconnects **forever** on its own (first retry immediate, then 250 ms → 30 s backoff), so a
  server-side `conn.disconnect(...)` is not terminal for the client.
- `isConnected` follows rivetkit's status (`connected` ⇒ `true`), and a successful ping re-asserts it.
  Actions are gated on it: rivetkit *queues* actions issued while offline instead of rejecting them, so
  the client races each action against "status left `connected`" and surfaces `IsOfflineError` rather
  than hanging.
- The live pull stream **survives disconnects**. On every transition back to `connected` the client
  re-runs a catch-up from its `lastSeen` sequence number, so events broadcast while the socket was down
  are recovered.
- Every received item is **deduped** against `lastSeen`; nothing with `seqNum <= lastSeen` is ever
  emitted.
- If a live event's first fresh event does not chain onto `lastSeen` (a missed broadcast), the client
  **repairs the gap** by re-running the catch-up instead of emitting a hole.
- Transient failures (`IsOfflineError`, `UnknownError`) inside the live stream are retried with
  jittered backoff (`reconnect.baseDelay` → `reconnect.maxDelay`). Only `BackendIdMismatchError` ends
  the stream.
- If rivetkit itself gives up (status `idle`), a watchdog recreates the connection with the same
  backoff.
- Non-live pulls do not retry internally — they surface `IsOfflineError` and LiveStore's leader retries.

---

## Sleep & hibernation

Rivet puts an idle actor to **sleep** (its process state is dropped, SQLite persists) and wakes it on the
next action. Everything this actor keeps in memory per wake — head, `backendId`, push semaphore, the
per-connection auth cache — is rebuilt from SQLite in `makeStoreCtx`, so sleep is always safe; the
only cost is the wake (`onMigrate` → wake effect: two small SQLite reads, ~30–100 ms end to end on
a local engine).

**Tuning.** `sleepTimeout` (default 30 s) and the other rivetkit actor options are set through the
`actor` option (requires this repo's `@rivetkit/effect` patch, see above):

```ts
makeLiveStoreSyncActor({
  actor: { sleepTimeout: 60_000, sleepGracePeriod: 15_000, actionTimeout: 60_000 },
})
```

There is **no registry-level default** for these in rivetkit 2.3.17: `RegistryConfig` only carries
`maxIncomingMessageSize` / `maxOutgoingMessageSize` into the actor runtime config, so `sleepTimeout`
is per actor definition (`registryOptions` therefore has no such knob).

**What we verified empirically** (rivetkit 2.3.17, local engine, `sleepTimeout: 1500`;
`tests/hibernation.integration.test.ts`, details in `docs/spike-results.md` Part C):

- With **no open connection** the actor sleeps ~`sleepTimeout` after its last action and the next
  action wakes it (head and `backendId` come back from SQLite).
- While **any action/event WebSocket connection is open — i.e. while any `makeRivetSync` client is
  connected — the actor does not sleep on its own**, even though rivetkit reports those connections as
  hibernatable (`conn.isHibernatable === true`) and its docs say hibernatable connections do not
  prevent sleep. We saw no idle sleep in 12 s with `sleepTimeout: 1500`, with or without
  `canHibernateWebSocket: true`, and with `connectionLivenessInterval` raised to 60 s. The actor
  sleeps within one `sleepTimeout` of the last connection closing. In practice: **an actor is awake
  for as long as a client is connected, and `sleepTimeout` governs how long it lingers afterwards.**
  (The client's liveness ping — every 10 s by default — would keep it awake as well, so lower
  `ping.requestInterval` never helps here.)
- When the actor **does sleep with connections open** (forced through the test-only `TestSleep`
  action, rivetkit's `c.sleep()`), **hibernation itself works**: the client observes no
  `disconnected` transition while the actor is asleep, the next action (from any client) wakes the
  actor with the same connections — same `conn.id`, same `conn.params` — back in `c.conns`, and the
  hibernated client's live pull receives the fan-out of a push made by another client over that
  socket (~30 ms after the push). `validatePayload` is re-run for each rehydrated connection on the
  first fan-out after the wake, exactly as for a fresh one.
- **Upstream bug (2.3.17, intermittent):** the *first message the hibernated client itself sends
  after the wake* is sometimes refused by the engine — `hibernatable websocket message index out of
  sequence … gap=1`, close code `1008 ws.message_index_skip` — because the persisted message index
  of that connection lags one behind (seen in most, not all, runs; timing-independent). The client
  sees a transport blip: the in-flight action fails with `IsOfflineError` (LiveStore retries it;
  `ws/*` closes are classified as offline for that reason), rivetkit reconnects at once, and the
  live pull re-catches up from `lastSeen`. No events are lost; the retried push is admitted against
  the head reloaded from SQLite.

Nothing in the client needs to know about sleep: everything a sleep can cause (a dropped
connection, a failed in-flight action) is exactly what the reconnect path already handles.

---

## Browser / worker notes

- Import **`livestore-sync-rivet-actors/client`** only. It is built `platform: 'neutral'` and reaches
  for `rivetkit/client` (rivetkit's browser build) and nothing else.
- **Never import `livestore-sync-rivet-actors/server`** (or `rivetkit`, `rivetkit/errors`,
  `@rivetkit/effect`) from browser code — those are server entries and pull in the whole actor runtime.
  Rivet errors are duck-typed in the client precisely so it never needs `rivetkit/errors`.
- `livestore-sync-rivet-actors/common` (wire schemas and constants) is safe everywhere.
- In a LiveStore web adapter the sync backend runs in the **leader worker**, so that is where the
  rivetkit client ends up; bundle size and rivetkit's logger are rivetkit's concern.

---

## Development

```sh
pnpm install          # patches @rivetkit/effect; approve the native build scripts if prompted
pnpm build            # tsdown → dist/{client,common,server}.{mjs,d.mts}
pnpm typecheck        # tsc --noEmit over src, tests, examples
pnpm test:unit        # no engine required
pnpm test:conformance # boots a Rivet engine (see caveat below)
```

| Script | What it does |
|---|---|
| `pnpm build` | Builds `dist/` with tsdown (client + common as `platform: neutral`, server as `platform: node`). |
| `pnpm dev` | `tsdown --watch`. |
| `pnpm typecheck` | `tsc --noEmit`. |
| `pnpm test` | Both vitest projects. |
| `pnpm test:unit` | The `unit` project — fakes only, no engine, no network. |
| `pnpm test:conformance` | The `conformance` project: the LiveStore sync-provider suite, the Rivet-specific tests and the integration tests (incl. `tests/hibernation.integration.test.ts`, which boots a second actor layer with `sleepTimeout: 1500` in its own `livestore-hibernation` namespace). |
| `pnpm example:server` | `examples/node-server` — a bare actor host with a local engine. |
| `pnpm example:todo:server` | `examples/node-todo`'s sync server. |
| `pnpm example:todo` | `examples/node-todo`'s two-client convergence demo (run the server first). |

**Conformance-test caveat.** The suite spawns a local Rivet engine via `Registry.test`. That engine is
**detached**: it outlives the vitest process, keeps listening on `:6420`, and persists actor state in
`~/.rivetkit/var/engine/db`. Later runs reuse it, which is why every test must use a unique `storeId`.
Find it with `pgrep -fl rivet-engine`; delete `~/.rivetkit` for a clean slate.

**pnpm gotcha.** Use `pnpm --filter <pkg> run server`, not `pnpm --filter <pkg> server` — `server`
collides with a pnpm built-in command.

Compatibility evidence for all of the above (rivetkit transport behaviour, size limits, SQLite
semantics, codec round-trips) is recorded in [`docs/spike-results.md`](./docs/spike-results.md).

---

## Known limitations

- **Actors do not sleep while a client is connected** (rivetkit 2.3.17). Action/event WebSocket
  connections are hibernatable and hibernation itself works (see [Sleep & hibernation](#sleep--hibernation)),
  but the runtime's idle timer never fires while such a connection is open, so `sleepTimeout` only
  applies once the last client has disconnected. `tests/hibernation.integration.test.ts` pins this
  behaviour; if it starts failing on a rivetkit upgrade, hibernation-with-idle-clients has arrived and
  this section should be updated.
- **`sleepTimeout` & co. need the `@rivetkit/effect` patch** (item 4 above); with an unpatched SDK the
  `actor` option is silently ignored and rivetkit's defaults apply.
- **`SyncBackend.isSyncBackend` returns `false`** for this backend. The guard in
  `@livestore/common@0.5.0-dev.0` requires `connect` and `ping` to be *functions*, while the
  `SyncBackend` type declares them as `Effect` values (objects in Effect 4). LiveStore's own
  `makeMockSyncBackend` fails the same guard — it is an upstream staleness, not a shape mismatch, and
  nothing in LiveStore's runtime path depends on it.
- **Property-based conformance cases run with a small budget.** The large-batch property from
  LiveStore's `tests/sync-provider` suite (`tests/sync-provider-properties.test.ts`) runs 8
  fast-check cases per `pnpm test:conformance` without shrinking; raise it with `FC_NUM_RUNS` and
  replay a failure with `FC_SEED`.
- Everything is pinned to pre-release LiveStore / Effect / rivetkit versions, and the `./server` entry
  needs a patched `@rivetkit/effect` (see above).

---

## License

Apache-2.0 © Jason Antwi-Appah
