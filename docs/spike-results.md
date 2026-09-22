# T2 — Compatibility spike results

Environment: `effect@4.0.0-rc.112`, `@livestore/{common,utils}@0.5.0-dev.0`, `rivetkit@2.3.17`, `@rivetkit/effect@2.3.17` (patched, see below), TypeScript 7 (`tsc` native), vitest 4.1, macOS arm64.

> The throwaway `spike/` directory this document refers to was deleted in T17 once its findings were captured here. The probe patterns it exercised now live in `src/server/actor.ts`, `src/server/sqlite.ts` and the integration/conformance tests (`src/server/__tests__/actor.integration.test.ts`, `tests/`). File names below (`spike/a-sdk.test.ts`, …) are kept as provenance for each observation.

## Decision: **SDK path (patched `@rivetkit/effect`)**

Reasoning:
- After a 4-kind mechanical patch (`patches/@rivetkit__effect@2.3.17.patch`, registered in `pnpm-workspace.yaml` → `patchedDependencies`), a `tsc` probe over the SDK's own `src/**/*.ts` (incl. its `.test.ts`/`.test-d.ts`) goes from **339 errors → 0** against rc.112, with our strict tsconfig (`exactOptionalPropertyTypes: true` kept).
- `pnpm typecheck` is clean for the SDK + `spike/sdk-probe.ts` (quickstart Counter with `state`, `db({ onMigrate })`, `broadcast`, `conn.send`, LiveStore-typed action errors).
- End-to-end runtime works: `Registry.test` auto-spawns the engine; `Counter.client` actions and typed errors work; a raw `rivetkit/client` connection receives `conn.send`/`broadcast` events, `conn.params` is visible server-side, and the `EffectActionError` envelope arrives verbatim (`spike/a-sdk.test.ts`, 6/6 green).
- Cost: consumers who use the `./server` entry with `@rivetkit/effect` need the same patch until upstream targets an effect rc (upstream still uses `Schema.TaggedErrorClass`, removed in effect 4 rc; rc has `Schema.TaggedError<Self>(identifier?)(tag, fields)` with an identical call shape). The patch is 100 % mechanical, so a future upstream release is a drop-in.

### The patch (`pnpm patch @rivetkit/effect@2.3.17`)
1. `Schema.TaggedErrorClass<X>(id)(` → `Schema.TaggedError<X>(id)(` — 28 sites: `src/RivetError.ts` (26), `src/Actor.test-d.ts` (1), `src/Action.ts` (doc comment) — plus the same rename in `dist/RivetError.js`, `dist/Action.{js,d.ts}` (unused: the package's `exports` point at `./src/mod.ts`).
2. `src/internal/ActionDispatcher.ts`: `code: cond ? tag : undefined` → `...(cond ? { code: tag } : {})` (rivetkit's `UserErrorOptions.code?: string` under exactOptionalPropertyTypes).
3. `src/internal/ActorInstanceManager.ts` and `src/internal/ActorStateAdapter.ts`: `readonly state?: X` → `readonly state?: X | undefined`.
4. (added with the hibernation work) `src/Actor.ts` + `dist/Actor.{js,d.ts}`: `rivetkitActorOptionsKeys` = every key of rivetkit's `ActorOptionsInput` instead of `['name', 'icon']`, so `Actor.toLayer(…, { sleepTimeout, … })` reaches `Rivetkit.actor({ options })`. Verified at runtime: `registry.rivetkitActors.get('LiveStoreSync').config.options.sleepTimeout === 1500`, and rivetkit's `buildActorConfig` forwards it as `sleepTimeoutMs` to the native runtime.

Typecheck gotcha: exported layers need an explicit annotation under `declaration: true` — `export const CounterLive: Layer.Layer<never, never, Registry.Registry> = Counter.toLayer(...)` (otherwise TS2883 "inferred type cannot be named without a reference to …/Registry"). This confirms the plan's stated layer type.

## Part B — rivetkit core behaviour

### B1. Engine boot
- **`Registry.test`** (Effect SDK) sets `startEngine = true` when no `endpoint` is configured, `test.enabled = true`, `noWelcome = true`, then calls `rivetkitRegistry.start()` **without awaiting readiness**: the layer builds in ~11 ms and `fetch(endpoint/health)` right after fails on a cold engine. The first action call blocks until the engine is up (first test: **2.5 s cold**, **1.0 s warm**).
- **`setupTest`** (`rivetkit/test`) does **not** set `startEngine`; it needs `RIVET_RUN_ENGINE=1` or `registry.config.startEngine = true` before the call. It does await `waitForRegistryReady` (879 ms warm).
- Endpoint: `http://127.0.0.1:6420` (`ENGINE_HOST`/`ENGINE_PORT` defaults; config keys `engineHost`/`enginePort`; `registry.parseConfig().endpoint` returns it; `RIVET_ENDPOINT` env is *not* set by the spawn). The SDK's `Registry.test` propagates the resolved endpoint to its `Client`, but does not expose it — a raw client just uses `http://127.0.0.1:6420`.
- Binary: **not downloaded at runtime** — `@rivetkit/engine-cli` resolves the platform optional dep `@rivetkit/engine-cli-darwin-arm64` (87 MB `rivet-engine`, installed by pnpm; override with `RIVET_ENGINE_BINARY_PATH`).
- The engine is spawned **detached (PPID 1) and leaks past vitest exit**; the next run reuses it (no second PID spawned). Data: `~/.rivetkit/var/engine/db` (81 MB after this spike), logs: `~/.rivetkit/var/logs/rivet-engine/engine-<ts>-{stdout,stderr}.log`. Listens on 6420 (guard/API), 6430 and two more localhost ports.
  - Consequence: **actor state persists across test runs** (the Effect-client test failed once with `7/10/10` instead of `2/5/5` because key `effect-k` already existed). Tests must use unique store ids per run.
- The vitest process exits cleanly after the run (9.3 s wall vs 8.9 s reported Duration). The engine logs harmless `sqlite … transaction_closed` errors when the runner disconnects.

### B2. Live events
- `conn.send(name, arg)` from an action → `conn.on(name)` on the client: **yes** (`evt = [{"hello":1,"arr":[1,"a",null]}]`). `c.broadcast` also delivered to the same conn (`b:evt`).
- `c.conns` is a `Map<string, Conn>`; `conn.id` is a string UUID, `conn.params` is the `connect(params)` object verbatim (`[{"storeId":"raw-k","token":"x"}]`), `conn.send`, `conn.disconnect(reason)` (async) exist.
- Events emitted with **no subscriber are dropped** (emit `late`, then `conn.on('late')`, wait 300 ms → 0 received). A subscription registered after `connect()` but before the first action **is honoured** (subscription requests ride the same socket).
- Typing gotcha: with an untyped `createClient()`, `handle.connect()` returns `ActorConn<AnyActorDefinition>` which **omits `on`/`once`** (they exist only on `ActorConnRaw`, exported from `rivetkit/client`) → cast `conn as unknown as ActorConnRaw` (or `import type` the registry, which the client must not do).

### B3. Disconnect semantics
Server `conn.disconnect('unauthorized')`:
- Client sees WS close `code 1000, reason "unauthorized"`; logs `failed to parse close reason` (non-structured reason).
- `onStatusChange` sequence (ms from connect): `[3 connected] [119 disconnected] [119 connecting] [126 connected]` → **auto-reconnects immediately** (first retry is instant; then p-retry `forever: true, minTimeout 250 ms, maxTimeout 30 s`). A second kick behaves the same (`disconnected → connecting → connected` in 5 ms). rivetkit has no server-side notion of a *rejected* connection here: the kicked client just comes back.
- In-flight `conn.action(...)` whose handler disconnects the caller rejects with a **plain `Error`** (`ctor: Error`, `message: "Connection closed (code: 1000, reason: unauthorized)"`) — not an `ActorError`; `conn.onError` does **not** fire.
- Structured close reasons (e.g. `message.incoming_too_long`) DO reject in-flight actions with `ActorError { group: 'message', code: 'incoming_too_long', message: 'Connection closed: message.incoming_too_long' }`.
- `conn.action` while status is `disconnected`/`connecting`: **queued, never rejected** (`#sendMessage` → "no websocket, queueing message"); it resolved 9 ms later once reconnected. If the engine stays down it hangs indefinitely → the plan's `raceFirst(action, waitUntil(isConnected === false) → IsOfflineError)` is mandatory.
- `UserError('msg', { code: 'ServerAheadError', metadata: {...} })` thrown from a plain action → client `ActorError { group: 'user', code: 'ServerAheadError', message: 'server ahead msg', metadata: <verbatim, incl. nested arrays, null, 1.5> }`. **Metadata survives CBOR verbatim.**
- Effect-SDK typed errors (`Action.make({ error })` + `Effect.fail(new ServerAheadError(...))`) arrive as `ActorError { name: 'RivetError', group: 'user', code: 'ServerAheadError', message: 'Fail failed', metadata: { _tag: 'EffectActionError', version: 1, error: { _tag: 'ServerAheadError', minimumExpectedNum: 5, providedNum: 3 } }, statusCode: 500 }`. `UnknownError` likewise (`error.cause = { name: 'Error', message: 'inner boom' }`, `note`, `payload` preserved). A defect (`Effect.die`) or a plain `throw` → `ActorError { group: 'rivetkit', code: 'internal_error', message: 'An internal error occurred', metadata: null }`.

### B4. Message size
Defaults are registry-level (`Rivetkit.setup({...})`): **`maxIncomingMessageSize = 65 536`** (64 KB), **`maxOutgoingMessageSize = 1 048 576`** (1 MB). Enforcement happens in the native runtime.
- Incoming (client → actor action args): 60 KB OK; **70 KB and 900 KB fail** — the server closes the WS (`message.incoming_too_long`), the action rejects with that `ActorError`, and the client reconnects (~1 s).
- Outgoing (action result / `conn.send`): 900 KB action result OK; 900 KB `conn.send` event OK; 1.1 MB result → `ActorError { group: 'message', code: 'outgoing_too_long' }` (connection stays up).
- Raising the limit works: `Registry.layer({ noWelcome: true, maxIncomingMessageSize: 4_000_000 } as Registry.Options)` — the SDK's `Registry.Options` *type* only lists `endpoint|token|namespace|noWelcome|sqlite`, but the object is spread into `Rivetkit.setup`, so all registry keys pass through at runtime; with it a 900 KB action arg round-trips.

### B5. SQLite via `db({ onMigrate })`
- `CREATE TABLE … STRICT`: accepted (in `onMigrate` and in actions).
- `c.db.execute(sql, ...params)` returns **`Row[]` (array of plain row objects)**; for non-SELECT statements it returns `[]` (no `changes`/`lastInsertRowId`).
- `SELECT COUNT(*) AS total` → **`number`** (`0` on empty table); `MAX(seqNum)` on empty → **`null`**.
- Multi-row INSERT with 50 rows × 7 = **350 bound params works**; `null` binds SQL NULL; `WHERE seqNum > ? … LIMIT ?` paging works.
- `c.db.transaction(async tx => …, { name })` **rolls back when the callback throws** (the same error is rethrown; row count stayed 0) and commits otherwise; `tx.execute` return value is usable. Always pass `{ name }` — unnamed transactions log a warning.
- STRICT type violation surfaces to the action as a generic `"An internal error occurred"`; the real cause (`datatype mismatch`) is only in the engine log.

### B6. Codecs on rc.112 (`spike/c-codecs.test.ts`, 5/5)
- `Schema.Json` exists (`Codec<Json>`); `Schema.optionalKey`, `Schema.Option`, `Schema.toCodecJson`, `Schema.Defect()` all present.
- Cursor `Option(Struct({ eventSequenceNumber: Global.Schema, backendId: Option(String) }))` round-trips: `{"_tag":"Some","value":{"eventSequenceNumber":42,"backendId":{"_tag":"Some","value":"b1"}}}`, `{"_tag":"None"}`; branded int rejects `1.5`.
- `ServerAheadError` → `{"_tag":"ServerAheadError","minimumExpectedNum":5,"providedNum":3}`, decodes to an instance.
- `UnknownError` with `Schema.Defect()` cause: `Error` encodes as `{"name":"Error","message":"boom"}` and decodes back to an `Error`; string/object causes preserved; omitted `note`/`payload` stay absent. `Union([ServerAheadError, UnknownError])` dispatches by `_tag`.
- `Struct({ a: Number, b: optionalKey(Json) })` with `b` absent: encoded keys `['a']`, decoded keys `['a']`; explicit `b: undefined` is **rejected** by `encodeSync`. Over rivetkit's CBOR transport an absent key stays absent (server-side `Object.keys` → `['a']`, both via the SDK decoder and a plain action), while an explicit `undefined` value **does** arrive as a present key with `undefined` — never send `undefined`.

### B7. Actor keys with spaces
`getOrCreate('Counter', ['test-store some name'])` works; server `c.key` / `Actor.CurrentAddress.key` = `['test-store some name']`.

## Adjustments the plan needs
1. **Message limits.** Default incoming is **64 KB**, not ~1 MB: a 900 KB push chunk (and even a single 120 KB event from the conformance "large batches" case) is rejected and drops the connection. Options: (a) require/document `Registry.layer({ maxIncomingMessageSize })` (export a `RECOMMENDED_REGISTRY_OPTIONS` constant, e.g. 1–4 MB) and keep `maxPushBytes` ≈ 900 KB; and/or (b) default the client `maxPushBytes` to ~60 KB (net of CBOR framing) when the server is unconfigured. Outgoing 1 MB default makes the server's 900 KB pull-page byte guard valid. The conformance suite must boot the registry with the raised incoming limit. The client should map `ActorError{group:'message', code:'incoming_too_long'|'outgoing_too_long'}` → `UnknownError` (not `IsOfflineError`, even though the socket drops).
2. **`RawAccess.execute` returns `Row[]`** — plan assumption confirmed; do not rely on `changes` (not returned). Always name transactions.
3. **Client error table.** In-flight action dropped by a non-structured close → plain `Error` matching `/^Connection (closed|lost)/` → `IsOfflineError`. Structured closes / rivet errors are `ActorError` (`name: 'RivetError'`, exported from `rivetkit/client` as both `ActorError` and `RivetError`) with `group`/`code`/`metadata`. Defects → `rivetkit/internal_error` → `UnknownError`. Typed envelope: `metadata._tag === 'EffectActionError'`, `metadata.error` decodes with `toCodecJson(ErrorSchema)`.
4. **Reconnect/offline.** rivetkit reconnects forever (instant first retry, then 250 ms → 30 s); a server `conn.disconnect('unauthorized')` does not stop reconnection, so the "rejected connection" cache in `connections.ts` will see the same client come back with a new `conn.id` every retry — cache by connection id is fine but expect churn; consider also refusing to send rather than repeatedly disconnecting. `isConnected` should follow `onStatusChange === 'connected'`; the `idle` watchdog is only needed for disposal/`connection_open_failed`. Actions never reject while offline → keep the `raceFirst` → `IsOfflineError` design.
5. **Testing.** `Registry.test` returns before the engine is ready (first action waits, so `beforeAll` needs no extra wait but a raw health check does). `setupTest` needs `startEngine`. The spawned engine is detached, persists in `~/.rivetkit/var`, and is reused by later runs → every test must use a unique `storeId` (e.g. nanoid) and the conformance provider must not assume a fresh engine; `fileParallelism: false` is still right (one engine, shared).
6. **Typing.** Raw client code must cast `conn` to `ActorConnRaw` for `on/once`; registry knobs beyond the SDK's `Registry.Options` need `as Registry.Options`; exported `toLayer` results need explicit `Layer.Layer<never, never, Registry.Registry>` annotations; `Handle` methods for void-payload actions are called as `h.GetCount(undefined)`.
7. **Events.** Unsubscribed events are dropped and events during a disconnect are lost → subscribe `pull` before the first `Pull` action and re-catch-up on every `connected` transition (already in the plan; now confirmed necessary).
8. **Patch distribution.** Keep `patches/@rivetkit__effect@2.3.17.patch` + `pnpm-workspace.yaml` `patchedDependencies`; README must tell consumers of `./server` to apply the same patch (pnpm `patchedDependencies`, or `npm`/`yarn` equivalents) until `@rivetkit/effect` ships an effect-rc-compatible release.

## Part C — Sleep & hibernation (rivetkit 2.3.17, local engine)

Method: `tests/hibernation.integration.test.ts` (permanent) plus a throwaway probe (deleted) driving a raw `rivetkit/client` and `makeRivetSync` clients against an actor with `actor: { sleepTimeout: 1500 }` and `testing: { enabled: true }`, observed through the test-only `TestInfo` action (`wakeCount`, `wokeAt`, `previousSleptAt`, head, `backendId`, `conns[] { id, clientId, hibernatable }` — the wake counter lives in module scope of `actor.ts`, so a count above 1 proves a sleep/wake cycle in the same runner process) and `TestSleep` (rivetkit's `c.sleep()`). Note that `TestInfo` through a stateless handle is itself an action: it wakes a sleeping actor and shows up in `conns` as a transient, non-hibernatable connection with `clientId: null`.

### C1. `sleepTimeout` is honoured when no connection is open
- Actor woken by a handle action at t=0 → `previousSleptAt` of the next wake = **t+1591 ms** (second sample: +1652 ms). The next handle action 4 s later woke it (`wakeCount` 1 → 2); wake round trip **68–218 ms** (first wake of a fresh actor at the high end), later wakes ~30 ms `wokeAt` after the request left.
- Head and `backendId` after the wake match the pre-sleep values (rebuilt in `makeStoreCtx`).
- The runtime logs `sqlite operation failed … transaction_closed` / `failed to sync scheduled actor alarm` at every sleep — harmless noise from rivetkit's own schedule sync racing the shutdown.

### C2. An open action/event WebSocket keeps the actor awake
- One idle raw WS connection (`handle.connect(params)`, subscribed to `pull`, no actions): `conn.isHibernatable === true`, yet **no sleep in 6 s and none in 12 s** (`wakeCount` stayed 1, `previousSleptAt` null). Same after one action on the connection, same through `makeRivetSync` with a live pull running and the client ping disabled.
- Unchanged with `canHibernateWebSocket: true` and with `connectionLivenessInterval: 60_000` / `connectionLivenessTimeout: 30_000`, so neither the raw-WebSocket hibernation flag nor the runner's liveness pings are the reason; the sleep timer (`rivetkit-core/src/actor/sleep.rs`, "sleep activity reset") simply does not fire while such a connection exists. This contradicts the docs ("no active connections (unless they are hibernatable WebSockets)") for 2.3.17 — hibernation is documented as beta.
- The rivetkit client sends no periodic WebSocket pings of its own (only a `keepNodeAliveInterval` timer), so the cause is server-side.
- Disposing the last connection: the actor slept **~1.5 s after its last action / ≤ 1.5 s after the disconnect** (`wokeAt` +1652 ms with the disconnect ~200 ms after the wake), i.e. the pending timer fires as soon as the connection count drops to zero. The `sleepTimeout` therefore governs how long an actor lingers after the last client leaves, not whether idle-but-connected clients cost an awake actor.

### C3. Hibernation works when the actor sleeps with connections open (`c.sleep()`)
Raw WS connection A (subscribed to `pull`), then `TestSleep` via a handle:
- Actor slept **122 ms** after the `TestSleep` request; A's `onStatusChange` log stayed `[connected]` — no close, no reconnect.
- `TestInfo` 2.5 s later woke the actor (`wakeCount` 2, round trip 64 ms) and `c.conns` contained **A with the same `conn.id` and the same `conn.params`** (the `clientId` decodes) plus the handle's transient connection.
- A push from a second connection B (its connect + push after the sleep): A received the `pull` event **29 ms** after the push was sent; head 1 on the new instance; A's own action afterwards worked on the same socket (A had sent no action before the sleep — see the bug below).
- Through `makeRivetSync`: client A's `isConnected` never left `true` across the sleep and its live pull received the fan-out without a re-catch-up.
- **Upstream off-by-one (intermittent):** when a hibernated connection sends its first message *after* the wake, the runner sometimes logs `hibernatable websocket message index out of sequence, closing connection previous_index=N expected_index=N+1 received_index=N+2 gap=1` and closes the socket with `1008 ws.message_index_skip` (rivetkit logs `unhandled action promise rejection`). Observed in 5 of 6 runs of the "own push after sleep" scenario (`previous_index=2 expected 3 received 4`) and at the dispose of a connection that had done one `Pull` before the sleep (`previous_index=1 expected 2 received 3`); a 1.5 s quiet period before the sleep (longer than `stateSaveInterval`) does not change it, so it is not a flush race; the run that did not hit it had a second live pull open on the same connection. The action-less connection of the raw probe never hit it. Client-side effect through `makeRivetSync`: the in-flight push fails with `IsOfflineError` ("connection lost during action" — the status flip wins the race; the structured `ws/message_index_skip` close is now classified as `IsOfflineError` as well, so the outcome does not depend on that race), rivetkit reconnects immediately (one `isConnected` dip, new `conn.id`), the retried push (same parent) is admitted against the head reloaded from SQLite, the live pull re-catches up and delivers it, and a stale push is still rejected with `ServerAheadError`. No data loss; test 4 of `tests/hibernation.integration.test.ts` accepts both outcomes and asserts the recovery invariants.
- Connections created by stateless handle actions (HTTP) are `hibernatable: false` and disappear with the request.

### C4. Consequences for the provider
- No client change: a hibernated connection never surfaces as a disconnect while the actor sleeps, and the one thing that does go wrong (the `ws.message_index_skip` close on the first post-wake message) is a plain transport blip that the existing `IsOfflineError` + reconnect + re-catch-up path handles.
- No server change beyond options: the lazy per-wake `connAuth` cache re-validates rehydrated connections on the first fan-out, and the head/`backendId` reload already existed. `makeLiveStoreSyncActor({ actor })` threads the rivetkit options; the wake bookkeeping (`StoreCtx.wake`) is diagnostic.
- No registry-level actor defaults exist in `RegistryConfigSchema` (only the two message-size limits flow into `buildActorConfig`), so `sleepTimeout` stays per actor definition.
- Test-infrastructure note: a runner that starts before its namespace exists can stay unroutable for a minute or more (`no_runner_config_configured`), and the engine's `/runners?namespace=` endpoint lists nothing for a live runner (it is `/envoys` in this engine), so polling `/runners` is a no-op. The shared helpers in `tests/harness/engine.ts` now create the namespace *before* `Registry.test` starts the runner, poll `/envoys?namespace=` until the runner is listed, and then retry a trivial action until it is routable; both the conformance harness and `tests/hibernation.integration.test.ts` use them.
