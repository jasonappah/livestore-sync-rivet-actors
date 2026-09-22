# `example-node-todo`

End-to-end demo of `livestore-sync-rivet-actors`: a Rivet actor server plus two
`@livestore/adapter-node` LiveStore clients in one process, syncing a tiny todo
schema in both directions.

## Prerequisites

- Node 22+ and pnpm (the repo's workspace tooling).
- Dependencies installed and the library built from the repo root — the example
  consumes `livestore-sync-rivet-actors` via `workspace:*`, so `dist/` must
  exist:

  ```sh
  pnpm install
  pnpm build
  ```

- No Rivet account needed: `RIVET_RUN_ENGINE=1` makes rivetkit start a local
  Rivet engine at `http://127.0.0.1:6420` from the bundled
  `@rivetkit/engine-cli` binary. Set `RIVET_ENDPOINT` to point both the server
  and the client at a different engine.
- Consumers of `livestore-sync-rivet-actors/server` need the workspace pnpm
  patch for `@rivetkit/effect@2.3.17` (see `patches/` and `pnpm-workspace.yaml`
  at the repo root).

## Run it

Terminal 1 — the sync server (note the `run`: `pnpm --filter <pkg> server`
collides with pnpm's built-in `server` command):

```sh
pnpm --filter example-node-todo run server
# or, from the repo root:
pnpm example:todo:server
```

Wait for the banner:

```
  RivetKit 2.3.17 (Engine - Serverful)
  - Endpoint:     http://127.0.0.1:6420 (local native)
  - Actors:       1
```

Terminal 2 — the two clients:

```sh
pnpm --filter example-node-todo start
# or, from the repo root:
pnpm example:todo
```

## What to expect

`src/main.ts` boots two in-memory stores (`client-a` and `client-b`) on the same
freshly generated `storeId`, both sending `syncPayload: { authToken: 'demo' }`
(the server's `validatePayload` rejects anything else).

1. `client-a` commits three `todo.created` events; the script polls
   `client-b` every 100 ms until it sees three rows.
2. `client-b` commits a `todo.completed` event; the script polls `client-a`
   until the row flips to `completed: true`.

Both stores are shut down and the process exits `0`. On a 15 s timeout it prints
the last observed rows on both sides and exits `1`.

```
storeId: node-todo-1790047337923
endpoint: http://127.0.0.1:6420
client-a committed 3 todo.created events
✓ client-b converged in 103 ms:
┌─────────┬──────────┬─────────────────┬───────────┐
│ (index) │ id       │ text            │ completed │
├─────────┼──────────┼─────────────────┼───────────┤
│ 0       │ 'todo-1' │ 'todo number 1' │ false     │
│ 1       │ 'todo-2' │ 'todo number 2' │ false     │
│ 2       │ 'todo-3' │ 'todo number 3' │ false     │
└─────────┴──────────┴─────────────────┴───────────┘
client-b committed todo.completed for 'todo-1'
✓ client-a converged in 102 ms:
┌─────────┬──────────┬─────────────────┬───────────┐
│ (index) │ id       │ text            │ completed │
├─────────┼──────────┼─────────────────┼───────────┤
│ 0       │ 'todo-1' │ 'todo number 1' │ true      │
│ 1       │ 'todo-2' │ 'todo number 2' │ false     │
│ 2       │ 'todo-3' │ 'todo number 3' │ false     │
└─────────┴──────────┴─────────────────┴───────────┘
✓ bidirectional sync through the Rivet actor works
```

## Notes / gotchas

- **The local engine outlives the server.** `RIVET_RUN_ENGINE=1` spawns the
  `rivet-engine` process detached, so stopping the server with Ctrl-C leaves it
  running and listening on `:6420`. Find it with `pgrep -fl rivet-engine` and
  kill it by hand when you want a clean slate. Later runs simply reuse it.
- **Engine state is persistent** under `~/.rivetkit/var/engine/db`, so actor
  state (the eventlog of every `storeId`) survives across runs. That is why
  `main.ts` generates a fresh `storeId` (`node-todo-<timestamp>`) on every run —
  reusing a `storeId` would replay the previous run's events. Deleting
  `~/.rivetkit` resets everything.
- **Distinct client identities.** `makeAdapter` defaults `clientId` to the
  machine hostname. Two stores in one process on the same `storeId` must pass
  explicit `clientId`s (`client-a` / `client-b` here), otherwise LiveStore
  treats them as one client.
- The server raises rivetkit's `maxIncomingMessageSize` to
  `RECOMMENDED_MAX_INCOMING_MESSAGE_SIZE` (4 MiB) via `registryOptions(...)`;
  rivetkit's default is 64 KB, which caps how large a single push may be.
