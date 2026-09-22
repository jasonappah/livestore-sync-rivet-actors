/**
 * Engine bootstrapping shared by the suites that boot a real `LiveStoreSync`
 * runner through `Registry.test` (`tests/harness/providers/rivet.ts`,
 * `tests/hibernation.integration.test.ts`).
 *
 * Sequence (see {@link preflightLayer} and {@link waitForRunner}):
 *
 * 1. *Before* `Registry.test` starts the runner: if the engine already
 *    answers `/health`, create the namespace. A runner that registers before
 *    its namespace exists can stay unroutable for a long time
 *    (`no_runner_config_configured` on every connect). On a cold engine this
 *    step is a no-op: `Registry.test` spawns the engine itself.
 * 2. After `Registry.test` returned: wait for `/health` (the auto-spawned
 *    engine takes a few seconds; until then `fetch` *rejects*, so the probe
 *    must treat that as a retryable failure), then create the namespace.
 * 3. Wait until the engine lists a live envoy (this engine's name for a
 *    runner connection) in the namespace. On engine 2.3.17 `/runners` and
 *    `/runners/names` stay empty for a connected rivetkit runner; `/envoys`
 *    is the endpoint that reflects registration.
 * 4. Wait until a trivial action on a throwaway actor succeeds. Being listed
 *    is not enough: on a cold engine the first actions after the envoy shows
 *    up still fail with `no_runner_config_configured` for a few hundred ms.
 *
 * Every wait is bounded (60 s engine, 30 s envoy, 30 s action) and fails with
 * an `EngineBootError` naming the endpoint and namespace, rather than letting
 * the first test time out.
 */

import { Data, Duration, Effect, Layer, Schedule } from '@livestore/utils/effect'

/** Where `Registry.test` spawns the engine (`ENGINE_HOST`/`ENGINE_PORT` defaults). */
export const DEFAULT_ENDPOINT = 'http://127.0.0.1:6420'

export interface EngineTarget {
  readonly endpoint: string
  readonly token: string | undefined
  readonly namespace: string
}

/** `RIVET_ENDPOINT` / `RIVET_TOKEN` from the environment, with the given namespace. */
export const engineTargetFromEnv = (namespace: string): EngineTarget => ({
  endpoint: process.env.RIVET_ENDPOINT ?? DEFAULT_ENDPOINT,
  token: process.env.RIVET_TOKEN,
  namespace,
})

export class EngineBootError extends Data.TaggedError('EngineBootError')<{
  readonly message: string
  readonly cause?: unknown
}> {}

const authHeaders = (target: EngineTarget): Record<string, string> =>
  target.token === undefined ? {} : { authorization: `Bearer ${target.token}` }

/** One engine API request; network errors (engine not listening yet) are failures, not defects. */
const request = (target: EngineTarget, path: string, init?: RequestInit) =>
  Effect.tryPromise({
    try: (signal) =>
      fetch(`${target.endpoint}${path}`, {
        ...init,
        headers: { ...authHeaders(target), ...init?.headers },
        signal,
      }),
    catch: (cause) => new EngineBootError({ message: `${path}: ${String(cause)}`, cause }),
  }).pipe(Effect.timeout('5 seconds'))

/** Succeeds once when `/health` answers 2xx. */
export const healthOk = (target: EngineTarget) =>
  request(target, '/health').pipe(
    Effect.filterOrFail(
      (response) => response.ok,
      (response) => new EngineBootError({ message: `engine health check failed: ${response.status}` }),
    ),
    Effect.asVoid,
  )

/** `Registry.test` returns before the auto-spawned engine accepts requests (~2.5 s cold). */
export const waitForEngine = (target: EngineTarget) =>
  healthOk(target).pipe(
    Effect.retry({ schedule: Schedule.spaced('250 millis'), times: 240 }),
    Effect.mapError(
      (error) =>
        new EngineBootError({
          message: `Rivet engine at ${target.endpoint} did not become healthy within 60 s (last: ${error.message})`,
          cause: error,
        }),
    ),
    Effect.withSpan('rivet harness: waitForEngine'),
  )

/** Creates the namespace if it does not exist yet (the API rejects duplicates; that is fine). */
export const ensureNamespace = (target: EngineTarget) =>
  request(target, '/namespaces', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ name: target.namespace, display_name: target.namespace }),
  }).pipe(
    Effect.tap((response) =>
      Effect.logDebug('rivet harness: ensureNamespace', { namespace: target.namespace, status: response.status }),
    ),
    Effect.ignore,
  )

/**
 * Provide to `Registry.test` (`Layer.provide(preflightLayer(target))`) so it
 * runs before the runner starts: creates the namespace when the engine is
 * already up. Never fails.
 */
export const preflightLayer = (target: EngineTarget) =>
  Layer.effectDiscard(
    healthOk(target).pipe(Effect.timeout('1 second'), Effect.andThen(ensureNamespace(target)), Effect.ignore),
  )

interface Envoy {
  readonly envoy_key: string
  readonly create_ts: number
  readonly stop_ts: number | null
}

/** Live envoys (runner connections) registered in the namespace. */
const listEnvoys = (target: EngineTarget) =>
  request(target, `/envoys?namespace=${encodeURIComponent(target.namespace)}`).pipe(
    Effect.flatMap((response) =>
      response.ok
        ? Effect.promise(() => response.json() as Promise<{ envoys?: ReadonlyArray<Envoy> }>)
        : Effect.fail(new EngineBootError({ message: `/envoys answered ${response.status}` })),
    ),
    Effect.map((body) => (body.envoys ?? []).filter((envoy) => envoy.stop_ts === null)),
  )

const ENVOY_WAIT = Duration.seconds(30)
const ROUTABLE_WAIT = Duration.seconds(30)

/**
 * Waits until a live envoy created at or after `since` (engine and harness
 * share the clock; 2 s slack) is listed in the namespace, i.e. until the
 * runner `Registry.test` started has registered.
 */
export const waitForEnvoy = (target: EngineTarget, since: number) =>
  listEnvoys(target).pipe(
    Effect.filterOrFail(
      (envoys) => envoys.some((envoy) => envoy.create_ts >= since - 2_000),
      (envoys) =>
        new EngineBootError({
          message: `no runner registered in namespace "${target.namespace}" yet (${envoys.length} older live envoys)`,
        }),
    ),
    Effect.retry({ schedule: Schedule.spaced('250 millis') }),
    Effect.timeoutOrElse({
      duration: ENVOY_WAIT,
      orElse: () =>
        Effect.fail(
          new EngineBootError({
            message: `no runner registered in namespace "${target.namespace}" at ${target.endpoint}/envoys within ${Duration.format(ENVOY_WAIT)}`,
          }),
        ),
    }),
    Effect.asVoid,
    Effect.withSpan('rivet harness: waitForEnvoy'),
  )

/**
 * Retries `probe` (a trivial action on a throwaway actor) with backoff until
 * it succeeds: the runner is registered *and* routes actions.
 */
export const waitUntilRoutable = <E>(target: EngineTarget, probe: Effect.Effect<unknown, E>) =>
  probe.pipe(
    Effect.timeout('5 seconds'),
    Effect.tapError((error) => Effect.logDebug('rivet harness: readiness probe failed, retrying', { error })),
    Effect.retry({
      schedule: Schedule.min([Schedule.exponential('100 millis'), Schedule.spaced('2 seconds')]),
    }),
    Effect.timeoutOrElse({
      duration: ROUTABLE_WAIT,
      orElse: () =>
        Effect.fail(
          new EngineBootError({
            message: `runner in namespace "${target.namespace}" at ${target.endpoint} did not answer a readiness action within ${Duration.format(ROUTABLE_WAIT)}`,
          }),
        ),
    }),
    Effect.asVoid,
    Effect.withSpan('rivet harness: waitUntilRoutable'),
  )

/**
 * Steps 2–4 of the module doc. Run after `Registry.test` (depend on
 * `Registry.Registry`); `since` is a `Date.now()` taken before the runner
 * started (see {@link bootClock}).
 */
export const waitForRunner = <E>(
  target: EngineTarget,
  { since, probe }: { readonly since: number; readonly probe: Effect.Effect<unknown, E> },
) =>
  Effect.gen(function* () {
    yield* waitForEngine(target)
    yield* ensureNamespace(target)
    yield* waitForEnvoy(target, since)
    // Listed is not yet routable: right after the envoy shows up the first
    // actions still fail with `no_runner_config_configured` for a few hundred ms.
    yield* waitUntilRoutable(target, probe)
  }).pipe(
    // The layer error reaches vitest wrapped in an `UnknownError`; log the reason where it is readable.
    Effect.tapError((error) => Effect.logError('rivet harness: runner did not become ready', error)),
  )

/** `Date.now()` at module load, i.e. before any layer (and so the runner) is built. */
export const bootClock = Date.now()
