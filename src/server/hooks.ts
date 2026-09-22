/**
 * Two small Effect helpers shared by every action handler:
 *
 * - {@link runHook} runs a user callback that may be sync, a promise, or an
 *   Effect, and normalises any failure to `UnknownError`.
 * - {@link mapToDeclaredErrors} collapses a handler's error channel down to
 *   the tags the action actually declares, so nothing undeclared (and no
 *   defect) can escape into the `@rivetkit/effect` error envelope.
 */

import { UnknownError } from '@livestore/common'
import { Cause, Effect, Option } from '@livestore/utils/effect'

/**
 * A user callback returning `void`, a `Promise<void>` or an `Effect`.
 * Failures on any of those paths are captured.
 */
export type Hook<Args extends ReadonlyArray<unknown>> = (...args: Args) => Effect.SyncOrPromiseOrEffect<void, unknown>

/**
 * Runs `hook` when it is defined, otherwise does nothing. Throws, rejections
 * and Effect failures alike surface as `UnknownError`.
 */
export const runHook = <Args extends ReadonlyArray<unknown>>(
  hook: Hook<Args> | undefined,
  ...args: Args
): Effect.Effect<void, UnknownError> => {
  if (hook === undefined) return Effect.void

  return Effect.trySyncOrPromiseOrEffect(() => hook(...args)).pipe(UnknownError.mapToUnknownError, Effect.asVoid)
}

/** Extracts the members of an error union whose `_tag` is in `Tags`. */
export type DeclaredError<E, Tags extends string> = Extract<E, { readonly _tag: Tags }>

/**
 * Lets failures whose `_tag` is listed in `tags` pass through unchanged and
 * turns everything else — undeclared failures *and* defects — into
 * `UnknownError`. Interruption is re-raised as-is.
 *
 * Every action handler ends with this so the declared error schema is a
 * complete description of what the wire can carry.
 */
export const mapToDeclaredErrors =
  <const Tags extends ReadonlyArray<string>>(tags: Tags) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, DeclaredError<E, Tags[number]> | UnknownError, R> => {
    const declared = new Set<string>(tags)

    const isDeclared = (error: unknown): error is DeclaredError<E, Tags[number]> =>
      typeof error === 'object' &&
      error !== null &&
      typeof (error as { _tag?: unknown })._tag === 'string' &&
      declared.has((error as { _tag: string })._tag)

    return effect.pipe(
      Effect.catchCause((cause): Effect.Effect<never, DeclaredError<E, Tags[number]> | UnknownError> => {
        // Interruption is not a failure of the handler — let it propagate.
        if (Cause.hasInterruptsOnly(cause) === true) return Effect.failCause(cause as Cause.Cause<never>)

        const error = Cause.findErrorOption(cause)
        if (Option.isSome(error) === true && isDeclared(error.value) === true) return Effect.fail(error.value)

        return Effect.fail(new UnknownError({ cause: Cause.squash(cause) }))
      }),
    )
  }
