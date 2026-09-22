/**
 * Provider registry for the conformance suite. Mirrors LiveStore's
 * `tests/sync-provider/src/providers/registry.ts`; this repo only ships the
 * Rivet provider.
 */

import type { Effect } from '@livestore/utils/effect'

import type { SyncProviderLayer } from '../types.ts'
import * as RivetProvider from './rivet.ts'

/** Shape of each entry in the provider registry. */
interface ProviderEntry {
  readonly name: string
  readonly layer: SyncProviderLayer
  readonly prepare: Effect.Effect<void, any, any>
}

// Single source of truth for sync providers used across CLI and tests
export const providerRegistry: {
  rivet: ProviderEntry
} = {
  rivet: { name: RivetProvider.name, layer: RivetProvider.layer, prepare: RivetProvider.prepare },
}

export type ProviderKey = keyof typeof providerRegistry

export const providerKeys = Object.keys(providerRegistry) as ProviderKey[]

/** Environment variable a CI matrix cell uses to pin its run to a single provider. */
export const providerSelectionEnvVar = 'TEST_SYNC_PROVIDER'

export const isProviderKey = (value: string): value is ProviderKey => Object.hasOwn(providerRegistry, value) === true

/**
 * Provider keys the current process should exercise: every provider by default, or the
 * single one pinned via `TEST_SYNC_PROVIDER`.
 *
 * Selection is resolved at collection time against the registry rather than by matching
 * test titles, so a cell can no longer select nothing and report success — an unknown key
 * throws, and renaming a suite cannot silently remove it from CI.
 */
export const selectedProviderKeys = (): ProviderKey[] => {
  const selected = process.env[providerSelectionEnvVar]
  if (selected === undefined || selected === '') return providerKeys

  if (isProviderKey(selected) === false) {
    throw new Error(
      `Unknown ${providerSelectionEnvVar}=${JSON.stringify(selected)}. Expected one of: ${providerKeys.join(', ')}`,
    )
  }

  return [selected]
}

/** Whether `key` is in the current selection — for suites hard-wired to one provider. */
export const isProviderSelected = (key: ProviderKey): boolean => selectedProviderKeys().includes(key)
