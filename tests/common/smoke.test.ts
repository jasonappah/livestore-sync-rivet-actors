import { SyncBackend } from '@livestore/common'
import { Schema } from '@livestore/utils/effect'
import { expect, it } from 'vitest'

/**
 * Proves the Effect 4 / LiveStore 0.5-dev wiring resolves at runtime: both
 * packages load, and the two symbols the sync provider is built on exist.
 */
it('resolves the LiveStore dev + Effect 4 toolchain', () => {
  expect(SyncBackend.pageInfoNoMore._tag).toBe('NoMore')
  expect(typeof Schema.toCodecJson).toBe('function')
})
