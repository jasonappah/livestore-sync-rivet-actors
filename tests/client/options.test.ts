import { Duration } from '@livestore/utils/effect'
import { describe, expect, it } from 'vitest'

import { ACTOR_NAME } from '../../src/common/mod.ts'
import { resolveRivetSyncOptions } from '../../src/client/options.ts'

describe('resolveRivetSyncOptions', () => {
  it('applies the documented defaults', () => {
    const resolved = resolveRivetSyncOptions({ endpoint: 'http://127.0.0.1:6420' })
    expect(resolved).toEqual({
      endpoint: 'http://127.0.0.1:6420',
      token: undefined,
      namespace: undefined,
      actorName: ACTOR_NAME,
      connectTimeout: Duration.seconds(10),
      ping: { enabled: true, requestTimeout: Duration.seconds(10), requestInterval: Duration.seconds(10) },
      pullPageSize: 100,
      maxPushBytes: 60_000,
      reconnect: { baseDelay: Duration.seconds(1), maxDelay: Duration.seconds(30) },
    })
  })

  it('normalises Duration.Input overrides and keeps explicit values', () => {
    const resolved = resolveRivetSyncOptions({
      endpoint: 'https://api.rivet.dev',
      token: 't',
      namespace: 'ns',
      actorName: 'Custom',
      connectTimeout: '2 seconds',
      ping: { enabled: false, requestInterval: 500 },
      pullPageSize: 25,
      maxPushBytes: 900_000,
      reconnect: { baseDelay: '250 millis' },
    })
    expect(resolved.token).toBe('t')
    expect(resolved.namespace).toBe('ns')
    expect(resolved.actorName).toBe('Custom')
    expect(Duration.toMillis(resolved.connectTimeout)).toBe(2000)
    expect(resolved.ping.enabled).toBe(false)
    expect(Duration.toMillis(resolved.ping.requestInterval)).toBe(500)
    expect(Duration.toMillis(resolved.ping.requestTimeout)).toBe(10_000)
    expect(resolved.pullPageSize).toBe(25)
    expect(resolved.maxPushBytes).toBe(900_000)
    expect(Duration.toMillis(resolved.reconnect.baseDelay)).toBe(250)
    expect(Duration.toMillis(resolved.reconnect.maxDelay)).toBe(30_000)
  })
})
