/**
 * In-memory stand-in for rivetkit's client/handle/conn trio, matching the
 * `RivetClientLike` seam of `src/client/connection.ts`.
 *
 * Every `connect()` yields a {@link FakeConn} that starts in `connecting`;
 * tests drive status transitions (`setStatus`), live events (`emit`) and
 * action outcomes (`onAction` / `pendingActions`) explicitly.
 */

import type { RivetClientLike, RivetConnLike } from '../../src/client/connection.ts'
import type { ConnStatus } from '../../src/client/types.ts'

export interface PendingAction {
  readonly name: string
  readonly args: unknown[]
  readonly signal: AbortSignal | undefined
  readonly resolve: (value: unknown) => void
  readonly reject: (reason: unknown) => void
}

export type FakeActionHandler = (name: string, args: unknown[]) => Promise<unknown>

export interface FakeConn extends RivetConnLike {
  readonly params: unknown
  readonly disposed: boolean
  /** Actions whose promise has not been settled by a handler yet (FIFO). */
  readonly pendingActions: PendingAction[]
  /** Every action call, in order. */
  readonly calls: Array<{ readonly name: string; readonly args: unknown[] }>
  /** Sets the status and notifies `onStatusChange` listeners (no-op if unchanged, like rivetkit). */
  setStatus(status: ConnStatus): void
  /** Delivers a connection event to `on(event)` listeners. */
  emit(event: string, ...args: unknown[]): void
  /** Fires `onError` listeners. */
  fireError(error: unknown): void
  /** Programmable action responder; when unset, actions stay pending until resolved via `pendingActions`. */
  onAction: FakeActionHandler | undefined
  /** Event names with at least one active `on()` listener. */
  subscribedEvents(): string[]
}

export interface FakeRivetClient {
  readonly client: RivetClientLike
  /** Every conn created via `connect()`, in creation order. */
  readonly conns: FakeConn[]
  /** `getOrCreate(name, key)` calls, in order. */
  readonly handles: Array<{ readonly name: string; readonly key: string | string[] }>
  readonly disposed: boolean
  /** Interleaved dispose order, e.g. `['conn:0', 'client']`. */
  readonly disposeOrder: string[]
}

const makeFakeConn = (params: unknown, index: number, disposeOrder: string[]): FakeConn => {
  let status: ConnStatus = 'connecting'
  let disposed = false
  const statusListeners = new Set<(status: ConnStatus) => void>()
  const errorListeners = new Set<(error: unknown) => void>()
  const eventListeners = new Map<string, Set<(...args: unknown[]) => void>>()
  const pendingActions: PendingAction[] = []
  const calls: Array<{ readonly name: string; readonly args: unknown[] }> = []

  const conn: FakeConn = {
    params,
    pendingActions,
    calls,
    onAction: undefined,
    get connStatus() {
      return status
    },
    get disposed() {
      return disposed
    },
    on(event, cb) {
      let set = eventListeners.get(event)
      if (set === undefined) {
        set = new Set()
        eventListeners.set(event, set)
      }
      set.add(cb)
      return () => {
        set!.delete(cb)
        if (set!.size === 0) eventListeners.delete(event)
      }
    },
    onStatusChange(cb) {
      statusListeners.add(cb)
      return () => {
        statusListeners.delete(cb)
      }
    },
    onError(cb) {
      errorListeners.add(cb)
      return () => {
        errorListeners.delete(cb)
      }
    },
    action(opts) {
      calls.push({ name: opts.name, args: opts.args })
      if (conn.onAction !== undefined) return conn.onAction(opts.name, opts.args)
      return new Promise<unknown>((resolve, reject) => {
        pendingActions.push({ name: opts.name, args: opts.args, signal: opts.signal, resolve, reject })
      })
    },
    async dispose() {
      if (disposed) return
      disposed = true
      disposeOrder.push(`conn:${index}`)
      conn.setStatus('idle')
    },
    setStatus(next) {
      if (next === status) return
      status = next
      for (const listener of [...statusListeners]) listener(next)
    },
    emit(event, ...args) {
      const set = eventListeners.get(event)
      if (set === undefined) return
      for (const listener of [...set]) listener(...args)
    },
    fireError(error) {
      for (const listener of [...errorListeners]) listener(error)
    },
    subscribedEvents() {
      return [...eventListeners.keys()]
    },
  }
  return conn
}

export const makeFakeRivetClient = (): FakeRivetClient => {
  const conns: FakeConn[] = []
  const handles: Array<{ readonly name: string; readonly key: string | string[] }> = []
  const disposeOrder: string[] = []
  let disposed = false

  const client: RivetClientLike = {
    getOrCreate(name, key) {
      handles.push({ name, key })
      return {
        connect(params) {
          const conn = makeFakeConn(params, conns.length, disposeOrder)
          conns.push(conn)
          return conn
        },
      }
    },
    async dispose() {
      disposed = true
      disposeOrder.push('client')
    },
  }

  return {
    client,
    conns,
    handles,
    disposeOrder,
    get disposed() {
      return disposed
    },
  }
}
