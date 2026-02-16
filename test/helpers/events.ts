import { once } from 'node:events'
import type { EventEmitter } from 'node:events'

/**
 * Wait for N events of a given type
 */
export async function waitForEvents(
  emitter: EventEmitter,
  event: string,
  count: number
): Promise<void> {
  let received = 0
  return new Promise((resolve) => {
    const handler = () => {
      received++
      if (received >= count) {
        emitter.off(event, handler)
        resolve()
      }
    }
    emitter.on(event, handler)
  })
}

/**
 * Create a controlled latch (manually resolvable promise)
 */
export function createLatch(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void
  const promise = new Promise<void>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

/**
 * Wrap a callback subscription in a promise (for notification tests)
 * This allows waiting for a notification without polling with sleep()
 */
export function promisifyCallback<T>(
  subscribe: (handler: (value: T) => void) => Promise<() => Promise<void>>
): Promise<{ value: Promise<T>; unsubscribe: () => Promise<void> }> {
  return new Promise((resolveSetup) => {
    let resolveValue!: (value: T) => void
    const value = new Promise<T>((r) => {
      resolveValue = r
    })

    subscribe((v: T) => {
      resolveValue(v)
    }).then((unsubscribe) => {
      resolveSetup({ value, unsubscribe })
    })
  })
}

/**
 * Create a promise that resolves when a callback is called N times
 */
export function waitForCallbacks(count: number): {
  callback: () => void
  promise: Promise<void>
} {
  let received = 0
  let resolve!: () => void
  const promise = new Promise<void>((r) => {
    resolve = r
  })
  const callback = () => {
    received++
    if (received >= count) {
      resolve()
    }
  }
  return { callback, promise }
}

/**
 * Re-export once for convenience
 */
export { once }
