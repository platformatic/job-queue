import type { Logger } from 'pino'

export function noop () {}

export const abstractLogger: Logger = {
  fatal: noop,
  error: noop,
  warn: noop,
  info: noop,
  debug: noop,
  trace: noop,
  // @ts-expect-error
  child (): Logger {
    return abstractLogger
  }
}

export function ensureLoggableError (error: Error): Error {
  Reflect.defineProperty(error, 'message', { enumerable: true })

  if ('code' in error) {
    Reflect.defineProperty(error, 'code', { enumerable: true })
  }

  if ('stack' in error) {
    Reflect.defineProperty(error, 'stack', { enumerable: true })
  }

  return error
}
