import type { MessageState } from '../types.ts'

export interface ParsedState {
  status: MessageState
  timestamp: number
  workerId?: string
  attempts?: number
}

/**
 * Parse job state string into components.
 * State format: "status:timestamp[:workerId]" or "failing:timestamp:attempts"
 */
export function parseState (state: string): ParsedState {
  const parts = state.split(':')
  const status = parts[0] as MessageState
  const timestamp = parseInt(parts[1], 10)

  if (status === 'failing') {
    return {
      status,
      timestamp,
      attempts: parseInt(parts[2], 10)
    }
  }

  return {
    status,
    timestamp,
    workerId: parts[2]
  }
}
