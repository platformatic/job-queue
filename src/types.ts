import type { Storage } from './storage/types.ts'
import type { Serde } from './serde/index.ts'

/**
 * Message stored in the queue
 */
export interface QueueMessage<TPayload> {
  id: string
  payload: TPayload
  createdAt: number
  attempts: number
  maxAttempts: number
  correlationId?: string
}

/**
 * Job state in the jobs registry
 */
export type MessageState = 'queued' | 'processing' | 'failing' | 'completed' | 'failed'

/**
 * Job status with metadata
 */
export interface MessageStatus<TResult = unknown> {
  id: string
  state: MessageState
  createdAt: number
  attempts: number
  result?: TResult
  error?: string
}

/**
 * Options for enqueue operation
 */
export interface EnqueueOptions {
  maxAttempts?: number
}

/**
 * Options for enqueueAndWait operation
 */
export interface EnqueueAndWaitOptions extends EnqueueOptions {
  timeout?: number
}

/**
 * Result of enqueue operation
 */
export type EnqueueResult<TResult = unknown> =
  | { status: 'queued' }
  | { status: 'duplicate'; existingState: MessageState }
  | { status: 'completed'; result: TResult }

/**
 * Result of cancel operation
 */
export type CancelResult =
  | { status: 'cancelled' }
  | { status: 'not_found' }
  | { status: 'processing' }
  | { status: 'completed' }

/**
 * Job passed to the handler
 */
export interface Job<TPayload> {
  id: string
  payload: TPayload
  attempts: number
  signal: AbortSignal
}

/**
 * Job handler function
 */
export type JobHandler<TPayload, TResult> =
  | ((job: Job<TPayload>) => Promise<TResult>)
  | ((job: Job<TPayload>, callback: (err: Error | null, result?: TResult) => void) => void)

/**
 * Queue configuration
 */
export interface QueueConfig<TPayload, TResult> {
  /** Storage backend (required) */
  storage: Storage

  /** Payload serializer (default: JSON) */
  payloadSerde?: Serde<TPayload>

  /** Result serializer (default: JSON) */
  resultSerde?: Serde<TResult>

  /** Unique worker ID (default: random UUID) */
  workerId?: string

  /** Parallel job processing (default: 1) */
  concurrency?: number

  /** Blocking dequeue timeout in seconds (default: 5) */
  blockTimeout?: number

  /** Default max retry attempts (default: 3) */
  maxRetries?: number

  /** Max processing time before job is considered stalled in ms (default: 30000) */
  visibilityTimeout?: number

  /** TTL for processing queue keys in ms (default: 604800000 = 7 days) */
  processingQueueTTL?: number

  /** Result retention in ms (default: 3600000 = 1 hour) */
  resultTTL?: number

  /** How long to keep completed/failed jobs in ms (default: 86400000 = 24h) */
  jobsTTL?: number
}

/**
 * Queue events (tuple format for EventEmitter)
 */
export interface QueueEvents<TResult> {
  error: [error: Error]
  completed: [id: string, result: TResult]
  failed: [id: string, error: Error]
  cancelled: [id: string]
  stalled: [id: string]
}
