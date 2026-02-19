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
  resultTTL?: number
  correlationId?: string
}

/**
 * Job state in the jobs registry
 */
export type MessageState = 'queued' | 'processing' | 'failing' | 'completed' | 'failed'

/**
 * Serialized error information
 */
export interface SerializedError {
  message: string
  code?: string
  stack?: string
}

/**
 * Job status with metadata
 */
export interface MessageStatus<TResult = unknown> {
  id: string
  state: MessageState
  createdAt: number
  attempts: number
  result?: TResult
  error?: SerializedError
}

/**
 * Result of updating TTL for a terminal job payload (result/error)
 */
export type UpdateResultTTLResult =
  | { status: 'updated' }
  | { status: 'not_found' }
  | { status: 'not_terminal' }
  | { status: 'missing_payload' }

/**
 * Options for enqueue operation
 */
export interface EnqueueOptions {
  maxAttempts?: number
  resultTTL?: number
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
 * Context passed to afterExecution hook.
 */
export interface AfterExecutionContext<TPayload, TResult> {
  id: string
  payload: TPayload
  attempts: number
  maxAttempts: number
  createdAt: number
  status: 'completed' | 'failed'
  result?: TResult
  error?: Error
  ttl: number
  workerId: string
  startedAt: number
  finishedAt: number
  durationMs: number
}

/**
 * Hook executed after handler execution and before writing terminal state.
 */
export type AfterExecutionHook<TPayload, TResult> = (
  context: AfterExecutionContext<TPayload, TResult>
) => void | Promise<void>

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

  /** Hook called after execution and before persisting terminal state */
  afterExecution?: AfterExecutionHook<TPayload, TResult>

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

  /** TTL for stored results and errors in ms (default: 3600000 = 1 hour) */
  resultTTL?: number
}

/**
 * Queue events (tuple format for EventEmitter)
 */
export interface QueueEvents<TResult> {
  started: []
  stopped: []
  error: [error: Error]
  enqueued: [id: string]
  completed: [id: string, result: TResult]
  failed: [id: string, error: Error]
  failing: [id: string, error: Error, attempt: number]
  requeued: [id: string]
  cancelled: [id: string]
  stalled: [id: string]
}
