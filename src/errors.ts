/**
 * Base error for job queue errors
 */
export class JobQueueError extends Error {
  code: string

  constructor (message: string, code: string) {
    super(message)
    this.name = 'JobQueueError'
    this.code = code
    Error.captureStackTrace(this, this.constructor)
  }
}

/**
 * Timeout waiting for job result
 */
export class TimeoutError extends JobQueueError {
  jobId: string

  constructor (jobId: string, timeout: number) {
    super(`Job '${jobId}' timed out after ${timeout}ms`, 'TIMEOUT')
    this.name = 'TimeoutError'
    this.jobId = jobId
  }
}

/**
 * Job failed after max retries
 */
export class MaxRetriesError extends JobQueueError {
  jobId: string
  attempts: number
  lastError: Error

  constructor (jobId: string, attempts: number, lastError: Error) {
    super(`Job '${jobId}' failed after ${attempts} attempts: ${lastError.message}`, 'MAX_RETRIES')
    this.name = 'MaxRetriesError'
    this.jobId = jobId
    this.attempts = attempts
    this.lastError = lastError
  }
}

/**
 * Job not found
 */
export class JobNotFoundError extends JobQueueError {
  jobId: string

  constructor (jobId: string) {
    super(`Job '${jobId}' not found`, 'JOB_NOT_FOUND')
    this.name = 'JobNotFoundError'
    this.jobId = jobId
  }
}

/**
 * Storage operation failed
 */
export class StorageError extends JobQueueError {
  cause?: Error

  constructor (message: string, cause?: Error) {
    super(message, 'STORAGE_ERROR')
    this.name = 'StorageError'
    this.cause = cause
  }
}

/**
 * Job was cancelled
 */
export class JobCancelledError extends JobQueueError {
  jobId: string

  constructor (jobId: string) {
    super(`Job '${jobId}' was cancelled`, 'JOB_CANCELLED')
    this.name = 'JobCancelledError'
    this.jobId = jobId
  }
}

/**
 * Job failed
 */
export class JobFailedError extends JobQueueError {
  jobId: string
  originalError: string

  constructor (jobId: string, errorMessage: string) {
    super(`Job '${jobId}' failed: ${errorMessage}`, 'JOB_FAILED')
    this.name = 'JobFailedError'
    this.jobId = jobId
    this.originalError = errorMessage
  }
}
