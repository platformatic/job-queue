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

  toJSON (): Record<string, unknown> {
    return {
      name: this.name,
      message: this.message,
      code: this.code,
      stack: this.stack
    }
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

  toJSON (): Record<string, unknown> {
    return {
      ...super.toJSON(),
      jobId: this.jobId
    }
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

  toJSON (): Record<string, unknown> {
    return {
      ...super.toJSON(),
      jobId: this.jobId,
      attempts: this.attempts,
      lastError: {
        name: this.lastError.name,
        message: this.lastError.message,
        stack: this.lastError.stack
      }
    }
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

  toJSON (): Record<string, unknown> {
    return {
      ...super.toJSON(),
      jobId: this.jobId
    }
  }
}

/**
 * Storage operation failed
 */
export class StorageError extends JobQueueError {
  override cause?: Error

  constructor (message: string, cause?: Error) {
    super(message, 'STORAGE_ERROR')
    this.name = 'StorageError'
    this.cause = cause
  }

  toJSON (): Record<string, unknown> {
    const json: Record<string, unknown> = { ...super.toJSON() }
    if (this.cause) {
      json.cause = {
        name: this.cause.name,
        message: this.cause.message,
        stack: this.cause.stack
      }
    }
    return json
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

  toJSON (): Record<string, unknown> {
    return {
      ...super.toJSON(),
      jobId: this.jobId
    }
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

  toJSON (): Record<string, unknown> {
    return {
      ...super.toJSON(),
      jobId: this.jobId,
      originalError: this.originalError
    }
  }
}
