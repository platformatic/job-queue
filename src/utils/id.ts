import { randomUUID, createHash } from 'node:crypto'

/**
 * Generate a random unique ID
 */
export function generateId (): string {
  return randomUUID()
}

/**
 * Generate a content-based ID by hashing the payload
 * Useful for deduplication based on content
 */
export function contentId (payload: unknown): string {
  return createHash('sha256')
    .update(JSON.stringify(payload))
    .digest('hex')
    .slice(0, 16)
}
