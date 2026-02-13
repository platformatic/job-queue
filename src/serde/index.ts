/**
 * Serialization/deserialization interface for queue messages
 */
export interface Serde<T> {
  serialize (value: T): Buffer
  deserialize (buffer: Buffer): T
}

/**
 * JSON serializer - default implementation
 */
export class JsonSerde<T> implements Serde<T> {
  serialize (value: T): Buffer {
    return Buffer.from(JSON.stringify(value))
  }

  deserialize (buffer: Buffer): T {
    return JSON.parse(buffer.toString()) as T
  }
}

/**
 * Create a JSON serde instance
 */
export function createJsonSerde<T> (): Serde<T> {
  return new JsonSerde<T>()
}
