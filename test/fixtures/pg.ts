import { PgStorage } from '../../src/storage/pg.ts'

/**
 * Create a PgStorage instance for testing.
 * Each test gets its own table prefix to avoid conflicts.
 */
export function createPgStorage (): PgStorage {
  const suffix = `${Date.now()}_${Math.random().toString(36).slice(2)}`
  return new PgStorage({
    connectionString: process.env.DATABASE_URL ?? 'postgresql://postgres:postgres@localhost:5432/job_queue_test',
    tablePrefix: `t${suffix}_`
  })
}
