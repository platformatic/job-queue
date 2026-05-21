import { SQLiteStorage } from '../../src/storage/sqlite.ts'

/**
 * Create a SQLiteStorage instance for testing.
 * Uses :memory: by default; each instance gets its own DB.
 */
export function createSQLiteStorage (overrides: { path?: string; tablePrefix?: string } = {}): SQLiteStorage {
  return new SQLiteStorage({
    path: overrides.path ?? ':memory:',
    tablePrefix: overrides.tablePrefix ?? 'jq_'
  })
}
