import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { FileStorage } from '../../src/storage/file.ts'

/**
 * Create a FileStorage instance for testing
 */
export async function createFileStorage (config?: {
  basePath?: string
}): Promise<{ storage: FileStorage; basePath: string; cleanup: () => Promise<void> }> {
  const basePath = config?.basePath ?? (await mkdtemp(join(tmpdir(), 'job-queue-test-')))

  const storage = new FileStorage({ basePath })

  const cleanup = async () => {
    await rm(basePath, { recursive: true, force: true })
  }

  return { storage, basePath, cleanup }
}
