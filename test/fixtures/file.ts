import { mkdtemp, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { FileStorage } from '../../src/storage/file.ts'

/**
 * Create a FileStorage instance for testing
 */
export async function createFileStorage (): Promise<{ storage: FileStorage, cleanup: () => Promise<void> }> {
  const basePath = await mkdtemp(join(tmpdir(), 'job-queue-test-'))

  const storage = new FileStorage({ basePath })

  const cleanup = async () => {
    await rm(basePath, { recursive: true, force: true })
  }

  return { storage, cleanup }
}
