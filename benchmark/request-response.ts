/**
 * Benchmark: Request/Response over Redis
 *
 * Measures latency and throughput for enqueueAndWait operations.
 * Uses worker threads for true parallelism between producer and consumer.
 *
 * Usage:
 *   REDIS_URL=redis://localhost:6379 node benchmark/request-response.ts
 */

import { Worker, isMainThread, parentPort, workerData } from 'node:worker_threads'
import { fileURLToPath } from 'node:url'
import { Queue, RedisStorage } from '../src/index.ts'

interface BenchmarkPayload {
  data: string
  timestamp: number
}

interface BenchmarkResult {
  processed: boolean
  latency: number
}

interface Stats {
  count: number
  totalMs: number
  minMs: number
  maxMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  throughput: number
}

interface BenchmarkConfig {
  name: string
  requests: number
  concurrency: number
  payloadSize: number
}

interface WorkerMessage {
  type: 'ready' | 'done' | 'error'
  error?: string
}

function calculateStats (latencies: number[], durationMs: number): Stats {
  const sorted = [...latencies].sort((a, b) => a - b)
  const count = sorted.length
  const totalMs = sorted.reduce((sum, l) => sum + l, 0)

  return {
    count,
    totalMs,
    minMs: sorted[0] ?? 0,
    maxMs: sorted[count - 1] ?? 0,
    p50Ms: sorted[Math.floor(count * 0.5)] ?? 0,
    p95Ms: sorted[Math.floor(count * 0.95)] ?? 0,
    p99Ms: sorted[Math.floor(count * 0.99)] ?? 0,
    throughput: count / (durationMs / 1000)
  }
}

function formatStats (name: string, stats: Stats): string {
  return `
${name}
${'─'.repeat(50)}
  Requests:    ${stats.count}
  Throughput:  ${stats.throughput.toFixed(2)} req/s
  Latency:
    min:       ${stats.minMs.toFixed(2)} ms
    p50:       ${stats.p50Ms.toFixed(2)} ms
    p95:       ${stats.p95Ms.toFixed(2)} ms
    p99:       ${stats.p99Ms.toFixed(2)} ms
    max:       ${stats.maxMs.toFixed(2)} ms
    avg:       ${(stats.totalMs / stats.count).toFixed(2)} ms
`
}

// Consumer worker code
async function runConsumer (): Promise<void> {
  const { redisUrl, keyPrefix } = workerData as { redisUrl: string, keyPrefix: string }

  const storage = new RedisStorage({
    url: redisUrl,
    keyPrefix
  })

  const queue = new Queue<BenchmarkPayload, BenchmarkResult>({
    storage,
    workerId: 'consumer',
    concurrency: 100
  })

  queue.execute(async (job) => {
    const latency = Date.now() - job.payload.timestamp
    return { processed: true, latency }
  })

  queue.on('error', (err) => {
    console.error('Consumer error:', err)
  })

  await queue.start()

  // Signal ready
  parentPort!.postMessage({ type: 'ready' })

  // Wait for done signal
  await new Promise<void>((resolve) => {
    parentPort!.on('message', async (msg: string) => {
      if (msg === 'stop') {
        await queue.stop()
        resolve()
      }
    })
  })

  parentPort!.postMessage({ type: 'done' })
}

// Main thread code
async function runBenchmark (
  redisUrl: string,
  keyPrefix: string,
  config: BenchmarkConfig
): Promise<Stats> {
  const { requests, concurrency, payloadSize } = config
  const payload = 'x'.repeat(payloadSize)
  const latencies: number[] = []

  // Create producer
  const storage = new RedisStorage({
    url: redisUrl,
    keyPrefix
  })

  const producer = new Queue<BenchmarkPayload, BenchmarkResult>({
    storage,
    workerId: 'producer'
  })

  await producer.start()

  let completed = 0
  let jobId = 0

  const startTime = performance.now()

  // Run requests with concurrency limit
  const workers: Promise<void>[] = []

  for (let i = 0; i < concurrency; i++) {
    workers.push((async () => {
      while (completed < requests) {
        const id = `${config.name}-${jobId++}`
        if (jobId > requests) break

        const requestStart = performance.now()
        try {
          await producer.enqueueAndWait(id, {
            data: payload,
            timestamp: Date.now()
          }, { timeout: 30000 })

          const latency = performance.now() - requestStart
          latencies.push(latency)
          completed++
        } catch (err) {
          console.error(`Request ${id} failed:`, err)
        }
      }
    })())
  }

  await Promise.all(workers)

  const durationMs = performance.now() - startTime

  await producer.stop()

  return calculateStats(latencies, durationMs)
}

async function main (): Promise<void> {
  const redisUrl = process.env.REDIS_URL
  if (!redisUrl) {
    console.error('REDIS_URL environment variable is required')
    process.exit(1)
  }

  console.log(`
╔══════════════════════════════════════════════════╗
║     Request/Response Benchmark over Redis        ║
╚══════════════════════════════════════════════════╝

Redis URL: ${redisUrl}
`)

  const keyPrefix = `bench:${Date.now()}:`

  // Start consumer worker
  const consumerWorker = new Worker(fileURLToPath(import.meta.url), {
    workerData: { redisUrl, keyPrefix }
  })

  // Wait for consumer to be ready
  await new Promise<void>((resolve, reject) => {
    consumerWorker.on('message', (msg: WorkerMessage) => {
      if (msg.type === 'ready') resolve()
      if (msg.type === 'error') reject(new Error(msg.error))
    })
    consumerWorker.on('error', reject)
  })

  console.log('Consumer worker started.\n')

  // Warm up
  console.log('Warming up...')
  const warmupStorage = new RedisStorage({ url: redisUrl, keyPrefix })
  const warmupQueue = new Queue<BenchmarkPayload, BenchmarkResult>({
    storage: warmupStorage,
    workerId: 'warmup-producer'
  })
  await warmupQueue.start()

  for (let i = 0; i < 100; i++) {
    await warmupQueue.enqueueAndWait(`warmup-${i}`, { data: 'warmup', timestamp: Date.now() }, { timeout: 5000 })
  }
  await warmupQueue.stop()
  console.log('Warmup complete.\n')

  // Run benchmarks
  console.log('Running benchmarks...\n')

  const benchmarks: BenchmarkConfig[] = [
    { name: 'baseline', requests: 100, concurrency: 1, payloadSize: 100 },
    { name: 'low-conc', requests: 500, concurrency: 10, payloadSize: 100 },
    { name: 'high-conc', requests: 1000, concurrency: 50, payloadSize: 100 },
    { name: 'large-payload', requests: 200, concurrency: 10, payloadSize: 10000 },
    { name: 'very-high-conc', requests: 2000, concurrency: 100, payloadSize: 100 }
  ]

  const labels = [
    'Single Request Baseline (100 requests, concurrency=1)',
    'Low Concurrency (500 requests, concurrency=10)',
    'High Concurrency (1000 requests, concurrency=50)',
    'Large Payload (200 requests, 10KB payload)',
    'Very High Concurrency (2000 requests, concurrency=100)'
  ]

  for (let i = 0; i < benchmarks.length; i++) {
    const stats = await runBenchmark(redisUrl, keyPrefix, benchmarks[i])
    console.log(formatStats(labels[i], stats))
  }

  // Stop consumer worker
  consumerWorker.postMessage('stop')
  await new Promise<void>((resolve) => {
    consumerWorker.on('message', (msg: WorkerMessage) => {
      if (msg.type === 'done') resolve()
    })
  })
  await consumerWorker.terminate()

  // Cleanup
  const cleanupStorage = new RedisStorage({ url: redisUrl, keyPrefix })
  await cleanupStorage.connect()
  await cleanupStorage.clear()
  await cleanupStorage.disconnect()

  console.log('Benchmark complete.')
}

// Entry point
if (isMainThread) {
  main().catch((err) => {
    console.error('Benchmark failed:', err)
    process.exit(1)
  })
} else {
  runConsumer().catch((err) => {
    parentPort!.postMessage({ type: 'error', error: err.message })
  })
}
