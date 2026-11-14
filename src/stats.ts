import { type Duration, type Outcome, type TimeProvider, check, onTelemetry, toMs, unrefSystemTime } from './core.ts';

export interface StatsOptions {
  /** Only count these pipelines (by `name`). Default: every pipeline in the process. */
  pipeline?: string | string[];
  /** Length of the rolling window. Default 10 s, kept as 10 buckets: data ages out one bucket (window / 10) at a time. */
  window?: Duration;
  /** Latency percentiles to report, as fractions. Default `[0.5, 0.9, 0.99]` → `p50`, `p90`, `p99`. */
  percentiles?: number[];
  /** Default: the system clock with unref'd timers. */
  timeProvider?: TimeProvider;
}

/** Milliseconds. `mean` and `max` are exact; percentiles are within 1% of the true value (see `createStats`). Keys `p50`, `p90`, `p99`, `p99.9`… */
export type LatencyStats = { mean: number; max: number } & Record<`p${string}`, number>;

export interface Counts {
  /** Finished pipeline executions (`OnPipelineExecuted`). */
  executions: number;
  successes: number;
  failures: number;
  timeouts: number;
  retries: number;
  /** Calls refused by an open circuit, a rate limiter, a bulkhead or a held lock. */
  rejections: number;
  fallbacks: number;
  cacheHits: number;
  cacheMisses: number;
  coalesced: number;
  latency: LatencyStats;
}

export interface StatsSnapshot extends Counts {
  /** Epoch ms when the snapshot was taken. */
  timestamp: number;
  /** Length of the rolling window the top-level counts cover, in ms. */
  window: number;
  /** Everything since `createStats()`. */
  total: Counts;
}

export interface Stats {
  snapshot(): StatsSnapshot;
  /** Calls `listener` with a snapshot every `interval`. Returns a function that stops it. */
  onSnapshot(listener: (snapshot: StatsSnapshot) => void, interval: Duration): () => void;
  /** Stops listening to telemetry and every `onSnapshot` timer. */
  dispose(): void;
}

type Counter = Exclude<keyof Counts, 'latency'>;
const COUNTERS: Record<string, Counter> = {
  OnTimeout: 'timeouts',
  OnRetry: 'retries',
  OnCircuitRejected: 'rejections',
  OnRateLimiterRejected: 'rejections',
  OnLockRejected: 'rejections',
  OnFallback: 'fallbacks',
  OnCacheHit: 'cacheHits',
  OnCacheMiss: 'cacheMisses',
  OnCoalesced: 'coalesced',
};

// Log-scale latency histogram: bin i holds (MIN·G^(i-1), MIN·G^i], bin 0 holds [0, MIN]. Reporting the bin's geometric
// middle keeps every percentile within ±1% of the true sample, and memory is bounded by the number of distinct bins
// (~1100 between 1 µs and 1 hour), not by the number of calls.
const MIN = 0.001;
const G = 1.02;
const LOG_G = Math.log(G);
const binOf = (ms: number) => (ms <= MIN ? 0 : Math.ceil(Math.log(ms / MIN) / LOG_G));
const valueOf = (bin: number) => (bin === 0 ? 0 : MIN * G ** (bin - 0.5));

interface Bucket {
  id: number;
  counts: Record<Counter, number>;
  bins: Map<number, number>;
  sum: number;
  min: number;
  max: number;
}

const emptyBucket = (id: number): Bucket => ({
  id,
  counts: { executions: 0, successes: 0, failures: 0, timeouts: 0, retries: 0, rejections: 0, fallbacks: 0, cacheHits: 0, cacheMisses: 0, coalesced: 0 },
  bins: new Map(),
  sum: 0,
  min: Infinity,
  max: 0,
});

const BUCKETS = 10;

/**
 * Opossum-style in-process statistics for every pipeline (or the ones named in `pipeline`), built on `onTelemetry`:
 * counts over a rolling window and since start, plus latency mean / max / percentiles of whole executions.
 *
 * ```ts
 * const stats = createStats({ pipeline: 'payments' });
 * app.get('/health/payments', () => stats.snapshot());
 * stats.onSnapshot((s) => log.info(s), '30s');
 * ```
 */
export function createStats(options: StatsOptions = {}): Stats {
  const time = options.timeProvider ?? unrefSystemTime;
  const windowMs = toMs(options.window ?? 10_000, 'window');
  check(windowMs > 0, 'window must be positive');
  const percentiles = options.percentiles ?? [0.5, 0.9, 0.99];
  check(percentiles.every((p) => p >= 0 && p <= 1), 'percentiles must be between 0 and 1');
  const names = options.pipeline === undefined ? undefined : new Set([options.pipeline].flat());
  const bucketMs = windowMs / BUCKETS;
  const ring: Bucket[] = Array.from({ length: BUCKETS }, () => emptyBucket(-Infinity));
  const total = emptyBucket(0);
  const timers = new Set<() => void>();

  const current = () => {
    const id = Math.floor(time.now() / bucketMs);
    const slot = ((id % BUCKETS) + BUCKETS) % BUCKETS;
    if (ring[slot]!.id !== id) ring[slot] = emptyBucket(id);
    return ring[slot]!;
  };

  const unsubscribe = onTelemetry((e) => {
    if (names && !names.has(e.source.pipelineName!)) return;
    const counter = COUNTERS[e.event];
    if (e.event === 'OnPipelineExecuted') {
      const { outcome, duration } = e.args as { outcome: Outcome<unknown>; duration: number };
      for (const b of [current(), total]) {
        b.counts.executions++;
        b.counts[outcome.ok ? 'successes' : 'failures']++;
        const bin = binOf(duration);
        b.bins.set(bin, (b.bins.get(bin) ?? 0) + 1);
        b.sum += duration;
        b.min = Math.min(b.min, duration);
        b.max = Math.max(b.max, duration);
      }
      return;
    }
    if (counter) {
      current().counts[counter]++;
      total.counts[counter]++;
    }
  });

  const summarize = (buckets: Bucket[]): Counts => {
    const counts = emptyBucket(0);
    for (const b of buckets) {
      for (const k of Object.keys(counts.counts) as Counter[]) counts.counts[k] += b.counts[k];
      for (const [bin, n] of b.bins) counts.bins.set(bin, (counts.bins.get(bin) ?? 0) + n);
      counts.sum += b.sum;
      counts.min = Math.min(counts.min, b.min);
      counts.max = Math.max(counts.max, b.max);
    }
    const n = counts.counts.executions;
    const latency = { mean: n ? counts.sum / n : 0, max: counts.max } as LatencyStats;
    const sorted = [...counts.bins].sort((a, b) => a[0] - b[0]);
    for (const p of percentiles) {
      // Nearest-rank percentile, clamped to the exact min/max so small samples report real values.
      const rank = Math.max(1, Math.ceil(p * n));
      let seen = 0;
      const bin = sorted.find(([, c]) => (seen += c) >= rank)?.[0];
      latency[`p${+(p * 100).toFixed(4)}`] = bin === undefined ? 0 : Math.min(counts.max, Math.max(counts.min, valueOf(bin)));
    }
    return { ...counts.counts, latency };
  };

  const snapshot = (): StatsSnapshot => {
    const oldest = Math.floor(time.now() / bucketMs) - BUCKETS;
    return { timestamp: time.now(), window: windowMs, ...summarize(ring.filter((b) => b.id > oldest)), total: summarize([total]) };
  };

  return {
    snapshot,
    onSnapshot(listener, interval) {
      const ms = toMs(interval, 'interval');
      check(ms > 0, 'interval must be positive');
      let handle: unknown;
      const tick = () => {
        handle = time.setTimeout(tick, ms);
        listener(snapshot());
      };
      handle = time.setTimeout(tick, ms);
      const stop = () => {
        time.clearTimeout(handle);
        timers.delete(stop);
      };
      timers.add(stop);
      return stop;
    },
    dispose() {
      unsubscribe();
      for (const stop of [...timers]) stop();
    },
  };
}
