import { type Duration, type TimeProvider, onTelemetry, toMs, unrefSystemTime } from './core.ts';
import { type StatsSnapshot, createStats } from './stats.ts';

export interface HystrixOptions {
  /** Pipeline names to report, one HystrixCommand each. */
  pipelines: string[];
  /** Hystrix command group. Default '@nodelibraries/resilience'. */
  group?: string;
  /** Rolling window. Default 10 s (Hystrix's default). */
  window?: Duration;
  /** How often the stream handler writes events. Default 1 s. */
  interval?: Duration;
  timeProvider?: TimeProvider;
}

const PERCENTILES = [0, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.995, 1];

interface StreamRequest {
  once(event: 'close', listener: () => void): unknown;
}
interface StreamResponse {
  writeHead(status: number, headers: Record<string, string>): unknown;
  write(chunk: string): unknown;
}

export interface HystrixStream {
  /** One Hystrix-format `HystrixCommand` object per pipeline, for the current window. */
  commands(): Array<Record<string, unknown>>;
  /** Node `(req, res)` handler streaming `text/event-stream` for the Hystrix dashboard / Turbine: `app.get('/hystrix.stream', stream.handler)`. */
  handler(req: StreamRequest, res: StreamResponse): void;
  /** Stops telemetry listeners and every open stream's timer. */
  dispose(): void;
}

/**
 * Legacy Hystrix dashboard metrics (opossum-hystrix compatible), from `createStats` and circuit breaker telemetry.
 * Mapping is approximate: errors are failed executions; short-circuits are circuit rejections; other rejections
 * (bulkhead, rate limit, lock) are reported as semaphore rejections; failures exclude both and timeouts.
 */
export function createHystrixStream(options: HystrixOptions): HystrixStream {
  const time = options.timeProvider ?? unrefSystemTime;
  const windowMs = toMs(options.window ?? 10_000, 'window');
  const intervalMs = toMs(options.interval ?? 1000, 'interval');
  const group = options.group ?? '@nodelibraries/resilience';
  const stats = new Map(options.pipelines.map((name) => [name, createStats({ pipeline: name, window: windowMs, percentiles: PERCENTILES, timeProvider: time })]));
  const open = new Set<string>();
  // shortcut: one timestamp per circuit rejection in the window; switch to buckets if short-circuit rates get huge.
  const shortCircuits = new Map<string, number[]>(options.pipelines.map((name) => [name, []]));
  const recent = (name: string) => {
    const list = shortCircuits.get(name)!;
    while (list.length && list[0]! <= time.now() - windowMs) list.shift();
    return list;
  };
  const unsubscribe = onTelemetry(({ event, source }) => {
    const name = source.pipelineName!;
    if (!stats.has(name)) return;
    if (event === 'OnCircuitOpened' || event === 'OnCircuitHalfOpened') open.add(name);
    else if (event === 'OnCircuitClosed') open.delete(name);
    else if (event === 'OnCircuitRejected') recent(name).push(time.now());
  });
  const streams = new Set<() => void>();

  const command = (name: string, s: StatsSnapshot): Record<string, unknown> => {
    const shortCircuited = recent(name).length;
    const semaphoreRejected = Math.max(0, s.rejections - shortCircuited);
    const latency = Object.fromEntries(PERCENTILES.map((p) => [String(+(p * 100).toFixed(4)), Math.round(s.latency[`p${+(p * 100).toFixed(4)}`]!)]));
    return {
      type: 'HystrixCommand',
      name,
      group,
      currentTime: s.timestamp,
      isCircuitBreakerOpen: open.has(name),
      errorPercentage: s.executions ? Math.round((s.failures / s.executions) * 100) : 0,
      errorCount: s.failures,
      requestCount: s.executions,
      rollingCountSuccess: s.successes,
      rollingCountFailure: Math.max(0, s.failures - s.rejections - s.timeouts),
      rollingCountTimeout: s.timeouts,
      rollingCountShortCircuited: shortCircuited,
      rollingCountSemaphoreRejected: semaphoreRejected,
      rollingCountThreadPoolRejected: 0,
      rollingCountFallbackSuccess: s.fallbacks,
      rollingCountFallbackFailure: 0,
      rollingCountFallbackRejection: 0,
      rollingCountExceptionsThrown: 0,
      rollingCountBadRequests: 0,
      rollingCountCollapsedRequests: s.coalesced,
      rollingCountResponsesFromCache: s.cacheHits,
      currentConcurrentExecutionCount: 0,
      rollingMaxConcurrentExecutionCount: 0,
      latencyExecute_mean: Math.round(s.latency.mean),
      latencyExecute: latency,
      latencyTotal_mean: Math.round(s.latency.mean),
      latencyTotal: latency,
      propertyValue_circuitBreakerEnabled: true,
      propertyValue_circuitBreakerForceOpen: false,
      propertyValue_circuitBreakerForceClosed: false,
      propertyValue_executionIsolationStrategy: 'SEMAPHORE',
      propertyValue_metricsRollingStatisticalWindowInMilliseconds: windowMs,
      propertyValue_requestCacheEnabled: false,
      propertyValue_requestLogEnabled: false,
      reportingHosts: 1,
    };
  };
  const commands = () => [...stats].map(([name, s]) => command(name, s.snapshot()));

  return {
    commands,
    handler(req, res) {
      res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache', connection: 'keep-alive' });
      let handle: unknown;
      const tick = () => {
        for (const c of commands()) res.write(`data: ${JSON.stringify(c)}\n\n`);
        handle = time.setTimeout(tick, intervalMs);
      };
      const stop = () => {
        time.clearTimeout(handle);
        streams.delete(stop);
      };
      streams.add(stop);
      req.once('close', stop);
      tick();
    },
    dispose() {
      unsubscribe();
      for (const s of stats.values()) s.dispose();
      for (const stop of [...streams]) stop();
    },
  };
}
