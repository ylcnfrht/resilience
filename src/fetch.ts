import { PipelineBuilder, type StrategyDefinition } from './pipeline-builder.ts';
import { type Duration, type MaybePromise, type Outcome, type OutcomeArgs, type ResilienceContext, type TimeProvider, defaultShouldHandle } from './core.ts';
import { type CircuitBreakerOptions, circuitBreakerStrategy } from './circuit-breaker.ts';
import { type ResiliencePipeline } from './pipeline.ts';
import { type ConcurrencyLimiterOptions, concurrencyLimiterStrategy } from './rate-limiter.ts';
import { type RetryOptions, retryStrategy } from './retry.ts';
import { timeoutStrategy } from './timeout.ts';

export interface RetryOnArgs<R> {
  /** The response, when one arrived (also for axios errors that carry one). */
  readonly response: R | undefined;
  /** The error, when the attempt failed. */
  readonly error: unknown;
  /** 0 for the first attempt. */
  readonly attemptNumber: number;
}

/** Status codes to retry (network errors and timeouts are still retried), or a predicate that decides alone. */
export type RetryOn<R> = readonly number[] | ((args: RetryOnArgs<R>) => MaybePromise<boolean>);

/** Per-request overrides: `http(url, { resilience: { retries: 0 } })`. */
export interface RequestResilience<R> {
  /** Retries for this request. `false` or 0: a single attempt. */
  retries?: number | false;
  retryOn?: RetryOn<R>;
  /** Budget for this request, retries included. `Infinity` disables it. */
  timeout?: Duration;
  /** Budget per attempt for this request. */
  attemptTimeout?: Duration;
  /** Force (`true`) or forbid (`false`) retrying, whatever the method and headers. */
  idempotent?: boolean;
}

/** Options shared by `createResilientFetch` and the axios integration. */
export interface HttpResilienceOptions<R> {
  /** Labels telemetry. Per-host pipelines are named `<name>/<key>`. */
  name?: string;
  /** Budget for the whole call, retries included. Default 30 s. */
  totalTimeout?: Duration;
  /** Budget per attempt. Default 10 s. */
  attemptTimeout?: Duration;
  /** Retry options merged over the defaults (3 retries, exponential backoff from 200 ms, full jitter, Retry-After honoured). `false` disables retries. */
  retry?: Omit<RetryOptions<R>, 'shouldHandle'> | false;
  /** What to retry. Default: network errors, timeouts and 408/429/500/502/503/504. */
  retryOn?: RetryOn<R>;
  /** Circuit breaker options merged over the defaults (50% failures over the last 20 calls, 10 s break). `false` disables it. */
  circuitBreaker?: Omit<CircuitBreakerOptions<R>, 'shouldHandle'> | false;
  /** Max attempts in flight per key (bulkhead), e.g. `20` or `{ permitLimit: 20, queueLimit: 100 }`. Default: no limit. */
  concurrency?: number | ConcurrencyLimiterOptions;
  /**
   * Which requests share a circuit breaker and limiter. Default `url => url.origin`: one per host, so a dead host doesn't
   * open the breaker for the others. `() => 'all'` shares one.
   */
  pipelineKey?: (url: URL) => string;
  /** Max per-key pipelines kept; the least recently used is dropped beyond it. Default 100. */
  maxPipelines?: number;
  /** Retry POST / PATCH without an `Idempotency-Key` header too. Default false: they could run twice. */
  retryUnsafeMethods?: boolean;
  timeProvider?: TimeProvider;
}

export interface ResilientFetchOptions extends HttpResilienceOptions<Response> {
  /** The fetch to wrap. Default `globalThis.fetch`. */
  fetch?: typeof fetch;
  /** Use your own pipeline instead of the built-in one (per-request `retries`, `timeout`, `retryOn` and per-host keys are then ignored). */
  pipeline?: ResiliencePipeline<Response>;
  /** Called before every attempt with a fresh Request; return a replacement, e.g. with a refreshed auth token. */
  onBeforeAttempt?: (request: Request, context: ResilienceContext) => MaybePromise<Request | void>;
}

export interface ResilientRequestInit extends RequestInit {
  resilience?: RequestResilience<Response>;
}

type FetchInput = Parameters<typeof fetch>[0];

/** A `fetch` that also takes `init.resilience`. Still assignable to `typeof fetch`. */
export type ResilientFetch = ((input: FetchInput, init?: ResilientRequestInit) => Promise<Response>) & typeof fetch;

/** @internal How the shared HTTP pipeline reads a client's responses and errors. */
export interface HttpAdapter<R extends { status: number }> {
  response(outcome: Outcome<R>): R | undefined;
  header(response: R, name: string): string | null | undefined;
  /** Which errors without a response are transient. Default: any not excluded by `defaultShouldHandle`. */
  isTransientError?(error: unknown): boolean;
  /** Release a response that is discarded for a retry. */
  discard?(response: R): unknown;
}

const REQUEST = '@ylcnfrht/resilience:http:request';
const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS', 'TRACE', 'PUT', 'DELETE']);
const TRANSIENT_STATUS: readonly number[] = [408, 429, 500, 502, 503, 504];

interface RequestState<R> {
  readonly url: URL | undefined;
  readonly resilience: RequestResilience<R>;
}
const isStream = (body: any) => body instanceof ReadableStream || typeof body?.pipe === 'function' || typeof body?.[Symbol.asyncIterator] === 'function';
const requestOf = <R>(context: ResilienceContext) => context.properties.get(REQUEST) as RequestState<R> | undefined;

function isTransient<R extends { status: number }>(args: OutcomeArgs<R>, adapter: HttpAdapter<R>, statuses: readonly number[]): boolean {
  const response = adapter.response(args.outcome);
  if (response) return statuses.includes(response.status);
  return defaultShouldHandle(args) && (adapter.isTransientError?.((args.outcome as { error: unknown }).error) ?? true);
}

const fetchAdapter: HttpAdapter<Response> = {
  response: (o) => (o.ok ? o.result : undefined),
  header: (r, name) => r.headers.get(name),
  // Drain the discarded response, or the connection stays busy (undici) until GC.
  discard: (r) => r.body?.cancel().catch(() => {}),
};

/** Network failures, timeouts and 408/429/500/502/503/504 responses. */
export function isTransientHttpOutcome(args: OutcomeArgs<Response>): boolean {
  return isTransient(args, fetchAdapter, TRANSIENT_STATUS);
}

/** Delay from a `Retry-After` header value (seconds or HTTP date), in ms; undefined when absent or unparseable. */
export function parseRetryAfter(value: string | null | undefined, now = Date.now()): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const date = Date.parse(value);
  return Number.isNaN(date) ? undefined : Math.max(0, date - now);
}

/** Delay from a response's `Retry-After` header, in ms; undefined when absent or unparseable. */
export function retryAfterMs(response: Response, now = Date.now()): number | undefined {
  return parseRetryAfter(response.headers.get('retry-after'), now);
}

/** @internal Url of a request, absolute; undefined when it can't be resolved (relative url outside a browser). */
export function toUrl(url: unknown, base?: string): URL | undefined {
  try {
    return new URL(String(url), base ?? (globalThis as { location?: { href: string } }).location?.href);
  } catch {
    return undefined;
  }
}

// Routes each attempt to the circuit breaker / limiter / attempt timeout of its key (Polly's SelectPipelineByAuthority).
function perKey<R extends { status: number }>(o: HttpResilienceOptions<R>, adapter: HttpAdapter<R>): StrategyDefinition<R> {
  const pipelines = new Map<string, ResiliencePipeline<R>>();
  const keyOf = o.pipelineKey ?? ((url: URL) => url.origin);
  const max = o.maxPipelines ?? 100;
  const build = (key: string, timeProvider: TimeProvider) => {
    const builder = new PipelineBuilder<R>({ name: o.name ? `${o.name}/${key}` : key, timeProvider });
    if (o.circuitBreaker !== false) {
      builder.add(circuitBreakerStrategy({ failureRatio: 0.5, samplingCount: 20, minimumThroughput: 10, breakDuration: '10s', ...o.circuitBreaker, shouldHandle: (args) => isTransient(args, adapter, TRANSIENT_STATUS) }));
    }
    if (o.concurrency) builder.add(concurrencyLimiterStrategy(o.concurrency));
    builder.add(timeoutStrategy({ timeoutGenerator: ({ context }) => requestOf(context)?.resilience.attemptTimeout ?? o.attemptTimeout ?? '10s' }));
    return builder.build();
  };
  return {
    name: 'PerKey',
    create: (source) => ({
      execute: (next, context) => {
        const url = requestOf(context)?.url;
        const key = url ? keyOf(url) : '';
        const pipeline = pipelines.get(key) ?? build(key, source.timeProvider);
        pipelines.delete(key); // re-insert: Map order is the LRU order
        pipelines.set(key, pipeline);
        if (pipelines.size > max) {
          // shortcut: an evicted key loses its breaker state; raise maxPipelines if you call more hosts than that.
          const oldest = pipelines.keys().next().value!;
          pipelines.get(oldest)!.dispose();
          pipelines.delete(oldest);
        }
        return pipeline.run(context, next);
      },
      dispose: () => {
        for (const p of pipelines.values()) p.dispose();
        pipelines.clear();
      },
    }),
  };
}

function buildPipeline<R extends { status: number }>(o: HttpResilienceOptions<R>, adapter: HttpAdapter<R>, keyed: StrategyDefinition<R>, retries?: number): ResiliencePipeline<R> {
  const builder = new PipelineBuilder<R>({ name: o.name, timeProvider: o.timeProvider });
  builder.add(timeoutStrategy({ timeoutGenerator: ({ context }) => requestOf(context)?.resilience.timeout ?? o.totalTimeout ?? '30s' }));
  const retry = o.retry || {};
  const max = retries ?? (o.retry === false ? 0 : (retry.maxRetryAttempts ?? 3));
  if (max > 0) {
    const time = builder.timeProvider;
    builder.add(retryStrategy<R>({
      delay: 200,
      backoffType: 'exponential',
      useJitter: 'full',
      delayGenerator:
        retry.respectRetryAfter === false
          ? undefined
          : ({ outcome }) => {
              const response = adapter.response(outcome);
              return response && parseRetryAfter(adapter.header(response, 'retry-after'), time.now());
            },
      ...retry,
      maxRetryAttempts: max,
      shouldHandle: async (args) => {
        const retryOn = requestOf<R>(args.context)?.resilience.retryOn ?? o.retryOn;
        if (typeof retryOn !== 'function') return isTransient(args, adapter, retryOn ?? TRANSIENT_STATUS);
        const { outcome } = args;
        return retryOn({ response: adapter.response(outcome), error: outcome.ok ? undefined : outcome.error, attemptNumber: args.attemptNumber });
      },
      onRetry: async (args) => {
        const response = adapter.response(args.outcome);
        if (response) await adapter.discard?.(response);
        await retry.onRetry?.(args);
      },
    }));
  }
  builder.add(keyed);
  return builder.build();
}

/** @internal The engine behind `createResilientFetch` and the axios integration. */
export function httpExecutor<R extends { status: number }>(o: HttpResilienceOptions<R>, adapter: HttpAdapter<R>, custom?: ResiliencePipeline<R>) {
  const keyed = perKey(o, adapter);
  // One pipeline per distinct per-request `retries` value (undefined = the configured one); they share the per-key state.
  const pipelines = new Map<number | undefined, ResiliencePipeline<R>>();
  const pipelineFor = (retries: number | undefined) => {
    let p = custom ?? pipelines.get(retries);
    if (!p) pipelines.set(retries, (p = buildPipeline(o, adapter, keyed, retries)));
    return p;
  };
  return {
    execute(request: { url: URL | undefined; method: string; idempotencyKey: boolean; body?: unknown; resilience?: RequestResilience<R>; signal?: AbortSignal }, call: (context: ResilienceContext) => Promise<R>): Promise<R> {
      const resilience = request.resilience ?? {};
      const retries = resilience.retries === false ? 0 : resilience.retries;
      return pipelineFor(retries).execute(call, {
        signal: request.signal,
        // A stream body can be sent once: a retry would resend it empty (or fail), so never repeat a started attempt.
        idempotent: !isStream(request.body) && (resilience.idempotent ?? (o.retryUnsafeMethods || SAFE_METHODS.has(request.method.toUpperCase()) || request.idempotencyKey)),
        properties: new Map([[REQUEST, { url: request.url, resilience } satisfies RequestState<R>]]),
      });
    },
    dispose(): void {
      for (const p of pipelines.values()) p.dispose();
    },
  };
}

/** The standard HTTP pipeline: total timeout → retry → (per host: circuit breaker → limiter → attempt timeout). */
export function standardHttpPipeline(options: HttpResilienceOptions<Response> = {}): ResiliencePipeline<Response> {
  return buildPipeline(options, fetchAdapter, perKey(options, fetchAdapter));
}

/**
 * A drop-in `fetch` with timeouts, retries and a circuit breaker per host:
 * `const http = createResilientFetch({ name: 'payments' }); const res = await http(url, { resilience: { retries: 1 } });`
 * Responses are returned as-is (a final 503 resolves, like fetch); only network errors and resilience rejections throw.
 */
export function createResilientFetch(options: ResilientFetchOptions = {}): ResilientFetch {
  const executor = httpExecutor(options, fetchAdapter, options.pipeline);
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const { onBeforeAttempt } = options;
  return ((input: FetchInput, init: ResilientRequestInit = {}) => {
    const { resilience, ...rest } = init;
    const request = input instanceof Request ? input : undefined;
    const method = rest.method ?? request?.method ?? 'GET';
    const headers = new Headers(rest.headers ?? request?.headers);
    return executor.execute(
      { url: toUrl(request?.url ?? input), method, idempotencyKey: headers.has('idempotency-key'), body: rest.body, resilience, signal: rest.signal ?? request?.signal },
      async (context) => {
        // A Request's body can be read once: clone it for every attempt.
        const source = request ? request.clone() : input;
        if (!onBeforeAttempt) return fetchImpl(source, { ...rest, signal: context.signal });
        const fresh = new Request(source, { ...rest, signal: context.signal });
        return fetchImpl((await onBeforeAttempt(fresh, context)) ?? fresh);
      },
    );
  }) as ResilientFetch;
}
