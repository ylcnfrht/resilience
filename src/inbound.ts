// Internal: shared by the http, fastify and koa integrations. Not a package subpath; http.ts re-exports the public parts.
import { type Duration, type TelemetrySource, isResilienceError } from './core.ts';
import { PipelineBuilder } from './pipeline-builder.ts';
import { type ResiliencePipeline } from './pipeline.ts';
import { concurrencyLimiterStrategy } from './rate-limiter.ts';
import { timeoutStrategy } from './timeout.ts';
import { SlidingWindowRateLimiterStrategy, getRateLimitInfo, partitionedRateLimiterStrategy, slidingWindowRateLimiterStrategy } from './window-rate-limiter.ts';

const STATUS: Record<string, number> = {
  TIMEOUT: 504,
  RATE_LIMITED: 429,
  RATE_LIMIT_DENIED: 403,
  PERMIT_LIMIT_EXCEEDED: 429,
  BULKHEAD_FULL: 503, // our capacity, not the client's rate: 503 (like @nestjs/resilience), not 429
  CIRCUIT_OPEN: 503,
  CIRCUIT_ISOLATED: 503,
  LOCK_NOT_ACQUIRED: 409,
};

export interface ResilienceErrorDescription {
  /** Stable machine-readable code, e.g. 'TIMEOUT', 'CIRCUIT_OPEN', 'BULKHEAD_FULL', 'RATE_LIMITED'. */
  readonly code: string;
  readonly httpStatus: number;
  /** Seconds, when known (for a `Retry-After` header or a GraphQL/RPC error payload). */
  readonly retryAfter: number | undefined;
  readonly message: string;
  readonly source: TelemetrySource | undefined;
}

/**
 * Transport-neutral description of a resilience error, or undefined for any other error. Use it to build an HTTP
 * response, GraphQL `extensions: { code, httpStatus, retryAfter }`, a gRPC status, or a WebSocket error frame.
 * Works across duplicate copies of this package (uses error codes, not `instanceof`).
 */
export function describeResilienceError(error: unknown): ResilienceErrorDescription | undefined {
  if (!isResilienceError(error)) return undefined;
  const ms = (error as { retryAfter?: unknown }).retryAfter;
  return {
    code: error.code,
    httpStatus: STATUS[error.code] ?? 503,
    retryAfter: typeof ms === 'number' ? Math.ceil(ms / 1000) : undefined,
    message: error.message,
    source: error.source,
  };
}

export interface InboundOptions {
  /** Budget per request. Everything called through a pipeline during the request inherits it (deadline propagation). */
  timeout?: Duration;
  /** Max requests in flight; beyond `queueLimit` waiting requests, reject fast with 429/503 (load shedding). */
  concurrency?: { permitLimit: number; queueLimit?: number; queueTimeout?: Duration };
  /**
   * Sliding-window rate limit, checked first; 429 beyond it. With `key` (it receives Express's `req` or Hono's `c`,
   * e.g. `(req) => req.ip`) each key gets its own limit (at most `maxPartitions` keys kept, default 100 000); without it, one limit for all.
   */
  rateLimit?: { key?: (req: any) => string; permitLimit: number; window: Duration; segmentsPerWindow?: number; maxPartitions?: number };
  /**
   * Headers sent when the pipeline has a rate limiter: `'draft'` (default) IETF `RateLimit-Limit/-Remaining/-Reset`,
   * `'legacy'` `X-RateLimit-*`, `'both'`, or `false`. Reset is in seconds from now in both forms.
   */
  rateLimitHeaders?: 'draft' | 'legacy' | 'both' | false;
  /** Bring your own pipeline instead (takes precedence). */
  pipeline?: ResiliencePipeline;
}

/** `context.properties` key of the inbound request (Express `req` / Hono `c`), for your own partitioned limiters. */
export const HTTP_REQUEST = '@nodelibraries/resilience.httpRequest';

export function inboundPipeline(options: InboundOptions): ResiliencePipeline {
  if (options.pipeline) return options.pipeline;
  const builder = new PipelineBuilder({ name: 'inbound' });
  const rl = options.rateLimit;
  if (rl) {
    const window = { permitLimit: rl.permitLimit, window: rl.window, segmentsPerWindow: rl.segmentsPerWindow };
    const key = rl.key;
    builder.add(
      key
        ? partitionedRateLimiterStrategy({
            partitionKey: (ctx) => key(ctx.properties.get(HTTP_REQUEST)),
            limiter: (_k, s) => new SlidingWindowRateLimiterStrategy(window, s),
            maxPartitions: rl.maxPartitions ?? 100_000,
          })
        : slidingWindowRateLimiterStrategy(window),
    );
  }
  if (options.concurrency) builder.add(concurrencyLimiterStrategy(options.concurrency));
  if (options.timeout !== undefined) builder.add(timeoutStrategy(options.timeout));
  return builder.build();
}

export function rateLimitHeaders(properties: Map<string, unknown>, mode: InboundOptions['rateLimitHeaders'] = 'draft'): Record<string, string> {
  const info = getRateLimitInfo({ properties });
  if (!info || !mode) return {};
  const headers: Record<string, string> = {};
  for (const prefix of mode === 'both' ? ['RateLimit-', 'X-RateLimit-'] : [mode === 'legacy' ? 'X-RateLimit-' : 'RateLimit-']) {
    headers[prefix + 'Limit'] = String(info.limit);
    headers[prefix + 'Remaining'] = String(info.remaining);
    headers[prefix + 'Reset'] = String(Math.ceil(info.resetAfter / 1000));
  }
  return headers;
}

/** Status, headers and JSON body for a resilience error; undefined for any other error. */
/** True when `error` is the inbound pipeline's own rejection (its deadline) re-thrown by a nested pipeline in the handler. */
export const isOwnRejection = (error: unknown, signal: AbortSignal | undefined): boolean => !!signal?.aborted && error === signal.reason;

export function rejection(error: unknown, properties: Map<string, unknown>, mode: InboundOptions['rateLimitHeaders']) {
  const d = describeResilienceError(error);
  if (!d) return undefined;
  const headers = rateLimitHeaders(properties, mode);
  if (d.retryAfter !== undefined) headers['retry-after'] = String(d.retryAfter);
  return { status: d.httpStatus, headers, body: { statusCode: d.httpStatus, error: (error as Error).name, message: d.message } };
}
