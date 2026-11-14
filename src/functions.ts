import { type Duration, type MaybePromise, type ResilienceContext, type TimeProvider, systemTime } from './core.ts';
import { ResiliencePipeline } from './pipeline.ts';
import { type RetryOptions, RetryStrategy } from './retry.ts';
import { type TimeoutOptions, TimeoutStrategy } from './timeout.ts';

// These build strategies directly instead of going through the builder, so a bundle that only uses
// retry() or withTimeout() doesn't pull in every other strategy.

interface CallOptions {
  signal?: AbortSignal;
  timeProvider?: TimeProvider;
}

/**
 * One-off retry, no pipeline needed: `await retry(() => fetchUser(id), { maxRetryAttempts: 5, backoffType: 'exponential' })`.
 * Throw `NonRetryableError` from `fn` to stop early.
 */
export function retry<T>(fn: (context: ResilienceContext) => MaybePromise<T>, options: RetryOptions<T> & CallOptions = {}): Promise<T> {
  const { signal, timeProvider = systemTime, ...retryOptions } = options;
  const source = { pipelineName: undefined, strategyName: retryOptions.name ?? 'Retry', timeProvider };
  return new ResiliencePipeline<T>([new RetryStrategy<T>(retryOptions, source)], { timeProvider }).execute(fn, { signal });
}

/** One-off timeout: `await withTimeout(({ signal }) => fetch(url, { signal }), '2s')`. Rejects with `TimeoutRejectedError`. */
export function withTimeout<T>(fn: (context: ResilienceContext) => MaybePromise<T>, timeout: Duration | TimeoutOptions, options: CallOptions = {}): Promise<T> {
  const timeProvider = options.timeProvider ?? systemTime;
  const o: TimeoutOptions = typeof timeout === 'object' ? timeout : { timeout };
  const source = { pipelineName: undefined, strategyName: o.name ?? 'Timeout', timeProvider };
  return new ResiliencePipeline<T>([new TimeoutStrategy<T>(o, source)], { timeProvider }).execute(fn, { signal: options.signal });
}
