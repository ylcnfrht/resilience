import { type BrokenCircuitError, type BulkheadRejectedError, type Duration, type RateLimiterRejectedError, type ResilienceContext, type TimeoutRejectedError } from './core.ts';
import { type AdaptiveConcurrencyLimiterOptions, adaptiveConcurrencyLimiterStrategy } from './adaptive-concurrency.ts';
import { type CacheOptions, type StaleFallbackOptions, cacheStrategy, coalesceStrategy, staleFallbackStrategy } from './cache.ts';
import {
  type ChaosBehaviorOptions,
  type ChaosFaultOptions,
  type ChaosLatencyOptions,
  type ChaosOutcomeOptions,
  chaosBehaviorStrategy,
  chaosFaultStrategy,
  chaosLatencyStrategy,
  chaosOutcomeStrategy,
} from './chaos.ts';
import { type CircuitBreakerOptions, circuitBreakerStrategy } from './circuit-breaker.ts';
import { type FallbackOptions, fallbackOnStrategy, fallbackStrategy, mapErrorStrategy } from './fallback.ts';
import { type HedgingOptions, hedgingStrategy } from './hedging.ts';
import { type LockLostError, type LockNotAcquiredError, type LockOptions, lockStrategy } from './lock.ts';
import { type CatchesOf, type ErrorsOf, PipelineBuilder, type StrategyDefinition, type WithErrors } from './pipeline-builder.ts';
import { type ResiliencePipeline } from './pipeline.ts';
import { type ConcurrencyLimiterOptions, type TokenBucketOptions, concurrencyLimiterStrategy, tokenBucketStrategy } from './rate-limiter.ts';
import { type RetryOptions, retryStrategy } from './retry.ts';
import { type TimeoutOptions, timeoutStrategy } from './timeout.ts';
import {
  type FixedWindowRateLimiterOptions,
  type PartitionedRateLimiterOptions,
  type SlidingWindowRateLimiterOptions,
  fixedWindowRateLimiterStrategy,
  partitionedRateLimiterStrategy,
  slidingWindowRateLimiterStrategy,
} from './window-rate-limiter.ts';

export { type ResiliencePipelineBuilderOptions, type StrategyFactory } from './pipeline-builder.ts';

/** The builder after adding a strategy that can raise `Added`. */
type Next<T, E, C extends string, Added> = ResiliencePipelineBuilder<T, WithErrors<E, C, Added>, C>;

/**
 * Polly-style builder with a shortcut for every strategy: `addRetry()`, `addTimeout()`, `addCircuitBreaker()`, …
 * Convenient, but it bundles every strategy. For the smallest browser bundle use `PipelineBuilder` with
 * `retryStrategy()`, `timeoutStrategy()`, … instead; the two are interchangeable.
 *
 * The type parameters track which errors the pipeline can raise, for `pipeline.settle()` + `Outcome.match()`.
 */
export class ResiliencePipelineBuilder<T = any, E = never, C extends string = never> extends PipelineBuilder<T, E, C> {
  override add<D extends StrategyDefinition<T, any>>(definition: D): ResiliencePipelineBuilder<T, WithErrors<E, C, ErrorsOf<D>>, C | CatchesOf<D>> {
    super.add(definition);
    return this as never;
  }

  override addPipeline<E2>(pipeline: ResiliencePipeline<T, E2>): Next<T, E, C, E2> {
    super.addPipeline(pipeline);
    return this as never;
  }

  addRetry(options: RetryOptions<T> = {}): this {
    return this.add(retryStrategy(options)) as never;
  }

  /** `addTimeout(5000)`, `addTimeout('5s')` or `addTimeout({ timeout: '5s', onTimeout })`. */
  addTimeout(options: Duration | TimeoutOptions): Next<T, E, C, TimeoutRejectedError> {
    return this.add(timeoutStrategy(options)) as never;
  }

  addCircuitBreaker(options: CircuitBreakerOptions<T> = {}): Next<T, E, C, BrokenCircuitError> {
    return this.add(circuitBreakerStrategy(options)) as never;
  }

  /** Without `shouldHandle` it catches every failure of the strategies added after it, so their errors leave the error type. */
  addFallback(options: FallbackOptions<T> & { shouldHandle?: undefined }): ResiliencePipelineBuilder<T, E, string>;
  addFallback(options: FallbackOptions<T>): this;
  addFallback(options: FallbackOptions<T>): unknown {
    return this.add(fallbackStrategy(options));
  }

  /** Fallback for these error codes only; they leave the `settle()` error type of the strategies added after it. */
  addFallbackOn<const K extends string>(codes: readonly K[], fallbackAction: NonNullable<FallbackOptions<T>['fallbackAction']>, options?: Omit<FallbackOptions<T>, 'fallbackAction' | 'shouldHandle'>): ResiliencePipelineBuilder<T, E, C | K> {
    return this.add(fallbackOnStrategy<T, K>(codes, fallbackAction, options)) as never;
  }

  /** Turns every failure of the strategies added after it into your own error type (theirs leave the error type). */
  addMapError<E2>(mapper: (error: unknown, context: ResilienceContext) => E2): ResiliencePipelineBuilder<T, E | E2, string> {
    return this.add(mapErrorStrategy<T, E2>(mapper)) as never;
  }

  /** Serve the last good value when a call fails (no caching of fresh calls): tenace's withStaleFallback. */
  addStaleFallback(options: StaleFallbackOptions<T>): this {
    return this.add(staleFallbackStrategy(options)) as never;
  }

  addHedging(options: HedgingOptions<T> = {}): this {
    return this.add(hedgingStrategy(options)) as never;
  }

  /** `addConcurrencyLimiter(10)`, `addConcurrencyLimiter(10, 50)` or full options. */
  addConcurrencyLimiter(options: number | ConcurrencyLimiterOptions, queueLimit = 0): Next<T, E, C, BulkheadRejectedError> {
    return this.add(concurrencyLimiterStrategy(options, queueLimit)) as never;
  }

  /** Token bucket rate limiter. */
  addRateLimiter(options: TokenBucketOptions): Next<T, E, C, RateLimiterRejectedError> {
    return this.add(tokenBucketStrategy(options)) as never;
  }

  /**
   * Concurrency limit that tunes itself from observed latency and failures (gradient2 / vegas / aimd),
   * so you don't have to guess the right number: it grows while the dependency keeps up and backs off when it queues.
   */
  addAdaptiveConcurrencyLimiter(options: AdaptiveConcurrencyLimiterOptions<T> = {}): Next<T, E, C, BulkheadRejectedError> {
    return this.add(adaptiveConcurrencyLimiterStrategy(options)) as never;
  }

  /** Fixed-window rate limit: `permitLimit` per `window`. Optional `permits(ctx)` weight and `queueTimeout`. */
  addFixedWindowRateLimiter(options: FixedWindowRateLimiterOptions): Next<T, E, C, RateLimiterRejectedError> {
    return this.add(fixedWindowRateLimiterStrategy(options)) as never;
  }

  /** Sliding-window rate limit: smooths the burst a fixed window allows at its edges. */
  addSlidingWindowRateLimiter(options: SlidingWindowRateLimiterOptions): Next<T, E, C, RateLimiterRejectedError> {
    return this.add(slidingWindowRateLimiterStrategy(options)) as never;
  }

  /** One limiter per key (user, tenant, API key…), created on demand and evicted when idle. */
  addPartitionedRateLimiter(options: PartitionedRateLimiterOptions<T>): Next<T, E, C, RateLimiterRejectedError> {
    return this.add(partitionedRateLimiterStrategy(options)) as never;
  }

  /** Cache successful results by key, coalesce concurrent identical calls, optionally serve stale values on failure. */
  addCache(options: CacheOptions<T>): this {
    return this.add(cacheStrategy(options)) as never;
  }

  /** Single-flight only: concurrent executions with the same key share one call; nothing is cached. */
  addCoalesce(options: Omit<CacheOptions<T>, 'ttl' | 'staleTtl' | 'store' | 'shouldCache'> = {}): this {
    return this.add(coalesceStrategy(options)) as never;
  }

  /** Run under a (distributed, with a custom store) lock: leases, renewal, fencing tokens. */
  addLock(options: LockOptions): Next<T, E, C, LockNotAcquiredError | LockLostError> {
    return this.add(lockStrategy(options)) as never;
  }

  addChaosFault(options: ChaosFaultOptions): this {
    return this.add(chaosFaultStrategy(options)) as never;
  }

  addChaosOutcome(options: ChaosOutcomeOptions<T>): this {
    return this.add(chaosOutcomeStrategy(options)) as never;
  }

  addChaosLatency(options: ChaosLatencyOptions): this {
    return this.add(chaosLatencyStrategy(options)) as never;
  }

  addChaosBehavior(options: ChaosBehaviorOptions): this {
    return this.add(chaosBehaviorStrategy(options)) as never;
  }

  override build(): ResiliencePipeline<T, E> {
    return super.build();
  }
}
