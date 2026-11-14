import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type Next,
  type ResilienceContext,
  type ResilienceStrategy,
  type StrategySource,
  RateLimiterRejectedError,
  check,
  fail,
  optionalMs,
  toMs,
  BulkheadRejectedError,
} from './core.ts';
import { type PermitRateLimiterOptions, type RejectionOptions, RateLimiterStrategy, TokenBucket, reject } from './window-rate-limiter.ts';

export interface OnRateLimiterRejectedArgs {
  readonly context: ResilienceContext;
  /** ms until a permit is likely available, when known. */
  readonly retryAfter: number | undefined;
}

export interface ConcurrencyLimiterOptions extends RejectionOptions {
  /** Max executions running at once. */
  permitLimit: number;
  /** Max executions waiting (FIFO) for a permit. Default 0. */
  queueLimit?: number;
  /** Longest a queued execution waits for a permit before it is rejected. Default: no limit. */
  queueTimeout?: Duration;
  /** Queue order: lower runs sooner (like a Unix nice value); FIFO within a priority. Default 0 for everyone. */
  priority?: (context: ResilienceContext) => number;
  /**
   * When the queue is full: `'reject-new'` (default) rejects the newcomer; `'drop-oldest'` rejects the longest-waiting
   * execution instead; `'drop-lowest-priority'` rejects the newest of the lowest priority, if the newcomer outranks it.
   * Dropped executions reject with `BulkheadRejectedError` reason `'dropped'`.
   */
  overflow?: 'reject-new' | 'drop-oldest' | 'drop-lowest-priority';
}

export interface TokenBucketOptions extends PermitRateLimiterOptions {
  /** Bucket size (largest burst), and the number of tokens it starts with. */
  tokenLimit: number;
  /** Refill interval. */
  replenishmentPeriod: Duration;
  /** Tokens added per period. Default `tokenLimit`. */
  tokensPerPeriod?: number;
}

/** Bulkhead: limits concurrent executions, queues a few, rejects the rest. */
export class ConcurrencyLimiterStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: ConcurrencyLimiterOptions;
  readonly #source: StrategySource;
  readonly #queueTimeout: number | undefined;
  // Sorted by priority, then arrival.
  readonly #queue: Waiter[] = [];
  #active = 0;
  #seq = 0;

  constructor(options: ConcurrencyLimiterOptions, source: StrategySource) {
    check(Number.isInteger(options.permitLimit) && options.permitLimit >= 1, 'permitLimit must be an integer >= 1');
    check((options.queueLimit ?? 0) >= 0, 'queueLimit must be >= 0');
    this.#queueTimeout = optionalMs(options.queueTimeout, 'queueTimeout');
    check(this.#queueTimeout === undefined || this.#queueTimeout > 0, 'queueTimeout must be > 0');
    this.#options = options;
    this.#source = source;
  }

  /** Executions running now. */
  get active(): number {
    return this.#active;
  }

  /** Executions waiting for a permit. */
  get queued(): number {
    return this.#queue.length;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const o = this.#options;
    if (this.#active < o.permitLimit) {
      this.#active++;
    } else {
      let priority: number;
      try {
        priority = o.priority?.(context) ?? 0;
      } catch (error) {
        return fail<T>(error);
      }
      if (this.#queue.length >= (o.queueLimit ?? 0) && !this.#dropFor(priority)) {
        return reject<T>(o, this.#source, context, new BulkheadRejectedError('full', this.#active, this.#queue.length));
      }
      const granted = await this.#wait(context.signal, priority);
      if (granted === 'timeout') return reject<T>(o, this.#source, context, new BulkheadRejectedError('queue-timeout', this.#active, this.#queue.length));
      if (granted === 'dropped') return reject<T>(o, this.#source, context, new BulkheadRejectedError('dropped', this.#active, this.#queue.length));
      if (granted !== true) return fail<T>(granted.reason);
    }
    try {
      return await next(context);
    } finally {
      // Hand the permit straight to the next waiter.
      const waiter = this.#queue[0];
      if (waiter) waiter.settle(true);
      else this.#active--;
    }
  }

  /** Rejects a queued execution to make room for a newcomer of `priority`, per `overflow`; false if none qualifies. */
  #dropFor(priority: number): boolean {
    const queue = this.#queue;
    const overflow = this.#options.overflow ?? 'reject-new';
    let victim: Waiter | undefined;
    if (overflow === 'drop-oldest') victim = queue.reduce<Waiter | undefined>((a, b) => (a && a.seq < b.seq ? a : b), undefined);
    else if (overflow === 'drop-lowest-priority' && queue.length && queue.at(-1)!.priority > priority) victim = queue.at(-1);
    victim?.settle('dropped');
    return victim !== undefined;
  }

  #wait(signal: AbortSignal, priority: number): Promise<true | 'timeout' | 'dropped' | { reason: unknown }> {
    if (signal.aborted) return Promise.resolve({ reason: signal.reason });
    return new Promise((resolve) => {
      const time = this.#source.timeProvider;
      const queue = this.#queue;
      let timer: unknown;
      const waiter: Waiter = {
        priority,
        seq: this.#seq++,
        settle: (result) => {
          queue.splice(queue.indexOf(waiter), 1);
          signal.removeEventListener('abort', onAbort);
          time.clearTimeout(timer);
          resolve(result);
        },
      };
      const onAbort = () => waiter.settle({ reason: signal.reason });
      let i = queue.length;
      while (i > 0 && queue[i - 1]!.priority > priority) i--;
      queue.splice(i, 0, waiter);
      signal.addEventListener('abort', onAbort, { once: true });
      if (this.#queueTimeout !== undefined) timer = time.setTimeout(() => waiter.settle('timeout'), this.#queueTimeout);
    });
  }
}

interface Waiter {
  readonly priority: number;
  readonly seq: number;
  settle(result: true | 'timeout' | 'dropped' | { reason: unknown }): void;
}

/** Token bucket rate limiter, e.g. for an external API quota. */
export class TokenBucketStrategy<T> extends RateLimiterStrategy<T> {
  constructor(options: TokenBucketOptions, source: StrategySource) {
    super(tokenBucket(options, source), options, source);
  }
}

function tokenBucket(options: TokenBucketOptions, source: StrategySource) {
  check(Number.isInteger(options.tokenLimit) && options.tokenLimit >= 1, 'tokenLimit must be an integer >= 1');
  const period = toMs(options.replenishmentPeriod, 'replenishmentPeriod');
  check(period > 0, 'replenishmentPeriod must be > 0');
  const perPeriod = options.tokensPerPeriod ?? options.tokenLimit;
  check(perPeriod >= 1, 'tokensPerPeriod must be >= 1');
  return new TokenBucket(options.tokenLimit, period, perPeriod, source.timeProvider.now());
}

/** `concurrencyLimiterStrategy(10)`, `concurrencyLimiterStrategy(10, 50)` or full options. */
export const concurrencyLimiterStrategy = <T = any>(options: number | ConcurrencyLimiterOptions, queueLimit = 0): StrategyDefinition<T, BulkheadRejectedError> => {
  const o = typeof options === 'number' ? { permitLimit: options, queueLimit } : options;
  return { name: o.name ?? 'ConcurrencyLimiter', create: (s) => new ConcurrencyLimiterStrategy<T>(o, s) };
};

/** Token bucket rate limiter (what `addRateLimiter` adds). */
export const tokenBucketStrategy = <T = any>(options: TokenBucketOptions): StrategyDefinition<T, RateLimiterRejectedError> => ({
  name: options.name ?? 'RateLimiter',
  create: (s) => new TokenBucketStrategy<T>(options, s),
});
