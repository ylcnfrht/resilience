/**
 * Plug existing infrastructure into the pipeline. Structurally typed: nothing here imports the libraries.
 */
import { type AcquireResult, type RateLimiterAlgorithm } from './window-rate-limiter.ts';

/** The part of rate-limiter-flexible's `RateLimiterAbstract` used here (any backend: Memory, Redis, Postgres, Mongo, …). */
export interface RateLimiterFlexibleLike {
  readonly points: number;
  consume(key: string, points?: number): Promise<unknown>;
  reward(key: string, points?: number): Promise<unknown>;
}

/**
 * A rate-limiter-flexible limiter as a `RateLimiterAlgorithm` for one key. The window runs on the limiter's own clock.
 *
 * ```ts
 * const rlf = new RateLimiterRedis({ storeClient: redis, points: 100, duration: 60 });
 * builder.addStrategy((s) => new RateLimiterStrategy(fromRateLimiterFlexible(rlf, 'api'), { queueTimeout: '1s' }, s));
 * // per key:
 * builder.addPartitionedRateLimiter({ partitionKey, limiter: (key, s) => new RateLimiterStrategy(fromRateLimiterFlexible(rlf, key), {}, s) });
 * ```
 */
export function fromRateLimiterFlexible(limiter: RateLimiterFlexibleLike, key: string): RateLimiterAlgorithm {
  return {
    permitLimit: limiter.points,
    async acquire(permits): Promise<AcquireResult> {
      try {
        await limiter.consume(key, permits);
        return { ok: true };
      } catch (rejection) {
        // Store failures are Errors; a rejection is a RateLimiterRes.
        if (rejection instanceof Error) throw rejection;
        // consume() counts points even when it rejects; give them back so a rejected (or waiting) call doesn't eat the budget.
        // shortcut: not atomic with consume, so concurrent callers may briefly see the inflated count and be rejected early.
        await limiter.reward(key, permits);
        return { ok: false, retryAfter: Math.max(1, (rejection as { msBeforeNext?: number }).msBeforeNext ?? 1) };
      }
    },
  };
}
