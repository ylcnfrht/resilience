import { type AcquireResult, type RateLimiterAlgorithm, checkState } from './window-rate-limiter.ts';

/** `BurstyRateLimiter.snapshot()`. */
export interface BurstyRateLimiterState {
  readonly kind: 'bursty';
  readonly primary: unknown;
  readonly burst: unknown;
}

/**
 * A steady limit plus a burst allowance: `burst` is only consulted when `primary` rejects, e.g. 10/s that may spill
 * into 20 extra per minute. Any algorithms work, sync or async (in-memory, Redis):
 *
 * ```ts
 * const now = s.timeProvider.now();
 * new RateLimiterStrategy(new BurstyRateLimiter(new WindowCounter(10, 1000, 1, now), new WindowCounter(20, 60_000, 1, now)), { queueTimeout: '1s' }, s);
 * ```
 */
export class BurstyRateLimiter implements RateLimiterAlgorithm {
  /** The larger of the two: a weight only the burst pool can hold still gets in through it. */
  readonly permitLimit: number;
  readonly primary: RateLimiterAlgorithm;
  readonly burst: RateLimiterAlgorithm;

  constructor(primary: RateLimiterAlgorithm, burst: RateLimiterAlgorithm) {
    this.primary = primary;
    this.burst = burst;
    this.permitLimit = Math.max(primary.permitLimit, burst.permitLimit);
  }

  /** Granted by `primary`: its remaining (the burst pool is not read). Granted by `burst`: the pool's remaining. */
  async acquire(permits: number, now: number): Promise<AcquireResult> {
    const p = permits <= this.primary.permitLimit ? await this.primary.acquire(permits, now) : undefined;
    if (p?.ok) return p;
    const b = permits <= this.burst.permitLimit ? await this.burst.acquire(permits, now) : undefined;
    if (b?.ok) return b;
    // Rejections take nothing, so a retry passes as soon as either limiter would admit it.
    return { ok: false, retryAfter: Math.min(p?.retryAfter ?? Infinity, b?.retryAfter ?? Infinity) };
  }

  /** Penalty / reward hit `primary`; a full reset (`-Infinity`, e.g. `control.delete()`) also refills the burst pool. */
  async adjust(points: number, now: number): Promise<void> {
    if (!this.primary.adjust) throw new TypeError('The primary rate limiter algorithm does not support penalty / reward.');
    await this.primary.adjust(points, now);
    if (points === -Infinity) await this.burst.adjust?.(points, now);
  }

  snapshot(now: number): BurstyRateLimiterState {
    const { primary, burst } = this;
    if (!primary.snapshot || !burst.snapshot) throw new TypeError('Both rate limiter algorithms must support snapshot / restore.');
    return { kind: 'bursty', primary: primary.snapshot(now), burst: burst.snapshot(now) };
  }

  restore(state: BurstyRateLimiterState, now: number): void {
    const { primary, burst } = this;
    if (!primary.restore || !burst.restore) throw new TypeError('Both rate limiter algorithms must support snapshot / restore.');
    checkState(state?.kind === 'bursty', 'bursty rate limiter');
    primary.restore(state.primary, now);
    burst.restore(state.burst, now);
  }
}
