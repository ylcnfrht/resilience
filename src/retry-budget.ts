import { type Duration, type TimeProvider, check, systemTime, toMs } from './core.ts';

export interface RetryBudgetOptions {
  /** Retries allowed as a fraction of original requests in the window. Default 0.2 (at most +20% load from retries). */
  ratio?: number;
  /** Retries always allowed per second, so low-traffic callers can still retry. Default 10. */
  minRetriesPerSecond?: number;
  /** Window the counts are kept for. Default 10 s. */
  window?: Duration;
  timeProvider?: TimeProvider;
}

/**
 * Caps retries relative to traffic, so a failing dependency gets at most `ratio` extra load instead of a retry storm
 * (every caller retrying 3x = 4x load exactly when the dependency is weakest). Share one budget across every pipeline
 * that calls the same dependency: `addRetry({ budget })`.
 */
export class RetryBudget {
  readonly #ratio: number;
  readonly #reserve: number;
  readonly #window: number;
  readonly #bucketSize: number;
  readonly #time: TimeProvider;
  // 10 buckets of { start, requests, retries } across the window.
  #buckets: Array<{ start: number; requests: number; retries: number }> = [];

  constructor(options: RetryBudgetOptions = {}) {
    this.#ratio = options.ratio ?? 0.2;
    this.#window = toMs(options.window ?? '10s', 'window');
    this.#reserve = (options.minRetriesPerSecond ?? 10) * (this.#window / 1000);
    this.#bucketSize = this.#window / 10;
    this.#time = options.timeProvider ?? systemTime;
    check(this.#ratio >= 0, 'ratio must be >= 0');
    check(this.#reserve >= 0, 'minRetriesPerSecond must be >= 0');
    check(this.#window > 0, 'window must be > 0');
  }

  /** @internal Called for every original (non-retry) attempt. */
  recordRequest(): void {
    this.#bucket().requests++;
  }

  /** @internal Takes one retry from the budget; false when exhausted. */
  tryRetry(): boolean {
    const { requests, retries } = this.stats();
    if (retries >= this.#reserve + this.#ratio * requests) return false;
    this.#bucket().retries++;
    return true;
  }

  /** Requests and retries counted in the current window. */
  stats(): { requests: number; retries: number } {
    this.#trim(this.#time.now());
    let requests = 0;
    let retries = 0;
    for (const b of this.#buckets) {
      requests += b.requests;
      retries += b.retries;
    }
    return { requests, retries };
  }

  #bucket() {
    const now = this.#time.now();
    this.#trim(now);
    let b = this.#buckets.at(-1);
    if (!b || now - b.start >= this.#bucketSize) this.#buckets.push((b = { start: now, requests: 0, retries: 0 }));
    return b;
  }

  #trim(now: number) {
    while (this.#buckets.length && now - this.#buckets[0]!.start >= this.#window) this.#buckets.shift();
  }
}
