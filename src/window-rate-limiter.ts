import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type MaybePromise,
  type Next,
  type ResilienceContext,
  type ResilienceStrategy,
  type StrategySource,
  RateLimiterRejectedError,
  check,
  fail,
  TimeoutRejectedError,
  delay,
  report,
  toMs,
  stamp,
} from './core.ts';
import type { OnRateLimiterRejectedArgs } from './rate-limiter.ts';

/** On success, `remaining` and `resetAfter` (ms until everything used so far is released) feed `getRateLimitInfo`, when known. */
export type AcquireResult =
  | { readonly ok: true; readonly remaining?: number; readonly resetAfter?: number }
  | { readonly ok: false; readonly retryAfter: number };

/**
 * The admission logic of a rate limiter, without waiting or telemetry. The in-memory `WindowCounter` implements it;
 * implement it over Redis (one Lua script per acquire) to share a limit between processes. `acquire` may be async.
 */
export interface RateLimiterAlgorithm {
  readonly permitLimit: number;
  /** Takes `permits` if they fit, else says how many ms until they would. `permits` is never above `permitLimit`. */
  acquire(permits: number, now: number): MaybePromise<AcquireResult>;
  /** Charges (`points` > 0) or refunds (`points` < 0) permits without admission check: penalty / reward. Optional. */
  adjust?(points: number, now: number): MaybePromise<void>;
  /** JSON-serializable state as of `now`, for `restore` (e.g. after a restart). Optional. */
  snapshot?(now: number): unknown;
  /** Loads a `snapshot`, aging it by the time passed since it was taken. Optional. */
  restore?(state: unknown, now: number): void;
}

/** `WindowCounter.snapshot()`: bucket counts newest first; `elapsed` = ms into the newest bucket at `at`. */
export interface WindowCounterState {
  readonly kind: 'window';
  readonly at: number;
  readonly elapsed: number;
  readonly counts: readonly number[];
}

/** `TokenBucket.snapshot()`: `elapsed` = ms since the last refill at `at`. */
export interface TokenBucketState {
  readonly kind: 'token-bucket';
  readonly at: number;
  readonly elapsed: number;
  readonly tokens: number;
}

const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);

/** @internal Restored state comes from outside (disk, another process): reject anything malformed. */
export function checkState(condition: boolean, what: string): asserts condition {
  if (!condition) throw new TypeError(`Invalid ${what} snapshot.`);
}

/** What the last rate limiter that admitted (or rejected) an execution reported. */
export interface RateLimitInfo {
  readonly limit: number;
  readonly remaining: number;
  /** ms until the permits used so far are all released (on rejection: until a retry may pass). */
  readonly resetAfter: number;
}

/** `context.properties` key of the `RateLimitInfo`. */
export const RATE_LIMIT_INFO = '@nodelibraries/resilience.rateLimit';

/** Limit, remaining permits and reset time the rate limiter reported for this execution, e.g. for `RateLimit-*` headers. */
export function getRateLimitInfo(context: { readonly properties: Map<string, unknown> }): RateLimitInfo | undefined {
  return context.properties.get(RATE_LIMIT_INFO) as RateLimitInfo | undefined;
}

/**
 * Segmented sliding window: the window is split into `segments` buckets and slides one bucket at a time.
 * With 1 segment it is a fixed window. Windows are aligned to `origin`; with `'first-request'` alignment a new
 * window starts at the first request after the window empties instead.
 */
export class WindowCounter implements RateLimiterAlgorithm {
  readonly permitLimit: number;
  readonly #segmentMs: number;
  readonly #counts: number[];
  readonly #firstRequest: boolean;
  #origin: number;
  #segment = 0;

  constructor(permitLimit: number, windowMs: number, segments: number, origin: number, alignment: 'fixed' | 'first-request' = 'fixed') {
    this.permitLimit = permitLimit;
    this.#origin = origin;
    this.#segmentMs = windowMs / segments;
    this.#counts = new Array<number>(segments).fill(0);
    this.#firstRequest = alignment === 'first-request';
  }

  snapshot(now: number): WindowCounterState {
    const counts = this.#counts;
    const n = counts.length;
    const current = this.#slide(now);
    return {
      kind: 'window',
      at: now,
      elapsed: now - (this.#origin + current * this.#segmentMs),
      counts: counts.map((_, i) => (current - i >= 0 ? counts[(current - i) % n]! : 0)),
    };
  }

  restore(state: WindowCounterState, now: number): void {
    const counts = this.#counts;
    const n = counts.length;
    checkState(state?.kind === 'window' && isNum(state.at) && isNum(state.elapsed), 'window counter');
    checkState(Array.isArray(state.counts) && state.counts.length === n && state.counts.every((c) => isNum(c) && c >= 0), 'window counter (segment count mismatch?)');
    // Re-anchor the windows on the snapshot's, its newest bucket being segment n-1 so older ones keep indexes >= 0.
    this.#segment = n - 1;
    this.#origin = state.at - state.elapsed - this.#segment * this.#segmentMs;
    for (let i = 0; i < n; i++) counts[n - 1 - i] = state.counts[i]!;
    this.#slide(now);
  }

  acquire(permits: number, now: number): AcquireResult {
    const counts = this.#counts;
    const n = counts.length;
    const current = this.#slide(now);
    let used = counts.reduce((a, b) => a + b, 0);
    if (used + permits <= this.permitLimit) {
      counts[current % n]! += permits;
      let newest = current;
      while (newest > Math.max(0, current - n + 1) && counts[newest % n] === 0) newest--;
      const resetAfter = counts[newest % n]! > 0 ? Math.max(0, Math.ceil(this.#origin + (newest + n) * this.#segmentMs - now)) : 0;
      return { ok: true, remaining: Math.max(0, this.permitLimit - used - permits), resetAfter };
    }
    // Oldest bucket first: bucket current-n+1+k (same slot as current+1+k) leaves the window when segment current+1+k starts.
    for (let k = 0; k < n; k++) {
      used -= counts[(current + 1 + k) % n]!;
      if (used + permits <= this.permitLimit) return { ok: false, retryAfter: Math.max(1, Math.ceil(this.#origin + (current + 1 + k) * this.#segmentMs - now)) };
    }
    throw new RangeError(`permits (${permits}) exceed permitLimit (${this.permitLimit})`);
  }

  adjust(points: number, now: number): void {
    const counts = this.#counts;
    const n = counts.length;
    const current = this.#slide(now);
    if (points >= 0) {
      counts[current % n]! += points;
      return;
    }
    // Refund the newest permits first; `-Infinity` empties the window.
    let left = -points;
    for (let i = current; i >= Math.max(0, current - n + 1) && left > 0; i--) {
      const take = Math.min(counts[i % n]!, left);
      counts[i % n]! -= take;
      left -= take;
    }
  }

  /** Clears the buckets that left the window; returns the current segment. */
  #slide(now: number): number {
    const counts = this.#counts;
    const n = counts.length;
    // max(): a clock that steps back must not move the window back.
    const current = Math.max(this.#segment, Math.floor((now - this.#origin) / this.#segmentMs));
    for (let i = Math.max(this.#segment + 1, current - n + 1); i <= current; i++) counts[i % n] = 0;
    if (this.#firstRequest && counts.every((c) => c === 0)) {
      this.#origin = now;
      return (this.#segment = 0);
    }
    this.#segment = current;
    return current;
  }
}

/** Token bucket: `tokenLimit` burst, `tokensPerPeriod` added every `period` ms. Tokens can go negative through `adjust` (penalty). */
export class TokenBucket implements RateLimiterAlgorithm {
  readonly permitLimit: number;
  readonly #period: number;
  readonly #perPeriod: number;
  #tokens: number;
  #lastRefill: number;

  constructor(tokenLimit: number, periodMs: number, tokensPerPeriod: number, now: number) {
    this.permitLimit = tokenLimit;
    this.#period = periodMs;
    this.#perPeriod = tokensPerPeriod;
    this.#tokens = tokenLimit;
    this.#lastRefill = now;
  }

  acquire(permits: number, now: number): AcquireResult {
    this.#refill(now);
    if (this.#tokens >= permits) {
      this.#tokens -= permits;
      return { ok: true, remaining: Math.max(0, Math.floor(this.#tokens)), resetAfter: this.#until(this.permitLimit, now) };
    }
    return { ok: false, retryAfter: Math.max(1, this.#until(permits, now)) };
  }

  adjust(points: number, now: number): void {
    this.#refill(now);
    this.#tokens = Math.min(this.permitLimit, this.#tokens - points);
  }

  snapshot(now: number): TokenBucketState {
    this.#refill(now);
    return { kind: 'token-bucket', at: now, elapsed: now - this.#lastRefill, tokens: this.#tokens };
  }

  restore(state: TokenBucketState, now: number): void {
    checkState(state?.kind === 'token-bucket' && isNum(state.at) && isNum(state.elapsed) && isNum(state.tokens), 'token bucket');
    this.#tokens = Math.min(this.permitLimit, state.tokens);
    this.#lastRefill = state.at - state.elapsed;
    this.#refill(now);
  }

  #refill(now: number) {
    const periods = Math.floor((now - this.#lastRefill) / this.#period);
    if (periods > 0) {
      this.#tokens = Math.min(this.permitLimit, this.#tokens + periods * this.#perPeriod);
      this.#lastRefill += periods * this.#period;
    }
  }

  /** ms until the bucket holds `tokens`. */
  #until(tokens: number, now: number): number {
    if (this.#tokens >= tokens) return 0;
    return Math.ceil((tokens - this.#tokens) / this.#perPeriod) * this.#period - (now - this.#lastRefill);
  }
}

/** A request asked for more permits than the limiter ever grants, so waiting can't help. */
export class PermitLimitExceededError extends RateLimiterRejectedError {
  name = 'PermitLimitExceededError';
  code = 'PERMIT_LIMIT_EXCEEDED' as const;
  readonly permits: number;
  readonly permitLimit: number;
  constructor(permits: number, permitLimit: number) {
    super();
    this.message = `The operation asked for ${permits} permits but the rate limiter allows at most ${permitLimit}; it can never be executed.`;
    this.permits = permits;
    this.permitLimit = permitLimit;
  }
}

/** The execution matched `deny`: it is refused regardless of the limit. HTTP maps it to 403. */
export class RateLimiterDeniedError extends RateLimiterRejectedError {
  name = 'RateLimiterDeniedError';
  code = 'RATE_LIMIT_DENIED' as const;
  constructor() {
    super(undefined, 'The operation is denied by the rate limiter.');
  }
}

interface RateLimitTarget {
  penalty(points: number, key?: string): Promise<void>;
  reward(points: number, key?: string): Promise<void>;
  block(duration: Duration, key?: string): void;
  unblock(key?: string): void;
  delete(key?: string): unknown;
}

/**
 * Manual control of the rate limiters it is passed to (`control` option): penalty, reward, block, unblock, delete.
 * `key` picks a partition of a partitioned limiter (omitted: every partition); single limiters ignore it.
 * Pass it to the partitioned limiter, not to the per-key limiters it creates.
 */
export class RateLimiterControl {
  readonly #targets = new Set<RateLimitTarget>();

  /** Charge `points` permits, e.g. for a failed login. */
  async penalty(points: number, key?: string): Promise<void> {
    await Promise.all([...this.#targets].map((t) => t.penalty(points, key)));
  }

  /** Give `points` permits back; `Infinity` resets the count. */
  async reward(points: number, key?: string): Promise<void> {
    await Promise.all([...this.#targets].map((t) => t.reward(points, key)));
  }

  /** Reject everything (for `key`) for `duration`. */
  block(duration: Duration, key?: string): void {
    for (const t of this.#targets) t.block(duration, key);
  }

  unblock(key?: string): void {
    for (const t of this.#targets) t.unblock(key);
  }

  /** Forget the state of `key` (or of the single limiter): fresh quota, no block. */
  async delete(key?: string): Promise<void> {
    await Promise.all([...this.#targets].map((t) => t.delete(key)));
  }

  /** @internal */
  attach(target: RateLimitTarget): void {
    this.#targets.add(target);
  }
}

/** @internal What every limiter (rate, concurrency, adaptive) shares for rejections. */
export interface RejectionOptions {
  name?: string;
  onRejected?: (args: OnRateLimiterRejectedArgs) => MaybePromise<void>;
}

/** @internal Reports `OnRateLimiterRejected`, calls `onRejected`, and fails with the stamped `error`. */
export async function reject<T>(options: RejectionOptions, source: StrategySource, context: ResilienceContext, error: RateLimiterRejectedError) {
  const args: OnRateLimiterRejectedArgs = { context, retryAfter: error.retryAfter };
  report(source, 'OnRateLimiterRejected', context, args);
  await options.onRejected?.(args);
  return fail<T>(stamp(error, source));
}

export interface PermitRateLimiterOptions extends RejectionOptions {
  /** Weight of an execution (e.g. batch size, cost). Default 1. */
  permits?: (context: ResilienceContext) => number;
  /** Longest an execution may wait for permits before it is rejected. Waiters are served first come, first served. Default 0: reject right away. */
  queueTimeout?: Duration;
  /** Most executions waiting (with `queueTimeout`); beyond it they are rejected at once. Default: no limit, as `queueTimeout` (default 0) already decides whether anyone waits. */
  queueLimit?: number;
  /** Once an execution is rejected for exceeding the limit, reject everything for this long (brute-force protection). */
  blockDuration?: Duration;
  /** Bypass the limiter, e.g. internal callers. */
  allow?: (context: ResilienceContext) => boolean;
  /** Reject with `RateLimiterDeniedError` right away (checked before `allow`). */
  deny?: (context: ResilienceContext) => boolean;
  control?: RateLimiterControl;
  /**
   * In-memory fallback for a remote algorithm: when `acquire` throws or exceeds `timeout`, `algorithm` answers instead
   * and `OnRateLimiterInsuranceUsed` is reported. Size it per process (e.g. the shared limit / instances).
   */
  insurance?: { algorithm: RateLimiterAlgorithm; timeout?: Duration };
  /**
   * `'even'`: delays granted executions so the quota left is spread evenly until the limit resets, instead of
   * bursting at the start of the window. The delay counts against `queueTimeout` when that is set; abort ends it.
   */
  spacing?: 'even';
  /** Least time between the starts of two granted executions (bottleneck's `minTime`); also the floor of `'even'` spacing. */
  minTime?: Duration;
}

/** `RateLimiterStrategy.snapshot()`: the algorithm's state plus the remaining block, as of `at`. */
export interface RateLimiterSnapshot {
  readonly at: number;
  readonly algorithm: unknown;
  readonly blockedFor: number;
}

/** `PartitionedRateLimiterStrategy.snapshot()`: per-key snapshots, least recently used first. */
export interface PartitionedRateLimiterSnapshot {
  readonly partitions: ReadonlyArray<readonly [string, RateLimiterSnapshot]>;
}

export interface FixedWindowRateLimiterOptions extends PermitRateLimiterOptions {
  /** Permits granted per window. */
  permitLimit: number;
  window: Duration;
  /**
   * `'fixed'` (default): windows follow each other from the limiter's creation. `'first-request'`: a window starts at
   * the first request after the previous one ended (per partition, when partitioned).
   */
  alignment?: 'fixed' | 'first-request';
}

export interface SlidingWindowRateLimiterOptions extends FixedWindowRateLimiterOptions {
  /** Buckets the window slides by. More = smoother and more exact, at O(segments) per call. Default 10. */
  segmentsPerWindow?: number;
}

export interface OnRateLimiterInsuranceUsedArgs {
  readonly context: ResilienceContext;
  /** Why the primary algorithm was skipped: its error, or a `TimeoutRejectedError`. */
  readonly error: unknown;
}

interface Acquired {
  readonly result: AcquireResult;
  readonly limit: number;
}

interface Waiter {
  readonly permits: number;
  readonly context: ResilienceContext;
  readonly deadline: number;
  resolve(acquired: Acquired): void;
  reject(error: unknown): void;
}

/** Runs any `RateLimiterAlgorithm` as a strategy: weights, FIFO waiting up to `queueTimeout`, block, allow/deny, insurance, telemetry. */
export class RateLimiterStrategy<T> implements ResilienceStrategy<T> {
  readonly #algorithm: RateLimiterAlgorithm;
  readonly #options: PermitRateLimiterOptions;
  readonly #source: StrategySource;
  readonly #queueTimeout: number;
  readonly #blockDuration: number;
  readonly #insuranceTimeout: number | undefined;
  readonly #minTime: number;
  readonly #queue: Waiter[] = [];
  #pumping = false;
  #wake: (() => void) | undefined;
  #blockedUntil = 0;
  #nextStart = 0;

  constructor(algorithm: RateLimiterAlgorithm, options: PermitRateLimiterOptions, source: StrategySource) {
    this.#queueTimeout = toMs(options.queueTimeout ?? 0, 'queueTimeout');
    check(this.#queueTimeout >= 0, 'queueTimeout must be >= 0');
    check(options.queueLimit === undefined || options.queueLimit >= 0, 'queueLimit must be >= 0');
    this.#blockDuration = toMs(options.blockDuration ?? 0, 'blockDuration');
    check(this.#blockDuration >= 0, 'blockDuration must be >= 0');
    this.#insuranceTimeout = options.insurance?.timeout === undefined ? undefined : toMs(options.insurance.timeout, 'insurance.timeout');
    check(this.#insuranceTimeout === undefined || this.#insuranceTimeout > 0, 'insurance.timeout must be > 0');
    this.#minTime = toMs(options.minTime ?? 0, 'minTime');
    check(this.#minTime >= 0, 'minTime must be >= 0');
    check(options.spacing === undefined || options.spacing === 'even', "spacing must be 'even'");
    this.#algorithm = algorithm;
    this.#options = options;
    this.#source = source;
    options.control?.attach(this);
  }

  /** Executions waiting for permits. */
  get queued(): number {
    return this.#queue.length;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const o = this.#options;
    const time = this.#source.timeProvider;
    const deadline = time.now() + this.#queueTimeout;
    let permits: number;
    try {
      if (o.deny?.(context)) return reject<T>(o, this.#source, context, new RateLimiterDeniedError());
      if (o.allow?.(context)) return next(context);
      permits = o.permits?.(context) ?? 1;
    } catch (error) {
      return fail<T>(error);
    }
    if (!(Number.isFinite(permits) && permits >= 0)) return fail<T>(new RangeError(`permits must be a finite number >= 0, got ${permits}`));
    const limit = this.#algorithm.permitLimit;
    if (permits > limit) return reject<T>(o, this.#source, context, new PermitLimitExceededError(permits, limit));
    let acquired: Acquired | undefined;
    try {
      acquired = await this.#admit(permits, context, deadline);
    } catch (error) {
      return fail<T>(error);
    }
    if (!acquired) return reject<T>(o, this.#source, context, new RateLimiterRejectedError(undefined, 'The rate limiter queue is full.'));
    const r = acquired.result;
    if (!r.ok) {
      const now = time.now();
      let retryAfter = r.retryAfter;
      // A rejection during a block does not extend it.
      if (this.#blockDuration > 0 && this.#blockedUntil <= now) {
        this.#blockedUntil = now + this.#blockDuration;
        retryAfter = Math.max(retryAfter, this.#blockDuration);
      }
      context.properties.set(RATE_LIMIT_INFO, { limit: acquired.limit, remaining: 0, resetAfter: retryAfter } satisfies RateLimitInfo);
      return reject<T>(o, this.#source, context, new RateLimiterRejectedError(retryAfter));
    }
    if (r.remaining !== undefined && r.resetAfter !== undefined) {
      context.properties.set(RATE_LIMIT_INFO, { limit: acquired.limit, remaining: r.remaining, resetAfter: r.resetAfter } satisfies RateLimitInfo);
    }
    const wait = this.#pace(r, deadline);
    if (wait > 0) {
      // shortcut: permits of an execution aborted while paced are not refunded; refund via adjust if that matters.
      try {
        await delay(wait, context.signal, time);
      } catch (error) {
        return fail<T>(error);
      }
    }
    return next(context);
  }

  /** ms to hold a granted execution back for `spacing` / `minTime`; reserves its start slot. */
  #pace(r: AcquireResult & { ok: true }, deadline: number): number {
    if (!this.#options.spacing && !this.#minTime) return 0;
    const now = this.#source.timeProvider.now();
    const latest = this.#options.queueTimeout === undefined ? Infinity : Math.max(now, deadline);
    const start = Math.min(Math.max(now, this.#nextStart), latest);
    let gap = this.#minTime;
    // Spread what is left of the quota (this execution included) over the time until the limit resets, counted from this start.
    if (this.#options.spacing === 'even' && r.remaining !== undefined && r.resetAfter !== undefined) {
      gap = Math.max(gap, Math.max(0, r.resetAfter - (start - now)) / (r.remaining + 1));
    }
    // max(): a start cut short by queueTimeout does not pull later slots forward, nor push them past its own slot.
    this.#nextStart = Math.max(this.#nextStart, start + gap);
    return start - now;
  }

  /** The algorithm's state and the block, JSON-serializable, for `restore` after a restart. */
  snapshot(): RateLimiterSnapshot {
    const a = this.#algorithm;
    if (!a.snapshot) throw new TypeError('This rate limiter algorithm does not support snapshot / restore.');
    const now = this.#source.timeProvider.now();
    return { at: now, algorithm: a.snapshot(now), blockedFor: Math.max(0, this.#blockedUntil - now) };
  }

  /** Loads a `snapshot()`; time passed since it was taken counts (windows expire, tokens refill, blocks run out). */
  restore(snapshot: RateLimiterSnapshot): void {
    const a = this.#algorithm;
    if (!a.restore) throw new TypeError('This rate limiter algorithm does not support snapshot / restore.');
    checkState(isNum(snapshot?.at) && isNum(snapshot.blockedFor), 'rate limiter');
    a.restore(snapshot.algorithm, this.#source.timeProvider.now());
    this.#blockedUntil = snapshot.blockedFor > 0 ? snapshot.at + snapshot.blockedFor : 0;
    this.#wake?.();
  }

  async penalty(points: number): Promise<void> {
    await this.#adjust(points);
  }

  async reward(points: number): Promise<void> {
    await this.#adjust(-points);
    this.#wake?.();
  }

  block(duration: Duration): void {
    this.#blockedUntil = this.#source.timeProvider.now() + toMs(duration, 'duration');
  }

  unblock(): void {
    this.#blockedUntil = 0;
    this.#wake?.();
  }

  /** Fresh quota and no block. */
  async delete(): Promise<void> {
    this.#blockedUntil = 0;
    await this.reward(Infinity);
  }

  async #adjust(points: number) {
    if (!this.#algorithm.adjust) throw new TypeError('This rate limiter algorithm does not support penalty / reward.');
    await this.#algorithm.adjust(points, this.#source.timeProvider.now());
  }

  /** Resolves with the admission (granted or not), or undefined when the queue is full. */
  async #admit(permits: number, context: ResilienceContext, deadline: number): Promise<Acquired | undefined> {
    const queue = this.#queue;
    let firstWait = 0;
    if (queue.length === 0) {
      const first = await this.#acquire(permits, context);
      if (first.result.ok || this.#source.timeProvider.now() + first.result.retryAfter > deadline || this.#options.queueLimit === 0) return first;
      firstWait = first.result.retryAfter;
    }
    // Checked after the first acquire too: others may have queued while it was awaited.
    if (queue.length >= (this.#options.queueLimit ?? Infinity)) return undefined;
    const { signal } = context;
    if (signal.aborted) throw signal.reason;
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        const i = queue.indexOf(waiter);
        if (i >= 0) queue.splice(i, 1);
        if (i === 0) this.#wake?.();
        reject(signal.reason);
      };
      const waiter: Waiter = {
        permits,
        context,
        deadline,
        resolve: (a) => (signal.removeEventListener('abort', onAbort), resolve(a)),
        reject: (e) => (signal.removeEventListener('abort', onAbort), reject(e)),
      };
      signal.addEventListener('abort', onAbort, { once: true });
      queue.push(waiter);
      if (!this.#pumping) void this.#pump(firstWait);
    });
  }

  /** Serves waiters in arrival order: the head waits until its permits fit, nobody overtakes it. */
  async #pump(wait: number) {
    const queue = this.#queue;
    const time = this.#source.timeProvider;
    this.#pumping = true;
    for (;;) {
      if (wait > 0) {
        await new Promise<void>((resolve) => {
          const timer = time.setTimeout(resolve, wait);
          this.#wake = () => (time.clearTimeout(timer), resolve());
        });
        this.#wake = undefined;
      }
      const w = queue[0];
      if (!w) break;
      let acquired: Acquired;
      try {
        acquired = await this.#acquire(w.permits, w.context);
      } catch (error) {
        if (queue[0] === w) queue.shift(), w.reject(error);
        wait = 0;
        continue;
      }
      // shortcut: permits acquired for a waiter that aborted meanwhile (async algorithm only) are not refunded; refund via adjust if that matters.
      wait = 0;
      if (queue[0] !== w) continue;
      const r = acquired.result;
      if (r.ok || time.now() + r.retryAfter > w.deadline) queue.shift(), w.resolve(acquired);
      else wait = r.retryAfter;
    }
    this.#pumping = false;
  }

  async #acquire(permits: number, context: ResilienceContext): Promise<Acquired> {
    const now = this.#source.timeProvider.now();
    if (this.#blockedUntil > now) return { result: { ok: false, retryAfter: this.#blockedUntil - now }, limit: this.#algorithm.permitLimit };
    const insurance = this.#options.insurance?.algorithm;
    if (!insurance) return { result: await this.#algorithm.acquire(permits, now), limit: this.#algorithm.permitLimit };
    try {
      return { result: await this.#primary(permits, now), limit: this.#algorithm.permitLimit };
    } catch (error) {
      report(this.#source, 'OnRateLimiterInsuranceUsed', context, { context, error } satisfies OnRateLimiterInsuranceUsedArgs);
      // Degraded mode: a weight above the smaller insurance limit is charged the whole insurance limit instead of failing.
      return { result: await insurance.acquire(Math.min(permits, insurance.permitLimit), now), limit: insurance.permitLimit };
    }
  }

  #primary(permits: number, now: number): Promise<AcquireResult> {
    const result = new Promise<AcquireResult>((resolve) => resolve(this.#algorithm.acquire(permits, now)));
    const ms = this.#insuranceTimeout;
    if (ms === undefined) return result;
    const time = this.#source.timeProvider;
    return new Promise((resolve, reject) => {
      const timer = time.setTimeout(() => reject(new TimeoutRejectedError(ms)), ms);
      void result.then(resolve, reject).finally(() => time.clearTimeout(timer));
    });
  }
}

function windowCounter(options: FixedWindowRateLimiterOptions, segments: number, source: StrategySource) {
  check(Number.isInteger(options.permitLimit) && options.permitLimit >= 1, 'permitLimit must be an integer >= 1');
  const window = toMs(options.window, 'window');
  check(window > 0, 'window must be > 0');
  check(Number.isInteger(segments) && segments >= 1, 'segmentsPerWindow must be an integer >= 1');
  check(options.alignment === undefined || options.alignment === 'fixed' || options.alignment === 'first-request', "alignment must be 'fixed' or 'first-request'");
  return new WindowCounter(options.permitLimit, window, segments, source.timeProvider.now(), options.alignment);
}

/** `permitLimit` per window; the count resets at each window boundary (a burst of 2x can straddle a boundary). */
export class FixedWindowRateLimiterStrategy<T> extends RateLimiterStrategy<T> {
  constructor(options: FixedWindowRateLimiterOptions, source: StrategySource) {
    super(windowCounter(options, 1, source), options, source);
  }
}

/** `permitLimit` in any `window`-long span (to one segment's precision): no 2x burst at window edges. */
export class SlidingWindowRateLimiterStrategy<T> extends RateLimiterStrategy<T> {
  constructor(options: SlidingWindowRateLimiterOptions, source: StrategySource) {
    super(windowCounter(options, options.segmentsPerWindow ?? 10, source), options, source);
  }
}

export interface PartitionedRateLimiterOptions<T> {
  name?: string;
  /** Which partition an execution counts against, e.g. `(ctx) => ctx.properties.get('userId') as string`. */
  partitionKey: (context: ResilienceContext) => string;
  /** Creates the limiter of a new partition, e.g. `(key, s) => new SlidingWindowRateLimiterStrategy({ permitLimit: 100, window: '1m' }, s)`. */
  limiter: (key: string, source: StrategySource) => ResilienceStrategy<T>;
  /**
   * A partition unused this long (and with nothing running) is dropped, resetting its limit. Default 10 minutes.
   * Keep it at least as long as the limiter's window, or an evicted partition gets a fresh quota early.
   */
  idleTimeout?: Duration;
  /**
   * Most partitions kept; past it the least recently used partition with nothing running is dropped (its limit resets).
   * When all of them are running, a new key is rejected with `RateLimiterRejectedError`. Default: no limit.
   */
  maxPartitions?: number;
  /** Penalty / reward / block per key. The per-key limiters must be `RateLimiterStrategy`s (fixed, sliding, token bucket). A block is lost if its partition is evicted, so keep `idleTimeout` >= the block. */
  control?: RateLimiterControl;
}

interface Partition<T> {
  readonly strategy: ResilienceStrategy<T>;
  active: number;
  lastUsed: number;
}

/** One limiter per key (user, tenant, API key), so one noisy key can't use up the others' quota. */
export class PartitionedRateLimiterStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: PartitionedRateLimiterOptions<T>;
  readonly #source: StrategySource;
  readonly #idleTimeout: number;
  // Map order = least recently used first: every touch re-inserts the entry.
  readonly #partitions = new Map<string, Partition<T>>();

  constructor(options: PartitionedRateLimiterOptions<T>, source: StrategySource) {
    this.#idleTimeout = toMs(options.idleTimeout ?? '10m', 'idleTimeout');
    check(this.#idleTimeout > 0, 'idleTimeout must be > 0');
    check(options.maxPartitions === undefined || (Number.isInteger(options.maxPartitions) && options.maxPartitions >= 1), 'maxPartitions must be an integer >= 1');
    this.#options = options;
    this.#source = source;
    options.control?.attach(this);
  }

  /** Partitions currently held in memory. */
  get partitions(): number {
    return this.#partitions.size;
  }

  /** Keys of the partitions held in memory, least recently used first. */
  keys(): string[] {
    return [...this.#partitions.keys()];
  }

  /** Drops the partition of `key` (every partition when omitted): it starts over with a fresh quota and no block. */
  delete(key?: string): boolean {
    if (key !== undefined) return this.#partitions.delete(key);
    this.#partitions.clear();
    return true;
  }

  /** Snapshots of the `limit` most recently used partitions (default all), for `restore` after a restart. */
  snapshot(limit = Infinity): PartitionedRateLimiterSnapshot {
    const keys = this.keys();
    return { partitions: keys.slice(Math.max(0, keys.length - limit)).map((key) => [key, this.#limiters(key, false)[0]!.snapshot()] as const) };
  }

  /** Recreates the partitions of a `snapshot()` (still bounded by `maxPartitions`) and loads their state. */
  restore(snapshot: PartitionedRateLimiterSnapshot): void {
    checkState(Array.isArray(snapshot?.partitions), 'partitioned rate limiter');
    for (const [key, state] of snapshot.partitions) this.#limiters(String(key), true)[0]!.restore(state);
  }

  async penalty(points: number, key?: string): Promise<void> {
    await Promise.all(this.#limiters(key, true).map((l) => l.penalty(points)));
  }

  async reward(points: number, key?: string): Promise<void> {
    await Promise.all(this.#limiters(key, false).map((l) => l.reward(points)));
  }

  block(duration: Duration, key?: string): void {
    for (const l of this.#limiters(key, true)) l.block(duration);
  }

  unblock(key?: string): void {
    for (const l of this.#limiters(key, false)) l.unblock();
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    let key: string;
    try {
      key = this.#options.partitionKey(context);
    } catch (error) {
      return fail<T>(error);
    }
    const partitions = this.#partitions;
    const partition = this.#touch(key, 1);
    // Still over maxPartitions: every other partition is running, so this new key can't get one.
    if (partitions.size > (this.#options.maxPartitions ?? Infinity)) {
      partitions.delete(key);
      return reject<T>(this.#options, this.#source, context, new RateLimiterRejectedError(undefined, 'Every rate limiter partition is in use (maxPartitions).'));
    }
    try {
      return await partition.strategy.execute(next, context);
    } finally {
      partition.active--;
      partition.lastUsed = this.#source.timeProvider.now();
      if (partitions.get(key) === partition) {
        partitions.delete(key);
        partitions.set(key, partition);
      }
    }
  }

  /** Gets or creates the partition of `key` and marks it most recently used. */
  #touch(key: string, active: number): Partition<T> {
    const now = this.#source.timeProvider.now();
    const partitions = this.#partitions;
    this.#evictIdle(now);
    let partition = partitions.get(key);
    if (partition) partitions.delete(key);
    else partition = { strategy: this.#options.limiter(key, this.#source), active: 0, lastUsed: 0 };
    partitions.set(key, partition);
    partition.active += active;
    partition.lastUsed = now;
    this.#evictOverflow();
    return partition;
  }

  /** The limiter of `key` (created if `create`), or of every partition when `key` is omitted. */
  #limiters(key: string | undefined, create: boolean): RateLimiterStrategy<T>[] {
    const found = key === undefined ? [...this.#partitions.values()] : create ? [this.#touch(key, 0)] : [this.#partitions.get(key)].filter((p) => p !== undefined);
    return found.map(({ strategy }) => {
      if (!(strategy instanceof RateLimiterStrategy)) throw new TypeError('Rate limiter control needs RateLimiterStrategy partitions (fixed / sliding window, token bucket).');
      return strategy;
    });
  }

  #evictIdle(now: number) {
    for (const [key, p] of this.#partitions) {
      if (p.lastUsed + this.#idleTimeout > now) break; // the rest were used more recently
      if (p.active === 0) this.#partitions.delete(key);
    }
  }

  #evictOverflow() {
    const max = this.#options.maxPartitions;
    if (max === undefined) return;
    for (const [key, p] of this.#partitions) {
      if (this.#partitions.size <= max) return;
      if (p.active === 0) this.#partitions.delete(key);
    }
  }
}

export const fixedWindowRateLimiterStrategy = <T = any>(options: FixedWindowRateLimiterOptions): StrategyDefinition<T, RateLimiterRejectedError> => ({
  name: options.name ?? 'FixedWindowRateLimiter',
  create: (s) => new FixedWindowRateLimiterStrategy<T>(options, s),
});

export const slidingWindowRateLimiterStrategy = <T = any>(options: SlidingWindowRateLimiterOptions): StrategyDefinition<T, RateLimiterRejectedError> => ({
  name: options.name ?? 'SlidingWindowRateLimiter',
  create: (s) => new SlidingWindowRateLimiterStrategy<T>(options, s),
});

export const partitionedRateLimiterStrategy = <T = any>(options: PartitionedRateLimiterOptions<T>): StrategyDefinition<T, RateLimiterRejectedError> => ({
  name: options.name ?? 'PartitionedRateLimiter',
  create: (s) => new PartitionedRateLimiterStrategy<T>(options, s),
});
