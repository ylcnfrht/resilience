import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type Next,
  type Outcome,
  type OutcomeArgs,
  type ResilienceContext,
  type ResilienceStrategy,
  type StrategySource,
  TimeoutRejectedError,
  check,
  fail,
  isAbortError,
  isClientError,
  optionalMs,
  report,
  BulkheadRejectedError,
} from './core.ts';
import { type RejectionOptions, reject } from './window-rate-limiter.ts';

/** One finished execution, fed to the limit algorithm. */
export interface LimitSample {
  /** Latency of the execution in ms. */
  readonly rtt: number;
  /** Executions running when this one started (itself included). */
  readonly inflight: number;
  /** The dependency showed overload (timeout, rejection, transient error). */
  readonly dropped: boolean;
  /** The current limit (may be fractional). */
  readonly limit: number;
}

/** Computes the next concurrency limit from a sample. Stateful: give each strategy its own instance. The strategy clamps the result to [minLimit, maxLimit]. */
export interface LimitAlgorithm {
  onSample(sample: LimitSample): number;
}

// log10 is 0 at limit 1 and < 1 below 10: the floor keeps small limits able to move.
const log = (limit: number) => Math.max(1, Math.log10(limit));

// About `limit` samples finish per round trip and all of them describe the same congestion. Acting fully on each one overshoots
// (the limit swings 2-3x around capacity), so per-sample moves are damped by sqrt(limit): steady, yet large limits still move fast.
const damp = (limit: number) => 1 / Math.sqrt(limit);

export interface VegasOptions {
  /** Grow while the estimated queue is below `alpha * log10(limit)`. Default 3. */
  alpha?: number;
  /** Shrink when the estimated queue is above `beta * log10(limit)`. Default 6. */
  beta?: number;
}

/**
 * TCP Vegas applied to concurrency. The fastest latency seen is the no-queue latency; with `limit` calls in flight at latency `rtt`,
 * `limit * (1 - minRtt / rtt)` of them are waiting in the dependency's queue. Keep that queue between alpha and beta: below means
 * unused capacity, above only adds latency. Thresholds scale with log10(limit) so large limits tolerate a longer queue.
 * Instead of Vegas' fixed +-1 steps, the limit moves toward the band by the queue error (damped), which converges in a few round trips.
 */
export function vegas(options: VegasOptions = {}): LimitAlgorithm {
  const alpha = options.alpha ?? 3;
  const beta = options.beta ?? 6;
  check(alpha > 0 && beta > alpha, 'vegas: need 0 < alpha < beta');
  // shortcut: minRtt never resets, so a permanently slower dependency settles at a lower limit; add a periodic probe (drop to minLimit, re-measure) if that matters.
  let minRtt = Infinity;
  return {
    onSample({ rtt, dropped, limit }) {
      const step = log(limit);
      if (dropped) return limit - beta * step; // explicit overload: drop at least the tolerated queue
      if (rtt <= 0) return limit;
      minRtt = Math.min(minRtt, rtt);
      const queue = limit * (1 - minRtt / rtt);
      const error = queue < alpha * step ? alpha * step - queue : queue > beta * step ? beta * step - queue : 0;
      return limit + error * damp(limit);
    },
  };
}

export interface Gradient2Options {
  /** How much the short-term latency may exceed the long-term one before the limit shrinks. Default 1.5. */
  tolerance?: number;
  /** Samples averaged by the short-term latency EMA. Default 10. */
  shortWindow?: number;
  /** Samples averaged by the long-term latency EMA. Default 600. */
  longWindow?: number;
  /** Weight of each new limit estimate (0..1]; lower is steadier. Default 0.2. */
  smoothing?: number;
  /** Headroom added on top of `limit * gradient`, so the limit can grow. Default `sqrt(limit)`. */
  queueSize?: (limit: number) => number;
  /** Multiplier applied on a drop. Default 0.9. */
  backoffRatio?: number;
}

/**
 * Gradient: compares short-term latency to a long-term baseline. `gradient = tolerance * baseline / short`, clamped to [0.5, 1]:
 * 1 while latency is within tolerance (grow by `queueSize`), below 1 when latency climbs (shrink in proportion). The new estimate is
 * smoothed into the old one so one slow call does not swing the limit.
 *
 * Differs from a plain long-term EMA on purpose: under sustained overload a plain EMA drifts up to the overloaded latency, which then
 * looks normal and the limit creeps up without bound. Here the baseline drops to any lower short-term latency at once, and rises
 * (slowly, over `longWindow` samples) only while latency is within tolerance.
 */
export function gradient2(options: Gradient2Options = {}): LimitAlgorithm {
  const tolerance = options.tolerance ?? 1.5;
  const smoothing = options.smoothing ?? 0.2;
  const backoff = options.backoffRatio ?? 0.9;
  const queueSize = options.queueSize ?? Math.sqrt;
  const shortK = 2 / ((options.shortWindow ?? 10) + 1);
  const longK = 2 / ((options.longWindow ?? 600) + 1);
  check(tolerance >= 1, 'gradient2: tolerance must be >= 1');
  check(smoothing > 0 && smoothing <= 1, 'gradient2: smoothing must be in (0, 1]');
  check(backoff > 0 && backoff < 1, 'gradient2: backoffRatio must be in (0, 1)');
  // shortcut: like vegas, a dependency that gets permanently slower (2x+ at low load) settles at a lower limit; add a probe if that matters.
  let short: number | undefined;
  let baseline: number | undefined;
  return {
    onSample({ rtt, dropped, limit }) {
      if (dropped) return limit * backoff;
      // A 0 ms sample (coarse clock) carries no latency signal and would pin the baseline at 0 for good.
      if (!(rtt > 0)) return limit;
      short = short === undefined ? rtt : short + shortK * (rtt - short);
      if (baseline === undefined || short < baseline) baseline = short;
      else if (short <= tolerance * baseline) baseline += longK * (short - baseline);
      const gradient = Math.max(0.5, Math.min(1, (tolerance * baseline) / short));
      const estimate = limit * gradient + queueSize(limit);
      const weight = smoothing * damp(limit);
      return limit * (1 - weight) + estimate * weight;
    },
  };
}

export interface AimdOptions {
  /** Added per round trip (`increase / limit` per sample, as TCP does). Default 1. */
  increase?: number;
  /** Multiplier applied on a drop. Default 0.9. */
  backoffRatio?: number;
}

/** Additive increase, multiplicative decrease: the TCP Reno baseline. Ignores latency, reacts only to drops (so pair it with a timeout). */
export function aimd(options: AimdOptions = {}): LimitAlgorithm {
  const increase = options.increase ?? 1;
  const backoff = options.backoffRatio ?? 0.9;
  check(increase > 0, 'aimd: increase must be > 0');
  check(backoff > 0 && backoff < 1, 'aimd: backoffRatio must be in (0, 1)');
  return { onSample: ({ dropped, limit }) => (dropped ? limit * backoff : limit + increase / limit) };
}

export interface OnLimitChangedArgs {
  readonly oldLimit: number;
  readonly newLimit: number;
  readonly sample: LimitSample;
}

interface LimiterReader {
  limit: () => number;
  inflight: () => number;
  queued: () => number;
}

/** Reads the live limit of one adaptive limiter (health checks, dashboards). */
export class AdaptiveLimitProvider {
  #source: LimiterReader | undefined;

  get limit(): number {
    return this.#source?.limit() ?? 0;
  }

  get inflight(): number {
    return this.#source?.inflight() ?? 0;
  }

  get queued(): number {
    return this.#source?.queued() ?? 0;
  }

  /** @internal */
  attach(source: LimiterReader): void {
    if (this.#source) throw new Error('This provider is already attached to a limiter.');
    this.#source = source;
  }
}

export interface AdaptiveConcurrencyLimiterOptions<T = unknown> extends RejectionOptions {
  /** Default 'gradient2'. Pass an object for custom tuning (`vegas({ alpha: 2 })`) or your own algorithm. */
  algorithm?: 'vegas' | 'gradient2' | 'aimd' | LimitAlgorithm;
  /** Default 20. */
  initialLimit?: number;
  /** Default 1. */
  minLimit?: number;
  /** Default 1000. */
  maxLimit?: number;
  /** Max executions waiting (FIFO) for a permit. Default 0. */
  queueLimit?: number;
  /** Longest a queued execution waits for a permit before it is rejected. Default: no limit. */
  queueTimeout?: Duration;
  /** Which outcomes signal overload. Default: timeouts, rejections and transient errors; not client errors (4xx) or aborts. */
  shouldDrop?: (args: OutcomeArgs<T>) => boolean;
  /** Read `limit` / `inflight` / `queued` from outside. */
  limitProvider?: AdaptiveLimitProvider;
  onLimitChanged?: (args: OnLimitChangedArgs) => void;
}

export function defaultShouldDrop({ outcome, context }: OutcomeArgs<unknown>): boolean {
  if (outcome.ok) return false;
  if (outcome.error instanceof TimeoutRejectedError || context.signal.reason instanceof TimeoutRejectedError) return true;
  return !context.signal.aborted && !isAbortError(outcome.error) && !isClientError(outcome.error);
}

const algorithms = { vegas, gradient2, aimd };

/** Concurrency limiter whose limit follows the dependency: it grows while latency stays flat and shrinks when latency or failures show queueing. */
export class AdaptiveConcurrencyLimiterStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: AdaptiveConcurrencyLimiterOptions<T>;
  readonly #source: StrategySource;
  readonly #algorithm: LimitAlgorithm;
  readonly #min: number;
  readonly #max: number;
  readonly #queueTimeout: number | undefined;
  readonly #shouldDrop: (args: OutcomeArgs<T>) => boolean;
  readonly #queue: Array<() => void> = [];
  #limit: number;
  #inflight = 0;
  #admitted = 0;
  #cutAt = 0;

  constructor(options: AdaptiveConcurrencyLimiterOptions<T>, source: StrategySource) {
    const a = options.algorithm ?? 'gradient2';
    this.#algorithm = typeof a === 'string' ? algorithms[a]?.() : a;
    this.#min = options.minLimit ?? 1;
    this.#max = options.maxLimit ?? 1000;
    this.#limit = options.initialLimit ?? Math.min(this.#max, Math.max(this.#min, 20));
    check(typeof this.#algorithm?.onSample === 'function', "algorithm must be 'vegas', 'gradient2', 'aimd' or an object with onSample()");
    check(this.#min >= 1 && this.#max >= this.#min, 'need 1 <= minLimit <= maxLimit');
    check(this.#limit >= this.#min && this.#limit <= this.#max, 'initialLimit must be within [minLimit, maxLimit]');
    check((options.queueLimit ?? 0) >= 0, 'queueLimit must be >= 0');
    this.#queueTimeout = optionalMs(options.queueTimeout, 'queueTimeout');
    check(this.#queueTimeout === undefined || this.#queueTimeout > 0, 'queueTimeout must be > 0');
    this.#shouldDrop = options.shouldDrop ?? defaultShouldDrop;
    this.#options = options;
    this.#source = source;
    options.limitProvider?.attach({ limit: () => this.limit, inflight: () => this.#inflight, queued: () => this.#queue.length });
  }

  /** Current concurrency limit (whole permits). */
  get limit(): number {
    return Math.floor(this.#limit);
  }

  get inflight(): number {
    return this.#inflight;
  }

  get queued(): number {
    return this.#queue.length;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    if (this.#inflight < this.limit) {
      this.#inflight++;
    } else if (this.#queue.length < (this.#options.queueLimit ?? 0)) {
      const granted = await this.#wait(context.signal);
      if (granted !== true) return granted === 'timeout' ? this.#reject(context, 'queue-timeout') : fail<T>(granted.reason);
    } else {
      return this.#reject(context, 'full');
    }
    const time = this.#source.timeProvider;
    const inflight = this.#inflight;
    const seq = ++this.#admitted;
    const start = time.now();
    let outcome: Outcome<T> | undefined;
    try {
      return (outcome = await next(context));
    } finally {
      this.#inflight--;
      // A caller abort says nothing about the dependency; a timeout abort does (it was too slow).
      const callerAborted = context.signal.aborted && !(context.signal.reason instanceof TimeoutRejectedError);
      const dropped = outcome !== undefined && !callerAborted && this.#shouldDrop({ outcome, context });
      // TCP's one cut per round trip: calls admitted before the last cut ran under the old limit, so their drops are not news.
      if (outcome && !callerAborted && !(dropped && seq <= this.#cutAt)) {
        if (dropped) this.#cutAt = this.#admitted;
        this.#sample({ rtt: time.now() - start, inflight, dropped, limit: this.#limit }, context);
      }
      this.#drain();
    }
  }

  #sample(sample: LimitSample, context: ResilienceContext) {
    let next = this.#algorithm.onSample(sample);
    // App-limited: with less than half the limit in use, low latency proves nothing about higher concurrency, so don't grow.
    if (next > sample.limit && sample.inflight * 2 < sample.limit) next = sample.limit;
    next = Math.min(this.#max, Math.max(this.#min, Number.isFinite(next) ? next : sample.limit));
    const oldLimit = this.limit;
    this.#limit = next;
    if (this.limit === oldLimit) return;
    const args: OnLimitChangedArgs = { oldLimit, newLimit: this.limit, sample };
    report(this.#source, 'OnLimitChanged', context, args);
    this.#options.onLimitChanged?.(args);
  }

  // Hand permits to waiters while there is room; the limit may have grown by more than one.
  #drain() {
    while (this.#queue.length && this.#inflight < this.limit) {
      this.#inflight++;
      this.#queue.shift()!();
    }
  }

  #reject(context: ResilienceContext, reason: 'full' | 'queue-timeout') {
    return reject<T>(this.#options, this.#source, context, new BulkheadRejectedError(reason, this.#inflight, this.#queue.length));
  }

  #wait(signal: AbortSignal): Promise<true | 'timeout' | { reason: unknown }> {
    if (signal.aborted) return Promise.resolve({ reason: signal.reason });
    return new Promise((resolve) => {
      const time = this.#source.timeProvider;
      let timer: unknown;
      const leave = (result: 'timeout' | { reason: unknown }) => {
        this.#queue.splice(this.#queue.indexOf(grant), 1);
        signal.removeEventListener('abort', onAbort);
        time.clearTimeout(timer);
        resolve(result);
      };
      const grant = () => {
        signal.removeEventListener('abort', onAbort);
        time.clearTimeout(timer);
        resolve(true);
      };
      const onAbort = () => leave({ reason: signal.reason });
      this.#queue.push(grant);
      signal.addEventListener('abort', onAbort, { once: true });
      if (this.#queueTimeout !== undefined) timer = time.setTimeout(() => leave('timeout'), this.#queueTimeout);
    });
  }
}

export const adaptiveConcurrencyLimiterStrategy = <T = any>(options: AdaptiveConcurrencyLimiterOptions<T> = {}): StrategyDefinition<T, BulkheadRejectedError> => ({
  name: options.name ?? 'AdaptiveConcurrencyLimiter',
  create: (s) => new AdaptiveConcurrencyLimiterStrategy<T>(options, s),
});
