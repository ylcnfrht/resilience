import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type MaybePromise,
  type Next,
  type OutcomeArgs,
  type ResilienceContext,
  type ResilienceStrategy,
  type ShouldHandle,
  type StrategySource,
  check,
  delay,
  discardOutcome,
  fail,
  isBrokenCircuitError,
  isNonRetryableError,
  isRejection,
  nextAttemptNumber,
  optionalMs,
  report,
  resolvePredicate,
  toMs,
} from './core.ts';
import { type RetryBudget } from './retry-budget.ts';

export type BackoffType = 'constant' | 'linear' | 'exponential';

/**
 * - `true`: Polly's jitter (±25% for constant/linear; decorrelated jitter V2 for exponential, which grows ~×2 per
 *   attempt by design and ignores `factor`).
 * - `'full'`: random between 0 and the computed delay, best for spreading a fleet of callers.
 * - `'equal'`: half the computed delay plus a random half, never shorter than half.
 */
export type Jitter = boolean | 'full' | 'equal';

export interface RetryPredicateArgs<T> extends OutcomeArgs<T> {
  /** 0 for the first (original) attempt. */
  readonly attemptNumber: number;
}

export interface OnRetryArgs<T> extends RetryPredicateArgs<T> {
  readonly retryDelay: number;
  /** How long the failed attempt took, in ms. */
  readonly duration: number;
}

export interface OnFailedAttemptArgs<T> extends RetryPredicateArgs<T> {
  /** Retries still allowed after this failure (0 on the last one). */
  readonly retriesLeft: number;
  readonly duration: number;
}

/** `OnExecutionAttempt` telemetry: emitted by the pipeline for every call of the callback. */
export interface OnExecutionAttemptArgs<T> extends RetryPredicateArgs<T> {
  readonly duration: number;
}

export interface RetryOptions<T> {
  name?: string;
  /** Default: transient errors (not 4xx except 408/429, not aborts, not rejections). An open circuit is never retried. */
  shouldHandle?: ShouldHandle<RetryPredicateArgs<T>>;
  /** Retries in addition to the original call. Default 3. `Infinity` retries until aborted. */
  maxRetryAttempts?: number;
  /** Base delay. Default 2000 ms. */
  delay?: Duration;
  /** Default 'constant'. */
  backoffType?: BackoffType;
  /** Growth per retry for `'exponential'` (delay × factor^attempt). Default 2. Ignored with `useJitter: true` (decorrelated jitter). */
  factor?: number;
  useJitter?: Jitter;
  /** Caps computed delays (not those from `delayGenerator`). */
  maxDelay?: Duration;
  /** Return a delay (e.g. from a Retry-After header); `undefined` or negative falls back to the computed delay. */
  delayGenerator?: (args: RetryPredicateArgs<T>) => MaybePromise<Duration | undefined>;
  /** Wait at least an error's `retryAfter` (ms), e.g. a rate-limit rejection's; still capped by `maxDelay`. Default true. */
  respectRetryAfter?: boolean;
  /** Wait this long before the first attempt too (e.g. to let a just-started dependency come up). */
  initialDelay?: Duration;
  /** Stop scheduling retries once this much time has passed since the first attempt (the attempt in flight isn't cancelled). */
  maxRetryTime?: Duration;
  /** Return false for failures that shouldn't use up a retry (e.g. a token refresh); they are retried without counting. */
  shouldConsumeRetry?: (args: RetryPredicateArgs<T>) => MaybePromise<boolean>;
  /** Before each retry's delay. */
  onRetry?: (args: OnRetryArgs<T>) => MaybePromise<void>;
  /** After every handled failure, the last one included, with how many retries are left. */
  onFailedAttempt?: (args: OnFailedAttemptArgs<T>) => MaybePromise<void>;
  /** When retries run out (or `maxRetryTime` passes) and the last outcome is still a failure. */
  onRetryExhausted?: (args: RetryPredicateArgs<T>) => MaybePromise<void>;
  /** Random source in [0, 1) used for jitter. */
  randomizer?: () => number;
  /** Shared cap on retries relative to traffic (retry-storm protection). Share one instance per dependency. */
  budget?: RetryBudget;
  /** Called when the budget refuses a retry; the last outcome is returned. */
  onBudgetExhausted?: (args: RetryPredicateArgs<T>) => MaybePromise<void>;
}

export class RetryStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: RetryOptions<T>;
  readonly #source: StrategySource;
  readonly #shouldHandle;
  readonly #max: number;
  readonly #delay: number;
  readonly #factor: number;
  readonly #maxDelay: number | undefined;
  readonly #initialDelay: number;
  readonly #maxRetryTime: number | undefined;

  constructor(options: RetryOptions<T>, source: StrategySource) {
    this.#options = options;
    this.#source = source;
    this.#shouldHandle = resolvePredicate(options.shouldHandle);
    this.#max = options.maxRetryAttempts ?? 3;
    this.#delay = toMs(options.delay ?? 2000, 'delay');
    this.#factor = options.factor ?? 2;
    this.#maxDelay = optionalMs(options.maxDelay, 'maxDelay');
    this.#initialDelay = toMs(options.initialDelay ?? 0, 'initialDelay');
    this.#maxRetryTime = optionalMs(options.maxRetryTime, 'maxRetryTime');
    check(this.#max >= 0, 'maxRetryAttempts must be >= 0');
    check(this.#delay >= 0, 'delay must be >= 0');
    check(this.#factor >= 1, 'factor must be >= 1');
    check(this.#maxDelay === undefined || this.#maxDelay >= 0, 'maxDelay must be >= 0');
    check(this.#initialDelay >= 0, 'initialDelay must be >= 0');
    check(this.#maxRetryTime === undefined || this.#maxRetryTime > 0, 'maxRetryTime must be > 0');
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const o = this.#options;
    const time = context.timeProvider ?? this.#source.timeProvider;
    const jitter = { prev: 0 };
    const firstStarted = time.now();
    let retriesUsed = 0;
    if (this.#initialDelay > 0) {
      try {
        await delay(this.#initialDelay, context.signal, time);
      } catch (error) {
        return fail(error);
      }
    }
    o.budget?.recordRequest();

    for (let attemptNumber = 0; ; attemptNumber++) {
      const started = time.now();
      const outcome = await next(attemptNumber === 0 ? context : { ...context, attemptNumber: nextAttemptNumber(context) });
      const args: RetryPredicateArgs<T> = { outcome, context, attemptNumber };
      const duration = time.now() - started;
      // Hard stops: an open circuit protects a failing dependency (retrying only adds load); NonRetryableError is the caller's call.
      const stop = context.signal.aborted || (!outcome.ok && (isBrokenCircuitError(outcome.error) || isNonRetryableError(outcome.error)));
      // A non-idempotent call is only repeated when it never started (a rejection).
      const mayRepeat = context.idempotent || (!outcome.ok && isRejection(outcome.error));
      const handled = !stop && mayRepeat && (await this.#shouldHandle(args));
      if (!handled) return outcome;

      const consumes = o.shouldConsumeRetry ? await o.shouldConsumeRetry(args) : true;
      const retriesLeft = consumes ? Math.max(0, this.#max - retriesUsed - 1) : this.#max - retriesUsed;
      await o.onFailedAttempt?.({ ...args, retriesLeft, duration });
      if (consumes && retriesUsed >= this.#max) return this.#exhausted(args, outcome);

      let retryDelay = await this.#nextDelay(args, attemptNumber, jitter);
      if (this.#maxRetryTime !== undefined && time.now() + retryDelay - firstStarted > this.#maxRetryTime) return this.#exhausted(args, outcome);
      if (o.budget && !o.budget.tryRetry()) {
        report(this.#source, 'OnRetryBudgetExhausted', context, args);
        await o.onBudgetExhausted?.(args);
        return outcome;
      }
      if (consumes) retriesUsed++;
      discardOutcome(outcome); // a handled success (e.g. a stream) is replaced by the retry

      const onRetryArgs: OnRetryArgs<T> = { ...args, retryDelay, duration };
      report(this.#source, 'OnRetry', context, onRetryArgs);
      await o.onRetry?.(onRetryArgs);
      try {
        await delay(retryDelay, context.signal, time);
      } catch (error) {
        return fail(error);
      }
    }
  }

  async #nextDelay(args: RetryPredicateArgs<T>, attemptNumber: number, jitter: { prev: number }): Promise<number> {
    const o = this.#options;
    const generated = await o.delayGenerator?.(args);
    let retryDelay = generated === undefined ? -1 : toMs(generated, 'delayGenerator result');
    if (retryDelay < 0) retryDelay = computeDelay(o.backoffType ?? 'constant', o.useJitter ?? false, attemptNumber, this.#delay, this.#maxDelay, jitter, o.randomizer, this.#factor);
    const hinted = (o.respectRetryAfter ?? true) && !args.outcome.ok ? (args.outcome.error as { retryAfter?: unknown } | null)?.retryAfter : undefined;
    if (typeof hinted === 'number' && hinted > retryDelay) retryDelay = this.#maxDelay === undefined ? hinted : Math.min(hinted, this.#maxDelay);
    return retryDelay;
  }

  async #exhausted(args: RetryPredicateArgs<T>, outcome: RetryPredicateArgs<T>['outcome']) {
    report(this.#source, 'OnRetryExhausted', args.context, args);
    await this.#options.onRetryExhausted?.(args);
    return outcome;
  }
}

/** Delay before retry number `attemptNumber + 1`. `state` carries the decorrelated-jitter value between attempts. */
export function computeDelay(
  type: BackoffType,
  jitter: Jitter,
  attemptNumber: number,
  base: number,
  maxDelay: number | undefined,
  state: { prev: number } = { prev: 0 },
  random: () => number = Math.random,
  factor = 2,
): number {
  if (jitter === true && type === 'exponential') {
    const d = decorrelatedJitterV2(attemptNumber, base, state, random);
    return maxDelay === undefined ? d : Math.min(d, maxDelay);
  }
  let d = type === 'constant' ? base : type === 'linear' ? base * (attemptNumber + 1) : base * factor ** attemptNumber;
  if (maxDelay !== undefined) d = Math.min(d, maxDelay); // cap first, so 'full'/'equal' spread over the capped value
  if (jitter === 'full') return random() * d;
  if (jitter === 'equal') return d / 2 + random() * (d / 2);
  if (jitter === true) {
    const j = d + d * (random() * 0.5 - 0.25); // ±25%, as in Polly
    return maxDelay === undefined ? j : Math.min(j, maxDelay);
  }
  return d;
}

// Polly's "DecorrelatedJitterBackoffV2" (Polly.Contrib.WaitAndRetry): exponential growth with smoothed randomness.
function decorrelatedJitterV2(attempt: number, base: number, state: { prev: number }, random: () => number): number {
  const t = attempt + random();
  const next = 2 ** t * Math.tanh(Math.sqrt(4 * t));
  if (!Number.isFinite(next)) return Infinity;
  const value = next - state.prev;
  state.prev = next;
  return (value / 1.4) * base;
}

/** `new PipelineBuilder().add(retryStrategy({ ... }))`: same as `addRetry`, but only bundles retry. */
export const retryStrategy = <T = any>(options: RetryOptions<T> = {}): StrategyDefinition<T> => ({ name: options.name ?? 'Retry', create: (s) => new RetryStrategy(options, s) });
