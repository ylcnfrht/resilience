import type { StrategyDefinition } from './pipeline-builder.ts';
import { type Duration, type MaybePromise, type Next, type ResilienceContext, type ResilienceStrategy, type StrategySource, TimeoutRejectedError, check, delay, discardOutcome, fail, linkSignals, optionalMs, report, stamp, toMs } from './core.ts';

export interface OnTimeoutArgs {
  readonly context: ResilienceContext;
  readonly timeout: number;
}

export interface TimeoutOptions {
  name?: string;
  /** Default 30 s. `Infinity` disables the timeout. */
  timeout?: Duration;
  timeoutGenerator?: (args: { readonly context: ResilienceContext }) => MaybePromise<Duration>;
  onTimeout?: (args: OnTimeoutArgs) => MaybePromise<void>;
  /**
   * `false` (default): give up as soon as the time is up, even if the callback ignores the signal.
   * `true`: abort the signal, then wait for the callback to finish (Polly v8 behaviour), so no work outlives the timeout.
   */
  cooperative?: boolean;
  /** With `cooperative`: if the callback still succeeds after the deadline, return its result instead of the timeout error. */
  keepLateResult?: boolean;
}

/**
 * Aborts `context.signal` after the timeout and returns a TimeoutRejectedError. By default it returns immediately, even if
 * the callback ignores the signal; with `cooperative: true` it waits for the callback to finish (Polly v8 behaviour).
 */
export class TimeoutStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: TimeoutOptions;
  readonly #source: StrategySource;
  readonly #timeout: number;

  constructor(options: TimeoutOptions, source: StrategySource) {
    this.#timeout = optionalMs(options.timeout, 'timeout') ?? 30_000;
    check(this.#timeout > 0, 'timeout must be > 0');
    this.#options = options;
    this.#source = source;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const generated = await this.#options.timeoutGenerator?.({ context });
    const timeout = generated === undefined ? this.#timeout : toMs(generated, 'timeoutGenerator result');
    if (!(timeout > 0) || timeout === Infinity) return next(context);

    const error = stamp(new TimeoutRejectedError(timeout), this.#source);
    const link = linkSignals(context.signal);
    const time = context.timeProvider ?? this.#source.timeProvider;
    let timedOutFlag = false;
    // delay() chains timers, so timeouts beyond ~24.8 days still fire.
    const stop = new AbortController();
    const timedOut = delay(timeout, stop.signal, time).then(
      () => {
        timedOutFlag = true;
        link.controller.abort(error);
        return fail<T>(error);
      },
      () => fail<T>(error),
    );
    try {
      const run = next({ ...context, signal: link.signal });
      const outcome = this.#options.cooperative ? await run : await Promise.race([run, timedOut]);
      if (!timedOutFlag || context.signal.aborted) return outcome;
      if (this.#options.cooperative && this.#options.keepLateResult && outcome.ok) return outcome;
      const args: OnTimeoutArgs = { context, timeout };
      report(this.#source, 'OnTimeout', context, args);
      await this.#options.onTimeout?.(args);
      discardOutcome(outcome); // a cooperative callback's late success is dropped
      return fail<T>(error);
    } finally {
      stop.abort();
      link.dispose();
    }
  }
}

/** `timeoutStrategy(5000)`, `timeoutStrategy('5s')` or `timeoutStrategy({ timeout: '5s', onTimeout })`. */
export const timeoutStrategy = <T = any>(options: Duration | TimeoutOptions): StrategyDefinition<T, TimeoutRejectedError> => {
  const o: TimeoutOptions = typeof options === 'object' ? options : { timeout: options };
  return { name: o.name ?? 'Timeout', create: (s) => new TimeoutStrategy<T>(o, s) };
};
