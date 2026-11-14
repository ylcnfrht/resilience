import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type MaybePromise,
  type Next,
  type Outcome,
  type OutcomeArgs,
  type ResilienceContext,
  type ResilienceStrategy,
  type ShouldHandle,
  type StrategySource,
  MAX_TIMER,
  capture,
  discardOutcome,
  isRejection,
  nextAttemptNumber,
  linkSignals,
  check,
  report,
  resolvePredicate,
  toMs,
  unwrapOutcome,
} from './core.ts';

export interface HedgingActionArgs<T> {
  readonly attemptNumber: number;
  readonly context: ResilienceContext;
  /** The rest of the pipeline plus the original callback. */
  readonly callback: (context: ResilienceContext) => Promise<T>;
}

export interface HedgingDelayArgs {
  readonly attemptNumber: number;
  readonly context: ResilienceContext;
}

export interface OnHedgingArgs {
  readonly attemptNumber: number;
  /** ms since the primary attempt started. */
  readonly duration: number;
  readonly context: ResilienceContext;
}

export interface HedgingOptions<T> {
  name?: string;
  shouldHandle?: ShouldHandle<OutcomeArgs<T>>;
  /** Hedged attempts in addition to the primary. Default 1. */
  maxHedgedAttempts?: number;
  /**
   * ms to wait before starting another attempt. Default 2000.
   * `> 0` latency mode, `0` parallel mode (all at once), `< 0` fallback mode (next attempt only after a failure).
   */
  delay?: Duration;
  /** Delay before the attempt about to start; `undefined` uses `delay`. */
  delayGenerator?: (args: HedgingDelayArgs) => MaybePromise<Duration | undefined>;
  /** Return a different action for hedged attempts (e.g. another endpoint), or `undefined` to repeat the original. */
  actionGenerator?: (args: HedgingActionArgs<T>) => ((context: ResilienceContext) => MaybePromise<T>) | undefined;
  onHedging?: (args: OnHedgingArgs) => MaybePromise<void>;
}

const HEDGE = Symbol('hedge');
type Settled<T> = { attemptNumber: number; outcome: Outcome<T> };

/**
 * Starts extra attempts when the current one is slow or fails; the first acceptable outcome wins and the rest are aborted.
 * Unlike Polly v8 it does not wait for aborted attempts to finish.
 */
export class HedgingStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: HedgingOptions<T>;
  readonly #source: StrategySource;
  readonly #shouldHandle;
  readonly #max: number;
  readonly #delay: number;

  constructor(options: HedgingOptions<T>, source: StrategySource) {
    this.#options = options;
    this.#source = source;
    this.#shouldHandle = resolvePredicate(options.shouldHandle);
    this.#max = options.maxHedgedAttempts ?? 1;
    this.#delay = toMs(options.delay ?? 2000, 'delay');
    check(this.#max >= 1, 'maxHedgedAttempts must be >= 1');
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const time = context.timeProvider ?? this.#source.timeProvider;
    const started = time.now();
    const running = new Map<number, { link: ReturnType<typeof linkSignals>; context: ResilienceContext; promise: Promise<Settled<T>> }>();
    const links: Array<ReturnType<typeof linkSignals>> = [];
    let attempts = 0;
    // Handled (rejected) outcomes; all but the one returned are discarded on exit.
    const handled: Outcome<T>[] = [];
    let returned: Outcome<T> | undefined;

    const launch = async () => {
      const attemptNumber = attempts++;
      if (attemptNumber > 0) {
        const args: OnHedgingArgs = { attemptNumber, duration: time.now() - started, context };
        report(this.#source, 'OnHedging', context, args);
        await this.#options.onHedging?.(args);
      }
      const link = linkSignals(context.signal);
      links.push(link);
      // Each attempt gets its own copy of the properties, so parallel attempts can't overwrite each other;
      // the winner's copy is merged back (Polly does the same).
      const ctx: ResilienceContext = { ...context, signal: link.signal, attemptNumber: attemptNumber > 0 ? nextAttemptNumber(context) : context.attemptNumber, properties: new Map(context.properties) };
      const custom = attemptNumber > 0 ? this.#options.actionGenerator?.({ attemptNumber, context: ctx, callback: (c) => next(c).then(unwrapOutcome) }) : undefined;
      const run = custom ? capture(() => custom(ctx)) : next(ctx);
      running.set(attemptNumber, { link, context: ctx, promise: run.then((outcome) => ({ attemptNumber, outcome })) });
    };

    try {
      await launch();
      let primary: Outcome<T> | undefined;
      for (;;) {
        // A non-idempotent call is never duplicated while one may be in flight.
        const canHedge = attempts <= this.#max && context.idempotent;
        const generated = canHedge ? await this.#options.delayGenerator?.({ attemptNumber: attempts, context }) : undefined;
        const wait = !canHedge ? -1 : generated === undefined ? this.#delay : toMs(generated, 'delayGenerator result');
        const settled = Promise.race([...running.values()].map((r) => r.promise));
        let timer: unknown;
        const winner =
          wait >= 0
            ? await Promise.race([settled, new Promise<typeof HEDGE>((resolve) => (timer = time.setTimeout(() => resolve(HEDGE), Math.min(wait, MAX_TIMER))))])
            : await settled;
        if (timer !== undefined) time.clearTimeout(timer);

        if (winner === HEDGE) {
          await launch();
          continue;
        }
        const finished = running.get(winner.attemptNumber)!;
        running.delete(winner.attemptNumber);
        if (!(await this.#shouldHandle({ outcome: winner.outcome, context }))) {
          for (const [key, value] of finished.context.properties) context.properties.set(key, value);
          return winner.outcome;
        }
        handled.push(winner.outcome);
        if (winner.attemptNumber === 0) primary = winner.outcome;
        const startedNothing = !winner.outcome.ok && isRejection(winner.outcome.error);
        if (attempts <= this.#max && !context.signal.aborted && (context.idempotent || startedNothing)) await launch();
        else if (running.size === 0) return (returned = primary ?? winner.outcome);
      }
    } finally {
      // On every exit (a win, or a throwing generator / onHedging): abort the losers and close results they already produced.
      for (const r of running.values()) {
        r.link.controller.abort();
        void r.promise.then(({ outcome }) => discardOutcome(outcome));
      }
      for (const o of handled) if (o !== returned) discardOutcome(o);
      for (const link of links) link.dispose();
    }
  }
}

export const hedgingStrategy = <T = any>(options: HedgingOptions<T> = {}): StrategyDefinition<T> => ({ name: options.name ?? 'Hedging', create: (s) => new HedgingStrategy(options, s) });
