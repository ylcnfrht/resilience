import { type Duration, type MaybePromise, type Outcome, type ResilienceContext, type TimeProvider, TimeoutRejectedError, check, delay, linkSignals, systemTime, toMs } from './core.ts';
import { ResiliencePipeline } from './pipeline.ts';

interface BatchOptions {
  /** Each task / item runs through it separately (e.g. its own retries and timeout). */
  pipeline?: ResiliencePipeline;
  signal?: AbortSignal;
}

export interface MapOptions extends BatchOptions {
  /** Max items running at once. Default: all. */
  concurrency?: number;
  /** `true` (default): reject on the first failure and abort the rest. `false`: run every item and return an `Outcome` per item. */
  stopOnError?: boolean;
}

export interface WaitForOptions {
  /** Pause between checks: fixed, or per poll (e.g. backoff: `(n) => Math.min(100 * 2 ** n, 5000)`). */
  interval: Duration | ((attempt: number) => Duration);
  /** Rejects with `TimeoutRejectedError` after this long. */
  timeout: Duration;
  signal?: AbortSignal;
  timeProvider?: TimeProvider;
}

type Tasks<T extends readonly unknown[]> = { readonly [K in keyof T]: (context: ResilienceContext) => MaybePromise<T[K]> };

/** Runs the tasks in parallel. On the first failure the others are aborted (through their context signal) and it rejects. */
export async function all<T extends readonly unknown[]>(tasks: Tasks<T>, options: BatchOptions = {}): Promise<T> {
  const { controller, signal, dispose } = linkSignals(options.signal);
  const pipeline = options.pipeline ?? ResiliencePipeline.empty;
  return (Promise.all(tasks.map((task) => pipeline.execute(task, { signal }))) as unknown as Promise<T>)
    .catch((error: unknown) => {
      controller.abort(error);
      throw error;
    })
    .finally(dispose);
}

/** Runs every task (through the optional pipeline) and returns one Outcome per task, in order; never rejects except on caller abort. */
export async function allSettled<T extends readonly unknown[]>(tasks: Tasks<T>, options: BatchOptions = {}): Promise<{ [K in keyof T]: Outcome<T[K]> }> {
  const pipeline = options.pipeline ?? ResiliencePipeline.empty;
  const outcomes = await Promise.all(tasks.map((task) => pipeline.executeOutcome(task, { signal: options.signal })));
  options.signal?.throwIfAborted();
  return outcomes as unknown as { [K in keyof T]: Outcome<T[K]> };
}

/** Maps items with at most `concurrency` running at once; results come back in input order. */
export function map<I, R>(items: readonly I[], fn: (item: I, index: number, context: ResilienceContext) => MaybePromise<R>, options: MapOptions & { stopOnError: false }): Promise<Outcome<R>[]>;
export function map<I, R>(items: readonly I[], fn: (item: I, index: number, context: ResilienceContext) => MaybePromise<R>, options?: MapOptions): Promise<R[]>;
export async function map<I, R>(items: readonly I[], fn: (item: I, index: number, context: ResilienceContext) => MaybePromise<R>, options: MapOptions = {}): Promise<R[] | Outcome<R>[]> {
  const concurrency = options.concurrency ?? Infinity;
  check(concurrency >= 1, 'concurrency must be >= 1');
  const stopOnError = options.stopOnError ?? true;
  const pipeline = options.pipeline ?? ResiliencePipeline.empty;
  const { controller, signal, dispose } = linkSignals(options.signal);
  const outcomes = new Array<Outcome<R>>(items.length);
  let nextIndex = 0;
  const worker = async () => {
    while (nextIndex < items.length && !signal.aborted) {
      const i = nextIndex++;
      const outcome = await pipeline.executeOutcome((ctx) => fn(items[i]!, i, ctx), { signal });
      if (!outcome.ok && stopOnError) {
        controller.abort(outcome.error);
        throw outcome.error;
      }
      outcomes[i] = outcome;
    }
  };
  try {
    await Promise.all(Array.from({ length: Math.min(concurrency, items.length) }, worker));
    signal.throwIfAborted(); // caller cancelled: some items never ran
  } finally {
    dispose();
  }
  return stopOnError ? outcomes.map((o) => (o as { result: R }).result) : outcomes;
}

/**
 * Polls `condition` every `interval` until it returns true. Rejects with `TimeoutRejectedError` after `timeout`,
 * and with the error if `condition` throws (wrap it in `.catch(() => false)` to treat errors as "not yet").
 */
export async function waitFor(condition: (signal: AbortSignal) => MaybePromise<boolean>, options: WaitForOptions): Promise<void> {
  const time = options.timeProvider ?? systemTime;
  const intervalAt = (n: number) => toMs(typeof options.interval === 'function' ? options.interval(n) : options.interval, 'interval');
  const interval = intervalAt(0);
  const timeout = toMs(options.timeout, 'timeout');
  check(interval >= 0, 'interval must be >= 0');
  check(timeout > 0, 'timeout must be > 0');
  const { controller, signal, dispose } = linkSignals(options.signal);
  const timer = time.setTimeout(() => controller.abort(new TimeoutRejectedError(timeout)), timeout);
  // Also stops waiting for a condition call that ignores the signal.
  const aborted = new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));
  aborted.catch(() => {});
  try {
    let polls = 0;
    signal.throwIfAborted();
    while (!(await Promise.race([condition(signal), aborted]))) {
      await delay(intervalAt(++polls), signal, time);
      signal.throwIfAborted();
    }
  } catch (error) {
    throw signal.aborted ? signal.reason : error;
  } finally {
    time.clearTimeout(timer);
    dispose();
  }
}
