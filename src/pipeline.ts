import {
  type ExecuteOptions,
  type MaybePromise,
  type Next,
  type ResilienceContext,
  type ResilienceStrategy,
  type TelemetrySource,
  type TimeProvider,
  type Instrumentation,
  asOutcome,
  isKnownError,
  type Outcome,
  UnexpectedError,
  ambient,
  capture,
  createContext,
  discard,
  fail,
  globalHooks,
  instrumented,
  isNonRetryableError,
  isResilienceError,
  linkSignals,
  report,
  systemTime,
  telemetryEnabled,
  unwrapOutcome,
} from './core.ts';

export interface PipelineOptions {
  name?: string;
  timeProvider?: TimeProvider;
  /** Names of the strategies, same order (for `describe()` / `getStrategy()`). */
  strategyNames?: ReadonlyArray<string | undefined>;
  tags?: Readonly<Record<string, string>>;
  /** Extra instrumentation for this pipeline only (runs inside the global ones), or `false` for none at all. */
  instrumentation?: Instrumentation | false;
  /** `false`: emit no telemetry events. */
  telemetry?: boolean;
}

export interface StrategyDescriptor {
  readonly name: string | undefined;
  /** Strategy class name, e.g. 'RetryStrategy'. */
  readonly kind: string;
  readonly strategy: ResilienceStrategy;
}

/**
 * `T`: what callbacks may return. `E` (type-only): the errors the pipeline's strategies can raise, for `settle()`.
 */
export class ResiliencePipeline<T = any, E = any> {
  static readonly empty: ResiliencePipeline<any> = new ResiliencePipeline([]);

  readonly #strategies: ReadonlyArray<ResilienceStrategy<T>>;
  readonly #names: ReadonlyArray<string | undefined>;
  readonly name: string | undefined;
  readonly #time: TimeProvider;
  readonly #source: TelemetrySource;
  #enabled = true;
  readonly #instrumentation: Instrumentation | false | undefined;
  #disposed = false;

  constructor(strategies: ReadonlyArray<ResilienceStrategy<T>>, options: PipelineOptions = {}) {
    this.#strategies = strategies;
    this.#names = options.strategyNames ?? [];
    this.name = options.name;
    this.#time = options.timeProvider ?? systemTime;
    this.#source = { pipelineName: this.name, strategyName: undefined, ...(options.tags && { tags: options.tags }), ...(options.telemetry === false && { silent: true }) };
    this.#instrumentation = options.instrumentation;
  }

  /** Runs the callback through the pipeline; throws the final error. */
  execute<R extends T>(callback: (context: ResilienceContext) => MaybePromise<R>, options?: ExecuteOptions): Promise<R> {
    return this.executeOutcome(callback, options).then(unwrapOutcome);
  }

  /** Like `execute`, but returns the final outcome instead of throwing. */
  async executeOutcome<R extends T>(callback: (context: ResilienceContext) => MaybePromise<R>, options: ExecuteOptions = {}): Promise<Outcome<R>> {
    if (this.#disposed) return fail(new Error(`The resilience pipeline${this.name ? ` '${this.name}'` : ''} has been disposed.`));
    // Inherit cancellation from the execution we run inside: an outer timeout also stops this call (deadline propagation).
    const parent = ambient.getStore()?.signal;
    const link = parent && options.signal ? linkSignals(parent, options.signal) : undefined;
    const context = createContext({ ...options, signal: link?.signal ?? options.signal ?? parent });
    const time = options.timeProvider ?? this.#time;
    const source = this.#source;
    const own = this.#instrumentation;

    const last: Next<R> = (ctx) =>
      ctx.signal.aborted
        ? Promise.resolve(fail(ctx.signal.reason))
        : instrumented('attempt', { source, context: ctx }, async () => {
            const started = time.now();
            const injected = await globalHooks.beforeCallback?.(ctx, time);
            const own = options.abortOnReturn ? linkSignals(ctx.signal) : undefined;
            const attemptContext = own ? { ...ctx, signal: own.signal } : ctx;
            const outcome = injected ?? (await capture(() => ambient.run(attemptContext, () => callback(attemptContext))));
            if (own) {
              own.controller.abort(new DOMException('The operation returned; cancelling leftover work.', 'AbortError'));
              own.dispose();
            }
            // Every call of the callback, whatever strategy caused it (retry, hedging, or the single attempt).
            report(source, 'OnExecutionAttempt', ctx, { outcome, context: ctx, attemptNumber: ctx.attemptNumber, duration: time.now() - started });
            return outcome;
          }, own);

    const started = time.now();
    // Before anything runs (opossum's `fire`): lets dashboards count in-flight executions and calls that never finish.
    report(source, 'OnPipelineExecuting', context, { context });
    try {
      const result = await instrumented('execution', { source, context }, () => (this.#enabled ? this.run(context, last as unknown as Next<T>) : (last(context) as Promise<Outcome<T>>)), own);
      // The wrapper only exists to stop retries; callers get the original error.
      const outcome = !result.ok && isNonRetryableError(result.error) ? fail<R>(result.error.cause) : (result as Outcome<R>);
      // One event per execution: what metrics adapters (OpenTelemetry, Prometheus) turn into latency histograms and error rates.
      report(source, 'OnPipelineExecuted', context, { outcome, duration: time.now() - started });
      return outcome;
    } finally {
      link?.dispose();
    }
  }

  /**
   * Like `executeOutcome`, but the error is typed: the errors this pipeline's strategies can raise (`E`), the errors your
   * callback returns with `Outcome.fromError(…)` (`E2`), or `UnexpectedError` wrapping anything else (a thrown bug, a
   * client error). Pair it with `Outcome.match()` for exhaustive handling:
   *
   * ```ts
   * const outcome = await pipeline.settle(async () => (user ? user : Outcome.fromError(new NotFound(id))));
   * Outcome.match(outcome, { ok: (u) => u, NOT_FOUND: () => null, TIMEOUT: () => cached, CIRCUIT_OPEN: () => cached, UNEXPECTED: (e) => { throw e } });
   * ```
   * Returned errors count as failures for the strategies (they can be retried) unless your predicates say otherwise.
   */
  async settle<R extends T, E2 = never>(callback: (context: ResilienceContext) => MaybePromise<R | Outcome<R, E2>>, options?: ExecuteOptions): Promise<Outcome<R, E | E2 | UnexpectedError>> {
    const returned = new Set<unknown>();
    const outcome = await this.executeOutcome(async (ctx) => {
      const value = await callback(ctx);
      const converted = asOutcome(value); // ours, or a registered foreign Result (neverthrow…)
      if (!converted) return value as R;
      if (converted.ok) return converted.result as R;
      returned.add(converted.error);
      throw converted.error;
    }, options);
    if (outcome.ok || returned.has(outcome.error) || this.isError(outcome.error)) return outcome as Outcome<R, E | E2>;
    return fail(new UnexpectedError(outcome.error));
  }

  /** Is this one of the errors this pipeline can produce (`E`)? Resilience errors and `mapError` results. */
  isError(error: unknown): error is E {
    return isResilienceError(error) || isKnownError(error);
  }

  /**
   * Runs a streaming call (an AsyncIterable or a ReadableStream) through the pipeline. Everything up to the first chunk is
   * protected: retries, timeouts (time to first chunk), breaker, limiters. Once the first chunk arrives the stream is
   * committed: later errors reach the consumer and are not retried or replaced by a fallback, which would repeat data
   * already delivered. Breaking out of the loop closes the underlying stream.
   */
  async *executeStream<R>(factory: (context: ResilienceContext) => MaybePromise<AsyncIterable<R> | ReadableStream<R>>, options?: ExecuteOptions): AsyncGenerator<R, void, undefined> {
    // Each attempt carries its own iterator, so a late hedged/timed-out attempt can never take over the winner's stream.
    const result: unknown = await (this as unknown as ResiliencePipeline<unknown>).execute(async (ctx) => {
      const it = iterate(await factory(ctx));
      try {
        const first = await it.next();
        if (ctx.signal.aborted) throw ctx.signal.reason;
        return { [streamAttempt]: true, first, it, claimed: false, [discard]: () => it.return?.().catch(() => {}) } satisfies StreamAttempt<R>;
      } catch (error) {
        await it.return?.().catch(() => {});
        throw error;
      }
    }, options);
    let it: AsyncIterator<R>;
    let first: IteratorResult<R> | undefined;
    if (isStreamAttempt<R>(result)) {
      if (result.claimed) throw new TypeError('executeStream: a cache strategy cannot cache streams');
      result.claimed = true;
      ({ it, first } = result);
    } else {
      // A fallback (or other strategy) produced the value: a stream is iterated, anything else is one chunk.
      const isStream = result != null && (Symbol.asyncIterator in Object(result) || 'getReader' in Object(result));
      it = iterate(isStream ? (result as AsyncIterable<R>) : (async function* () { yield result as R; })());
    }
    try {
      for (let r = first ?? (await it.next()); !r.done; r = await it.next()) yield r.value;
    } finally {
      await it.return?.();
    }
  }

  /** `const getUser = pipeline.wrap(api.getUser)`: a function with the same arguments that runs through the pipeline. */
  wrap<A extends unknown[], R extends T>(fn: (...args: A) => MaybePromise<R>, options?: ExecuteOptions): (...args: A) => Promise<R> {
    return (...args: A) => this.execute(() => fn(...args), options);
  }

  /** Kill switch: while disabled, calls bypass every strategy (no retries, no breaker, no limits). */
  disable(): void {
    this.#enabled = false;
  }

  enable(): void {
    this.#enabled = true;
  }

  get enabled(): boolean {
    return this.#enabled;
  }

  /** The strategies in execution order (outermost first), e.g. to assert a pipeline's configuration in tests. */
  describe(): StrategyDescriptor[] {
    return this.#strategies.map((strategy, i) => ({ name: this.#names[i], kind: strategy.constructor?.name ?? 'Strategy', strategy }));
  }

  /** The strategy instance named `name` (or of class `kind`), to read its live state: `getStrategy<CircuitBreakerStrategy>('CircuitBreaker')?.state`. */
  getStrategy<S extends ResilienceStrategy = ResilienceStrategy>(nameOrKind: string): S | undefined {
    return this.describe().find((d) => d.name === nameOrKind || d.kind === nameOrKind)?.strategy as S | undefined;
  }

  /** True once `dispose()` was called (opossum's isShutdown). */
  get disposed(): boolean {
    return this.#disposed;
  }

  /** Stops background work (health checks, store sync timers). Later executions fail. */
  dispose(): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const strategy of this.#strategies) strategy.dispose?.();
  }

  /** @internal Strategies added first wrap the ones added later. */
  run(context: ResilienceContext, last: Next<T>): Promise<Outcome<T>> {
    const call =
      (i: number): Next<T> =>
      async (ctx) => {
        if (i === this.#strategies.length) return last(ctx);
        // Per-strategy success/failure with duration (cockatiel's onSuccess/onFailure), only measured when someone listens.
        const measure = !this.#source.silent && telemetryEnabled();
        const started = measure ? this.#time.now() : 0;
        let outcome: Outcome<T>;
        try {
          outcome = await this.#strategies[i]!.execute(call(i + 1), ctx);
        } catch (error) {
          outcome = fail(error);
        }
        if (measure) report({ ...this.#source, strategyName: this.#names[i] }, 'OnStrategyExecuted', ctx, { outcome, duration: this.#time.now() - started, context: ctx });
        return outcome;
      };
    return call(0)(context);
  }
}

// Some browsers' ReadableStream isn't async-iterable yet; fall back to its reader.
function iterate<R>(source: AsyncIterable<R> | ReadableStream<R>): AsyncIterator<R> {
  if (Symbol.asyncIterator in source) return (source as AsyncIterable<R>)[Symbol.asyncIterator]();
  const reader = (source as ReadableStream<R>).getReader();
  return {
    next: () => reader.read() as Promise<IteratorResult<R>>,
    return: async () => {
      await reader.cancel();
      return { done: true, value: undefined };
    },
  };
}


const streamAttempt = Symbol('@ylcnfrht/resilience.streamAttempt');
type StreamAttempt<R> = { [streamAttempt]: true; first: IteratorResult<R>; it: AsyncIterator<R>; claimed: boolean; [discard]: () => unknown };
const isStreamAttempt = <R>(value: unknown): value is StreamAttempt<R> => typeof value === 'object' && value !== null && streamAttempt in value;
