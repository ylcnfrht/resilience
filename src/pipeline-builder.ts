import { type Instrumentation, type ResilienceStrategy, type StrategySource, type TimeProvider, systemTime, unrefSystemTime } from './core.ts';
import { ResiliencePipeline } from './pipeline.ts';

export type StrategyFactory<T> = (source: StrategySource) => ResilienceStrategy<T>;

/** A strategy waiting to be built: what `retryStrategy(...)`, `timeoutStrategy(...)` etc. return. */
export interface StrategyDefinition<T = any, E = never> {
  /** Labels telemetry events. */
  readonly name: string | undefined;
  readonly create: StrategyFactory<T>;
  /** Type-only: the errors this strategy can raise, collected into `pipeline.settle()`'s error type. */
  readonly errors?: E;
  /** Type-only: error codes it catches from the strategies added after it (`string` = all, e.g. a default fallback). */
  readonly catches?: string;
}

/** Drops the errors whose code(s) are all in `Caught` (`string` = everything with a code). */
type DropCaught<A, Caught extends string> = [Caught] extends [never] ? A : A extends { readonly code: infer K } ? ([K] extends [Caught] ? never : A) : A;
/** Errors a pipeline accumulates when a strategy raising `Added` is added after fallbacks catching `Caught`. */
export type WithErrors<E, Caught extends string, Added> = E | DropCaught<Added, Caught>;
/** @internal */
export type ErrorsOf<D> = D extends StrategyDefinition<any, infer X> ? X : never;
/** @internal */
export type CatchesOf<D> = D extends { readonly catches: infer K } ? (K extends string ? K : never) : never;
/** The error type of a built pipeline: `PipelineErrors<typeof pipeline>`. */
export type PipelineErrors<P> = P extends ResiliencePipeline<any, infer E> ? E : never;

export interface ResiliencePipelineBuilderOptions {
  /** Labels telemetry events. */
  name?: string;
  /** Clock and timers for every strategy. Default: the system clock. Use `FakeTimeProvider` in tests. */
  timeProvider?: TimeProvider;
  /** Don't let pending timers (retry delays, timeouts) keep the process alive. Ignored when `timeProvider` is set. */
  unrefTimers?: boolean;
  /** Labels added to every telemetry event of this pipeline (e.g. `{ group: 'payments', team: 'billing' }`). */
  tags?: Readonly<Record<string, string>>;
  /** Instrumentation for this pipeline only (inside the global ones), or `false` to opt out of all instrumentation. */
  instrumentation?: Instrumentation | false;
  /** `false`: this pipeline emits no telemetry events (no metrics, stats or span events). */
  telemetry?: boolean;
}

/**
 * The minimal builder: only the strategies you `add` end up in your bundle.
 *
 * ```ts
 * import { PipelineBuilder, retryStrategy, timeoutStrategy } from '@nodelibraries/resilience';
 * const pipeline = new PipelineBuilder().add(retryStrategy({ maxRetryAttempts: 3 })).add(timeoutStrategy('2s')).build();
 * ```
 *
 * `ResiliencePipelineBuilder` adds Polly-style `addRetry()` / `addTimeout()` / … shortcuts on top, at the cost of
 * bundling every strategy. Strategies run in the order they are added: the first one added is the outermost.
 * Options are validated and strategy state (e.g. circuit breaker) is created on `build()`.
 */
export class PipelineBuilder<T = any, E = never, Caught extends string = never> {
  readonly name: string | undefined;
  readonly timeProvider: TimeProvider;
  readonly tags: Readonly<Record<string, string>> | undefined;
  readonly #options: ResiliencePipelineBuilderOptions;
  readonly #definitions: StrategyDefinition<T, any>[] = [];

  constructor(options: ResiliencePipelineBuilderOptions = {}) {
    this.name = options.name;
    this.timeProvider = options.timeProvider ?? (options.unrefTimers ? unrefSystemTime : systemTime);
    this.tags = options.tags;
    this.#options = options;
  }

  add<D extends StrategyDefinition<T, any>>(definition: D): PipelineBuilder<T, WithErrors<E, Caught, ErrorsOf<D>>, Caught | CatchesOf<D>> {
    this.#definitions.push(definition);
    return this as never;
  }

  /** Plug in your own strategy. */
  addStrategy(create: StrategyFactory<T>, name?: string): this {
    this.#definitions.push({ name, create });
    return this;
  }

  /** Nest an existing pipeline (and its state) as a single strategy. */
  addPipeline<E2>(pipeline: ResiliencePipeline<T, E2>): PipelineBuilder<T, WithErrors<E, Caught, E2>, Caught> {
    return this.addStrategy(() => ({ execute: (next, context) => pipeline.run(context, next) })) as never;
  }

  build(): ResiliencePipeline<T, E> {
    return new ResiliencePipeline(
      this.#definitions.map((d) =>
        d.create({ pipelineName: this.name, strategyName: d.name, timeProvider: this.timeProvider, ...(this.tags && { tags: this.tags }), ...(this.#options.telemetry === false && { silent: true }) }),
      ),
      {
        name: this.name,
        timeProvider: this.timeProvider,
        strategyNames: this.#definitions.map((d) => d.name),
        tags: this.tags,
        instrumentation: this.#options.instrumentation,
        telemetry: this.#options.telemetry,
      },
    );
  }
}
