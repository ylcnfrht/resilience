import { ResiliencePipelineBuilder } from './builder.ts';
import { type ResilienceStrategy, type TimeProvider } from './core.ts';
import { ResiliencePipeline, type StrategyDescriptor } from './pipeline.ts';

export type ConfigurePipeline<K> = (builder: ResiliencePipelineBuilder, context: { readonly key: K }) => void;

const LIMITER_KINDS = new Set([
  'ConcurrencyLimiterStrategy',
  'TokenBucketStrategy',
  'AdaptiveConcurrencyLimiterStrategy',
  'FixedWindowRateLimiterStrategy',
  'SlidingWindowRateLimiterStrategy',
  'PartitionedRateLimiterStrategy',
  'RateLimiterStrategy',
]);

export interface RegisteredStrategy<K> extends StrategyDescriptor {
  readonly key: K;
}

/**
 * A pipeline handed out by the registry. Its identity never changes, so callers can keep the reference;
 * `registry.reload(key)` swaps what runs underneath (new options, fresh state).
 */
class ReloadablePipeline<T = any> extends ResiliencePipeline<T> {
  current: ResiliencePipeline<T>;

  constructor(initial: ResiliencePipeline<T>, timeProvider?: TimeProvider) {
    // Honours the inner pipeline's kill switch too (`getPipeline(k).disable()` and the built pipeline's own).
    const forward: ResilienceStrategy<T> = { execute: (next, context) => (this.current.enabled ? this.current.run(context, next) : next(context)) };
    super([forward], { name: initial.name, timeProvider });
    this.current = initial;
  }

  override describe(): StrategyDescriptor[] {
    return this.current.describe();
  }

  override dispose(): void {
    this.current.dispose();
    super.dispose();
  }
}

/** Builds each keyed pipeline once and caches it, so stateful strategies (circuit breakers, limiters) are shared. */
export class ResiliencePipelineRegistry<K = string> {
  readonly #pipelines = new Map<K, ReloadablePipeline>();
  readonly #builders = new Map<K, ConfigurePipeline<K>>();
  readonly #timeProvider: TimeProvider | undefined;
  #disposed = false;

  readonly #pipelineName: (key: K) => string;

  constructor(options: { timeProvider?: TimeProvider; /** Telemetry name for a key's pipeline. Default `String(key)`. */ pipelineName?: (key: K) => string } = {}) {
    this.#timeProvider = options.timeProvider;
    this.#pipelineName = options.pipelineName ?? String;
  }

  /** Registers how to build `key`; returns false if a builder is already registered. */
  tryAddBuilder(key: K, configure: ConfigurePipeline<K>): boolean {
    if (this.#builders.has(key)) return false;
    this.#builders.set(key, configure);
    return true;
  }

  tryGetPipeline<T = any>(key: K): ResiliencePipeline<T> | undefined {
    const existing = this.#pipelines.get(key);
    if (existing) return existing;
    const configure = this.#builders.get(key);
    return configure && this.#create(key, configure);
  }

  getPipeline<T = any>(key: K): ResiliencePipeline<T> {
    const pipeline = this.tryGetPipeline<T>(key);
    if (!pipeline) throw new Error(`No resilience pipeline registered for key '${String(key)}'.`);
    return pipeline;
  }

  getOrAddPipeline<T = any>(key: K, configure: ConfigurePipeline<K>): ResiliencePipeline<T> {
    return this.tryGetPipeline<T>(key) ?? this.#create(key, configure);
  }

  /**
   * Rebuild `key`'s pipeline, optionally with a new configuration (e.g. limits from a feature flag).
   * Existing references keep working and pick up the new pipeline; the old one is disposed and its state dropped.
   */
  reload(key: K, configure?: ConfigurePipeline<K>): void {
    if (configure) this.#builders.set(key, configure);
    const handle = this.#pipelines.get(key);
    const builderFn = this.#builders.get(key);
    if (!handle || !builderFn) return;
    const previous = handle.current;
    handle.current = this.#build(key, builderFn);
    previous.dispose();
  }

  /** Keys of the pipelines built so far. */
  keys(): K[] {
    return [...this.#pipelines.keys()];
  }

  /** Every circuit breaker in the registry's pipelines, e.g. for a health endpoint: `strategy.state`, `strategy.metrics`. */
  circuitBreakers(): RegisteredStrategy<K>[] {
    return this.#strategies((kind) => kind === 'CircuitBreakerStrategy');
  }

  /** Every concurrency / rate limiter, e.g. to report `active` / `queued` / `limit`. */
  limiters(): RegisteredStrategy<K>[] {
    return this.#strategies((kind) => LIMITER_KINDS.has(kind));
  }

  /** Dispose every pipeline (background timers, store sync). The registry can't be used afterwards. */
  dispose(): void {
    this.#disposed = true;
    for (const pipeline of this.#pipelines.values()) pipeline.dispose();
    this.#pipelines.clear();
  }

  #strategies(match: (kind: string) => boolean): RegisteredStrategy<K>[] {
    return [...this.#pipelines].flatMap(([key, pipeline]) => pipeline.describe().filter((d) => match(d.kind)).map((d) => ({ ...d, key })));
  }

  #build(key: K, configure: ConfigurePipeline<K>): ResiliencePipeline {
    const builder = new ResiliencePipelineBuilder({ name: this.#pipelineName(key), timeProvider: this.#timeProvider });
    configure(builder, { key });
    return builder.build();
  }

  #create(key: K, configure: ConfigurePipeline<K>): ResiliencePipeline {
    if (this.#disposed) throw new Error('The resilience pipeline registry has been disposed.');
    const pipeline = new ReloadablePipeline(this.#build(key, configure), this.#timeProvider);
    this.#pipelines.set(key, pipeline);
    return pipeline;
  }
}
