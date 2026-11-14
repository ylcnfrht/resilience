import { type DynamicModule, Inject, Module } from '@nestjs/common';
import { DiscoveryModule, DiscoveryService, MetadataScanner } from '@nestjs/core';
import { ResiliencePipelineBuilder } from './builder.ts';
import { getResilienceContext, getResilienceSignal } from './core.ts';
import { type CircuitBreakerOptions } from './circuit-breaker.ts';
import { type FallbackOptions } from './fallback.ts';
import { type HedgingOptions } from './hedging.ts';
import { type ConcurrencyLimiterOptions, type TokenBucketOptions } from './rate-limiter.ts';
import { ResiliencePipelineRegistry } from './registry.ts';
import { type RetryOptions } from './retry.ts';
import { type TimeoutOptions } from './timeout.ts';

type Step = (builder: ResiliencePipelineBuilder, registry: ResiliencePipelineRegistry) => void;

// Keyed by the class prototype and method name, so wrapping decorators above or below don't hide the steps.
const steps = new WeakMap<object, Map<string | symbol, Step[]>>();

/** Low-level decorator: configure the method's pipeline directly. The other decorators are shortcuts for this. */
export function Resilience(configure: Step): MethodDecorator {
  return (target, key) => {
    const methods = steps.get(target) ?? new Map<string | symbol, Step[]>();
    steps.set(target, methods);
    const list = methods.get(key) ?? [];
    list.unshift(configure); // decorators apply bottom-up; unshift keeps the top decorator outermost
    methods.set(key, list);
  };
}

/** Run the method through a pipeline registered in `ResilienceModule.forRoot({ pipelines })`. State is shared with every other user of that name. */
export const UsePipeline = (name: string) => Resilience((b, registry) => b.addPipeline(registry.getPipeline(name)));
export const Retry = (options: RetryOptions<any> = {}) => Resilience((b) => b.addRetry(options));
export const Timeout = (options: number | TimeoutOptions) => Resilience((b) => b.addTimeout(options));
export const CircuitBreaker = (options: CircuitBreakerOptions<any> = {}) => Resilience((b) => b.addCircuitBreaker(options));
export const Fallback = (options: FallbackOptions<any>) => Resilience((b) => b.addFallback(options));
export const Hedging = (options: HedgingOptions<any> = {}) => Resilience((b) => b.addHedging(options));
export const ConcurrencyLimit = (options: number | ConcurrencyLimiterOptions, queueLimit = 0) => Resilience((b) => b.addConcurrencyLimiter(options, queueLimit));
export const RateLimit = (options: TokenBucketOptions) => Resilience((b) => b.addRateLimiter(options));

// Re-exported for convenience: inside a decorated method they return that execution's context / signal.
export { getResilienceContext, getResilienceSignal };

export const RESILIENCE_MODULE_OPTIONS = Symbol('RESILIENCE_MODULE_OPTIONS');

export interface ResilienceModuleAsyncOptions {
  imports?: NonNullable<DynamicModule['imports']>;
  inject?: any[];
  useFactory: (...args: any[]) => ResilienceModuleOptions | Promise<ResilienceModuleOptions>;
}

export interface ResilienceModuleOptions {
  /** Named pipelines, usable with `@UsePipeline(name)` or by injecting `ResiliencePipelineRegistry`. */
  pipelines?: Record<string, (builder: ResiliencePipelineBuilder) => void>;
}

/** Finds decorated provider methods at startup and wraps each with its own pipeline. */
class ResilienceExplorer {
  readonly #discovery: DiscoveryService;
  readonly #scanner: MetadataScanner;
  readonly #registry: ResiliencePipelineRegistry;

  constructor(discovery: DiscoveryService, scanner: MetadataScanner, registry: ResiliencePipelineRegistry) {
    this.#discovery = discovery;
    this.#scanner = scanner;
    this.#registry = registry;
  }

  #done = false;

  // Runs from ResilienceModule.configure() (before Nest captures route handlers) or, without an HTTP adapter, at init.
  explore() {
    if (this.#done) return;
    this.#done = true;
    for (const wrapper of [...this.#discovery.getProviders(), ...this.#discovery.getControllers()]) {
      const instance = wrapper.instance as Record<string, unknown> | undefined;
      if (!instance || typeof instance !== 'object') continue;
      const proto = Object.getPrototypeOf(instance);
      for (const name of this.#scanner.getAllMethodNames(proto)) {
        let owner = proto; // the class that defines the method actually called (a subclass override isn't wrapped)
        while (!Object.hasOwn(owner, name)) owner = Object.getPrototypeOf(owner);
        const list = steps.get(owner)?.get(name);
        if (!list) continue;
        const label = `${proto.constructor.name}.${name}`;
        // Request-scoped instances are created per request, after this runs: refuse rather than silently not wrap.
        if (!wrapper.isDependencyTreeStatic()) throw new Error(`@ylcnfrht/resilience: ${label} is request-scoped; resilience decorators need a singleton`);
        const builder = new ResiliencePipelineBuilder({ name: label });
        for (const step of list) step(builder, this.#registry);
        const pipeline = builder.build();
        const original = proto[name] as (...args: unknown[]) => unknown;
        const wrapped = (...args: unknown[]) => pipeline.execute(() => original.apply(instance, args));
        // Nest reads guards, pipes, interceptors and response metadata from the handler it calls.
        for (const k of Reflect.getMetadataKeys(original)) Reflect.defineMetadata(k, Reflect.getMetadata(k, original), wrapped);
        instance[name] = wrapped;
      }
    }
  }

  onModuleInit() {
    this.explore();
  }
}

export class ResilienceModule {
  readonly #explorer: ResilienceExplorer;

  constructor(explorer: ResilienceExplorer) {
    this.#explorer = explorer;
  }

  /** Nest calls this before it registers routes, so decorated controller methods are wrapped for HTTP too. */
  configure() {
    this.#explorer.explore();
  }

  static forRoot(options: ResilienceModuleOptions = {}): DynamicModule {
    return ResilienceModule.forRootAsync({ useFactory: () => options });
  }

  /**
   * Options from configuration or other providers:
   * `ResilienceModule.forRootAsync({ imports: [ConfigModule], inject: [ConfigService], useFactory: (config) => ({ pipelines: … }) })`.
   */
  static forRootAsync(options: ResilienceModuleAsyncOptions): DynamicModule {
    return {
      module: ResilienceModule,
      global: true,
      imports: [DiscoveryModule, ...(options.imports ?? [])],
      providers: [
        { provide: RESILIENCE_MODULE_OPTIONS, useFactory: options.useFactory, inject: options.inject ?? [] },
        {
          provide: ResiliencePipelineRegistry,
          useFactory: (resolved: ResilienceModuleOptions) => {
            const registry = new ResiliencePipelineRegistry();
            for (const [name, configure] of Object.entries(resolved.pipelines ?? {})) registry.tryAddBuilder(name, (b) => configure(b));
            // Nest calls this on app.close(): stops health checks and store sync timers.
            return Object.assign(registry, { onModuleDestroy: () => registry.dispose() });
          },
          inject: [RESILIENCE_MODULE_OPTIONS],
        },
        {
          provide: ResilienceExplorer,
          useFactory: (d: DiscoveryService, s: MetadataScanner, r: ResiliencePipelineRegistry) => new ResilienceExplorer(d, s, r),
          inject: [DiscoveryService, MetadataScanner, ResiliencePipelineRegistry],
        },
      ],
      exports: [ResiliencePipelineRegistry],
    };
  }
}
Module({})(ResilienceModule);
(Inject(ResilienceExplorer) as ParameterDecorator)(ResilienceModule, undefined, 0);
