import type { StrategyDefinition } from './pipeline-builder.ts';
import { type Duration, type MaybePromise, type Next, type Outcome, type ResilienceContext, type ResilienceStrategy, type StrategySource, check, delay, fail, globalHooks, report, toMs } from './core.ts';

type ContextArgs = { readonly context: ResilienceContext };

export interface ChaosOptions {
  name?: string;
  /** Default true. Ignored when `enabledGenerator` is set. */
  enabled?: boolean;
  enabledGenerator?: (args: ContextArgs) => MaybePromise<boolean>;
  /** Probability in [0, 1]. Default 0.001. Ignored when `injectionRateGenerator` is set. */
  injectionRate?: number;
  injectionRateGenerator?: (args: ContextArgs) => MaybePromise<number>;
  randomizer?: () => number;
}

export interface ChaosFaultOptions extends ChaosOptions {
  fault?: unknown;
  faultGenerator?: (args: ContextArgs) => MaybePromise<unknown>;
  onFaultInjected?: (args: ContextArgs & { readonly fault: unknown }) => MaybePromise<void>;
}

export interface ChaosOutcomeOptions<T> extends ChaosOptions {
  outcome?: Outcome<T>;
  outcomeGenerator?: (args: ContextArgs) => MaybePromise<Outcome<T> | undefined>;
  onOutcomeInjected?: (args: ContextArgs & { readonly outcome: Outcome<T> }) => MaybePromise<void>;
}

export interface ChaosLatencyOptions extends ChaosOptions {
  /** Default 30 s. */
  latency?: Duration;
  latencyGenerator?: (args: ContextArgs) => MaybePromise<Duration>;
  onLatencyInjected?: (args: ContextArgs & { readonly latency: number }) => MaybePromise<void>;
}

export interface ChaosBehaviorOptions extends ChaosOptions {
  behavior: (args: ContextArgs) => MaybePromise<void>;
  onBehaviorInjected?: (args: ContextArgs) => MaybePromise<void>;
}

/** Returns an outcome to short-circuit the execution, or undefined to continue to the callback. */
type Inject<T> = (context: ResilienceContext) => Promise<Outcome<T> | undefined>;

export class ChaosStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: ChaosOptions;
  readonly #inject: Inject<T>;

  constructor(options: ChaosOptions, inject: Inject<T>) {
    const rate = options.injectionRate ?? 0.001;
    check(rate >= 0 && rate <= 1, 'injectionRate must be in [0, 1]');
    this.#options = options;
    this.#inject = inject;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    if (await this.#shouldInject(context)) {
      const injected = await this.#inject(context);
      if (injected) return injected;
    }
    return next(context);
  }

  async #shouldInject(context: ResilienceContext) {
    const o = this.#options;
    const enabled = o.enabledGenerator ? await o.enabledGenerator({ context }) : (o.enabled ?? true);
    if (!enabled) return false;
    const rate = o.injectionRateGenerator ? await o.injectionRateGenerator({ context }) : (o.injectionRate ?? 0.001);
    return (o.randomizer ?? Math.random)() < rate;
  }
}

export function chaosFault<T>(o: ChaosFaultOptions, source: StrategySource) {
  check(o.fault !== undefined || o.faultGenerator !== undefined, 'fault or faultGenerator is required');
  return new ChaosStrategy<T>(o, async (context) => {
    const fault = o.faultGenerator ? await o.faultGenerator({ context }) : o.fault;
    if (fault === undefined) return undefined;
    report(source, 'Chaos.OnFault', context, { context, fault });
    await o.onFaultInjected?.({ context, fault });
    return fail<T>(fault);
  });
}

export function chaosOutcome<T>(o: ChaosOutcomeOptions<T>, source: StrategySource) {
  check(o.outcome !== undefined || o.outcomeGenerator !== undefined, 'outcome or outcomeGenerator is required');
  return new ChaosStrategy<T>(o, async (context) => {
    const outcome = o.outcomeGenerator ? await o.outcomeGenerator({ context }) : o.outcome;
    if (outcome === undefined) return undefined;
    report(source, 'Chaos.OnOutcome', context, { context, outcome });
    await o.onOutcomeInjected?.({ context, outcome });
    return outcome;
  });
}

export function chaosLatency<T>(o: ChaosLatencyOptions, source: StrategySource) {
  const fixed = toMs(o.latency ?? 30_000, 'latency');
  return new ChaosStrategy<T>(o, async (context) => {
    const latency = o.latencyGenerator ? toMs(await o.latencyGenerator({ context }), 'latencyGenerator result') : fixed;
    report(source, 'Chaos.OnLatency', context, { context, latency });
    await o.onLatencyInjected?.({ context, latency });
    try {
      await delay(latency, context.signal, context.timeProvider ?? source.timeProvider);
    } catch (error) {
      return fail<T>(error);
    }
    return undefined;
  });
}

export function chaosBehavior<T>(o: ChaosBehaviorOptions, source: StrategySource) {
  check(typeof o.behavior === 'function', 'behavior is required');
  return new ChaosStrategy<T>(o, async (context) => {
    report(source, 'Chaos.OnBehavior', context, { context });
    await o.onBehaviorInjected?.({ context });
    await o.behavior({ context });
    return undefined;
  });
}

export const chaosFaultStrategy = <T = any>(options: ChaosFaultOptions): StrategyDefinition<T> => ({ name: options.name ?? 'Chaos.Fault', create: (s) => chaosFault<T>(options, s) });
export const chaosOutcomeStrategy = <T = any>(options: ChaosOutcomeOptions<T>): StrategyDefinition<T> => ({ name: options.name ?? 'Chaos.Outcome', create: (s) => chaosOutcome(options, s) });
export const chaosLatencyStrategy = <T = any>(options: ChaosLatencyOptions): StrategyDefinition<T> => ({ name: options.name ?? 'Chaos.Latency', create: (s) => chaosLatency<T>(options, s) });
export const chaosBehaviorStrategy = <T = any>(options: ChaosBehaviorOptions): StrategyDefinition<T> => ({ name: options.name ?? 'Chaos.Behavior', create: (s) => chaosBehavior<T>(options, s) });

export interface GlobalChaosOptions {
  /** Probability per callback in [0, 1]. Default 0.05. */
  injectionRate?: number;
  /** Only inject where this returns true (e.g. one pipeline, or one operationKey). */
  filter?: (context: ResilienceContext) => boolean;
  /** Error to fail with (or a function creating it). Omit to inject latency only. */
  fault?: unknown | ((context: ResilienceContext) => unknown);
  /** Delay added before the callback. */
  latency?: Duration;
  randomizer?: () => number;
}

/**
 * Inject faults and/or latency into every pipeline in the process, without touching their configuration:
 * game days, staging soak tests, "does my whole app survive a slow database?". Returns a function that turns it off.
 */
export function setGlobalChaos(options: GlobalChaosOptions): () => void {
  const rate = options.injectionRate ?? 0.05;
  check(rate >= 0 && rate <= 1, 'injectionRate must be in [0, 1]');
  check(options.fault !== undefined || options.latency !== undefined, 'fault or latency is required');
  const latency = options.latency === undefined ? 0 : toMs(options.latency, 'latency');
  const source = { pipelineName: undefined, strategyName: 'Chaos.Global' };
  const hook: NonNullable<typeof globalHooks.beforeCallback> = async (context, time) => {
    if ((options.randomizer ?? Math.random)() >= rate || (options.filter && !options.filter(context))) return undefined;
    report(source, 'Chaos.OnGlobal', context, { context, latency, fault: options.fault !== undefined });
    if (latency > 0) {
      try {
        await delay(latency, context.signal, time);
      } catch (error) {
        return fail(error);
      }
    }
    if (options.fault === undefined) return undefined;
    return fail(typeof options.fault === 'function' ? (options.fault as (c: ResilienceContext) => unknown)(context) : options.fault);
  };
  globalHooks.beforeCallback = hook;
  return () => {
    if (globalHooks.beforeCallback === hook) globalHooks.beforeCallback = undefined;
  };
}

/**
 * Picks one of several faults/outcomes by weight (Polly's FaultGenerator / OutcomeGenerator):
 * `faultGenerator: weighted([{ weight: 80, value: () => new TimeoutError() }, { weight: 20, value: () => new Error('500') }])`.
 * Values may be factories (called per injection) or constants.
 */
export function weighted<V>(entries: ReadonlyArray<{ readonly weight: number; readonly value: V | (() => V) }>, randomizer: () => number = Math.random): () => V {
  const total = entries.reduce((sum, e) => sum + e.weight, 0);
  check(total > 0 && entries.every((e) => e.weight >= 0), 'weights must be >= 0 and sum to more than 0');
  return () => {
    let pick = randomizer() * total;
    for (const e of entries) {
      pick -= e.weight;
      if (pick < 0) return typeof e.value === 'function' ? (e.value as () => V)() : e.value;
    }
    const last = entries.at(-1)!.value; // floating-point edge
    return typeof last === 'function' ? (last as () => V)() : last;
  };
}

