import type { StrategyDefinition } from './pipeline-builder.ts';
import { type MaybePromise, type Next, type OutcomeArgs, type ResilienceContext, type ResilienceStrategy, type ShouldHandle, type StrategySource, capture, check, discardOutcome, defaultFallbackShouldHandle, fail, isAbortError, isResilienceError, markKnownError, report, resolvePredicate } from './core.ts';

export interface FallbackOptions<T> {
  name?: string;
  /** Default: every failure except aborts and 4xx client errors, including an open circuit or a full limiter. */
  shouldHandle?: ShouldHandle<OutcomeArgs<T>>;
  /** Produces the substitute value; throwing here makes the execution fail with that error. Give either this or `fallbackValue`. */
  fallbackAction?: (args: OutcomeArgs<T>) => MaybePromise<T>;
  /** A fixed substitute value (cockatiel's `fallback(policy, value)`). */
  fallbackValue?: T;
  onFallback?: (args: OutcomeArgs<T>) => MaybePromise<void>;
}

export class FallbackStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: FallbackOptions<T>;
  readonly #source: StrategySource;
  readonly #shouldHandle;

  constructor(options: FallbackOptions<T>, source: StrategySource) {
    check(options.fallbackAction !== undefined || 'fallbackValue' in options, 'fallback needs fallbackAction or fallbackValue');
    this.#options = options;
    this.#source = source;
    this.#shouldHandle = resolvePredicate(options.shouldHandle, defaultFallbackShouldHandle);
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const outcome = await next(context);
    const args: OutcomeArgs<T> = { outcome, context };
    if (!(await this.#shouldHandle(args))) return outcome;
    report(this.#source, 'OnFallback', context, args);
    await this.#options.onFallback?.(args);
    const { fallbackAction, fallbackValue } = this.#options;
    const replacement = await capture(() => (fallbackAction ? fallbackAction(args) : (fallbackValue as T)));
    discardOutcome(outcome); // a handled success (e.g. a stream) is replaced
    return replacement;
  }
}

/** Without `shouldHandle` it catches every failure of the strategies added after it (they leave the `settle()` error type). */
export function fallbackStrategy<T = any>(options: FallbackOptions<T> & { shouldHandle?: undefined }): StrategyDefinition<T> & { readonly catches: string };
export function fallbackStrategy<T = any>(options: FallbackOptions<T>): StrategyDefinition<T>;
export function fallbackStrategy<T = any>(options: FallbackOptions<T>): StrategyDefinition<T> {
  return { name: options.name ?? 'Fallback', create: (s) => new FallbackStrategy(options, s), ...(!options.shouldHandle && { catches: '' }) };
}

/**
 * Fallback for specific error codes only: `fallbackOnStrategy(['TIMEOUT', 'CIRCUIT_OPEN'], () => cached)`.
 * Those codes leave the `settle()` error type; others pass through (tenace's withFallbackOn).
 */
export function fallbackOnStrategy<T = any, const C extends string = string>(
  codes: readonly C[],
  fallbackAction: NonNullable<FallbackOptions<T>['fallbackAction']>,
  options: Omit<FallbackOptions<T>, 'fallbackAction' | 'shouldHandle'> = {},
): StrategyDefinition<T> & { readonly catches: C } {
  const shouldHandle = ({ outcome }: OutcomeArgs<T>) => !outcome.ok && codes.includes((outcome.error as { code?: C } | null)?.code as C);
  return { name: options.name ?? 'Fallback', create: (s) => new FallbackStrategy({ ...options, fallbackAction, shouldHandle }, s), catches: codes[0]! };
}

/**
 * Turns every failure of the strategies added after it into your own error (e.g. a domain error), which joins the
 * `settle()` error type in their place: `mapErrorStrategy((e) => new PaymentUnavailable({ cause: e }))`. Caller aborts pass through.
 */
export function mapErrorStrategy<T = any, E2 = never>(mapper: (error: unknown, context: ResilienceContext) => E2, name = 'MapError'): StrategyDefinition<T, E2> & { readonly catches: string } {
  return {
    name,
    catches: '',
    create: () => ({
      async execute(next: Next<T>, context: ResilienceContext) {
        const outcome = await next(context);
        if (outcome.ok || isAbortError(outcome.error) || (context.signal.aborted && !isResilienceError(context.signal.reason))) return outcome;
        const mapped = mapper(outcome.error, context);
        markKnownError(mapped);
        return fail<T>(mapped);
      },
    }),
  };
}
