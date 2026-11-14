// No static `node:` imports: the core must load in browsers and edge runtimes too.
function nodeBuiltin<M>(id: string): M | undefined {
  try {
    return (globalThis as { process?: { getBuiltinModule?: (id: string) => unknown } }).process?.getBuiltinModule?.(id) as M | undefined;
  } catch {
    return undefined;
  }
}

interface ContextStorage<S> {
  getStore(): S | undefined;
  run<R>(store: S, fn: () => R): R;
}

export type MaybePromise<X> = X | Promise<X>;

/**
 * The result of an execution: either a value or an error. Strategies pass outcomes around instead of throwing.
 * `E` is the error type when it's known (see `pipeline.settle()`), `unknown` otherwise.
 */
export type Outcome<T, E = unknown> = { readonly ok: true; readonly result: T } | { readonly ok: false; readonly error: E };

const OUTCOME_BRAND = /* @__PURE__ */ Symbol.for('@nodelibraries/resilience.outcome');

/** The `code`s of an error union: `ErrorCodes<TimeoutRejectedError | NotFound>` → `'TIMEOUT' | 'NOT_FOUND'`. */
export type ErrorCodes<E> = E extends { readonly code: infer C } ? (C extends string ? C : never) : never;
/** The member(s) of an error union with a given `code`. */
export type ErrorWithCode<E, C> = E extends { readonly code: infer K } ? (C extends K ? E : never) : never;
/** The success type of an outcome. */
export type OutcomeValue<O> = O extends { readonly ok: true; readonly result: infer T } ? T : never;
/** The error type of an outcome. */
export type OutcomeError<O> = O extends { readonly ok: false; readonly error: infer E } ? E : never;
/** One handler per error `code` in `E` (a missing one is a compile error), plus `ok`. */
// One mapped type (not an intersection), so handler parameters get contextual types inside a generic call.
export type OutcomeHandlers<T, E, X> = { readonly [K in 'ok' | ErrorCodes<E>]: K extends 'ok' ? (result: T) => X : (error: ErrorWithCode<E, K>) => X };

// Non-enumerable, so outcomes still deep-equal plain `{ ok, result }` objects.
const brandOutcome = <O extends object>(outcome: O): O => Object.defineProperty(outcome, OUTCOME_BRAND, { value: true });

// Standalone so internal code (and bundles that never touch the `Outcome` helpers) don't pull in `match` & co.
export const ok = <T>(result: T): Outcome<T, never> => brandOutcome({ ok: true as const, result });
export const fail = <T = never, E = unknown>(error: E): Outcome<T, E> => brandOutcome({ ok: false as const, error });
export const isOutcome = (value: unknown): value is Outcome<unknown, unknown> => typeof value === 'object' && value !== null && (value as Record<symbol, unknown>)[OUTCOME_BRAND] === true;

/** Teaches `settle()` and `Outcome.from()` another Result type (neverthrow, ts-results, Effect's Either…). */
export interface ResultAdapter {
  is(value: unknown): boolean;
  toOutcome(value: unknown): Outcome<unknown, unknown>;
}

const resultAdapters = new Map<string, ResultAdapter>();

/** `registerResultAdapter('neverthrow', { is: (v) => v instanceof Ok || v instanceof Err, toOutcome: (r) => r.isOk() ? Outcome.fromResult(r.value) : Outcome.fromError(r.error) })` */
export function registerResultAdapter(name: string, adapter: ResultAdapter): void {
  resultAdapters.set(name, adapter);
}

export function unregisterResultAdapter(name: string): void {
  resultAdapters.delete(name);
}

/** @internal Our outcome, or a foreign Result converted by a registered adapter, or undefined. */
export function asOutcome(value: unknown): Outcome<unknown, unknown> | undefined {
  if (isOutcome(value)) return value;
  for (const adapter of resultAdapters.values()) if (adapter.is(value)) return adapter.toOutcome(value);
  return undefined;
}

// Errors produced on purpose (returned by callbacks, made by mapError), so settle() / isError() keep them typed.
const knownErrors = new WeakSet<object>();
/** @internal */
export const markKnownError = (error: unknown): void => void (typeof error === 'object' && error !== null && knownErrors.add(error));
/** @internal */
export const isKnownError = (error: unknown): boolean => typeof error === 'object' && error !== null && knownErrors.has(error);

type Settled<O> = O extends PromiseLike<infer U> ? U : O;

export const Outcome = {
  fromResult: ok,
  fromError: fail,
  /** Was this value created with `Outcome.fromResult` / `Outcome.fromError` (from any copy of the package)? */
  is: isOutcome,
  /** Converts a foreign Result via a registered adapter (or returns ours as is); undefined if it isn't one. */
  from: asOutcome,
  /** Runs `fn` and captures a throw/rejection as a failed outcome; `catch` maps the error (typed). Stays synchronous for a synchronous `fn`. */
  try: tryOutcome,
  /** `ok(values)` if every outcome succeeded, otherwise the first failure. */
  all<const O extends readonly Outcome<any, any>[]>(outcomes: O): Outcome<{ -readonly [K in keyof O]: OutcomeValue<O[K]> }, OutcomeError<O[number]>> {
    const results: unknown[] = [];
    for (const o of outcomes) {
      if (!o.ok) return o as never;
      results.push(o.result);
    }
    return ok(results) as never;
  },
  /** Splits outcomes into their values and their errors, keeping order within each. */
  partition<O extends Outcome<any, any>>(outcomes: readonly O[]): { values: OutcomeValue<O>[]; errors: OutcomeError<O>[] } {
    const values: OutcomeValue<O>[] = [];
    const errors: OutcomeError<O>[] = [];
    for (const o of outcomes) (o.ok ? values.push(o.result) : errors.push(o.error as OutcomeError<O>));
    return { values, errors };
  },
  /** The value, or throws the error. */
  unwrap: unwrapOutcome,
  /** The error (optionally checking its `code`), or throws if the outcome succeeded / has another code. */
  unwrapErr<E, C extends ErrorCodes<E> = ErrorCodes<E>>(outcome: Outcome<unknown, E>, code?: C): ErrorWithCode<E, C> {
    if (outcome.ok) throw new TypeError('Outcome.unwrapErr: the outcome succeeded.');
    const actual = (outcome.error as { code?: unknown } | null)?.code;
    if (code !== undefined && actual !== code) throw new TypeError(`Outcome.unwrapErr: expected code '${code}', got '${String(actual)}'.`, { cause: outcome.error });
    return outcome.error as ErrorWithCode<E, C>;
  },
  /**
   * Exhaustive handling by error code: `Outcome.match(await pipeline.settle(cb), { ok: (v) => …, TIMEOUT: (e) => …, CIRCUIT_OPEN: (e) => …, UNEXPECTED: (e) => … })`.
   * Add a breaker and forget its handler: compile error.
   */
  match<T, E, H extends OutcomeHandlers<T, E, unknown>>(outcome: Outcome<T, E>, handlers: H): ReturnType<H[keyof H] & ((...args: any) => any)> {
    type X = ReturnType<H[keyof H] & ((...args: any) => any)>;
    if (outcome.ok) return (handlers as unknown as { ok: (r: T) => X }).ok(outcome.result);
    const code = (outcome.error as { code?: unknown } | null)?.code;
    const handler = (handlers as unknown as Record<string, ((e: unknown) => X) | undefined>)[String(code)];
    if (!handler) throw new TypeError(`Outcome.match: no handler for error code '${String(code)}'.`, { cause: outcome.error });
    return handler(outcome.error);
  },
};

// `any` (e.g. JSON.parse) is treated as synchronous instead of distributing into a confusing union.
type TryResult<R, E> = 0 extends 1 & R ? Outcome<R, E> : R extends PromiseLike<unknown> ? Promise<Outcome<Settled<R>, E>> : Outcome<R, E>;
function tryOutcome<R>(fn: () => R): TryResult<R, unknown>;
function tryOutcome<R, E>(options: { try: () => R; catch: (error: unknown) => E }): TryResult<R, E>;
function tryOutcome(arg: (() => unknown) | { try: () => unknown; catch: (error: unknown) => unknown }): unknown {
  const fn = typeof arg === 'function' ? arg : arg.try;
  const map = typeof arg === 'function' ? (e: unknown) => e : arg.catch;
  try {
    const value = fn();
    if (value && typeof (value as PromiseLike<unknown>).then === 'function') return Promise.resolve(value).then(ok, (e) => fail(map(e)));
    return ok(value);
  } catch (error) {
    return fail(map(error));
  }
}

/**
 * @internal The strategies' lean, always-async capture: no error mapping, so they don't bundle `Outcome.try`'s overloads.
 * Public code uses `Outcome.try`, which also maps errors and stays synchronous for a synchronous `fn`.
 */
export async function capture<T>(fn: () => MaybePromise<T>): Promise<Outcome<T>> {
  try {
    return ok(await fn());
  } catch (error) {
    return fail(error);
  }
}

/** @internal `Outcome.unwrap` as a standalone function: `promise.then(unwrapOutcome)` without bundling the `Outcome` helpers. */
export function unwrapOutcome<T>(outcome: Outcome<T, unknown>): T {
  if (outcome.ok) return outcome.result;
  throw outcome.error;
}

export interface ResilienceContext {
  /** Aborted when the caller cancels, or when a strategy (timeout, hedging) gives up on this execution. */
  readonly signal: AbortSignal;
  readonly operationKey: string | undefined;
  /** Shared bag for passing data between strategies and the callback. */
  readonly properties: Map<string, unknown>;
  /**
   * 0 for the first attempt, then 1, 2, … in the order attempts start. Use it for logging or per-attempt idempotency keys.
   * One counter per execution numbers every retry and hedged attempt, however retry and hedging are nested
   * (retry around hedging, retry inside hedged branches), so a number is never repeated within an execution.
   * Strategies' own counters (`RetryPredicateArgs.attemptNumber`, `OnHedgingArgs.attemptNumber`) stay local.
   */
  readonly attemptNumber: number;
  /** `false` when the call must not run twice (e.g. a payment): retry and hedging then only repeat calls that never started. */
  readonly idempotent: boolean;
  /** Operation name for telemetry (spans, metrics); defaults to the pipeline name. */
  readonly operationName?: string;
  /** Extra low-cardinality telemetry attributes for this execution. */
  readonly attributes?: Readonly<Record<string, string | number | boolean>>;
  /**
   * Clock for this execution's own timing (retry delays, timeouts, hedging delays, chaos latency). Stateful strategies
   * (breakers, limiters, caches, locks) keep the pipeline's clock, since their state spans executions.
   */
  readonly timeProvider?: TimeProvider;
}

export interface ExecuteOptions {
  /** Cancels the execution. Combined with the signal of the execution this one runs inside, if any. */
  signal?: AbortSignal;
  operationKey?: string;
  properties?: Map<string, unknown>;
  /** Default true. `false`: never repeat a call that may have reached the dependency (no retries/hedging of started calls). */
  idempotent?: boolean;
  /** Abort each attempt's signal as soon as its callback settles, so background work it started (timers, streams) stops too. */
  abortOnReturn?: boolean;
  /** Operation name for telemetry (spans, metrics) instead of the pipeline name. */
  name?: string;
  /** Extra low-cardinality telemetry attributes for this execution (added to spans and OTel metrics). */
  attributes?: Readonly<Record<string, string | number | boolean>>;
  /** Clock for this execution's retry delays / timeouts / hedging delays (e.g. a FakeTimeProvider in one test). */
  timeProvider?: TimeProvider;
}

/** A typed key for `context.properties` (Polly's ResiliencePropertyKey): `const TENANT = new ResiliencePropertyKey<string>('tenant')`. */
export class ResiliencePropertyKey<V> {
  readonly key: string;
  constructor(key: string) {
    this.key = key;
  }
  get(context: { readonly properties: Map<string, unknown> }): V | undefined {
    return context.properties.get(this.key) as V | undefined;
  }
  set(context: { readonly properties: Map<string, unknown> }, value: V): void {
    context.properties.set(this.key, value);
  }
}

// Created on first use: Workers forbid `new AbortController()` at module scope ("Disallowed operation called within global scope").
let neverAbortedSignal: AbortSignal | undefined;
const neverAborted = (): AbortSignal => (neverAbortedSignal ??= new AbortController().signal);

// The execution's attempt counter: a symbol key, so `{ ...context }` copies share it but it stays out of the public type.
const ATTEMPTS = Symbol('attempts');
type Counted = ResilienceContext & { [ATTEMPTS]?: { n: number } };

/** @internal Next `attemptNumber` of this execution (retry and hedging call it for every attempt after their first). */
export const nextAttemptNumber = (context: ResilienceContext): number => ++((context as Counted)[ATTEMPTS] ??= { n: context.attemptNumber }).n;

export function createContext(options: ExecuteOptions = {}): ResilienceContext {
  const context: Counted = {
    [ATTEMPTS]: { n: 0 },
    signal: options.signal ?? neverAborted(),
    operationKey: options.operationKey,
    properties: options.properties ?? new Map(),
    attemptNumber: 0,
    idempotent: options.idempotent ?? true,
    ...(options.name !== undefined && { operationName: options.name }),
    ...(options.attributes && { attributes: options.attributes }),
    ...(options.timeProvider && { timeProvider: options.timeProvider }),
  };
  return context;
}

export type Next<T> = (context: ResilienceContext) => Promise<Outcome<T>>;

export interface ResilienceStrategy<T = any> {
  execute(next: Next<T>, context: ResilienceContext): Promise<Outcome<T>>;
  /** Release timers and background work (health checks, store sync). Called by `pipeline.dispose()`. */
  dispose?(): void;
}

/**
 * A child signal that aborts when any parent aborts. Unlike `AbortSignal.any`, `dispose()` detaches it from the parents:
 * `AbortSignal.any` keeps every child alive for as long as a long-lived parent (a server's or worker's signal) lives,
 * which leaked ~50 MB per 100k executions in our measurements.
 */
export function linkSignals(...parents: Array<AbortSignal | undefined>): { readonly controller: AbortController; readonly signal: AbortSignal; dispose(): void } {
  const controller = new AbortController();
  const detach: Array<() => void> = [];
  for (const parent of parents) {
    if (!parent || parent === neverAbortedSignal) continue;
    if (parent.aborted) {
      controller.abort(parent.reason);
      break;
    }
    const onAbort = () => controller.abort(parent.reason);
    parent.addEventListener('abort', onAbort, { once: true });
    detach.push(() => parent.removeEventListener('abort', onAbort));
  }
  return { controller, signal: controller.signal, dispose: () => detach.forEach((d) => d()) };
}

export interface OutcomeArgs<T> {
  readonly outcome: Outcome<T>;
  readonly context: ResilienceContext;
}

export type ShouldHandle<A extends OutcomeArgs<any>> = ((args: A) => MaybePromise<boolean>) | PredicateBuilder<any>;

export const isAbortError = (error: unknown): boolean => error instanceof Error && error.name === 'AbortError';

/**
 * A 4xx answer means "this request is wrong", not "the dependency is down": retrying won't help and it says nothing
 * about the dependency's health. Detected from a numeric `status` / `statusCode` (fetch wrappers, axios, AWS/Google SDKs).
 * 408 and 429 are excluded: they are the dependency pushing back, so they behave like 5xx.
 */
export function isClientError(error: unknown): boolean {
  if (typeof error !== 'object' || error === null) return false;
  const e = error as { status?: unknown; statusCode?: unknown; response?: { status?: unknown } };
  const status = [e.status, e.statusCode, e.response?.status].find((s) => typeof s === 'number') as number | undefined;
  return status !== undefined && status >= 400 && status < 500 && status !== 408 && status !== 429;
}

// Brands are registered symbols, so they match across duplicate copies of this package (ESM + CJS, nested installs),
// where `instanceof` silently fails.
const ERROR_BRAND = /* @__PURE__ */ Symbol.for('@nodelibraries/resilience.error');
const REJECTION_BRAND = /* @__PURE__ */ Symbol.for('@nodelibraries/resilience.rejection');
const NON_RETRYABLE_BRAND = /* @__PURE__ */ Symbol.for('@nodelibraries/resilience.non-retryable');
const branded = (value: unknown, brand: symbol): boolean => typeof value === 'object' && value !== null && (value as Record<symbol, unknown>)[brand] === true;

/** Any error raised by a strategy (timeout, open circuit, limiter, lock…). Works across duplicate package copies. */
export const isResilienceError = (error: unknown): error is ExecutionRejectedError => branded(error, ERROR_BRAND);
const hasCode = (error: unknown, ...codes: string[]) => isResilienceError(error) && codes.includes(error.code);
export const isTimeoutError = (error: unknown): error is TimeoutRejectedError => hasCode(error, 'TIMEOUT');
export const isBrokenCircuitError = (error: unknown): error is BrokenCircuitError => hasCode(error, 'CIRCUIT_OPEN', 'CIRCUIT_ISOLATED');
export const isRateLimitError = (error: unknown): error is RateLimiterRejectedError => hasCode(error, 'RATE_LIMITED', 'BULKHEAD_FULL', 'PERMIT_LIMIT_EXCEEDED', 'RATE_LIMIT_DENIED');
export const isNonRetryableError = (error: unknown): error is NonRetryableError => branded(error, NON_RETRYABLE_BRAND);

/** The call never started because a strategy refused it (open circuit, full limiter). It says nothing about the dependency. */
export const isRejection = (error: unknown): boolean => branded(error, REJECTION_BRAND);

/** The caller gave up (client disconnected, user cancelled). A timeout abort is not a caller abort: the dependency was too slow. */
const callerAborted = (context: ResilienceContext) => context.signal.aborted && !isTimeoutError(context.signal.reason);

/**
 * Default for retry, circuit breaker and hedging: transient failures only.
 * Not handled: results, aborts, client errors (4xx except 408/429), rejections from other strategies.
 */
export function defaultShouldHandle({ outcome, context }: OutcomeArgs<unknown>): boolean {
  if (outcome.ok || callerAborted(context)) return false;
  const e = outcome.error;
  return !isAbortError(e) && !isClientError(e) && !isRejection(e);
}

/** Default for fallback: every failure except aborts and client errors, so it also covers an open circuit or a full limiter. */
export function defaultFallbackShouldHandle({ outcome, context }: OutcomeArgs<unknown>): boolean {
  if (outcome.ok || callerAborted(context)) return false;
  return !isAbortError(outcome.error) && !isClientError(outcome.error);
}

/** Milliseconds, or a string like `'250ms'`, `'30s'`, `'5m'`, `'1h'`, `'1d'`, `'2w'`. */
export type Duration = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd' | 'w'}`;

const UNITS: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 };

export function toMs(value: Duration, name = 'duration'): number {
  // Any number syntax the `Duration` type accepts ('1e3ms', '.5s', '+5s'); NaN is always an error.
  const match = /^(.+?)(ms|s|m|h|d|w)$/.exec(String(value).trim());
  const ms = typeof value === 'number' ? value : match ? Number(match[1]) * UNITS[match[2]!]! : NaN;
  if (Number.isNaN(ms)) throw new RangeError(`${name}: invalid duration '${value}'. Use milliseconds or a string like '500ms', '30s', '5m'.`);
  return ms;
}

export const optionalMs = (value: Duration | undefined, name: string): number | undefined => (value === undefined ? undefined : toMs(value, name));

// The execution the current code runs in, so nested pipelines inherit its signal without passing it around.
// Uses AsyncLocalStorage where the runtime has it (Node, Bun, Deno, Workers with nodejs_compat); a no-op elsewhere (browsers).
const AsyncLocalStorageClass = nodeBuiltin<{ AsyncLocalStorage: new () => ContextStorage<ResilienceContext> }>('node:async_hooks')?.AsyncLocalStorage;
export const ambient: ContextStorage<ResilienceContext> = AsyncLocalStorageClass ? new AsyncLocalStorageClass() : { getStore: () => undefined, run: (_store, fn) => fn() };

/** Context of the resilient execution the calling code runs in (works in nested services), or undefined. */
export function getResilienceContext(): ResilienceContext | undefined {
  return ambient.getStore();
}

/** Shortcut for `getResilienceContext()?.signal`: pass it to fetch / your DB driver from deep inside a call. */
export function getResilienceSignal(): AbortSignal | undefined {
  return ambient.getStore()?.signal;
}

type ErrorClass<E> = abstract new (...args: any[]) => E;

/** Builds a `shouldHandle` predicate: `new PredicateBuilder().handle(HttpError, e => e.status >= 500).handleResult(r => r.status === 429)`. */
export class PredicateBuilder<T = any> {
  readonly #predicates: Array<(outcome: Outcome<T>) => boolean> = [];

  handle<E>(type: ErrorClass<E>, predicate: (error: E) => boolean = () => true): this {
    this.#predicates.push((o) => !o.ok && o.error instanceof type && predicate(o.error as E));
    return this;
  }

  /** Matches when the error, or any error in its `cause` chain, is a `type` (Polly's HandleInner). */
  handleInner<E>(type: ErrorClass<E>, predicate: (error: E) => boolean = () => true): this {
    this.#predicates.push((o) => {
      for (let e: unknown = o.ok ? undefined : o.error, depth = 0; e !== undefined && depth < 10; e = (e as { cause?: unknown })?.cause, depth++) {
        if (e instanceof type && predicate(e as E)) return true;
      }
      return false;
    });
    return this;
  }

  handleResult(predicate: (result: T) => boolean): this {
    this.#predicates.push((o) => o.ok && predicate(o.result));
    return this;
  }

  build(): (args: OutcomeArgs<T>) => boolean {
    const predicates = [...this.#predicates];
    return ({ outcome }) => predicates.some((p) => p(outcome));
  }
}

export function resolvePredicate<A extends OutcomeArgs<any>>(
  p: ShouldHandle<A> | undefined,
  fallback: (args: OutcomeArgs<unknown>) => boolean = defaultShouldHandle,
): (args: A) => MaybePromise<boolean> {
  if (p === undefined) return fallback;
  // Duck-typed (not instanceof) so PredicateBuilder isn't bundled for callers that pass a plain function.
  return typeof p === 'function' ? p : p.build();
}

/** Base of every error a strategy raises. `code` is stable and machine-readable; `source` names the pipeline/strategy. */
export class ExecutionRejectedError extends Error {
  name = 'ExecutionRejectedError';
  code: string = 'EXECUTION_REJECTED';
  /** Pipeline and strategy that raised it (set by the strategy). */
  source: TelemetrySource | undefined;

  constructor(message?: string, options?: ErrorOptions) {
    super(message, options);
    // Set here rather than as a computed class field, which bundlers treat as a side effect (no tree-shaking).
    (this as Record<symbol, unknown>)[ERROR_BRAND] = true;
  }

  toJSON(): Record<string, unknown> {
    const retryAfter = (this as { retryAfter?: number }).retryAfter;
    return { name: this.name, code: this.code, message: this.message, ...(retryAfter !== undefined && { retryAfter }), ...(this.source && { source: this.source }) };
  }
}

/** Sets `error.source` (if not set yet) and returns the error. */
export function stamp<E>(error: E, source: TelemetrySource): E {
  if (isResilienceError(error) && !error.source) error.source = { pipelineName: source.pipelineName, strategyName: source.strategyName };
  return error;
}

/**
 * Throw (or wrap an error in) this to stop retrying right away: retry gives up and the caller receives the original error.
 * Like p-retry's `AbortError`. `throw new NonRetryableError(error)` or `throw new NonRetryableError('invalid card')`.
 */
export class NonRetryableError extends Error {
  name = 'NonRetryableError';
  constructor(errorOrMessage: unknown) {
    const cause = errorOrMessage instanceof Error ? errorOrMessage : new Error(String(errorOrMessage));
    super(cause.message, { cause });
    (this as Record<symbol, unknown>)[NON_RETRYABLE_BRAND] = true;
  }
}

/** What `pipeline.settle()` wraps any error that isn't a resilience error or one your callback returned typed. */
export class UnexpectedError extends Error {
  name = 'UnexpectedError';
  readonly code = 'UNEXPECTED' as const;
  constructor(cause: unknown) {
    super(cause instanceof Error ? cause.message : String(cause), { cause });
  }
}

export class TimeoutRejectedError extends ExecutionRejectedError {
  name = 'TimeoutRejectedError';
  code = 'TIMEOUT' as const;
  readonly timeout: number;
  constructor(timeout: number) {
    super(`The operation didn't complete within the allowed timeout of ${timeout}ms.`);
    this.timeout = timeout;
  }
}

/**
 * A strategy refused to start the call (open circuit, full limiter, lock held). The dependency was never called,
 * so these are never counted by circuit breakers and not retried by default.
 */
export class CallRejectedError extends ExecutionRejectedError {
  name = 'CallRejectedError';
  code = 'REJECTED';
  constructor(message?: string, options?: ErrorOptions) {
    super(message, options);
    (this as Record<symbol, unknown>)[REJECTION_BRAND] = true;
  }
}

export class BrokenCircuitError extends CallRejectedError {
  name = 'BrokenCircuitError';
  code: 'CIRCUIT_OPEN' | 'CIRCUIT_ISOLATED' = 'CIRCUIT_OPEN';
  /** Milliseconds until the circuit allows a probe call. */
  readonly retryAfter: number | undefined;
  constructor(message = 'The circuit is now open and is not allowing calls.', retryAfter?: number, options?: ErrorOptions) {
    super(message, options);
    this.retryAfter = retryAfter;
  }
}

export class IsolatedCircuitError extends BrokenCircuitError {
  name = 'IsolatedCircuitError';
  code = 'CIRCUIT_ISOLATED' as const;
  constructor() {
    super('The circuit is manually held open and is not allowing calls.');
  }
}

export class RateLimiterRejectedError extends CallRejectedError {
  name = 'RateLimiterRejectedError';
  code: 'RATE_LIMITED' | 'BULKHEAD_FULL' | 'PERMIT_LIMIT_EXCEEDED' | 'RATE_LIMIT_DENIED' = 'RATE_LIMITED';
  readonly retryAfter: number | undefined;
  constructor(retryAfter?: number, message?: string) {
    super(message ?? (retryAfter === undefined ? 'The operation could not be executed due to rate limiting.' : `The operation could not be executed due to rate limiting. Retry after ${retryAfter}ms.`));
    this.retryAfter = retryAfter;
  }
}

/** A concurrency limiter (bulkhead) is full: a capacity problem on our side, so HTTP maps it to 503, not 429. */
export class BulkheadRejectedError extends RateLimiterRejectedError {
  name = 'BulkheadRejectedError';
  code = 'BULKHEAD_FULL' as const;
  /**
   * `'full'`: no slot and no queue room; `'queue-timeout'`: waited `queueTimeout` without getting a slot;
   * `'dropped'`: removed from the queue to make room (`overflow: 'drop-oldest' | 'drop-lowest-priority'`).
   */
  readonly reason: 'full' | 'queue-timeout' | 'dropped';
  readonly active: number;
  readonly queued: number;
  constructor(reason: 'full' | 'queue-timeout' | 'dropped', active: number, queued: number) {
    super(
      undefined,
      reason === 'full'
        ? `The bulkhead is full (${active} running, ${queued} queued).`
        : reason === 'queue-timeout'
          ? `Waited too long for a bulkhead slot (${active} running, ${queued} queued).`
          : `Dropped from the bulkhead queue to make room for another execution (${active} running, ${queued} queued).`,
    );
    this.reason = reason;
    this.active = active;
    this.queued = queued;
  }
}

export const TELEMETRY_CHANNEL = '@nodelibraries/resilience';
const channel = nodeBuiltin<{ channel(name: string): { readonly hasSubscribers: boolean; publish(message: unknown): void } }>('node:diagnostics_channel')?.channel(TELEMETRY_CHANNEL);

export interface TelemetrySource {
  readonly pipelineName: string | undefined;
  readonly strategyName: string | undefined;
  /** Free-form labels from the builder (`tags: { team: 'payments' }`), e.g. a group for dashboards. */
  readonly tags?: Readonly<Record<string, string>>;
  /** No telemetry events from this pipeline (builder option `telemetry: false`). */
  readonly silent?: boolean;
}

/** What strategies receive when the pipeline is built. */
export interface StrategySource extends TelemetrySource {
  readonly timeProvider: TimeProvider;
}

/** Published on the `@nodelibraries/resilience` diagnostics channel and to `onTelemetry` listeners. */
export interface TelemetryEvent {
  readonly event: string;
  readonly source: TelemetrySource;
  readonly context: ResilienceContext;
  readonly args: unknown;
}

const listeners = new Set<(event: TelemetryEvent) => void>();

/** Subscribe to every strategy event in the process. Works in every runtime (diagnostics_channel is Node-only). Returns an unsubscribe function. */
export function onTelemetry(listener: (event: TelemetryEvent) => void): () => void {
  listeners.add(listener);
  return () => void listeners.delete(listener);
}

/** Resolves with the next telemetry event matching `filter` (e.g. in tests: `await nextTelemetry((e) => e.event === 'OnCircuitOpened')`). */
export function nextTelemetry(filter: (event: TelemetryEvent) => boolean = () => true, signal?: AbortSignal): Promise<TelemetryEvent> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const onAbort = () => {
      off();
      reject(signal!.reason);
    };
    const off = onTelemetry((event) => {
      if (!filter(event)) return;
      off();
      signal?.removeEventListener('abort', onAbort);
      resolve(event);
    });
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/** @internal Cheap check before measuring anything only telemetry needs. */
export const telemetryEnabled = (): boolean => listeners.size > 0 || channel?.hasSubscribers === true;

/** Emit a telemetry event, e.g. from a custom strategy: `report(source, 'OnMyEvent', context, args)`. */
export function report(source: TelemetrySource, event: string, context: ResilienceContext, args: unknown): void {
  if (source.silent || (!listeners.size && !channel?.hasSubscribers)) return;
  const message: TelemetryEvent = { event, source: { pipelineName: source.pipelineName, strategyName: source.strategyName, ...(source.tags && { tags: source.tags }) }, context, args };
  for (const listener of listeners) {
    // A broken listener must never fail or retry the call it observes.
    try {
      listener(message);
    } catch {}
  }
  channel?.publish(message);
}

/** Clock and timers used by every strategy. Swap it for `FakeTimeProvider` (`@nodelibraries/resilience/testing`) to test without waiting. */
export interface TimeProvider {
  /** Epoch milliseconds. */
  now(): number;
  setTimeout(callback: () => void, ms: number): unknown;
  clearTimeout(handle: unknown): void;
}

export const systemTime: TimeProvider = {
  now: () => Date.now(),
  setTimeout: (callback, ms) => setTimeout(callback, ms),
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

/** System clock whose pending timers don't keep the process alive (CLIs, scripts, tests). Node, Bun and Deno ≥ 2.8 (older Deno timers have no unref); same as `systemTime` elsewhere. */
export const unrefSystemTime: TimeProvider = {
  now: () => Date.now(),
  setTimeout: (callback, ms) => {
    const handle = setTimeout(callback, ms) as { unref?: () => void };
    handle.unref?.();
    return handle;
  },
  clearTimeout: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};

export interface ExecutionInfo {
  readonly source: TelemetrySource;
  readonly context: ResilienceContext;
}

/**
 * Wraps executions and attempts, e.g. to open a tracing span around them (`@nodelibraries/resilience/otel` uses this).
 * Each hook must call `run()` exactly once and return what it returns.
 */
export interface Instrumentation {
  /** Around a whole pipeline execution, retries included. */
  execution?<R>(info: ExecutionInfo, run: () => Promise<R>): Promise<R>;
  /** Around each call of the user callback (every retry / hedged attempt); `info.context.attemptNumber` tells which. */
  attempt?<R>(info: ExecutionInfo, run: () => Promise<R>): Promise<R>;
}

const instrumentations: Instrumentation[] = [];

/** Register instrumentation for every pipeline in the process. Returns a function that removes it. */
export function instrument(instrumentation: Instrumentation): () => void {
  instrumentations.push(instrumentation);
  return () => {
    const i = instrumentations.indexOf(instrumentation);
    if (i >= 0) instrumentations.splice(i, 1);
  };
}

/** @internal */
export function instrumented<R>(kind: 'execution' | 'attempt', info: ExecutionInfo, run: () => Promise<R>, own?: Instrumentation | false): Promise<R> {
  if (own === false || (!instrumentations.length && !own)) return run();
  // The first registered instrumentation is the outermost; a pipeline's own one runs innermost.
  const all = own ? [...instrumentations, own] : instrumentations;
  return all.reduceRight<() => Promise<R>>((inner, i) => (i[kind] ? () => i[kind]!(info, inner) : inner), run)();
}

/** @internal Set by `setGlobalChaos()` (chaos.ts); runs before every callback in every pipeline. */
export const globalHooks: { beforeCallback?: (context: ResilienceContext, time: TimeProvider) => Promise<Outcome<never> | undefined> } = {};

/** @internal Called on successful results a strategy throws away (executeStream closes that attempt's stream). */
export const discard = Symbol('discard');

/** @internal Retry (handled result), hedging (losers) and fallback (replaced result) call this on what they drop. */
export const discardOutcome = (outcome: Outcome<unknown>): void => void (outcome.ok && (outcome.result as { [discard]?: () => unknown } | null)?.[discard]?.());

// setTimeout fires immediately for delays above this.
export const MAX_TIMER = 2 ** 31 - 1;

export function delay(ms: number, signal: AbortSignal, time: TimeProvider = systemTime): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal.aborted) return reject(signal.reason);
    // No timer for a zero delay: resolves right away, also on a FakeTimeProvider nobody advances.
    if (!(ms > 0)) return resolve();
    if (ms === Infinity) return signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    let timer: unknown;
    const onAbort = () => {
      time.clearTimeout(timer);
      reject(signal.reason);
    };
    // Delays beyond ~24.8 days are chained, since setTimeout fires immediately for anything longer.
    const wait = (remaining: number) => {
      timer = time.setTimeout(() => {
        if (remaining > MAX_TIMER) return wait(remaining - MAX_TIMER);
        signal.removeEventListener('abort', onAbort);
        resolve();
      }, Math.min(remaining, MAX_TIMER));
    };
    wait(ms);
    signal.addEventListener('abort', onAbort, { once: true });
  });
}

export function check(condition: boolean, message: string): void {
  if (!condition) throw new RangeError(message);
}
