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
  BrokenCircuitError,
  IsolatedCircuitError,
  check,
  createContext,
  fail,
  isRejection,
  stamp,
  report,
  resolvePredicate,
  toMs,
} from './core.ts';

export type CircuitState = 'closed' | 'open' | 'half-open' | 'isolated';

export interface CircuitMetrics {
  /** Calls recorded in the current window (successes + failures; ignored calls are not counted). */
  readonly total: number;
  readonly failureCount: number;
  /** failureCount / total, 0 when empty. */
  readonly failureRate: number;
}

export interface BreakDurationArgs extends CircuitMetrics {
  /** Consecutive half-open rounds that failed; use it for growing break durations. */
  readonly halfOpenAttempts: number;
  readonly context: ResilienceContext;
}

export interface OnCircuitOpenedArgs<T> {
  readonly outcome: Outcome<T> | undefined;
  readonly breakDuration: number;
  readonly isManual: boolean;
  readonly context: ResilienceContext;
}

export interface OnCircuitClosedArgs<T> {
  readonly outcome: Outcome<T> | undefined;
  readonly isManual: boolean;
  readonly context: ResilienceContext;
}

export interface CircuitBreakerOptions<T> {
  name?: string;
  /**
   * Which outcomes are failures. Default: transient errors (not 4xx except 408/429, not caller aborts).
   * Errors that aren't failures are ignored, not counted as successes. Rejections from other strategies are never counted.
   */
  shouldHandle?: ShouldHandle<OutcomeArgs<T>>;
  /** Failure ratio in the window that breaks the circuit. Default 0.1. */
  failureRatio?: number;
  /**
   * Break after this many failures in a row (a success resets the count). Simple and good for low traffic.
   * When set without `failureRatio`, the ratio check is off and only this rule applies.
   */
  consecutiveFailures?: number;
  /** Minimum recorded calls in the window before the ratio is considered. Default 100. */
  minimumThroughput?: number;
  /**
   * Time window only: also require this many calls per second (averaged over `samplingDuration`) before the ratio counts,
   * so a quiet period's few failures can't open the circuit (cockatiel's SamplingBreaker minimumRps).
   */
  minimumRps?: number;
  /** Time-based window: calls of the last `samplingDuration`. Default 30 s. Ignored when `samplingCount` is set. */
  samplingDuration?: Duration;
  /** Count-based window: the last `samplingCount` calls. Better for low or bursty traffic. */
  samplingCount?: number;
  /** How long the circuit stays open. Default 5 s. */
  breakDuration?: Duration;
  breakDurationGenerator?: (args: BreakDurationArgs) => MaybePromise<Duration>;
  /**
   * Probe calls let through while half-open. Default 1 (Polly). When all have finished, the circuit closes if their
   * failure ratio is below `failureRatio`, and opens again otherwise.
   */
  halfOpenMaxCalls?: number;
  /**
   * Fraction of finished probes that must succeed to close, in (0, 1]; e.g. 0.8 with `halfOpenMaxCalls: 5` needs 4 successes.
   * Default: close unless the probes' failure ratio reaches `failureRatio`.
   */
  halfOpenSuccessThreshold?: number;
  /**
   * Custom rule for when a closed circuit opens; replaces `failureRatio`, `minimumThroughput` and the sampling window
   * (`consecutiveFailures`, if set, still applies too).
   */
  breaker?: CircuitBreakerPolicy;
  manualControl?: CircuitBreakerManualControl;
  stateProvider?: CircuitBreakerStateProvider;
  /** Don't open during this period after creation (the window still fills), so a cold start's first errors can't trip it. */
  warmUpDuration?: Duration;
  /** Resume from a snapshot taken with `stateProvider.snapshot()`, e.g. after a serverless cold start or a restart. */
  initialState?: CircuitSnapshot;
  /**
   * How long a half-open probe may run before it counts as a failure and the circuit opens again. Default 30 s.
   * Without it a probe that never settles would keep the circuit half-open (rejecting everything) forever.
   */
  halfOpenMaxWait?: Duration;
  /** Called (and `OnCircuitRejected` reported) every time the circuit refuses a call: the "short-circuited" count. */
  onRejected?: (args: OnCircuitRejectedArgs) => MaybePromise<void>;
  /**
   * Background behaviour plugged into the breaker, e.g. `circuitHealthCheck({ check, interval })` or
   * `sharedCircuitState({ store, key })` from this package. Kept out of the core breaker so you only bundle what you use.
   */
  extensions?: CircuitBreakerExtension[];
  onOpened?: (args: OnCircuitOpenedArgs<T>) => MaybePromise<void>;
  onClosed?: (args: OnCircuitClosedArgs<T>) => MaybePromise<void>;
  onHalfOpened?: (args: { readonly context: ResilienceContext }) => MaybePromise<void>;
}

export interface OnCircuitRejectedArgs {
  readonly context: ResilienceContext;
  readonly state: CircuitState;
  /** ms until the circuit lets a probe through (0 when half-open with all probe slots taken). */
  readonly retryAfter: number | undefined;
}

/** What a breaker extension can see and do. */
export interface CircuitBreakerHandle {
  readonly source: StrategySource;
  readonly state: CircuitState;
  snapshot(): CircuitSnapshot;
  /** Take over a state decided elsewhere (another instance). Ignored unless it's newer than ours; returns whether it was applied. */
  adopt(snapshot: CircuitSnapshot): boolean;
  /** Open the circuit for its break duration, e.g. after a failed health check. */
  trip(reason?: unknown): Promise<void>;
  /** Called after every local state change (not after `adopt`). */
  onChange(listener: (snapshot: CircuitSnapshot) => void): void;
}

/** Plugs background behaviour into a breaker; may return a function that stops it (called on dispose). */
export type CircuitBreakerExtension = (breaker: CircuitBreakerHandle) => (() => void) | void;

/**
 * Decides when a closed circuit opens (cockatiel's IBreaker). Only calls made while closed are recorded; `now` is the
 * pipeline's time provider. One instance per breaker: it holds that breaker's history.
 */
export interface CircuitBreakerPolicy {
  record(failed: boolean, now: number): void;
  /** Asked after each recorded failure (outside warm-up). */
  shouldOpen(now: number): boolean;
  /** Called when the circuit closes. */
  reset(): void;
  /** For `stateProvider.metrics` and `breakDurationGenerator`; zeros when absent. */
  metrics?(now: number): CircuitMetrics;
  /** JSON-serializable history, stored as `CircuitSnapshot.window`. */
  snapshot?(): unknown;
  /** Receives `initialState.window`, which may come from another policy or be malformed: validate it. */
  restore?(state: unknown): void;
}

interface CircuitController {
  isolate(): Promise<void>;
  close(): Promise<void>;
}

/** Manually isolate (hold open) or close one or more circuit breakers, e.g. from an admin endpoint or a health check. */
export class CircuitBreakerManualControl {
  readonly #targets = new Set<CircuitController>();
  readonly #holds = new Set<object>();

  constructor(isIsolated = false) {
    if (isIsolated) this.#holds.add({}); // only close() lifts it
  }

  get isIsolated(): boolean {
    return this.#holds.size > 0;
  }

  /**
   * Holds the circuits isolated until every isolation is released (stacking) or `close()` is called.
   * Resolves to the release function of this isolation; releasing twice, or after `close()`, does nothing.
   */
  async isolate(): Promise<() => Promise<void>> {
    const hold = {};
    this.#holds.add(hold);
    await this.#each('isolate');
    return async () => {
      if (this.#holds.delete(hold) && !this.#holds.size) await this.#each('close');
    };
  }

  /** Closes the circuits now, dropping every outstanding isolation. */
  async close(): Promise<void> {
    this.#holds.clear();
    await this.#each('close');
  }

  async #each(action: keyof CircuitController) {
    await Promise.all([...this.#targets].map((t) => t[action]()));
  }

  /** @internal */
  attach(target: CircuitController): void {
    this.#targets.add(target);
  }
}

/** JSON-serializable breaker state, for persisting across restarts and serverless invocations. */
export interface CircuitSnapshot {
  readonly state: CircuitState;
  /** Epoch ms when an open circuit lets a probe through. */
  readonly openUntil: number;
  readonly consecutiveFailures: number;
  readonly halfOpenAttempts: number;
  /** Epoch ms of the last state change; decides which instance's state wins in a shared store. */
  readonly updatedAt?: number;
  /** Rolling-window contents (the policy's `snapshot()`), restored by `initialState` so failure history survives a restart. */
  readonly window?: unknown;
}

interface BreakerReader {
  state: () => CircuitState;
  warmingUp: () => boolean;
  metrics: () => CircuitMetrics;
  snapshot: () => CircuitSnapshot;
}

/** Reads the state and window metrics of a single circuit breaker (health checks, dashboards, persistence). */
export class CircuitBreakerStateProvider {
  #source: BreakerReader | undefined;

  get circuitState(): CircuitState {
    return this.#source?.state() ?? 'closed';
  }

  /** True during `warmUpDuration`, while failures are recorded but can't open the circuit (opossum's warmUp). */
  get isWarmingUp(): boolean {
    return this.#source?.warmingUp() ?? false;
  }

  get metrics(): CircuitMetrics {
    return this.#source?.metrics() ?? { total: 0, failureCount: 0, failureRate: 0 };
  }

  /** Current state, rolling window included, as plain JSON; pass it back as `initialState` to resume. */
  snapshot(): CircuitSnapshot {
    return this.#source?.snapshot() ?? { state: 'closed', openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, updatedAt: 0 };
  }

  /** @internal */
  attach(source: BreakerReader): void {
    if (this.#source) throw new Error('This state provider is already attached to a circuit breaker.');
    this.#source = source;
  }
}

type Verdict = 'success' | 'failure' | 'ignored';

export class CircuitBreakerStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: CircuitBreakerOptions<T>;
  readonly #source: StrategySource;
  readonly #shouldHandle;
  readonly #failureRatio: number;
  readonly #consecutiveLimit: number | undefined;
  #consecutive = 0;
  readonly #warmUpUntil: number;
  readonly #breakDuration: number;
  readonly #halfOpenMaxCalls: number;
  readonly #successThreshold: number | undefined;
  readonly #policy: CircuitBreakerPolicy;
  #state: CircuitState = 'closed';
  // Bumped on every transition: outcomes of calls admitted under an earlier state are stale and ignored.
  #generation = 0;
  #blockedUntil = 0;
  #probes = { started: 0, finished: 0, failures: 0 };
  #halfOpenAttempts = 0;
  #lastFailure: Outcome<T> | undefined;
  readonly #halfOpenMaxWait: number;
  #updatedAt = 0;
  readonly #changeListeners: Array<(snapshot: CircuitSnapshot) => void> = [];
  readonly #stops: Array<() => void> = [];

  constructor(options: CircuitBreakerOptions<T>, source: StrategySource) {
    this.#options = options;
    this.#source = source;
    this.#shouldHandle = resolvePredicate(options.shouldHandle);
    this.#failureRatio = options.failureRatio ?? 0.1;
    this.#consecutiveLimit = options.consecutiveFailures;
    this.#warmUpUntil = source.timeProvider.now() + toMs(options.warmUpDuration ?? 0, 'warmUpDuration');
    check(this.#consecutiveLimit === undefined || (Number.isInteger(this.#consecutiveLimit) && this.#consecutiveLimit >= 1), 'consecutiveFailures must be an integer >= 1');
    const minimumThroughput = options.minimumThroughput ?? 100;
    this.#breakDuration = toMs(options.breakDuration ?? 5000, 'breakDuration');
    this.#halfOpenMaxCalls = options.halfOpenMaxCalls ?? 1;
    this.#halfOpenMaxWait = toMs(options.halfOpenMaxWait ?? '30s', 'halfOpenMaxWait');
    check(this.#halfOpenMaxWait > 0, 'halfOpenMaxWait must be > 0');
    check(this.#failureRatio > 0 && this.#failureRatio <= 1, 'failureRatio must be in (0, 1]');
    check(Number.isInteger(minimumThroughput) && minimumThroughput >= 1, 'minimumThroughput must be an integer >= 1');
    check(this.#breakDuration > 0, 'breakDuration must be > 0');
    check(Number.isInteger(this.#halfOpenMaxCalls) && this.#halfOpenMaxCalls >= 1, 'halfOpenMaxCalls must be an integer >= 1');
    const threshold = (this.#successThreshold = options.halfOpenSuccessThreshold);
    check(threshold === undefined || (threshold > 0 && threshold <= 1), 'halfOpenSuccessThreshold must be in (0, 1]');
    // consecutiveFailures alone turns the ratio rule off.
    const ratio = options.consecutiveFailures === undefined || options.failureRatio !== undefined ? this.#failureRatio : Infinity;
    const rule = (m: CircuitMetrics) => m.total >= minimumThroughput && m.failureRate >= ratio;
    if (options.breaker) this.#policy = options.breaker;
    else if (options.samplingCount !== undefined) {
      const size = options.samplingCount;
      check(Number.isInteger(size) && size >= 1, 'samplingCount must be an integer >= 1');
      check(options.minimumRps === undefined, 'minimumRps needs a time window (samplingDuration), not samplingCount');
      check(minimumThroughput <= size, `minimumThroughput (${minimumThroughput}) can never be reached by a window of ${size} calls; lower it or raise samplingCount`);
      this.#policy = new CountWindow(size, rule);
    } else {
      const duration = toMs(options.samplingDuration ?? 30_000, 'samplingDuration');
      check(duration > 0, 'samplingDuration must be > 0');
      const rps = options.minimumRps;
      check(rps === undefined || rps > 0, 'minimumRps must be > 0');
      this.#policy = new TimeWindow(duration, rps === undefined ? rule : (m) => m.total / (duration / 1000) >= rps && rule(m));
    }

    options.stateProvider?.attach({
      state: () => this.#state,
      warmingUp: () => this.warmingUp,
      metrics: () => this.metrics,
      snapshot: () => this.snapshot(),
    });
    options.manualControl?.attach({ isolate: () => this.#isolate(), close: () => this.#close(undefined, createContext(), true) });
    const initial = options.initialState;
    if (initial) {
      // A probe that was in flight when the snapshot was taken is gone; resume half-open as open-and-due.
      this.#state = initial.state === 'half-open' ? 'open' : initial.state;
      this.#blockedUntil = initial.state === 'half-open' ? 0 : initial.openUntil;
      this.#consecutive = initial.consecutiveFailures;
      this.#halfOpenAttempts = initial.halfOpenAttempts;
      this.#updatedAt = initial.updatedAt ?? 0;
      if (initial.window !== undefined) this.#policy.restore?.(initial.window);
    }
    if (options.manualControl?.isIsolated) this.#state = 'isolated';

    if (options.extensions?.length) {
      const self = this;
      const handle: CircuitBreakerHandle = {
        source,
        get state() {
          return self.#state;
        },
        snapshot: () => this.snapshot(),
        adopt: (snapshot) => this.#adopt(snapshot),
        trip: (reason) => this.#open(reason === undefined ? undefined : fail(reason), createContext()),
        onChange: (listener) => void this.#changeListeners.push(listener),
      };
      for (const extension of options.extensions) {
        const stop = extension(handle);
        if (stop) this.#stops.push(stop);
      }
    }
  }

  /** True during `warmUpDuration`, while failures are recorded but can't open the circuit. */
  get warmingUp(): boolean {
    return this.#source.timeProvider.now() < this.#warmUpUntil;
  }

  /** Current state. */
  get state(): CircuitState {
    return this.#state;
  }

  /** Window counts (closed state). */
  get metrics(): CircuitMetrics {
    return this.#policy.metrics?.(this.#source.timeProvider.now()) ?? metrics(0, 0);
  }

  /** The last outcome counted as a failure, if any. */
  get lastFailure(): Outcome<T> | undefined {
    return this.#lastFailure;
  }

  /** Plain-JSON state for persistence; pass it back as `initialState`. */
  snapshot(): CircuitSnapshot {
    return { state: this.#state, openUntil: this.#state === 'open' ? this.#blockedUntil : 0, consecutiveFailures: this.#consecutive, halfOpenAttempts: this.#halfOpenAttempts, updatedAt: this.#updatedAt, window: this.#policy.snapshot?.() };
  }

  /** Open the circuit now for its normal break duration (unlike isolate, it half-opens by itself afterwards). */
  async trip(): Promise<void> {
    await this.#open(undefined, createContext());
  }

  /** Close the circuit now and clear its window. */
  async reset(): Promise<void> {
    await this.#close(undefined, createContext(), true);
  }

  /** Stops extensions (health checks, state sync). */
  dispose(): void {
    for (const stop of this.#stops.splice(0)) stop();
  }

  #adopt(remote: CircuitSnapshot): boolean {
    const updatedAt = remote.updatedAt ?? 0;
    if (updatedAt <= this.#updatedAt || this.#options.manualControl?.isIsolated) return false;
    this.#transition(remote.state === 'half-open' ? 'open' : remote.state);
    this.#blockedUntil = remote.state === 'half-open' ? 0 : remote.openUntil;
    this.#consecutive = remote.consecutiveFailures;
    this.#halfOpenAttempts = remote.halfOpenAttempts;
    this.#updatedAt = updatedAt;
    return true;
  }

  #changed() {
    if (!this.#changeListeners.length) return;
    const snapshot = this.snapshot();
    for (const listener of this.#changeListeners) listener(snapshot);
  }

  async #rejected(error: BrokenCircuitError, context: ResilienceContext) {
    stamp(error, this.#source);
    const args: OnCircuitRejectedArgs = { context, state: this.#state, retryAfter: error.retryAfter };
    report(this.#source, 'OnCircuitRejected', context, args);
    await this.#options.onRejected?.(args);
    return fail<T>(error);
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    if (this.#state === 'isolated') return this.#rejected(new IsolatedCircuitError(), context);
    let halfOpened = false;
    if (this.#state === 'open') {
      const wait = this.#blockedUntil - this.#source.timeProvider.now();
      if (wait > 0) return this.#rejected(this.#broken(wait), context);
      this.#transition('half-open');
      halfOpened = true;
    }
    // Claim a probe slot synchronously, before any await, so concurrent callers can't all slip through.
    const probe = this.#state === 'half-open';
    if (probe) {
      if (this.#probes.started >= this.#halfOpenMaxCalls) return this.#rejected(this.#broken(0), context);
      this.#probes.started++;
    }
    const generation = this.#generation;
    const time = this.#source.timeProvider;
    // A probe that runs too long counts as a failed probe, so a hanging call can't wedge the circuit half-open.
    const probeTimer = probe
      ? time.setTimeout(() => {
          if (generation !== this.#generation || this.#state !== 'half-open') return;
          this.#halfOpenAttempts++;
          report(this.#source, 'OnHalfOpenProbeTimedOut', context, { context, halfOpenMaxWait: this.#halfOpenMaxWait });
          void this.#open(undefined, context);
        }, this.#halfOpenMaxWait)
      : undefined;

    let outcome: Outcome<T> | undefined;
    let verdict: Verdict = 'ignored';
    try {
      if (halfOpened) {
        report(this.#source, 'OnCircuitHalfOpened', context, { context });
        await this.#options.onHalfOpened?.({ context });
      }
      outcome = await next(context);
      verdict = await this.#judge(outcome, context);
    } finally {
      if (probeTimer !== undefined) time.clearTimeout(probeTimer);
      // Always release, even if a callback threw: a leaked probe slot would keep the circuit half-open forever.
      await this.#record(generation, probe, verdict, outcome, context);
    }
    return outcome;
  }

  async #judge(outcome: Outcome<T>, context: ResilienceContext): Promise<Verdict> {
    if (!outcome.ok && isRejection(outcome.error)) return 'ignored';
    if (await this.#shouldHandle({ outcome, context })) return 'failure';
    return outcome.ok ? 'success' : 'ignored';
  }

  async #record(generation: number, probe: boolean, verdict: Verdict, outcome: Outcome<T> | undefined, context: ResilienceContext) {
    if (generation !== this.#generation) return;
    if (verdict === 'failure') this.#lastFailure = outcome;

    if (!probe) {
      if (verdict === 'ignored') return;
      this.#policy.record(verdict === 'failure', this.#source.timeProvider.now());
      this.#consecutive = verdict === 'failure' ? this.#consecutive + 1 : 0;
      if (verdict === 'failure' && this.#shouldBreak()) await this.#open(outcome, context);
      return;
    }

    if (verdict === 'ignored') {
      this.#probes.started--; // free the slot for another probe
      return;
    }
    this.#probes.finished++;
    if (verdict === 'failure') this.#probes.failures++;
    if (this.#probes.finished < this.#halfOpenMaxCalls) return;
    const { failures, finished } = this.#probes;
    const threshold = this.#successThreshold;
    if (threshold === undefined ? failures > 0 && failures / finished >= this.#failureRatio : (finished - failures) / finished < threshold) {
      this.#halfOpenAttempts++;
      await this.#open(outcome, context);
    } else {
      await this.#close(outcome, context, false);
    }
  }

  #shouldBreak() {
    const now = this.#source.timeProvider.now();
    if (now < this.#warmUpUntil) return false;
    if (this.#consecutiveLimit !== undefined && this.#consecutive >= this.#consecutiveLimit) return true;
    return this.#policy.shouldOpen(now);
  }

  #transition(to: CircuitState) {
    this.#state = to;
    this.#generation++;
    this.#updatedAt = Math.max(this.#updatedAt + 1, this.#source.timeProvider.now());
    if (to === 'half-open') this.#probes = { started: 0, finished: 0, failures: 0 };
    if (to === 'closed') {
      this.#policy.reset();
      this.#consecutive = 0;
    }
  }

  #broken(retryAfter: number) {
    const last = this.#lastFailure;
    return new BrokenCircuitError(undefined, retryAfter, last && !last.ok ? { cause: last.error } : undefined);
  }

  async #open(outcome: Outcome<T> | undefined, context: ResilienceContext) {
    if (this.#options.manualControl?.isIsolated) return; // isolation holds until released
    const metrics = this.metrics;
    // Open synchronously so concurrent calls are blocked while an async breakDurationGenerator runs.
    this.#transition('open');
    this.#blockedUntil = Infinity;
    let breakDuration = this.#breakDuration;
    try {
      const generated = await this.#options.breakDurationGenerator?.({ ...metrics, halfOpenAttempts: this.#halfOpenAttempts, context });
      if (generated !== undefined) breakDuration = toMs(generated, 'breakDurationGenerator result');
    } catch {} // a broken generator falls back to breakDuration rather than holding the circuit open forever
    this.#blockedUntil = this.#source.timeProvider.now() + breakDuration;
    this.#changed();
    const args: OnCircuitOpenedArgs<T> = { outcome, breakDuration, isManual: false, context };
    report(this.#source, 'OnCircuitOpened', context, args);
    await this.#options.onOpened?.(args);
  }

  async #isolate() {
    this.#transition('isolated');
    this.#changed();
    const context = createContext();
    const args: OnCircuitOpenedArgs<T> = { outcome: undefined, breakDuration: Infinity, isManual: true, context };
    report(this.#source, 'OnCircuitOpened', context, args);
    await this.#options.onOpened?.(args);
  }

  async #close(outcome: Outcome<T> | undefined, context: ResilienceContext, isManual: boolean) {
    this.#transition('closed');
    this.#halfOpenAttempts = 0;
    this.#lastFailure = undefined;
    this.#changed();
    const args: OnCircuitClosedArgs<T> = { outcome, isManual, context };
    report(this.#source, 'OnCircuitClosed', context, args);
    await this.#options.onClosed?.(args);
  }
}

const metrics = (total: number, failureCount: number): CircuitMetrics => ({ total, failureCount, failureRate: total ? failureCount / total : 0 });

/** The last `size` recorded calls. */
class CountWindow implements CircuitBreakerPolicy {
  readonly #size: number;
  readonly #rule: (m: CircuitMetrics) => boolean;
  #calls: boolean[] = [];
  #failures = 0;

  constructor(size: number, rule: (m: CircuitMetrics) => boolean) {
    this.#size = size;
    this.#rule = rule;
  }

  record(failed: boolean) {
    this.#calls.push(failed);
    if (failed) this.#failures++;
    // shortcut: Array#shift is O(n); fine for typical sizes (≤ a few thousand), switch to a ring buffer beyond that.
    if (this.#calls.length > this.#size && this.#calls.shift()) this.#failures--;
  }

  shouldOpen() {
    return this.#rule(this.metrics());
  }

  metrics() {
    return metrics(this.#calls.length, this.#failures);
  }

  reset() {
    this.#calls = [];
    this.#failures = 0;
  }

  snapshot() {
    return this.#calls.map(Number); // 0/1: half the JSON of booleans
  }

  restore(state: unknown) {
    if (!Array.isArray(state)) return;
    this.reset();
    for (const call of state.slice(-this.#size)) if (call === 0 || call === 1) this.record(call === 1);
  }
}

type Bucket = [start: number, total: number, failures: number];

/** Calls of the last `window` ms, kept in 10 buckets. */
class TimeWindow implements CircuitBreakerPolicy {
  #buckets: Bucket[] = [];
  readonly #window: number;
  readonly #rule: (m: CircuitMetrics) => boolean;

  constructor(window: number, rule: (m: CircuitMetrics) => boolean) {
    this.#window = window;
    this.#rule = rule;
  }

  record(failed: boolean, now: number) {
    this.#trim(now);
    let b = this.#buckets.at(-1);
    if (!b || now - b[0] >= this.#window / 10) this.#buckets.push((b = [now, 0, 0]));
    b[1]++;
    if (failed) b[2]++;
  }

  shouldOpen(now: number) {
    return this.#rule(this.metrics(now));
  }

  metrics(now: number) {
    this.#trim(now);
    let total = 0;
    let failures = 0;
    for (const b of this.#buckets) {
      total += b[1];
      failures += b[2];
    }
    return metrics(total, failures);
  }

  reset() {
    this.#buckets = [];
  }

  snapshot() {
    return this.#buckets.map((b) => [...b]);
  }

  restore(state: unknown) {
    if (!Array.isArray(state)) return;
    this.#buckets = state.filter((b): b is Bucket => Array.isArray(b) && b.length === 3 && b.every(Number.isFinite) && b[1] >= b[2] && b[2] >= 0);
  }

  #trim(now: number) {
    while (this.#buckets.length && now - this.#buckets[0]![0] >= this.#window) this.#buckets.shift();
  }
}

export const circuitBreakerStrategy = <T = any>(options: CircuitBreakerOptions<T> = {}): StrategyDefinition<T, BrokenCircuitError> => ({
  name: options.name ?? 'CircuitBreaker',
  create: (s) => new CircuitBreakerStrategy(options, s),
});
