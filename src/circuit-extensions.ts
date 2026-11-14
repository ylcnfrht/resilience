import { type CircuitBreakerExtension, type CircuitSnapshot } from './circuit-breaker.ts';
import { type Duration, type MaybePromise, type TimeProvider, check, createContext, report, toMs } from './core.ts';

// Runs `task` every `ms` in the background without keeping the process alive; returns a stop function.
function every(time: TimeProvider, ms: number, task: () => Promise<void>): () => void {
  let stopped = false;
  let timer: unknown;
  const schedule = () => {
    if (stopped) return;
    timer = time.setTimeout(() => void task().finally(schedule), ms);
    (timer as { unref?: () => void } | undefined)?.unref?.();
  };
  schedule();
  return () => {
    stopped = true;
    time.clearTimeout(timer);
  };
}

export interface CircuitHealthCheckOptions {
  /** Resolve (anything but `false`) when healthy; throw, reject or return `false` when not. */
  check: (signal: AbortSignal) => MaybePromise<unknown>;
  interval: Duration;
  /** Default: `interval`. A check that takes longer fails. */
  timeout?: Duration;
}

/**
 * Probes the dependency in the background while the circuit is closed; a failing probe opens it (opossum's healthCheck).
 * `addCircuitBreaker({ extensions: [circuitHealthCheck({ check: () => db.ping(), interval: '10s' })] })`
 */
export function circuitHealthCheck(options: CircuitHealthCheckOptions): CircuitBreakerExtension {
  const interval = toMs(options.interval, 'healthCheck.interval');
  const timeout = toMs(options.timeout ?? interval, 'healthCheck.timeout');
  check(interval > 0 && timeout > 0, 'healthCheck interval and timeout must be > 0');
  return (breaker) => {
    const time = breaker.source.timeProvider;
    return every(time, interval, async () => {
      if (breaker.state !== 'closed') return;
      const controller = new AbortController();
      let timer: unknown;
      try {
        const healthy = await Promise.race([
          Promise.resolve(options.check(controller.signal)),
          new Promise<never>((_, reject) => (timer = time.setTimeout(() => reject(new Error(`Health check timed out after ${timeout}ms`)), timeout))),
        ]);
        if (healthy === false) throw new Error('Health check returned false');
      } catch (error) {
        controller.abort(error);
        report(breaker.source, 'OnHealthCheckFailed', createContext(), { error });
        if (breaker.state === 'closed') await breaker.trip(error);
      } finally {
        time.clearTimeout(timer);
      }
    });
  };
}

/** Shared storage for breaker state across instances. The newest `updatedAt` wins. */
export interface CircuitStateStore {
  get(key: string): MaybePromise<CircuitSnapshot | undefined>;
  set(key: string, snapshot: CircuitSnapshot): MaybePromise<void>;
}

/** In-process `CircuitStateStore`: shares one breaker state between pipelines in the same process (and in tests). */
export class MemoryCircuitStateStore implements CircuitStateStore {
  readonly #entries = new Map<string, CircuitSnapshot>();
  get(key: string): CircuitSnapshot | undefined {
    return this.#entries.get(key);
  }
  set(key: string, snapshot: CircuitSnapshot): void {
    const current = this.#entries.get(key);
    if (!current || (snapshot.updatedAt ?? 0) > (current.updatedAt ?? 0)) this.#entries.set(key, snapshot); // newer wins, same rule as RedisCircuitStateStore
  }
}

export interface SharedCircuitStateOptions {
  store: CircuitStateStore;
  /** One key per logical breaker (e.g. per downstream service). */
  key: string;
  /** How often to pull the shared state. Default 5 s. Lower = faster agreement, more store traffic. */
  syncInterval?: Duration;
}

/**
 * Shares the breaker's state with other instances (servers, processes) through a store, e.g. `RedisCircuitStateStore`:
 * one instance seeing the dependency fail opens the circuit for all of them.
 * `addCircuitBreaker({ extensions: [sharedCircuitState({ store, key: 'payments' })] })`
 */
export function sharedCircuitState(options: SharedCircuitStateOptions): CircuitBreakerExtension {
  const syncInterval = toMs(options.syncInterval ?? '5s', 'syncInterval');
  check(syncInterval > 0, 'syncInterval must be > 0');
  return (breaker) => {
    const failed = (error: unknown) => report(breaker.source, 'OnCircuitStoreError', createContext(), { error });
    const pull = async () => {
      try {
        const remote = await options.store.get(options.key);
        if (remote && breaker.adopt(remote)) report(breaker.source, 'OnCircuitStateSynced', createContext(), { snapshot: remote });
      } catch (error) {
        failed(error);
      }
    };
    breaker.onChange(({ window: _history, ...snapshot }) => {
      // The rolling window is this instance's own traffic history: keep it out of the shared store.
      Promise.resolve()
        .then(() => options.store.set(options.key, snapshot))
        .catch(failed);
    });
    void pull();
    return every(breaker.source.timeProvider, syncInterval, pull);
  };
}
