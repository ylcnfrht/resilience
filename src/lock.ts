import type { StrategyDefinition } from './pipeline-builder.ts';
import {
  type Duration,
  type MaybePromise,
  type Next,
  type ResilienceContext,
  type ResilienceStrategy,
  type StrategySource,
  type TimeProvider,
  CallRejectedError,
  check,
  delay,
  fail,
  getResilienceContext,
  report,
  systemTime,
  toMs,
  linkSignals,
  stamp,
} from './core.ts';

/**
 * Lease storage behind `LockStrategy`. Every method must be atomic for its key, so adapters (Redis, Postgres) can be swapped in:
 * - `acquire`: if the key has no live lease, store `{ owner, expires: now + ttlMs }`, bump the key's fencing token and return it;
 *   otherwise return undefined. Tokens must only ever increase per key, also across expiry and release.
 * - `renew`: if the live lease belongs to `owner`, push its expiry to `now + ttlMs` and return true; otherwise false.
 * - `release`: delete the lease only if it belongs to `owner` (never someone else's lease that took over after expiry).
 */
export interface LockStore {
  acquire(key: string, owner: string, ttlMs: number): Promise<{ fencingToken: number } | undefined>;
  renew(key: string, owner: string, ttlMs: number): Promise<boolean>;
  release(key: string, owner: string): Promise<void>;
}

/** In-process `LockStore`: mutual exclusion inside one Node process only. */
export class MemoryLockStore implements LockStore {
  readonly #time: TimeProvider;
  readonly #leases = new Map<string, { owner: string; expiresAt: number }>();
  // shortcut: one counter per key ever locked is kept forever (needed for monotonic tokens); fine unless keys are unbounded.
  readonly #tokens = new Map<string, number>();

  constructor(options: { timeProvider?: TimeProvider } = {}) {
    this.#time = options.timeProvider ?? systemTime;
  }

  async acquire(key: string, owner: string, ttlMs: number) {
    if (this.#live(key)) return undefined;
    const fencingToken = (this.#tokens.get(key) ?? 0) + 1;
    this.#tokens.set(key, fencingToken);
    this.#leases.set(key, { owner, expiresAt: this.#time.now() + ttlMs });
    return { fencingToken };
  }

  async renew(key: string, owner: string, ttlMs: number) {
    const lease = this.#live(key);
    if (lease?.owner !== owner) return false;
    lease.expiresAt = this.#time.now() + ttlMs;
    return true;
  }

  async release(key: string, owner: string) {
    if (this.#live(key)?.owner === owner) this.#leases.delete(key);
  }

  #live(key: string) {
    const lease = this.#leases.get(key);
    if (lease && lease.expiresAt <= this.#time.now()) this.#leases.delete(key);
    return this.#leases.get(key);
  }
}

/** The lock was held by someone else for longer than `wait`. */
export class LockNotAcquiredError extends CallRejectedError {
  name = 'LockNotAcquiredError';
  code = 'LOCK_NOT_ACQUIRED' as const;
  readonly key: string;
  constructor(key: string) {
    super(`Could not acquire the lock '${key}'.`);
    this.key = key;
  }
}

/** The lease could not be renewed (another owner took it, or the store kept failing until it expired). */
export class LockLostError extends Error {
  name = 'LockLostError';
  readonly code = 'LOCK_LOST' as const;
  readonly key: string;
  constructor(key: string) {
    super(`Lost the lock '${key}' while running.`);
    this.key = key;
  }
}

/** `context.properties` key holding the fencing token of the lock the execution runs under. */
export const LOCK_TOKEN = '@nodelibraries/resilience.lockToken';

/** Fencing token of the innermost lock the calling code runs under. Pass it to the protected resource so it can reject stale writers. */
export function getLockToken(context: ResilienceContext | undefined = getResilienceContext()): number | undefined {
  return context?.properties.get(LOCK_TOKEN) as number | undefined;
}

export interface OnLockLostArgs {
  readonly context: ResilienceContext;
  readonly key: string;
}

export interface LockOptions {
  name?: string;
  /** Lock key for this execution, e.g. `() => 'nightly-report'` or `(ctx) => \`order:${ctx.operationKey}\``. */
  key: (context: ResilienceContext) => string;
  /** Default: an in-memory store shared by every lock on the same clock (the pipeline's time provider). Use a distributed store to lock across processes. */
  store?: LockStore;
  /** Lease length. Renewed every ttl/3 while the callback runs. Default 30 s. */
  ttl?: Duration;
  /** How long to wait for a held lock before rejecting with `LockNotAcquiredError`. Default 0: fail fast. */
  wait?: Duration;
  /** Called when renewal fails; the callback's signal is then aborted with a `LockLostError`. */
  onLockLost?: (args: OnLockLostArgs) => MaybePromise<void>;
}

// One default store per clock, so leases expire on the pipeline's (possibly fake) time. Created on first use: no import side effects.
const sharedStores = new WeakMap<TimeProvider, MemoryLockStore>();

/** Runs the callback only while holding a lease on `key`: a mutex across executions (and processes, with a shared store). */
export class LockStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: LockOptions;
  readonly #source: StrategySource;
  readonly #store: LockStore;
  readonly #ttl: number;
  readonly #wait: number;

  constructor(options: LockOptions, source: StrategySource) {
    this.#ttl = toMs(options.ttl ?? 30_000, 'ttl');
    check(this.#ttl > 0, 'ttl must be > 0');
    this.#wait = toMs(options.wait ?? 0, 'wait');
    check(this.#wait >= 0, 'wait must be >= 0');
    this.#options = options;
    this.#source = source;
    const time = source.timeProvider;
    let store = options.store ?? sharedStores.get(time);
    if (!store) sharedStores.set(time, (store = new MemoryLockStore({ timeProvider: time })));
    this.#store = store;
  }

  async execute(next: Next<T>, context: ResilienceContext) {
    const time = this.#source.timeProvider;
    const store = this.#store;
    const key = this.#options.key(context);
    const owner = crypto.randomUUID();
    const deadline = time.now() + this.#wait;

    let lease: { fencingToken: number } | undefined;
    // shortcut: waiters poll with backoff instead of being notified, so a release is noticed up to 1 s late; fine for a lease lock.
    for (let pause = 50; !(lease = await store.acquire(key, owner, this.#ttl)); pause = Math.min(pause * 2, 1000)) {
      const left = deadline - time.now();
      if (left <= 0) {
        report(this.#source, 'OnLockRejected', context, { context, key });
        return fail<T>(stamp(new LockNotAcquiredError(key), this.#source));
      }
      try {
        await delay(Math.min(pause, left), context.signal, time);
      } catch (error) {
        return fail<T>(error);
      }
    }

    const lost = new LockLostError(key);
    const link = linkSignals(context.signal);
    let lostFlag = false;
    let expiresAt = time.now() + this.#ttl;
    let timer: unknown;
    let done = false;
    const renew = async () => {
      const sentAt = time.now();
      let renewed: boolean | 'error';
      try {
        renewed = await store.renew(key, owner, this.#ttl);
      } catch {
        renewed = 'error'; // a store hiccup: keep trying while the current lease is still valid
      }
      if (done) return;
      if (renewed === true) expiresAt = sentAt + this.#ttl;
      if (renewed === false || time.now() >= expiresAt) {
        lostFlag = true;
        link.controller.abort(lost);
        const args: OnLockLostArgs = { context, key };
        report(this.#source, 'OnLockLost', context, args);
        await this.#options.onLockLost?.(args);
        return;
      }
      schedule();
    };
    // A throwing onLockLost must not become an unhandled rejection.
    const schedule = () => (timer = time.setTimeout(() => void renew().catch(() => {}), this.#ttl / 3));
    schedule();

    const previous = context.properties.get(LOCK_TOKEN);
    context.properties.set(LOCK_TOKEN, lease.fencingToken);
    try {
      const outcome = await next({ ...context, signal: link.signal });
      return !outcome.ok && lostFlag ? fail<T>(lost) : outcome;
    } finally {
      link.dispose();
      done = true;
      time.clearTimeout(timer);
      if (previous === undefined) context.properties.delete(LOCK_TOKEN);
      else context.properties.set(LOCK_TOKEN, previous);
      // The lease expires anyway; a failing release must not replace the callback's outcome.
      await store.release(key, owner).catch(() => {});
    }
  }
}

export const lockStrategy = <T = any>(options: LockOptions): StrategyDefinition<T, LockNotAcquiredError | LockLostError> => ({ name: options.name ?? 'Lock', create: (s) => new LockStrategy<T>(options, s) });
