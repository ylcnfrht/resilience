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
  check,
  defaultFallbackShouldHandle,
  fail,
  ok,
  report,
  resolvePredicate,
  toMs,
} from './core.ts';

export interface CacheEntry {
  readonly value: unknown;
  /** Epoch ms (from the pipeline's time provider) when the value was stored. */
  readonly storedAt: number;
}

/** Async so remote stores (Redis, ...) can plug in. The strategy decides freshness from `storedAt`. */
export interface CacheStore {
  get(key: string): MaybePromise<CacheEntry | undefined>;
  /** `ttlMs` is how long the entry can still be useful (ttl + staleTtl): remote stores can expire it natively. */
  set(key: string, entry: CacheEntry, ttlMs: number): MaybePromise<void>;
  delete(key: string): MaybePromise<void>;
}

/** In-process store, bounded by `maxEntries` (least recently used goes first). */
export class MemoryCacheStore implements CacheStore {
  readonly #entries = new Map<string, CacheEntry>();
  readonly #maxEntries: number;

  constructor(options: { maxEntries?: number } = {}) {
    this.#maxEntries = options.maxEntries ?? 1000;
    check(Number.isInteger(this.#maxEntries) && this.#maxEntries >= 1, 'maxEntries must be an integer >= 1');
  }

  get size(): number {
    return this.#entries.size;
  }

  get(key: string): CacheEntry | undefined {
    const entry = this.#entries.get(key);
    if (entry) {
      // Map keeps insertion order: re-inserting marks the key as most recently used.
      this.#entries.delete(key);
      this.#entries.set(key, entry);
    }
    return entry;
  }

  // Ignores ttlMs: the strategy deletes entries past their stale window when it reads them, LRU bounds the rest.
  set(key: string, entry: CacheEntry): void {
    this.#entries.delete(key);
    this.#entries.set(key, entry);
    if (this.#entries.size > this.#maxEntries) this.#entries.delete(this.#entries.keys().next().value!);
  }

  delete(key: string): void {
    this.#entries.delete(key);
  }

  clear(): void {
    this.#entries.clear();
  }
}

export interface OnCacheHitArgs<T> {
  readonly context: ResilienceContext;
  readonly key: string;
  readonly value: T;
  /** ms since the value was stored. */
  readonly age: number;
}

export interface OnCacheMissArgs {
  readonly context: ResilienceContext;
  readonly key: string;
}

export interface OnStaleServedArgs<T> extends OnCacheHitArgs<T> {
  /** The failure the stale value hides. */
  readonly outcome: Outcome<T>;
}

export interface CacheOptions<T> {
  name?: string;
  /** Cache key for the execution; `undefined` bypasses the cache. Default: `context.operationKey`. */
  key?: (context: ResilienceContext) => string | undefined;
  /** How long a value is served without calling. `0` with `coalesce` gives request deduplication without caching. */
  ttl: Duration;
  /** Default: a new `MemoryCacheStore` per strategy. Share one to share the cache between pipelines. */
  store?: CacheStore;
  /** Default: successful results only. Failures are never cached. */
  shouldCache?: (args: OutcomeArgs<T>) => MaybePromise<boolean>;
  /** Stale-if-error window after `ttl`: when the call fails, a value younger than ttl + staleTtl is returned instead. Default 0. */
  staleTtl?: Duration;
  /** Failures a stale value may hide. Default: every failure except aborts and client errors (4xx except 408/429). */
  shouldServeStale?: ShouldHandle<OutcomeArgs<T>>;
  /** Concurrent misses for the same key share one call (single-flight). Default true. */
  coalesce?: boolean;
  onCacheHit?: (args: OnCacheHitArgs<T>) => MaybePromise<void>;
  onCacheMiss?: (args: OnCacheMissArgs) => MaybePromise<void>;
  onStaleServed?: (args: OnStaleServedArgs<T>) => MaybePromise<void>;
}

interface Flight<T> {
  readonly controller: AbortController;
  promise: Promise<Outcome<T>>;
  waiters: number;
}

/** Caches results by key, coalesces concurrent misses, and can serve a stale value when the call fails. */
export class CacheStrategy<T> implements ResilienceStrategy<T> {
  readonly #options: CacheOptions<T>;
  readonly #source: StrategySource;
  readonly #ttl: number;
  readonly #staleTtl: number;
  readonly #store: CacheStore;
  readonly #shouldServeStale;
  readonly #flights = new Map<string, Flight<T>>();

  constructor(options: CacheOptions<T>, source: StrategySource) {
    this.#ttl = toMs(options.ttl, 'ttl');
    this.#staleTtl = toMs(options.staleTtl ?? 0, 'staleTtl');
    check(this.#ttl >= 0, 'ttl must be >= 0');
    check(this.#staleTtl >= 0, 'staleTtl must be >= 0');
    this.#options = options;
    this.#source = source;
    this.#store = options.store ?? new MemoryCacheStore();
    this.#shouldServeStale = resolvePredicate(options.shouldServeStale, defaultFallbackShouldHandle);
  }

  async execute(next: Next<T>, context: ResilienceContext): Promise<Outcome<T>> {
    const key = this.#options.key ? this.#options.key(context) : context.operationKey;
    if (key === undefined) return next(context);
    if (context.signal.aborted) return fail(context.signal.reason);

    const now = this.#source.timeProvider.now();
    // A broken store must not break the call: treat it as a miss.
    let entry = await this.#safe(() => this.#store.get(key)).catch((error: unknown) => this.#storeFailed('get', key, error, context));
    const age = entry ? now - entry.storedAt : 0;
    if (entry && age < this.#ttl) {
      const args: OnCacheHitArgs<T> = { context, key, value: entry.value as T, age };
      report(this.#source, 'OnCacheHit', context, args);
      await this.#options.onCacheHit?.(args);
      return ok(entry.value as T);
    }
    if (entry && age >= this.#ttl + this.#staleTtl) {
      entry = undefined;
      await this.#safe(() => this.#store.delete(key)).catch((error: unknown) => this.#storeFailed('delete', key, error, context));
    }

    const missArgs: OnCacheMissArgs = { context, key };
    report(this.#source, 'OnCacheMiss', context, missArgs);
    await this.#options.onCacheMiss?.(missArgs);

    const outcome = this.#options.coalesce === false ? await this.#load(key, next, context) : await this.#join(key, next, context);
    if (!outcome.ok && entry && (await this.#shouldServeStale({ outcome, context }))) {
      const args: OnStaleServedArgs<T> = { context, key, value: entry.value as T, age, outcome };
      report(this.#source, 'OnStaleServed', context, args);
      await this.#options.onStaleServed?.(args);
      return ok(entry.value as T);
    }
    return outcome;
  }

  // A broken store never fails the call (it's treated as a miss), but it is reported so it doesn't go unnoticed.
  #storeFailed(operation: 'get' | 'set' | 'delete', key: string, error: unknown, context: ResilienceContext): undefined {
    report(this.#source, 'OnCacheStoreFailed', context, { context, key, operation, error });
    return undefined;
  }

  // Turns a synchronous store throw into a rejection, so it is handled like an async failure.
  #safe<R>(fn: () => R): Promise<Awaited<R>> {
    return new Promise<Awaited<R>>((resolve) => resolve(fn() as Awaited<R>));
  }

  async #load(key: string, next: Next<T>, context: ResilienceContext): Promise<Outcome<T>> {
    const outcome = await next(context);
    const keep = this.#ttl + this.#staleTtl;
    if (outcome.ok && keep > 0 && (await (this.#options.shouldCache?.({ outcome, context }) ?? true))) {
      const entry: CacheEntry = { value: outcome.result, storedAt: this.#source.timeProvider.now() };
      await this.#safe(() => this.#store.set(key, entry, keep)).catch((error: unknown) => this.#storeFailed('set', key, error, context));
    }
    return outcome;
  }

  /** Runs one shared call per key. Each caller can leave on its own signal; the call is aborted only when every caller has left. */
  #join(key: string, next: Next<T>, context: ResilienceContext): Promise<Outcome<T>> {
    let flight = this.#flights.get(key);
    if (!flight) {
      const controller = new AbortController();
      const created: Flight<T> = { controller, waiters: 0, promise: undefined! };
      // The shared call gets its own signal: the first caller's cancellation must not cancel it for the others.
      created.promise = this.#load(key, next, { ...context, signal: controller.signal }).finally(() => {
        if (this.#flights.get(key) === created) this.#flights.delete(key);
      });
      this.#flights.set(key, created);
      flight = created;
    }
    else report(this.#source, 'OnCoalesced', context, { context, key, waiters: flight.waiters + 1 });
    const f = flight;
    f.waiters++;
    const { signal } = context;
    return new Promise((resolve) => {
      const onAbort = () => {
        resolve(fail(signal.reason));
        if (--f.waiters > 0) return;
        // Detach before aborting so a newcomer starts a fresh call instead of joining a dying one.
        if (this.#flights.get(key) === f) this.#flights.delete(key);
        f.controller.abort(signal.reason);
      };
      signal.addEventListener('abort', onAbort, { once: true });
      // A rejected shared load (e.g. a throwing shouldCache) fails every caller instead of leaving them pending.
      void f.promise.then(resolve, (error: unknown) => resolve(fail(error))).finally(() => signal.removeEventListener('abort', onAbort));
    });
  }
}

export const cacheStrategy = <T = any>(options: CacheOptions<T>): StrategyDefinition<T> => ({ name: options.name ?? 'Cache', create: (s) => new CacheStrategy<T>(options, s) });

/** Single-flight only: concurrent executions with the same key share one call; nothing is cached. */
export const coalesceStrategy = <T = any>(options: Omit<CacheOptions<T>, 'ttl' | 'staleTtl' | 'store' | 'shouldCache'> = {}): StrategyDefinition<T> => ({
  name: options.name ?? 'Coalesce',
  create: (s) => new CacheStrategy<T>({ ...options, ttl: 0 }, s),
});

export interface StaleFallbackOptions<T> extends Pick<CacheOptions<T>, 'name' | 'key' | 'shouldServeStale' | 'onStaleServed'> {
  /** How old a last-good value may be and still be served. */
  maxAge: Duration;
  /** Bound on remembered keys (LRU). Default 1000. */
  maxEntries?: number;
  store?: CacheStore;
}

/** Remembers the last good value per key and serves it when a call fails; fresh calls always go through (no caching). */
export const staleFallbackStrategy = <T = any>(options: StaleFallbackOptions<T>): StrategyDefinition<T> => ({
  name: options.name ?? 'StaleFallback',
  create: (s) =>
    new CacheStrategy<T>(
      { ...options, ttl: 0, staleTtl: options.maxAge, coalesce: false, store: options.store ?? new MemoryCacheStore({ maxEntries: options.maxEntries ?? 1000 }) },
      s,
    ),
});

