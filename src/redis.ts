/**
 * Redis-backed state for sharing caches, locks, rate limits and circuit state between processes.
 *
 * No Redis library is imported: pass any client with ioredis' `eval(script, numKeys, ...keysAndArgs)` (ioredis works as is;
 * wrap node-redis v4+ with `fromNodeRedis`). Every operation is one Lua script, so each is atomic on the Redis server.
 *
 * Redis Cluster: every script touches keys of a single hash slot only. The lock's lease and fencing counter share the
 * hash tag `{lock:<key>}`; so don't put `{`/`}` in `prefix`. Needs Redis >= 5 (scripts call `TIME` before writing).
 */
import { type CacheEntry, type CacheStore } from './cache.ts';
import { type CircuitSnapshot } from './circuit-breaker.ts';
import { type CircuitStateStore } from './circuit-extensions.ts';
import { type Duration, type StrategySource, check, toMs } from './core.ts';
import { type LockStore } from './lock.ts';
import { type AcquireResult, type PermitRateLimiterOptions, type RateLimiterAlgorithm, RateLimiterStrategy } from './window-rate-limiter.ts';

/** The one command used: ioredis' `eval` signature. */
export interface RedisClient {
  eval(script: string, numKeys: number, ...keysAndArgs: Array<string | number>): Promise<unknown>;
}

/** Adapts a node-redis v4+ client (`client.eval(script, { keys, arguments })`) to `RedisClient`. */
export function fromNodeRedis(client: { eval(script: string, options: { keys: string[]; arguments: string[] }): Promise<unknown> }): RedisClient {
  return {
    eval: (script, numKeys, ...rest) => client.eval(script, { keys: rest.slice(0, numKeys).map(String), arguments: rest.slice(numKeys).map(String) }),
  };
}

export interface RedisStoreOptions {
  client: RedisClient;
  /** Prepended to every key. Default `'rp:'`. */
  prefix?: string;
}

// shortcut: sends the script text on every call (EVAL); switch to EVALSHA with a NOSCRIPT fallback if bandwidth matters.
const run = (client: RedisClient, script: string, keys: string[], ...args: Array<string | number>) => client.eval(script, keys.length, ...keys, ...args);

const GET = `return redis.call('GET', KEYS[1])`;
const parse = <V>(raw: unknown): V | undefined => (raw == null ? undefined : (JSON.parse(String(raw)) as V));

/** `CacheStore` over Redis: entries are JSON (so values must be JSON-serializable) and expire natively after `ttlMs`. */
export class RedisCacheStore implements CacheStore {
  readonly #client: RedisClient;
  readonly #prefix: string;

  constructor(options: RedisStoreOptions) {
    this.#client = options.client;
    this.#prefix = options.prefix ?? 'rp:';
  }

  async get(key: string): Promise<CacheEntry | undefined> {
    return parse<CacheEntry>(await run(this.#client, GET, [this.#prefix + key]));
  }

  async set(key: string, entry: CacheEntry, ttlMs: number): Promise<void> {
    await run(this.#client, `redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])`, [this.#prefix + key], JSON.stringify(entry), Math.max(1, Math.ceil(ttlMs)));
  }

  async delete(key: string): Promise<void> {
    await run(this.#client, `return redis.call('DEL', KEYS[1])`, [this.#prefix + key]);
  }
}

// The lease is taken and the fencing counter bumped in one script, so tokens follow lease order exactly.
// SET NX on a key that exists returns nil, which Lua sees as false.
const LOCK_ACQUIRE = `
if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return false end
return redis.call('INCR', KEYS[2])`;
// Compare-and-act in one script: checking the owner from the client and then acting would race with expiry + takeover.
const LOCK_RENEW = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2])`;
const LOCK_RELEASE = `
if redis.call('GET', KEYS[1]) == ARGV[1] then redis.call('DEL', KEYS[1]) end
return 0`;

/**
 * `LockStore` on a single Redis primary (no Redlock): lease = `SET NX PX`, fencing token = `INCR` of a per-key counter.
 * The counter never expires (it must only grow), so one small key per lock key is kept forever.
 *
 * Caveats: Redis replication is async, so after a failover a lease (or counter increments) can be lost: two holders,
 * or a repeated fencing token. For tokens to survive restarts, run with AOF (`appendfsync everysec` or `always`).
 * The protected resource should still reject writes whose token is below the highest it has seen.
 */
export class RedisLockStore implements LockStore {
  readonly #client: RedisClient;
  readonly #prefix: string;

  constructor(options: RedisStoreOptions) {
    this.#client = options.client;
    this.#prefix = options.prefix ?? 'rp:';
  }

  // Same hash tag => same cluster slot, so both keys can be used in one script. `lock:` keeps the tag non-empty for key ''.
  #keys(key: string) {
    const lease = `${this.#prefix}{lock:${key}}`;
    return [lease, `${lease}:fence`];
  }

  async acquire(key: string, owner: string, ttlMs: number) {
    const token = await run(this.#client, LOCK_ACQUIRE, this.#keys(key), owner, Math.max(1, Math.ceil(ttlMs)));
    return token == null ? undefined : { fencingToken: Number(token) };
  }

  async renew(key: string, owner: string, ttlMs: number) {
    return Number(await run(this.#client, LOCK_RENEW, this.#keys(key).slice(0, 1), owner, Math.max(1, Math.ceil(ttlMs)))) === 1;
  }

  async release(key: string, owner: string) {
    await run(this.#client, LOCK_RELEASE, this.#keys(key).slice(0, 1), owner);
  }
}

// Same segmented sliding window as the in-memory WindowCounter, as a hash { segment index -> permits }.
// Uses the Redis server clock (TIME), so instances with skewed clocks still agree on the window.
// Returns 0 when admitted, else ms until enough old segments leave the window (-1: permits can never fit).
const RATE_LIMIT = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local limit, window, n, permits = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local seg = window / n
local current = math.floor(now / seg)
local raw = redis.call('HGETALL', KEYS[1])
local counts, used = {}, 0
for i = 1, #raw, 2 do
  local s = tonumber(raw[i])
  if s <= current - n then
    redis.call('HDEL', KEYS[1], raw[i])
  else
    counts[s] = tonumber(raw[i + 1])
    used = used + counts[s]
  end
end
if used + permits <= limit then
  redis.call('HINCRBYFLOAT', KEYS[1], current, permits)
  redis.call('PEXPIRE', KEYS[1], math.ceil(window))
  return 0
end
for s = current - n + 1, current do
  used = used - (counts[s] or 0)
  if used + permits <= limit then return math.max(1, math.ceil((s + n) * seg - now)) end
end
return -1`;

export interface RedisRateLimiterOptions extends RedisStoreOptions {
  /** Shared limit identity: every instance using the same key (and prefix) shares one budget. */
  key: string;
  /** Permits granted per window. */
  permitLimit: number;
  window: Duration;
  /** Buckets the window slides by; 1 = fixed window aligned to the epoch. Default 10. */
  segmentsPerWindow?: number;
}

/**
 * Sliding-window `RateLimiterAlgorithm` shared through Redis. Run it with `RateLimiterStrategy`:
 *
 * ```ts
 * builder.addStrategy((s) => new RateLimiterStrategy(new RedisRateLimiter({ client, key: 'api', permitLimit: 100, window: '1m' }), { queueTimeout: '2s' }, s));
 * ```
 *
 * The `now` passed to `acquire` is ignored: the window runs on the Redis server's clock.
 */
export class RedisRateLimiter implements RateLimiterAlgorithm {
  readonly permitLimit: number;
  readonly #client: RedisClient;
  readonly #key: string;
  readonly #window: number;
  readonly #segments: number;

  constructor(options: RedisRateLimiterOptions) {
    check(Number.isInteger(options.permitLimit) && options.permitLimit >= 1, 'permitLimit must be an integer >= 1');
    this.#window = toMs(options.window, 'window');
    check(this.#window > 0, 'window must be > 0');
    this.#segments = options.segmentsPerWindow ?? 10;
    check(Number.isInteger(this.#segments) && this.#segments >= 1, 'segmentsPerWindow must be an integer >= 1');
    this.permitLimit = options.permitLimit;
    this.#client = options.client;
    this.#key = `${options.prefix ?? 'rp:'}rl:${options.key}`;
  }

  async acquire(permits: number): Promise<AcquireResult> {
    const retryAfter = Number(await run(this.#client, RATE_LIMIT, [this.#key], this.permitLimit, this.#window, this.#segments, permits));
    if (retryAfter < 0) throw new RangeError(`permits (${permits}) exceed permitLimit (${this.permitLimit})`);
    return retryAfter === 0 ? { ok: true } : { ok: false, retryAfter };
  }
}

/**
 * `limiter` factory for `PartitionedRateLimiterStrategy`: one Redis-backed limit per partition key, shared by every instance.
 * Evicting an idle partition only drops the local object; its count lives on in Redis until the window passes.
 *
 * ```ts
 * builder.addPartitionedRateLimiter({
 *   partitionKey: (ctx) => ctx.properties.get('userId') as string,
 *   limiter: redisPartitionLimiter({ client, permitLimit: 100, window: '1m', queueTimeout: '1s' }),
 * });
 * // same as: limiter: (key, s) => new RateLimiterStrategy(new RedisRateLimiter({ client, key, permitLimit, window }), opts, s)
 * ```
 */
export function redisPartitionLimiter<T = any>(options: Omit<RedisRateLimiterOptions, 'key'> & PermitRateLimiterOptions) {
  return (key: string, source: StrategySource): RateLimiterStrategy<T> => new RateLimiterStrategy<T>(new RedisRateLimiter({ ...options, key }), options, source);
}

// Compare and write in one script, so a slow instance can't overwrite a newer state with its older one.
const CIRCUIT_SET = `
local current = redis.call('GET', KEYS[1])
if current and (tonumber(cjson.decode(current).updatedAt) or 0) >= tonumber(ARGV[2]) then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
return 1`;

/** `CircuitStateStore` over Redis: a write only lands if its `updatedAt` is newer than the stored one. Keys don't expire. */
export class RedisCircuitStateStore implements CircuitStateStore {
  readonly #client: RedisClient;
  readonly #prefix: string;

  constructor(options: RedisStoreOptions) {
    this.#client = options.client;
    this.#prefix = options.prefix ?? 'rp:';
  }

  async get(key: string): Promise<CircuitSnapshot | undefined> {
    return parse<CircuitSnapshot>(await run(this.#client, GET, [`${this.#prefix}cb:${key}`]));
  }

  async set(key: string, snapshot: CircuitSnapshot): Promise<void> {
    await run(this.#client, CIRCUIT_SET, [`${this.#prefix}cb:${key}`], JSON.stringify(snapshot), snapshot.updatedAt ?? 0);
  }
}
