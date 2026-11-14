import assert from 'node:assert/strict';
import { connect } from 'node:net';
import { after, test as nodeTest } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { Redis } from 'ioredis';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { CircuitBreakerManualControl } from '../src/circuit-breaker.ts';
import { sharedCircuitState } from '../src/circuit-extensions.ts';
import { BrokenCircuitError, RateLimiterRejectedError } from '../src/core.ts';
import { RedisCacheStore, RedisCircuitStateStore, RedisLockStore, RedisRateLimiter, fromNodeRedis, redisPartitionLimiter } from '../src/redis.ts';

// Probe once: without a Redis on 127.0.0.1:6379 these tests skip instead of failing.
const redisUp = await new Promise<boolean>((resolve) => {
  const socket = connect(6379, '127.0.0.1');
  const done = (up: boolean) => (socket.destroy(), resolve(up));
  socket.setTimeout(500, () => done(false)).once('connect', () => done(true)).once('error', () => done(false));
});
const skip = !redisUp && 'no Redis listening on 127.0.0.1:6379';
const test = (name: string, fn: () => Promise<void>) => nodeTest(name, { skip }, fn);

// Two clients play two app instances; a unique prefix per run keeps runs (and parallel runs) apart.
const a = (redisUp ? new Redis({ host: '127.0.0.1', port: 6379, lazyConnect: false }) : undefined) as Redis;
const b = (redisUp ? new Redis({ host: '127.0.0.1', port: 6379, lazyConnect: false }) : undefined) as Redis;
const prefix = `rp-test:${process.pid}:${Date.now()}:`;

after(async () => {
  if (!redisUp) return;
  const keys = await a.keys(`${prefix}*`);
  if (keys.length) await a.del(...keys);
  await Promise.all([a.quit(), b.quit()]);
});

async function until(condition: () => Promise<boolean>, timeoutMs = 3000) {
  const end = Date.now() + timeoutMs;
  while (!(await condition())) {
    if (Date.now() > end) throw new Error('condition not met in time');
    await sleep(20);
  }
}

test('redis cache: roundtrip, ttl, delete, shared between pipelines', async () => {
  const store = new RedisCacheStore({ client: a, prefix });
  await store.set('k', { value: { n: 1, s: 'x' }, storedAt: 123 }, 60_000);
  assert.deepEqual(await new RedisCacheStore({ client: b, prefix }).get('k'), { value: { n: 1, s: 'x' }, storedAt: 123 });
  await store.delete('k');
  assert.equal(await store.get('k'), undefined);

  // Native expiry: the key carries a PX ttl (checked without racing the clock), and a 1 ms ttl does expire.
  await store.set('ttl', { value: 1, storedAt: 1 }, 5000);
  const pttl = await a.pttl(`${prefix}ttl`);
  assert.ok(pttl > 0 && pttl <= 5000, `pttl ${pttl}`);
  await store.set('short', { value: 1, storedAt: 1 }, 0.2); // rounds up to Redis' minimum of 1 ms
  await until(async () => (await store.get('short')) === undefined);

  // node-redis shape: eval(script, { keys, arguments }).
  const nodeRedisLike = fromNodeRedis({ eval: (script, { keys, arguments: args }) => b.eval(script, keys.length, ...keys, ...args) });
  await new RedisCacheStore({ client: nodeRedisLike, prefix }).set('nr', { value: 'v', storedAt: 2 }, 60_000);
  assert.deepEqual(await store.get('nr'), { value: 'v', storedAt: 2 });

  let calls = 0;
  const make = (client: Redis) => new ResiliencePipelineBuilder().addCache({ ttl: '1m', key: () => 'user:1', store: new RedisCacheStore({ client, prefix }) }).build();
  assert.equal(await make(a).execute(() => ++calls), 1);
  assert.equal(await make(b).execute(() => ++calls), 1, 'second instance hits the shared cache');
  assert.equal(calls, 1);
});

test('redis lock: mutual exclusion across instances, increasing fencing tokens, ownership', async () => {
  const sa = new RedisLockStore({ client: a, prefix });
  const sb = new RedisLockStore({ client: b, prefix });
  const first = await sa.acquire('job', 'A', 10_000);
  assert.ok(first);
  assert.equal(await sb.acquire('job', 'B', 10_000), undefined);
  assert.equal(await sb.renew('job', 'B', 10_000), false);
  await sb.release('job', 'B'); // not the owner: no effect
  assert.equal(await sb.acquire('job', 'B', 10_000), undefined);
  assert.equal(await sa.renew('job', 'A', 10_000), true);
  await sa.release('job', 'A');
  const second = await sb.acquire('job', 'B', 10_000);
  assert.ok(second && second.fencingToken > first.fencingToken);
  await sb.release('job', 'B');

  // After expiry (no release) another owner takes over with a higher token, and the old owner can't renew.
  const third = await sa.acquire('job', 'A', 40);
  let fourth: Awaited<ReturnType<typeof sb.acquire>>;
  await until(async () => (fourth = await sb.acquire('job', 'B', 10_000)) !== undefined);
  assert.ok(third && fourth! && fourth.fencingToken > third.fencingToken);
  assert.equal(await sa.renew('job', 'A', 10_000), false);
  await sb.release('job', 'B');

  // LockStrategy on two "instances": never more than one callback at a time. Waiters poll with a 50 ms..1 s backoff;
  // this clock runs those pauses after 1 ms, so the test checks exclusion without waiting for the backoff.
  const fastTime = { now: () => Date.now(), setTimeout: (cb: () => void) => setTimeout(cb, 1), clearTimeout: (h: unknown) => clearTimeout(h as NodeJS.Timeout) };
  let running = 0;
  let maxRunning = 0;
  const pipelines = [a, b].map((client) => new ResiliencePipelineBuilder({ timeProvider: fastTime }).addLock({ key: () => 'report', store: new RedisLockStore({ client, prefix }), wait: '5s', ttl: '5s' }).build());
  await Promise.all(
    Array.from({ length: 6 }, (_, i) =>
      pipelines[i % 2]!.execute(async () => {
        maxRunning = Math.max(maxRunning, ++running);
        await sleep(15);
        running--;
      }),
    ),
  );
  assert.equal(maxRunning, 1);
});

test('redis rate limiter: two instances share one limit; retryAfter is accurate', async () => {
  const ra = new RedisRateLimiter({ client: a, prefix, key: 'api', permitLimit: 3, window: '1m' });
  const rb = new RedisRateLimiter({ client: b, prefix, key: 'api', permitLimit: 3, window: '1m' });
  assert.deepEqual(await ra.acquire(1), { ok: true });
  assert.deepEqual(await rb.acquire(2), { ok: true });
  const rejected = await ra.acquire(1);
  assert.equal(rejected.ok, false);
  // Sliding window of 10 x 6 s segments: the oldest permits leave between 54 s and 60 s from now.
  assert.ok(!rejected.ok && rejected.retryAfter > 50_000 && rejected.retryAfter <= 60_000, JSON.stringify(rejected));

  const short = new RedisRateLimiter({ client: a, prefix, key: 'short', permitLimit: 2, window: 300, segmentsPerWindow: 3 });
  assert.equal((await short.acquire(2)).ok, true);
  const wait = await new RedisRateLimiter({ client: b, prefix, key: 'short', permitLimit: 2, window: 300, segmentsPerWindow: 3 }).acquire(1);
  assert.ok(!wait.ok && wait.retryAfter > 0 && wait.retryAfter <= 300);
  const start = Date.now();
  await sleep(wait.retryAfter);
  await until(async () => (await short.acquire(1)).ok, 1000); // admitted once retryAfter has passed (Redis' clock, not ours)
  assert.ok(Date.now() - start < 1000);
  await assert.rejects(Promise.resolve(short.acquire(3)), /permits \(3\) exceed permitLimit \(2\)/);
});

test('redis rate limiter: per-key limits through PartitionedRateLimiterStrategy', async () => {
  const make = (client: Redis) =>
    new ResiliencePipelineBuilder<string>()
      .addPartitionedRateLimiter({ partitionKey: (ctx) => ctx.operationKey!, limiter: redisPartitionLimiter({ client, prefix, permitLimit: 2, window: '1m' }) })
      .build();
  const [p1, p2] = [make(a), make(b)];
  assert.equal(await p1.execute(() => 'ok', { operationKey: 'u1' }), 'ok');
  assert.equal(await p2.execute(() => 'ok', { operationKey: 'u1' }), 'ok');
  await assert.rejects(p1.execute(() => 'ok', { operationKey: 'u1' }), (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter! > 0);
  assert.equal(await p2.execute(() => 'ok', { operationKey: 'u2' }), 'ok', 'other keys keep their own budget');
});

test('redis circuit store: only a newer updatedAt overwrites', async () => {
  const store = new RedisCircuitStateStore({ client: a, prefix });
  const snap = (state: 'open' | 'closed', updatedAt: number) => ({ state, openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, updatedAt });
  assert.equal(await store.get('svc'), undefined);
  await store.set('svc', snap('open', 10));
  await new RedisCircuitStateStore({ client: b, prefix }).set('svc', snap('closed', 5));
  assert.deepEqual(await store.get('svc'), snap('open', 10), 'stale write ignored');
  await store.set('svc', snap('closed', 10));
  assert.equal((await store.get('svc'))?.state, 'open', 'equal updatedAt does not overwrite');
  await store.set('svc', snap('closed', 11));
  assert.deepEqual(await store.get('svc'), snap('closed', 11));
});

test('redis circuit store: two pipelines share one breaker state (end to end, real time)', async () => {
  const control = new CircuitBreakerManualControl();
  const make = (client: Redis, manualControl?: CircuitBreakerManualControl) =>
    new ResiliencePipelineBuilder()
      .addCircuitBreaker({ consecutiveFailures: 1, breakDuration: '30s', manualControl, extensions: [sharedCircuitState({ store: new RedisCircuitStateStore({ client, prefix }), key: 'payments', syncInterval: 25 })] })
      .build();
  const p1 = make(a, control);
  const p2 = make(b);
  try {
    assert.equal(await p2.execute(() => 'up'), 'up');
    await assert.rejects(p1.execute(() => Promise.reject(new Error('down'))), /down/);
    // Instance 2 never saw a failure but picks up the open state from Redis.
    await until(async () => (await p2.execute(() => 'up').catch((e: unknown) => e)) instanceof BrokenCircuitError);

    await control.close();
    await until(async () => (await p2.execute(() => 'up').catch(() => 'rejected')) === 'up');
  } finally {
    p1.dispose();
    p2.dispose();
  }
});
