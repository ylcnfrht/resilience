// Real-client coverage for src/redis.ts: node-redis (v4+) against the local Redis on 6379, and ioredis against a
// Redis Cluster when REDIS_CLUSTER is set, e.g. REDIS_CLUSTER=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103.
import assert from 'node:assert/strict';
import { connect } from 'node:net';
import { test } from 'node:test';
import { Cluster } from 'ioredis';
import { createClient } from 'redis';
import { CircuitBreakerManualControl } from '../src/circuit-breaker.ts';
import { sharedCircuitState } from '../src/circuit-extensions.ts';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { RateLimiterStrategy } from '../src/window-rate-limiter.ts';
import { BrokenCircuitError, RateLimiterRejectedError } from '../src/core.ts';
import { type RedisClient, RedisCacheStore, RedisCircuitStateStore, RedisLockStore, RedisRateLimiter, fromNodeRedis } from '../src/redis.ts';

const httpFail = (): never => {
  throw Object.assign(new Error('HTTP 503'), { status: 503 });
};

// The same scenario on every client: cache roundtrip, lock exclusivity + fencing, a shared rate limit, circuit store.
async function exercise(client: RedisClient, prefix: string) {
  const cache = new RedisCacheStore({ client, prefix });
  await cache.set('user:1', { value: { name: 'Ada' }, storedAt: 1 }, 60_000);
  assert.deepEqual(await cache.get('user:1'), { value: { name: 'Ada' }, storedAt: 1 });
  await cache.delete('user:1');
  assert.equal(await cache.get('user:1'), undefined);

  const locks = new RedisLockStore({ client, prefix });
  const first = await locks.acquire('job', 'a', 5000);
  assert.ok(first);
  assert.equal(await locks.acquire('job', 'b', 5000), undefined, 'held by a');
  assert.equal(await locks.renew('job', 'b', 5000), false, 'b is not the owner');
  await locks.release('job', 'a');
  const second = await locks.acquire('job', 'b', 5000);
  assert.ok(second && second.fencingToken > first.fencingToken, 'fencing token grows');
  await locks.release('job', 'b');
  // The empty key once produced an empty hash tag, splitting lease and fence keys across Cluster slots (CROSSSLOT).
  assert.ok(await locks.acquire('', 'a', 5000), 'empty key works');
  await locks.release('', 'a');

  const limiter = (key: string) => new RedisRateLimiter({ client, prefix, key, permitLimit: 2, window: '1m' });
  const limited = [limiter('api'), limiter('api')].map((algorithm) =>
    new ResiliencePipelineBuilder().addStrategy((s) => new RateLimiterStrategy(algorithm, {}, s)).build(),
  );
  await limited[0]!.execute(() => 1);
  await limited[1]!.execute(() => 1);
  await assert.rejects(limited[0]!.execute(() => 1), RateLimiterRejectedError, 'the 2-per-minute limit is shared');

  const store = new RedisCircuitStateStore({ client, prefix });
  const manualControl = new CircuitBreakerManualControl();
  const [a, b] = [0, 1].map((i) =>
    new ResiliencePipelineBuilder()
      .addCircuitBreaker({ consecutiveFailures: 1, breakDuration: '30s', ...(i === 0 && { manualControl }), extensions: [sharedCircuitState({ store, key: 'payments', syncInterval: 20 })] })
      .build(),
  );
  await assert.rejects(a!.execute(httpFail));
  // Instance b adopts the open circuit on one of its 20 ms syncs; poll for it instead of sleeping a fixed time.
  for (const end = Date.now() + 3000; !((await b!.execute(() => 1).catch((e: unknown) => e)) instanceof BrokenCircuitError); ) {
    assert.ok(Date.now() < end, 'instance b adopted the open circuit');
    await new Promise((r) => setTimeout(r, 10));
  }
  a!.dispose();
  b!.dispose();
}

// Probe once: without a Redis on 127.0.0.1:6379 these tests skip instead of failing.
const redisUp = await new Promise<boolean>((resolve) => {
  const socket = connect(6379, '127.0.0.1');
  const done = (up: boolean) => (socket.destroy(), resolve(up));
  socket.setTimeout(500, () => done(false)).once('connect', () => done(true)).once('error', () => done(false));
});
const skip = !redisUp && 'no Redis listening on 127.0.0.1:6379';

test('node-redis client (fromNodeRedis) against a real Redis', { skip }, async () => {
  const client = createClient({ url: 'redis://127.0.0.1:6379' });
  await client.connect();
  const prefix = `rp-test-noderedis-${process.pid}-${Date.now()}:`;
  try {
    await exercise(fromNodeRedis(client), prefix);
  } finally {
    for await (const keys of client.scanIterator({ MATCH: `${prefix}*` })) if (keys.length) await client.del(keys);
    await client.quit();
  }
});

const clusterNodes = process.env.REDIS_CLUSTER?.split(',').map((n) => ({ host: n.split(':')[0]!, port: Number(n.split(':')[1]) }));

test('ioredis Cluster: every script stays within one hash slot', { skip: !clusterNodes && 'set REDIS_CLUSTER=host:port,… to run' }, async () => {
  const cluster = new Cluster(clusterNodes!);
  const prefix = `rp-test-cluster-${process.pid}-${Date.now()}:`;
  try {
    await exercise(cluster as unknown as RedisClient, prefix);
  } finally {
    await Promise.all(
      cluster.nodes('master').map(async (node) => {
        const keys = await node.keys(`${prefix}*`);
        for (const key of keys) await cluster.del(key);
      }),
    );
    cluster.disconnect();
  }
});
