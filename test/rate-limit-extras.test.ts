import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import { BulkheadRejectedError, RateLimiterRejectedError, ResiliencePipelineBuilder, onTelemetry } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';
import { ConcurrencyLimiterStrategy, type ConcurrencyLimiterOptions, TokenBucketStrategy } from '../src/rate-limiter.ts';
import {
  type FixedWindowRateLimiterOptions,
  type RateLimiterAlgorithm,
  FixedWindowRateLimiterStrategy,
  PartitionedRateLimiterStrategy,
  RateLimiterControl,
  RateLimiterDeniedError,
  RateLimiterStrategy,
  WindowCounter,
  getRateLimitInfo,
} from '../src/window-rate-limiter.ts';
import { honoResilience, resilienceMiddleware } from '../src/http.ts';

const props = (entries: Record<string, unknown>) => ({ properties: new Map(Object.entries(entries)) });
const fixed = (time: FakeTimeProvider, o: FixedWindowRateLimiterOptions) =>
  new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new FixedWindowRateLimiterStrategy(o, s)).build();

test('FIFO: a heavy waiter is not overtaken by lighter ones that arrive later', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 10, window: '1s', queueTimeout: '10s', permits: (c) => c.properties.get('w') as number });
  await p.execute(() => 0, props({ w: 10 }));
  const order: string[] = [];
  const heavy = p.execute(() => order.push('heavy'), props({ w: 10 }));
  const light = p.execute(() => order.push('light'), props({ w: 1 }));
  await time.advance('1s');
  assert.deepEqual(order, ['heavy']);
  await time.advance('1s');
  await Promise.all([heavy, light]);
  assert.deepEqual(order, ['heavy', 'light']);
});

test('FIFO: queueLimit rejects beyond the queue; a waiter past its queueTimeout is rejected when it reaches the head', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 1, window: '1s', queueTimeout: '1500ms', queueLimit: 2 });
  await p.execute(() => 0);
  const a = p.execute(() => 'a');
  const b = p.execute(() => 'b');
  await assert.rejects(p.execute(() => 'c'), /queue is full/);
  const bRejected = assert.rejects(b, RateLimiterRejectedError); // its turn comes at 2s > 1.5s
  await time.advance('1s');
  assert.equal(await a, 'a');
  await bRejected;
  assert.equal(time.pendingTimers, 0);
});

test('token bucket: FIFO with queueTimeout, and rate-limit info after success', async () => {
  const time = new FakeTimeProvider();
  let strategy!: TokenBucketStrategy<unknown>;
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addStrategy((s) => (strategy = new TokenBucketStrategy({ tokenLimit: 3, replenishmentPeriod: '1s', tokensPerPeriod: 1, queueTimeout: '5s' }, s)))
    .build();
  const infos: unknown[] = [];
  for (let i = 0; i < 3; i++) await p.execute((ctx) => void infos.push(getRateLimitInfo(ctx)));
  assert.deepEqual(infos.at(-1), { limit: 3, remaining: 0, resetAfter: 3000 });
  const order: number[] = [];
  const all = [1, 2, 3].map((n) => p.execute(() => order.push(n)));
  await time.advance(0);
  assert.equal(strategy.queued, 3);
  await time.advance('1s');
  assert.deepEqual(order, [1]);
  await time.advance('2s');
  await Promise.all(all);
  assert.deepEqual(order, [1, 2, 3]);
});

test('window info: limit, remaining, resetAfter; rejection sets remaining 0', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 2, window: '1s' });
  await time.advance(200);
  const ctx = props({});
  await p.execute(() => 0, ctx);
  assert.deepEqual(getRateLimitInfo(ctx), { limit: 2, remaining: 1, resetAfter: 800 });
  await p.execute(() => 0);
  const rejected = props({});
  await assert.rejects(p.execute(() => 0, rejected));
  assert.deepEqual(getRateLimitInfo(rejected), { limit: 2, remaining: 0, resetAfter: 800 });
});

test('blockDuration: exceeding the limit blocks for that long, without extending on rejections during the block', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 1, window: '1s', blockDuration: '1m' });
  await p.execute(() => 0);
  await assert.rejects(p.execute(() => 0), (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter === 60_000);
  await time.advance('30s');
  await assert.rejects(p.execute(() => 0), (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter === 30_000);
  await time.advance('30s');
  await p.execute(() => 0);
});

test('control: penalty, reward, block, unblock, delete on a single limiter', async () => {
  const time = new FakeTimeProvider();
  const control = new RateLimiterControl();
  const p = fixed(time, { permitLimit: 3, window: '1m', control });
  await control.penalty(3);
  await assert.rejects(p.execute(() => 0), RateLimiterRejectedError);
  await control.reward(1);
  await p.execute(() => 0);
  control.block('10s');
  await control.reward(Infinity);
  await assert.rejects(p.execute(() => 0), (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter === 10_000);
  control.unblock();
  await p.execute(() => 0);
  control.block('10s');
  await control.delete();
  await p.execute(() => 0);
});

test('partitioned: control per key, keys() and delete(key)', async () => {
  const time = new FakeTimeProvider();
  const control = new RateLimiterControl();
  let strategy!: PartitionedRateLimiterStrategy<unknown>;
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addStrategy(
      (s) =>
        (strategy = new PartitionedRateLimiterStrategy(
          { partitionKey: (c) => c.properties.get('user') as string, limiter: (_k, src) => new FixedWindowRateLimiterStrategy({ permitLimit: 2, window: '1m' }, src), control },
          s,
        )),
    )
    .build();
  control.block('1h', 'mallory'); // creates the partition
  await assert.rejects(p.execute(() => 0, props({ user: 'mallory' })), RateLimiterRejectedError);
  await p.execute(() => 0, props({ user: 'alice' }));
  await control.penalty(1, 'alice');
  await assert.rejects(p.execute(() => 0, props({ user: 'alice' })), RateLimiterRejectedError);
  assert.deepEqual(strategy.keys(), ['mallory', 'alice']);
  assert.equal(strategy.delete('mallory'), true);
  await p.execute(() => 0, props({ user: 'mallory' }));
  await control.delete('alice');
  await p.execute(() => 0, props({ user: 'alice' }));
  control.unblock('nobody');
  assert.deepEqual(strategy.keys(), ['mallory', 'alice'], 'unblock does not create partitions');
});

test('allow bypasses, deny rejects with RateLimiterDeniedError (deny wins)', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, {
    permitLimit: 1,
    window: '1m',
    allow: (c) => c.properties.get('ip') === 'internal',
    deny: (c) => c.properties.get('ip') === 'bad',
  });
  for (let i = 0; i < 3; i++) await p.execute(() => 0, props({ ip: 'internal' }));
  await p.execute(() => 0, props({ ip: 'x' }));
  await assert.rejects(p.execute(() => 0, props({ ip: 'bad' })), (e: unknown) => e instanceof RateLimiterDeniedError && e instanceof RateLimiterRejectedError && e.code === 'RATE_LIMIT_DENIED');
});

test('insurance: a throwing or slow primary falls back to the in-memory algorithm and reports it', async () => {
  const time = new FakeTimeProvider();
  const events: unknown[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnRateLimiterInsuranceUsed' && events.push((e.args as { error: Error }).error.name)));
  let mode: 'throw' | 'hang' | 'ok' = 'throw';
  const remote: RateLimiterAlgorithm = {
    permitLimit: 100,
    acquire: () => (mode === 'throw' ? Promise.reject(new Error('redis down')) : mode === 'hang' ? new Promise(() => {}) : { ok: true }),
  };
  try {
    const p = new ResiliencePipelineBuilder({ timeProvider: time })
      .addStrategy((s) => new RateLimiterStrategy(remote, { insurance: { algorithm: new WindowCounter(1, 60_000, 1, time.now()), timeout: '50ms' } }, s))
      .build();
    await p.execute(() => 0);
    await assert.rejects(p.execute(() => 0), RateLimiterRejectedError); // insurance allows 1
    mode = 'hang';
    const hung = assert.rejects(p.execute(() => 0), RateLimiterRejectedError);
    await time.advance(50);
    await hung;
    mode = 'ok';
    await p.execute(() => 0);
    assert.deepEqual(events, ['Error', 'Error', 'TimeoutRejectedError']);
    assert.equal(time.pendingTimers, 0);
  } finally {
    off();
  }
});

const bulkhead = (time: FakeTimeProvider, o: ConcurrencyLimiterOptions) =>
  new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new ConcurrencyLimiterStrategy(o, s)).build();

test('bulkhead priority: lower runs sooner, FIFO within a priority', async () => {
  const time = new FakeTimeProvider();
  const p = bulkhead(time, { permitLimit: 1, queueLimit: 10, priority: (c) => c.properties.get('p') as number });
  let release!: () => void;
  const first = p.execute(() => new Promise<void>((r) => (release = r)));
  const order: string[] = [];
  const queued = [
    ['low1', 5],
    ['high1', 1],
    ['low2', 5],
    ['high2', 1],
  ].map(([name, pr]) => p.execute(() => void order.push(name as string), props({ p: pr })));
  await time.advance(0);
  release();
  await Promise.all([first, ...queued]);
  assert.deepEqual(order, ['high1', 'high2', 'low1', 'low2']);
});

test('bulkhead overflow: drop-oldest and drop-lowest-priority reject the victim with reason dropped', async () => {
  const time = new FakeTimeProvider();
  const isDropped = (e: unknown) => e instanceof BulkheadRejectedError && (e.reason as string) === 'dropped' && /Dropped/.test(e.message);

  const oldest = bulkhead(time, { permitLimit: 1, queueLimit: 1, overflow: 'drop-oldest' });
  let release!: () => void;
  const running = oldest.execute(() => new Promise<void>((r) => (release = r)));
  const a = assert.rejects(oldest.execute(() => 'a'), isDropped);
  const b = oldest.execute(() => 'b');
  await a;
  release();
  assert.equal(await b, 'b');
  await running;

  const lowest = bulkhead(time, { permitLimit: 1, queueLimit: 1, overflow: 'drop-lowest-priority', priority: (c) => c.properties.get('p') as number });
  const busy = lowest.execute(() => new Promise<void>((r) => (release = r)));
  const low = assert.rejects(lowest.execute(() => 'low', props({ p: 5 })), isDropped);
  await assert.rejects(lowest.execute(() => 'same', props({ p: 5 })), (e: unknown) => e instanceof BulkheadRejectedError && e.reason === 'full');
  const high = lowest.execute(() => 'high', props({ p: 1 }));
  await low;
  release();
  assert.equal(await high, 'high');
  await busy;
});

// --- HTTP ---

async function serve(middleware: ReturnType<typeof resilienceMiddleware>) {
  const server = http.createServer((req, res) => middleware(req, res, () => res.end('ok')));
  await new Promise<void>((ok) => server.listen(0, ok));
  return { url: `http://localhost:${(server.address() as AddressInfo).port}`, close: () => (server.closeAllConnections(), server.close()) };
}

test('resilienceMiddleware: per-key rateLimit with RateLimit-* headers, 429 + Retry-After beyond it', async () => {
  const s = await serve(resilienceMiddleware({ rateLimit: { key: (req: http.IncomingMessage) => String(req.headers['x-user']), permitLimit: 2, window: '1h' }, rateLimitHeaders: 'both' }));
  try {
    const get = (user: string) => fetch(s.url, { headers: { 'x-user': user } });
    const first = await get('a');
    assert.equal(first.status, 200);
    assert.equal(first.headers.get('ratelimit-limit'), '2');
    assert.equal(first.headers.get('ratelimit-remaining'), '1');
    assert.equal(first.headers.get('x-ratelimit-remaining'), '1');
    assert.ok(Number(first.headers.get('ratelimit-reset')) > 3500);
    await get('a');
    const limited = await get('a');
    assert.equal(limited.status, 429);
    assert.equal(limited.headers.get('ratelimit-remaining'), '0');
    assert.ok(Number(limited.headers.get('retry-after')) > 0);
    assert.equal((await get('b')).status, 200);
  } finally {
    s.close();
  }
});

test('honoResilience: rate limit headers on success and on 429', async () => {
  const mw = honoResilience({ rateLimit: { permitLimit: 1, window: '1m' } });
  const headers = new Map<string, string>();
  const c = {
    req: { raw: new Request('http://x/') },
    json: (body: unknown, status: number, h?: Record<string, string>) => new Response(JSON.stringify(body), { status, headers: h }),
    header: (name: string, value: string) => void headers.set(name, value),
  };
  await mw(c, async () => {});
  assert.equal(headers.get('RateLimit-Remaining'), '0');
  assert.equal(headers.get('RateLimit-Limit'), '1');
  const res = (await mw(c, async () => {})) as Response;
  assert.equal(res.status, 429);
  assert.equal(res.headers.get('ratelimit-remaining'), '0');
  assert.equal(res.headers.get('retry-after'), '60');
});
