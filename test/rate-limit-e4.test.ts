import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RateLimiterRejectedError, ResiliencePipelineBuilder } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';
import { TokenBucketStrategy, type TokenBucketOptions } from '../src/rate-limiter.ts';
import { BurstyRateLimiter } from '../src/bursty-rate-limiter.ts';
import {
  type FixedWindowRateLimiterOptions,
  type RateLimiterAlgorithm,
  FixedWindowRateLimiterStrategy,
  PartitionedRateLimiterStrategy,
  RateLimiterControl,
  RateLimiterStrategy,
  SlidingWindowRateLimiterStrategy,
  WindowCounter,
  getRateLimitInfo,
} from '../src/window-rate-limiter.ts';

const props = (entries: Record<string, unknown>) => ({ properties: new Map(Object.entries(entries)) });
const build = <S,>(time: FakeTimeProvider, make: (s: any) => S) => {
  let strategy!: S;
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => (strategy = make(s)) as any).build();
  return { pipeline, strategy: () => strategy };
};
const fixed = (time: FakeTimeProvider, o: FixedWindowRateLimiterOptions) => build(time, (s) => new FixedWindowRateLimiterStrategy(o, s));
const bucket = (time: FakeTimeProvider, o: TokenBucketOptions) => build(time, (s) => new TokenBucketStrategy(o, s));
const retryAfter = async (p: Promise<unknown>) => {
  const e = await p.then(() => assert.fail('expected a rejection'), (e: unknown) => e);
  assert.ok(e instanceof RateLimiterRejectedError);
  return e.retryAfter;
};
// Async like a Redis algorithm.
const async = (a: RateLimiterAlgorithm): RateLimiterAlgorithm => ({ permitLimit: a.permitLimit, acquire: async (p, n) => a.acquire(p, n) });

test('bursty: primary first, burst pool only when it rejects; retryAfter = the sooner of the two', async () => {
  const time = new FakeTimeProvider();
  const now = time.now();
  const { pipeline: p } = build(time, (s) => new RateLimiterStrategy(new BurstyRateLimiter(async(new WindowCounter(2, 1000, 1, now)), async(new WindowCounter(3, 60_000, 1, now))), {}, s));
  const infos = [];
  for (let i = 0; i < 5; i++) {
    const ctx = props({});
    await p.execute(() => 0, ctx);
    infos.push(getRateLimitInfo(ctx)?.remaining);
  }
  assert.deepEqual(infos, [1, 0, 2, 1, 0]);
  assert.equal(await retryAfter(p.execute(() => 0)), 1000); // primary resets first
  await time.advance('1s');
  await p.execute(() => 0);
  await p.execute(() => 0);
  assert.equal(await retryAfter(p.execute(() => 0)), 1000);
  // Burst pool empty and primary exhausted for 59s: only primary windows admit.
  await time.advance('59s');
  for (let i = 0; i < 5; i++) await p.execute(() => 0); // burst refilled at 60s
});

test('bursty: a weight above the primary limit goes to the burst pool; both rejecting picks the min retryAfter', async () => {
  const now = 0;
  const b = new BurstyRateLimiter(new WindowCounter(2, 1000, 1, now), new WindowCounter(5, 10_000, 1, now));
  assert.equal(b.permitLimit, 5);
  assert.deepEqual(await b.acquire(4, 0), { ok: true, remaining: 1, resetAfter: 10_000 });
  assert.deepEqual(await b.acquire(4, 100), { ok: false, retryAfter: 9900 }); // only the burst pool can hold 4
  await b.acquire(2, 100);
  await b.acquire(1, 100); // burst: 5/5
  assert.deepEqual(await b.acquire(1, 200), { ok: false, retryAfter: 800 });
  await b.adjust(-Infinity, 200);
  assert.deepEqual(await b.acquire(5, 200), { ok: true, remaining: 0, resetAfter: 9800 });
  const restored = new BurstyRateLimiter(new WindowCounter(2, 1000, 1, 0), new WindowCounter(5, 10_000, 1, 0));
  restored.restore(JSON.parse(JSON.stringify(b.snapshot(300))), 1000);
  assert.deepEqual(await restored.acquire(1, 1000), { ok: true, remaining: 1, resetAfter: 1000 }); // primary rolled over at 1000
  assert.deepEqual(await restored.acquire(5, 1000), { ok: false, retryAfter: 9000 }); // burst state came along
});

test('snapshot/restore: fixed window survives a restart and keeps its window end', async () => {
  const time = new FakeTimeProvider();
  const a = fixed(time, { permitLimit: 3, window: '1s' });
  await time.advance(200);
  await a.pipeline.execute(() => 0);
  await a.pipeline.execute(() => 0);
  const saved = JSON.stringify(a.strategy().snapshot());
  await time.advance(300); // "restart" at 500
  const b = fixed(time, { permitLimit: 3, window: '1s' });
  b.strategy().restore(JSON.parse(saved));
  await b.pipeline.execute(() => 0);
  assert.equal(await retryAfter(b.pipeline.execute(() => 0)), 500);
  await time.advance(500);
  await b.pipeline.execute(() => 0);
  assert.throws(() => b.strategy().restore({ at: 0, blockedFor: 0, algorithm: { kind: 'token-bucket' } } as any), TypeError);
});

test('snapshot/restore: sliding window ages its buckets; token bucket refills for the downtime; block carries over', async () => {
  const time = new FakeTimeProvider();
  const a = build(time, (s) => new SlidingWindowRateLimiterStrategy({ permitLimit: 4, window: '1s', segmentsPerWindow: 4 }, s));
  await a.pipeline.execute(() => 0);
  await a.pipeline.execute(() => 0);
  await time.advance(500);
  await a.pipeline.execute(() => 0);
  a.strategy().block('2s');
  const saved = JSON.parse(JSON.stringify(a.strategy().snapshot()));
  await time.advance(2100); // block over; all buckets left the window
  const b = build(time, (s) => new SlidingWindowRateLimiterStrategy({ permitLimit: 4, window: '1s', segmentsPerWindow: 4 }, s));
  b.strategy().restore(saved);
  for (let i = 0; i < 4; i++) await b.pipeline.execute(() => 0);
  const c = build(time, (s) => new SlidingWindowRateLimiterStrategy({ permitLimit: 4, window: '1s', segmentsPerWindow: 4 }, s));
  c.strategy().restore({ ...saved, at: time.now() - 600 }); // as if taken 600ms ago: still blocked for 1400
  assert.equal(await retryAfter(c.pipeline.execute(() => 0)), 1400);

  const t = bucket(time, { tokenLimit: 3, replenishmentPeriod: '1s', tokensPerPeriod: 1 });
  for (let i = 0; i < 3; i++) await t.pipeline.execute(() => 0);
  const tokens = JSON.parse(JSON.stringify(t.strategy().snapshot()));
  await time.advance(1500);
  const u = bucket(time, { tokenLimit: 3, replenishmentPeriod: '1s', tokensPerPeriod: 1 });
  u.strategy().restore(tokens);
  await u.pipeline.execute(() => 0);
  assert.equal(await retryAfter(u.pipeline.execute(() => 0)), 500);
});

test('snapshot/restore: partitioned limiter, all keys or the most recent N, bounded by maxPartitions', async () => {
  const time = new FakeTimeProvider();
  const control = new RateLimiterControl();
  const make = (maxPartitions?: number) =>
    build(time, (s) => new PartitionedRateLimiterStrategy({ partitionKey: (c) => c.properties.get('k') as string, limiter: (_k, s2) => new FixedWindowRateLimiterStrategy({ permitLimit: 2, window: '1m' }, s2), maxPartitions, control }, s));
  const a = make();
  for (const k of ['a', 'b', 'b', 'c']) await a.pipeline.execute(() => 0, props({ k }));
  control.block('1m', 'c');
  const all = JSON.parse(JSON.stringify(a.strategy().snapshot()));
  assert.deepEqual(a.strategy().snapshot(2).partitions.map(([k]) => k), ['b', 'c']);
  await time.advance('10s');
  const b = make();
  b.strategy().restore(all);
  assert.deepEqual(b.strategy().keys(), ['a', 'b', 'c']);
  await b.pipeline.execute(() => 0, props({ k: 'a' }));
  assert.equal(await retryAfter(b.pipeline.execute(() => 0, props({ k: 'b' }))), 50_000);
  assert.equal(await retryAfter(b.pipeline.execute(() => 0, props({ k: 'c' }))), 50_000); // block left
  const c = make(2);
  c.strategy().restore(all);
  assert.deepEqual(c.strategy().keys(), ['b', 'c']);
});

test("even spacing: a burst is spread over the window instead of running at its start", async () => {
  const time = new FakeTimeProvider();
  const { pipeline: p } = fixed(time, { permitLimit: 4, window: '1s', spacing: 'even' });
  const t0 = time.now();
  const starts: number[] = [];
  const all = [0, 1, 2, 3].map(() => p.execute(() => void starts.push(time.now() - t0)));
  await assert.rejects(p.execute(() => 0), RateLimiterRejectedError); // still limited by the window
  await time.runAll();
  await Promise.all(all);
  assert.deepEqual(starts, [0, 250, 500, 750]);
});

test('minTime spaces starts; queueTimeout caps the pacing delay; abort ends it', async () => {
  const time = new FakeTimeProvider();
  const t0 = time.now();
  const starts: number[] = [];
  const t = bucket(time, { tokenLimit: 3, replenishmentPeriod: '1s', minTime: 100 });
  const all = [0, 1, 2].map(() => t.pipeline.execute(() => void starts.push(time.now() - t0)));
  await time.runAll();
  await Promise.all(all);
  assert.deepEqual(starts, [0, 100, 200]);

  const capped = fixed(time, { permitLimit: 10, window: '1s', minTime: '1s', queueTimeout: 300 });
  const t1 = time.now();
  const s2: number[] = [];
  const c = [0, 1, 2].map(() => capped.pipeline.execute(() => void s2.push(time.now() - t1)));
  await time.runAll();
  await Promise.all(c);
  assert.deepEqual(s2, [0, 300, 300]);

  const ac = new AbortController();
  const paced = fixed(time, { permitLimit: 10, window: '1s', minTime: '1s' });
  await paced.pipeline.execute(() => 0);
  const aborted = assert.rejects(paced.pipeline.execute(() => 0, { signal: ac.signal }), /stop/);
  await time.advance(10);
  ac.abort(new Error('stop'));
  await aborted;
  assert.equal(time.pendingTimers, 0);
});

test("alignment 'first-request': the window starts at the first request, not at the limiter's start", async () => {
  const time = new FakeTimeProvider();
  const first = fixed(time, { permitLimit: 1, window: '1s', alignment: 'first-request' });
  const grid = fixed(time, { permitLimit: 1, window: '1s' });
  await time.advance(1500);
  const c1 = props({});
  const c2 = props({});
  await first.pipeline.execute(() => 0, c1);
  await grid.pipeline.execute(() => 0, c2);
  assert.equal(getRateLimitInfo(c1)?.resetAfter, 1000);
  assert.equal(getRateLimitInfo(c2)?.resetAfter, 500);
  await time.advance(700);
  assert.equal(await retryAfter(first.pipeline.execute(() => 0)), 300);
  await grid.pipeline.execute(() => 0); // new grid window at 2000
  await time.advance(2000); // idle: next request opens a fresh window at 4200
  const c3 = props({});
  await first.pipeline.execute(() => 0, c3);
  assert.equal(getRateLimitInfo(c3)?.resetAfter, 1000);
});
