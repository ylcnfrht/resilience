import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BulkheadRejectedError, RateLimiterRejectedError, createContext, ok, onTelemetry } from '../src/core.ts';
import { FakeTimeProvider } from '../src/testing.ts';
import { ConcurrencyLimiterStrategy, concurrencyLimiterStrategy, tokenBucketStrategy } from '../src/rate-limiter.ts';
import {
  type AcquireResult,
  type RateLimiterAlgorithm,
  FixedWindowRateLimiterStrategy,
  PartitionedRateLimiterStrategy,
  RateLimiterControl,
  RateLimiterStrategy,
  TokenBucket,
  WindowCounter,
  fixedWindowRateLimiterStrategy,
  partitionedRateLimiterStrategy,
  slidingWindowRateLimiterStrategy,
} from '../src/window-rate-limiter.ts';
import { BurstyRateLimiter } from '../src/bursty-rate-limiter.ts';
import { AdaptiveConcurrencyLimiterStrategy, AdaptiveLimitProvider, adaptiveConcurrencyLimiterStrategy, aimd, gradient2, vegas } from '../src/adaptive-concurrency.ts';

const defer = <T = void>() => {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
};
const src = (time: FakeTimeProvider) => ({ pipelineName: 'p', strategyName: 's', timeProvider: time });
const value = <T>(v: T) => async () => ok(v);
const gate = () => {
  const g = defer<void>();
  return { promise: g.promise, open: () => g.resolve(), next: <T>(v: T) => async () => (await g.promise, ok(v)) };
};
const abortedContext = (reason: unknown) => createContext({ signal: AbortSignal.abort(reason) });
const err = (o: { ok: boolean; error?: unknown }) => (assert.equal(o.ok, false), o.error);

// --- algorithms ---

test('WindowCounter: weight 0 reports the reset of the newest non-empty bucket; a weight above the limit throws', () => {
  const w = new WindowCounter(5, 1000, 10, 0); // 100 ms segments
  assert.deepEqual(w.acquire(0, 0), { ok: true, remaining: 5, resetAfter: 0 }, 'empty window: nothing to reset');
  w.acquire(2, 0);
  assert.deepEqual(w.acquire(0, 350), { ok: true, remaining: 3, resetAfter: 650 }, 'segment 0 leaves the window at 1000');
  assert.throws(() => w.acquire(6, 350), RangeError);
});

test('TokenBucket: penalty drives tokens negative, reward is capped at tokenLimit, weight 0 on a full bucket resets at once', () => {
  const b = new TokenBucket(3, 1000, 1, 0);
  assert.deepEqual(b.acquire(0, 0), { ok: true, remaining: 3, resetAfter: 0 });
  b.adjust(5, 0); // 3 - 5 = -2
  assert.deepEqual(b.acquire(1, 0), { ok: false, retryAfter: 3000 }, 'needs 3 refills to reach 1 token');
  b.adjust(-100, 0);
  assert.deepEqual(b.acquire(3, 0), { ok: true, remaining: 0, resetAfter: 3000 }, 'reward refilled to 3, not 98');
});

test('BurstyRateLimiter: a weight that fits neither pool is rejected with the primary retryAfter; missing capabilities throw', async () => {
  const time = 0;
  const bursty = new BurstyRateLimiter(new WindowCounter(10, 1000, 1, time), new WindowCounter(5, 60_000, 1, time));
  assert.equal((await bursty.acquire(8, 0)).ok, true);
  assert.deepEqual(await bursty.acquire(8, 100), { ok: false, retryAfter: 900 }, 'burst pool (5) cannot hold 8: only the primary answers');

  const bare: RateLimiterAlgorithm = { permitLimit: 1, acquire: () => ({ ok: true }) };
  const noAdjust = new BurstyRateLimiter(bare, bare);
  await assert.rejects(noAdjust.adjust(1, 0), /primary rate limiter algorithm does not support/);
  assert.throws(() => noAdjust.snapshot(0), /Both rate limiter algorithms/);
  assert.throws(() => noAdjust.restore({ kind: 'bursty', primary: 0, burst: 0 }, 0), /Both rate limiter algorithms/);

  const primary = new WindowCounter(1, 1000, 1, 0);
  const resetOnlyPrimary = new BurstyRateLimiter(primary, bare);
  primary.acquire(1, 0);
  await resetOnlyPrimary.adjust(-Infinity, 0); // the burst pool has no adjust: skipped, not thrown
  assert.equal(primary.acquire(1, 0).ok, true, 'primary was reset');

  const full = new BurstyRateLimiter(new WindowCounter(1, 1000, 1, 0), new WindowCounter(1, 1000, 1, 0));
  assert.throws(() => full.restore({ kind: 'window' } as never, 0), /Invalid bursty rate limiter snapshot/);
});

// --- RateLimiterStrategy ---

test('rate limiter: throwing permits / deny / allow callbacks fail the call with their error', async () => {
  const time = new FakeTimeProvider();
  const boom = new Error('boom');
  for (const o of [{ permits: () => { throw boom; } }, { deny: () => { throw boom; } }, { allow: () => { throw boom; } }]) {
    const s = new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1s', ...o }, src(time));
    assert.equal(err(await s.execute(value(1), createContext())), boom);
  }
});

test('rate limiter: snapshot / restore / penalty on an algorithm without them throw a TypeError', async () => {
  const time = new FakeTimeProvider();
  const s = new RateLimiterStrategy({ permitLimit: 1, acquire: () => ({ ok: true }) }, {}, src(time));
  assert.throws(() => s.snapshot(), TypeError);
  assert.throws(() => s.restore({ at: 0, algorithm: null, blockedFor: 0 }), TypeError);
  await assert.rejects(s.penalty(1), /does not support penalty/);
});

test('rate limiter: an already aborted caller that would have to wait fails with its abort reason', async () => {
  const time = new FakeTimeProvider();
  const s = new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1s', queueTimeout: '1s' }, src(time));
  await s.execute(value(1), createContext());
  const reason = new Error('gone');
  assert.equal(err(await s.execute(value(2), abortedContext(reason))), reason);
  assert.equal(s.queued, 0);
});

test('rate limiter: reward, unblock and restore wake the queue head at once instead of after its retryAfter', async () => {
  const time = new FakeTimeProvider();
  const control = new RateLimiterControl();
  const s = new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1m', queueTimeout: '2m', control }, src(time));
  await s.execute(value(0), createContext());

  const rewarded = s.execute(value('rewarded'), createContext());
  await time.advance(0);
  assert.equal(s.queued, 1);
  await control.reward(1);
  await time.advance(0);
  assert.deepEqual(await rewarded, ok('rewarded'));

  control.block('1m');
  const unblocked = s.execute(value('unblocked'), createContext());
  await time.advance(0);
  assert.equal(s.queued, 1);
  await control.reward(Infinity); // the window is free, but the block still holds the head
  await time.advance(0);
  assert.equal(s.queued, 1, 'still blocked');
  control.unblock();
  await time.advance(0);
  assert.deepEqual(await unblocked, ok('unblocked'));

  const fresh = new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1m' }, src(time)).snapshot();
  const restored = s.execute(value('restored'), createContext());
  await time.advance(0);
  assert.equal(s.queued, 1);
  s.restore(fresh);
  await time.advance(0);
  assert.deepEqual(await restored, ok('restored'));
  assert.equal(time.now(), Date.UTC(2026, 0, 1), 'no time passed: every grant came from a wake-up');
});

test('rate limiter: an async algorithm that throws while the queue is served fails that waiter only', async () => {
  const time = new FakeTimeProvider();
  const boom = new Error('redis down');
  let calls = 0;
  const algorithm: RateLimiterAlgorithm = {
    permitLimit: 1,
    acquire: async () => {
      calls++;
      if (calls === 1) return { ok: false, retryAfter: 100 };
      if (calls === 2) throw boom;
      return { ok: true };
    },
  };
  const s = new RateLimiterStrategy(algorithm, { queueTimeout: '1s' }, src(time));
  const first = s.execute(value(1), createContext());
  await time.advance(100);
  assert.equal(err(await first), boom);
  assert.deepEqual(await s.execute(value(2), createContext()), ok(2), 'the limiter still works afterwards');
});

test('rate limiter: a waiter aborted while its async acquire is in flight is skipped when the answer arrives', async () => {
  const time = new FakeTimeProvider();
  const pending = defer<AcquireResult>();
  let calls = 0;
  const algorithm: RateLimiterAlgorithm = { permitLimit: 1, acquire: () => (++calls === 1 ? { ok: false, retryAfter: 100 } : pending.promise) };
  const s = new RateLimiterStrategy(algorithm, { queueTimeout: '1s' }, src(time));
  const controller = new AbortController();
  let ran = false;
  const call = s.execute(async () => ((ran = true), ok(1)), createContext({ signal: controller.signal }));
  await time.advance(100);
  assert.equal(calls, 2, 'the pump is waiting on the second acquire');
  controller.abort(new Error('left'));
  pending.resolve({ ok: true });
  assert.match(String(err(await call)), /left/);
  await time.advance(0);
  assert.equal(ran, false);
  assert.equal(s.queued, 0);
});

test('rate limiter: insurance without a timeout answers when the primary throws', async () => {
  const time = new FakeTimeProvider();
  const used: unknown[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnRateLimiterInsuranceUsed' && used.push((e.args as { error: unknown }).error)));
  try {
    const boom = new Error('primary down');
    const primary: RateLimiterAlgorithm = { permitLimit: 10, acquire: () => { throw boom; } };
    const s = new RateLimiterStrategy(primary, { insurance: { algorithm: new WindowCounter(1, 1000, 1, time.now()) } }, src(time));
    assert.deepEqual(await s.execute(value(1), createContext()), ok(1));
    assert.ok(err(await s.execute(value(2), createContext())) instanceof RateLimiterRejectedError, 'the insurance limit (1) applies');
    assert.deepEqual(used, [boom, boom]);
  } finally {
    off();
  }
});

// --- partitioned ---

test('partitioned: a throwing partitionKey fails the call', async () => {
  const time = new FakeTimeProvider();
  const boom = new Error('no user');
  const s = new PartitionedRateLimiterStrategy({ partitionKey: () => { throw boom; }, limiter: (_k, so) => new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1s' }, so) }, src(time));
  assert.equal(err(await s.execute(value(1), createContext())), boom);
  assert.equal(s.partitions, 0);
});

test('partitioned: maxPartitions is a hard bound; a new key is rejected while every partition is running', async () => {
  const time = new FakeTimeProvider();
  const rejected: unknown[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnRateLimiterRejected' && rejected.push(e.source.strategyName)));
  try {
    const s = new PartitionedRateLimiterStrategy<string>(
      { partitionKey: (c) => c.operationKey!, maxPartitions: 2, limiter: (_k, so) => new FixedWindowRateLimiterStrategy({ permitLimit: 10, window: '1s' }, so) },
      src(time),
    );
    const g = gate();
    const a = s.execute(g.next('a'), createContext({ operationKey: 'a' }));
    const b = s.execute(g.next('b'), createContext({ operationKey: 'b' }));
    const c = await s.execute(value('c'), createContext({ operationKey: 'c' }));
    assert.ok(err(c) instanceof RateLimiterRejectedError);
    assert.match(String(err(c)), /maxPartitions/);
    assert.deepEqual(s.keys(), ['a', 'b'], 'the rejected key left nothing behind');
    assert.deepEqual(rejected, ['s']);
    g.open();
    await Promise.all([a, b]);
    assert.deepEqual(await s.execute(value('c'), createContext({ operationKey: 'c' })), ok('c'), 'an idle partition is evicted for it now');
    assert.deepEqual(s.keys(), ['b', 'c']);
  } finally {
    off();
  }
});

test('partitioned: delete(key) / delete(), and control on missing keys or foreign partitions', async () => {
  const time = new FakeTimeProvider();
  const control = new RateLimiterControl();
  const s = new PartitionedRateLimiterStrategy<number>(
    { partitionKey: (c) => c.operationKey!, control, limiter: (_k, so) => new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1m' }, so) },
    src(time),
  );
  await s.execute(value(1), createContext({ operationKey: 'a' }));
  await s.execute(value(1), createContext({ operationKey: 'b' }));
  await control.reward(1, 'missing');
  control.unblock('missing');
  assert.deepEqual(s.keys(), ['a', 'b'], 'reward / unblock never create a partition');
  control.block('1m', 'c');
  assert.deepEqual(s.keys(), ['a', 'b', 'c'], 'block creates one, so the block exists when the key shows up');
  assert.ok(err(await s.execute(value(1), createContext({ operationKey: 'c' }))) instanceof RateLimiterRejectedError);
  control.unblock('c');
  assert.deepEqual(await s.execute(value(2), createContext({ operationKey: 'c' })), ok(2));
  control.block('1m'); // no key: every partition
  for (const k of ['a', 'b', 'c']) assert.ok(err(await s.execute(value(3), createContext({ operationKey: k }))) instanceof RateLimiterRejectedError, k);
  control.unblock();
  await control.reward(Infinity);
  assert.deepEqual(await s.execute(value(4), createContext({ operationKey: 'a' })), ok(4));

  assert.equal(s.delete('a'), true);
  assert.equal(s.delete('a'), false);
  assert.equal(s.delete(), true);
  assert.equal(s.partitions, 0);

  const foreign = new PartitionedRateLimiterStrategy({ partitionKey: () => 'k', limiter: (_k, so) => new ConcurrencyLimiterStrategy({ permitLimit: 1 }, so) }, src(time));
  assert.throws(() => foreign.block('1s', 'k'), /needs RateLimiterStrategy partitions/);
});

test('strategy definitions: default and custom names', () => {
  const window = { permitLimit: 1, window: '1s' } as const;
  const partitioned = { partitionKey: () => 'k', limiter: () => ({ execute: value(1) }) };
  assert.equal(fixedWindowRateLimiterStrategy(window).name, 'FixedWindowRateLimiter');
  assert.equal(fixedWindowRateLimiterStrategy({ ...window, name: 'x' }).name, 'x');
  assert.equal(slidingWindowRateLimiterStrategy(window).name, 'SlidingWindowRateLimiter');
  assert.equal(slidingWindowRateLimiterStrategy({ ...window, name: 'x' }).name, 'x');
  assert.equal(partitionedRateLimiterStrategy(partitioned).name, 'PartitionedRateLimiter');
  assert.equal(partitionedRateLimiterStrategy({ ...partitioned, name: 'x' }).name, 'x');
  assert.equal(tokenBucketStrategy({ tokenLimit: 1, replenishmentPeriod: '1s', name: 'x' }).name, 'x');
  assert.equal(concurrencyLimiterStrategy({ permitLimit: 1, name: 'x' }).name, 'x');
  assert.equal(adaptiveConcurrencyLimiterStrategy().name, 'AdaptiveConcurrencyLimiter');
  assert.equal(adaptiveConcurrencyLimiterStrategy({ name: 'x' }).name, 'x');
  const time = new FakeTimeProvider();
  assert.ok(adaptiveConcurrencyLimiterStrategy().create(src(time)) instanceof AdaptiveConcurrencyLimiterStrategy);
});

// --- concurrency limiter ---

test('concurrency limiter: active / queued, a throwing priority fails the call, an aborted caller does not queue', async () => {
  const time = new FakeTimeProvider();
  const boom = new Error('priority');
  const s = new ConcurrencyLimiterStrategy<number>({ permitLimit: 1, queueLimit: 2, priority: (c) => { if (c.operationKey === 'bad') throw boom; return 0; } }, src(time));
  const g = gate();
  const running = s.execute(g.next(1), createContext());
  const queued = s.execute(value(2), createContext());
  assert.deepEqual([s.active, s.queued], [1, 1]);
  assert.equal(err(await s.execute(value(3), createContext({ operationKey: 'bad' }))), boom);
  const reason = new Error('gone');
  assert.equal(err(await s.execute(value(4), abortedContext(reason))), reason);
  assert.deepEqual([s.active, s.queued], [1, 1]);
  g.open();
  assert.deepEqual(await Promise.all([running, queued]), [ok(1), ok(2)]);
  assert.deepEqual([s.active, s.queued], [0, 0]);

  const noQueue = new ConcurrencyLimiterStrategy<number>({ permitLimit: 1 }, src(time));
  const g2 = gate();
  const busy = noQueue.execute(g2.next(1), createContext());
  const full = err(await noQueue.execute(value(2), createContext()));
  assert.ok(full instanceof BulkheadRejectedError && full.reason === 'full', 'queueLimit defaults to 0');
  g2.open();
  await busy;
});

test('concurrency limiter: drop-oldest drops by arrival, not by queue position', async () => {
  const time = new FakeTimeProvider();
  const s = new ConcurrencyLimiterStrategy<string>({ permitLimit: 1, queueLimit: 3, overflow: 'drop-oldest', priority: (c) => Number(c.operationKey) }, src(time));
  const g = gate();
  const running = s.execute(g.next('run'), createContext());
  const oldest = s.execute(value('p5'), createContext({ operationKey: '5' }));
  const mid = s.execute(value('p1'), createContext({ operationKey: '1' })); // queued ahead of the oldest
  const young = s.execute(value('p3'), createContext({ operationKey: '3' }));
  const newcomer = s.execute(value('p9'), createContext({ operationKey: '9' }));
  const dropped = err(await oldest);
  assert.ok(dropped instanceof BulkheadRejectedError && dropped.reason === 'dropped');
  g.open();
  assert.deepEqual(await Promise.all([running, mid, young, newcomer]), [ok('run'), ok('p1'), ok('p3'), ok('p9')]);
});

// --- adaptive concurrency ---

test('limit algorithms: drops back off, 0 ms samples change nothing, bad tuning throws', () => {
  const v = vegas();
  assert.equal(v.onSample({ rtt: 10, inflight: 10, dropped: true, limit: 100 }), 100 - 6 * 2, 'limit - beta * log10(limit)');
  assert.equal(v.onSample({ rtt: 0, inflight: 10, dropped: false, limit: 100 }), 100);
  assert.throws(() => vegas({ alpha: 6, beta: 3 }), /alpha < beta/);
  const g = gradient2();
  assert.equal(g.onSample({ rtt: 10, inflight: 10, dropped: true, limit: 100 }), 90);
  assert.equal(g.onSample({ rtt: 0, inflight: 10, dropped: false, limit: 100 }), 100);
  assert.throws(() => gradient2({ tolerance: 0.5 }), /tolerance/);
  assert.throws(() => gradient2({ smoothing: 0 }), /smoothing/);
  assert.throws(() => gradient2({ backoffRatio: 1 }), /backoffRatio/);
  const a = aimd({ increase: 2 });
  assert.equal(a.onSample({ rtt: 1, inflight: 4, dropped: false, limit: 4 }), 4.5);
  assert.equal(a.onSample({ rtt: 1, inflight: 4, dropped: true, limit: 10 }), 9);
  assert.throws(() => aimd({ increase: 0 }), /increase/);
  assert.throws(() => aimd({ backoffRatio: 0 }), /backoffRatio/);
});

test('adaptive: an unattached provider reads zeros, a provider attaches once, the strategy exposes inflight / queued', async () => {
  const provider = new AdaptiveLimitProvider();
  assert.deepEqual([provider.limit, provider.inflight, provider.queued], [0, 0, 0]);
  const time = new FakeTimeProvider();
  const s = new AdaptiveConcurrencyLimiterStrategy<number>({ initialLimit: 1, queueLimit: 1, limitProvider: provider }, src(time));
  assert.throws(() => new AdaptiveConcurrencyLimiterStrategy({ limitProvider: provider }, src(time)), /already attached/);
  const g = gate();
  const running = s.execute(g.next(1), createContext());
  const queued = s.execute(value(2), createContext());
  assert.deepEqual([s.inflight, s.queued, provider.inflight, provider.queued, provider.limit], [1, 1, 1, 1, 1]);
  g.open();
  assert.deepEqual(await Promise.all([running, queued]), [ok(1), ok(2)]);
});

test('adaptive: default queueLimit 0 rejects at once; a non-finite algorithm result keeps the limit; an aborted caller does not queue', async () => {
  const time = new FakeTimeProvider();
  const s = new AdaptiveConcurrencyLimiterStrategy<number>({ initialLimit: 1, algorithm: { onSample: () => NaN } }, src(time));
  const g = gate();
  const running = s.execute(g.next(1), createContext());
  const full = err(await s.execute(value(2), createContext()));
  assert.ok(full instanceof BulkheadRejectedError && full.reason === 'full');
  g.open();
  await running;
  assert.equal(s.limit, 1, 'NaN ignored');

  const q = new AdaptiveConcurrencyLimiterStrategy<number>({ initialLimit: 1, queueLimit: 1 }, src(time));
  const g2 = gate();
  const busy = q.execute(g2.next(1), createContext());
  const reason = new Error('gone');
  assert.equal(err(await q.execute(value(2), abortedContext(reason))), reason);
  assert.equal(q.queued, 0);
  g2.open();
  await busy;
});
