import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  type AdaptiveConcurrencyLimiterOptions,
  AdaptiveConcurrencyLimiterStrategy,
  AdaptiveLimitProvider,
  type LimitAlgorithm,
} from '../src/adaptive-concurrency.ts';
import { RateLimiterRejectedError, ResiliencePipelineBuilder, TimeoutRejectedError, onTelemetry } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const fixed: LimitAlgorithm = { onSample: (s) => s.limit };

function setup(options: AdaptiveConcurrencyLimiterOptions = {}, timeout?: number) {
  const time = new FakeTimeProvider();
  const provider = new AdaptiveLimitProvider();
  const builder = new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy(
    (s) => new AdaptiveConcurrencyLimiterStrategy({ limitProvider: provider, ...options }, s),
    'AdaptiveConcurrencyLimiter',
  );
  // The builder mutates in place; no reassignment needed (reassigning would fight the typed-error tracking).
  if (timeout !== undefined) builder.addTimeout(timeout);
  return { time, provider, pipeline: builder.build() };
}

/** A dependency with `capacity` workers: beyond that, calls queue inside it and latency grows linearly with concurrency. */
function dependency(time: FakeTimeProvider, capacity = 10, base = 10) {
  let busy = 0;
  return () => {
    busy++;
    const ms = base * Math.max(1, busy / capacity);
    return new Promise<number>((resolve) =>
      time.setTimeout(() => {
        busy--;
        resolve(ms);
      }, ms),
    );
  };
}

/** `clients` callers, each making `calls` calls back to back (closed loop). Returns the limit seen after every call. */
async function load(options: AdaptiveConcurrencyLimiterOptions, clients = 40, calls = 25) {
  const { time, provider, pipeline } = setup({ queueLimit: 1000, ...options });
  const call = dependency(time);
  const limits: number[] = [];
  const latencies: number[] = [];
  const run = async () => {
    for (let i = 0; i < calls; i++) {
      latencies.push(await pipeline.execute(call));
      limits.push(provider.limit);
    }
  };
  const all = Promise.all(Array.from({ length: clients }, run));
  await time.runAll();
  await all;
  const tail = limits.slice(-300);
  return { limits, tail: { min: Math.min(...tail), max: Math.max(...tail) }, p50: latencies.slice(-300).sort((a, b) => a - b)[150]! };
}

for (const algorithm of ['vegas', 'gradient2'] as const) {
  for (const initialLimit of [1, 200]) {
    test(`${algorithm}: converges near capacity 10 starting from ${initialLimit}`, async () => {
      const { limits, tail, p50 } = await load({ algorithm, initialLimit });
      // Full throughput needs >= 10; latency stays within ~2x of the unloaded 10ms (without a limiter: 40 callers -> 40ms).
      assert.ok(tail.min >= 10 && tail.max <= 25, `${algorithm} from ${initialLimit}: tail ${tail.min}..${tail.max}, path ${limits.filter((_, i) => i % 100 === 0)}`);
      assert.ok(p50 <= 20, `p50 latency ${p50}ms`);
    });
  }
}

test('aimd: grows while calls succeed (latency is ignored)', async () => {
  const { tail } = await load({ algorithm: 'aimd', initialLimit: 10 }, 40, 10);
  assert.ok(tail.min > 10, `grew to ${tail.min}`);
});

test('timeouts shrink the limit, one cut per round trip', async () => {
  const changes: Array<[number, number]> = [];
  const { time, provider, pipeline } = setup({ algorithm: 'aimd', initialLimit: 20, onLimitChanged: (a) => void changes.push([a.oldLimit, a.newLimit]) }, 100);
  const hang = () => new Promise<never>(() => {});
  // 10 calls time out together: they were all admitted under the same limit, so only the first one cuts it.
  const batch = Array.from({ length: 10 }, () => assert.rejects(pipeline.execute(hang), TimeoutRejectedError));
  await time.runAll();
  await Promise.all(batch);
  assert.equal(provider.limit, 18); // 20 * 0.9
  // A later call is news: it cuts again.
  const later = assert.rejects(pipeline.execute(hang), TimeoutRejectedError);
  await time.runAll();
  await later;
  assert.equal(provider.limit, 16);
  assert.deepEqual(changes, [[20, 18], [18, 16]]);
  assert.equal(provider.inflight, 0);
});

test('transient errors shrink the limit; client errors (4xx) and caller aborts do not', async () => {
  const { pipeline, provider } = setup({ algorithm: 'aimd', initialLimit: 20 });
  const status = (s: number) => () => Promise.reject(Object.assign(new Error(`HTTP ${s}`), { status: s }));
  for (let i = 0; i < 10; i++) await assert.rejects(pipeline.execute(status(404)));
  assert.equal(provider.limit, 20);
  const controller = new AbortController();
  const aborted = assert.rejects(pipeline.execute(() => Promise.reject(new Error('cancelled')), { signal: controller.signal }));
  controller.abort();
  await aborted;
  assert.equal(provider.limit, 20);
  await assert.rejects(pipeline.execute(status(503)));
  assert.equal(provider.limit, 18);
});

test('queues at the limit, rejects when the queue is full, and hands permits on in order', async () => {
  const events: string[] = [];
  const off = onTelemetry((e) => void events.push(e.event));
  let rejected = 0;
  const { time, provider, pipeline } = setup({ algorithm: fixed, initialLimit: 2, queueLimit: 1, onRejected: () => void rejected++ });
  const order: number[] = [];
  const work = (n: number) => () => new Promise<number>((r) => time.setTimeout(() => (order.push(n), r(n)), 10));
  const running = [pipeline.execute(work(1)), pipeline.execute(work(2)), pipeline.execute(work(3))];
  assert.equal(provider.inflight, 2);
  assert.equal(provider.queued, 1);
  await assert.rejects(pipeline.execute(work(4)), RateLimiterRejectedError);
  assert.equal(rejected, 1);
  await time.runAll();
  assert.deepEqual(await Promise.all(running), [1, 2, 3]);
  assert.deepEqual(order, [1, 2, 3]);
  assert.ok(events.includes('OnRateLimiterRejected'));
  off();
});

test('abort and queueTimeout while queued free the slot', async () => {
  const { time, provider, pipeline } = setup({ algorithm: fixed, initialLimit: 1, queueLimit: 2, queueTimeout: '50ms' });
  const done = pipeline.execute(() => new Promise((r) => time.setTimeout(() => r('first'), 1000)));
  const controller = new AbortController();
  const aborted = assert.rejects(pipeline.execute(() => 'never', { signal: controller.signal }), { name: 'AbortError' });
  const timedOut = assert.rejects(pipeline.execute(() => 'never'), RateLimiterRejectedError);
  assert.equal(provider.queued, 2);
  controller.abort();
  await aborted;
  assert.equal(provider.queued, 1);
  await time.advance('50ms');
  await timedOut;
  assert.equal(provider.queued, 0);
  await time.runAll();
  assert.equal(await done, 'first');
  assert.equal(provider.inflight, 0);
});

test('OnLimitChanged telemetry, clamping to [minLimit, maxLimit], and option validation', async () => {
  const events: unknown[] = [];
  const off = onTelemetry((e) => e.event === 'OnLimitChanged' && void events.push(e.args));
  const grow: LimitAlgorithm = { onSample: (s) => s.limit * 100 };
  const { pipeline, provider } = setup({ algorithm: grow, initialLimit: 1, maxLimit: 5 });
  await pipeline.execute(() => 1);
  assert.equal(provider.limit, 5);
  assert.deepEqual((events[0] as { oldLimit: number; newLimit: number }).newLimit, 5);
  off();
  const source = { pipelineName: undefined, strategyName: undefined, timeProvider: new FakeTimeProvider() };
  assert.throws(() => new AdaptiveConcurrencyLimiterStrategy({ initialLimit: 0 }, source), RangeError);
  assert.throws(() => new AdaptiveConcurrencyLimiterStrategy({ minLimit: 10, maxLimit: 5 }, source), RangeError);
  assert.throws(() => new AdaptiveConcurrencyLimiterStrategy({ algorithm: 'nope' as 'vegas' }, source), RangeError);
});
