import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RateLimiterRejectedError, ResiliencePipelineBuilder, onTelemetry } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';
import {
  type FixedWindowRateLimiterOptions,
  type PartitionedRateLimiterOptions,
  type SlidingWindowRateLimiterOptions,
  FixedWindowRateLimiterStrategy,
  PartitionedRateLimiterStrategy,
  PermitLimitExceededError,
  SlidingWindowRateLimiterStrategy,
} from '../src/window-rate-limiter.ts';

const fixed = (time: FakeTimeProvider, o: FixedWindowRateLimiterOptions) =>
  new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new FixedWindowRateLimiterStrategy(o, s), 'FixedWindow').build();
const sliding = (time: FakeTimeProvider, o: SlidingWindowRateLimiterOptions) =>
  new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new SlidingWindowRateLimiterStrategy(o, s), 'SlidingWindow').build();

const rejectedAfter = (retryAfter: number) => (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter === retryAfter;
const props = (entries: Record<string, unknown>) => ({ properties: new Map(Object.entries(entries)) });

test('fixed window: permitLimit per window, resets at the boundary, retryAfter = time to the boundary', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 2, window: '1s' });
  assert.equal(await p.execute(() => 1), 1);
  await time.advance(300);
  assert.equal(await p.execute(() => 2), 2);
  await assert.rejects(p.execute(() => 3), rejectedAfter(700));
  await time.advance(700);
  assert.equal(await p.execute(() => 4), 4);
});

test('burst at the window edge: fixed lets 2x through, sliding does not', async () => {
  const time = new FakeTimeProvider();
  const f = fixed(time, { permitLimit: 2, window: '1s' });
  const s = sliding(time, { permitLimit: 2, window: '1s', segmentsPerWindow: 10 });
  await time.advance(900);
  for (const p of [f, s]) await Promise.all([p.execute(() => 0), p.execute(() => 0)]);
  await time.advance(100); // new fixed window
  await Promise.all([f.execute(() => 0), f.execute(() => 0)]); // 4 calls within 100ms
  // The 2 permits taken at 900ms leave the sliding window at 1900ms.
  await assert.rejects(s.execute(() => 0), rejectedAfter(900));
  await time.advance(899);
  await assert.rejects(s.execute(() => 0), rejectedAfter(1));
  await time.advance(1);
  await s.execute(() => 0);
});

test('sliding window: retryAfter is when enough old segments slide out', async () => {
  const time = new FakeTimeProvider();
  const p = sliding(time, { permitLimit: 3, window: '1s', segmentsPerWindow: 4 }); // 250ms segments
  await p.execute(() => 0); // segment 0
  await time.advance(300);
  await p.execute(() => 0); // segment 1
  await p.execute(() => 0);
  await time.advance(300); // t=600, segment 2
  await assert.rejects(p.execute(() => 0), rejectedAfter(400)); // segment 0 leaves at t=1000
  await time.advance(400);
  await p.execute(() => 0);
  await assert.rejects(p.execute(() => 0), rejectedAfter(250)); // segment 1 (2 permits) leaves at t=1250
});

test('queueTimeout: waits for the next window and then runs; rejects at once when the wait would be longer', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 1, window: '1s', queueTimeout: '1s' });
  await p.execute(() => 0);
  let done = false;
  const waiting = p.execute(() => 'late').then((v) => ((done = true), v));
  await time.advance(999);
  assert.equal(done, false);
  await time.advance(1);
  assert.equal(await waiting, 'late');

  const strict = fixed(time, { permitLimit: 1, window: '1s', queueTimeout: '500ms' });
  await strict.execute(() => 0);
  await assert.rejects(strict.execute(() => 0), rejectedAfter(1000));
  assert.equal(time.pendingTimers, 0);
});

test('weights: permits(context) is charged against the limit; a weight above permitLimit fails fast', async () => {
  const time = new FakeTimeProvider();
  const rejected: Array<number | undefined> = [];
  const p = sliding(time, {
    permitLimit: 10,
    window: '1m',
    queueTimeout: '1h',
    permits: (ctx) => ctx.properties.get('cost') as number,
    onRejected: (a) => void rejected.push(a.retryAfter),
  });
  await p.execute(() => 0, props({ cost: 6 }));
  await p.execute(() => 0, props({ cost: 4 }));
  await assert.rejects(p.execute(() => 0, props({ cost: 11 })), (e: unknown) => e instanceof PermitLimitExceededError && e instanceof RateLimiterRejectedError && /at most 10/.test(e.message));
  assert.deepEqual(rejected, [undefined]);
  assert.equal(time.pendingTimers, 0, 'did not wait');
  await p.execute(() => 0, props({ cost: 0 })); // free
  await assert.rejects(p.execute(() => 0, props({ cost: -1 })), RangeError);
});

test('telemetry: rejections publish OnRateLimiterRejected', async () => {
  const time = new FakeTimeProvider();
  const events: string[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnRateLimiterRejected' && events.push(`${e.source.strategyName}:${e.event}`)));
  try {
    const p = fixed(time, { permitLimit: 1, window: '1s' });
    await p.execute(() => 0);
    await assert.rejects(p.execute(() => 0), RateLimiterRejectedError);
  } finally {
    off();
  }
  assert.deepEqual(events, ['FixedWindow:OnRateLimiterRejected']);
});

test('abort while waiting: rejects with the abort reason and clears the timer', async () => {
  const time = new FakeTimeProvider();
  const p = fixed(time, { permitLimit: 1, window: '1m', queueTimeout: '1m' });
  await p.execute(() => 0);
  const controller = new AbortController();
  const waiting = assert.rejects(p.execute(() => 0, { signal: controller.signal }), { name: 'AbortError' });
  await time.advance(10);
  assert.equal(time.pendingTimers, 1);
  controller.abort();
  await waiting;
  assert.equal(time.pendingTimers, 0);
});

const partitioned = (time: FakeTimeProvider, o: Partial<PartitionedRateLimiterOptions<unknown>> = {}) => {
  let strategy!: PartitionedRateLimiterStrategy<unknown>;
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
    .addStrategy(
      (s) =>
        (strategy = new PartitionedRateLimiterStrategy(
          {
            partitionKey: (ctx) => ctx.properties.get('user') as string,
            limiter: (_key, src) => new FixedWindowRateLimiterStrategy({ permitLimit: 1, window: '1s' }, src),
            ...o,
          },
          s,
        )),
      'PerUser',
    )
    .build();
  return { pipeline, strategy };
};

test('partitioned: user A exhausting its limit does not affect user B', async () => {
  const time = new FakeTimeProvider();
  const { pipeline: p } = partitioned(time);
  await p.execute(() => 0, props({ user: 'a' }));
  await assert.rejects(p.execute(() => 0, props({ user: 'a' })), rejectedAfter(1000));
  assert.equal(await p.execute(() => 'b', props({ user: 'b' })), 'b');
  await assert.rejects(p.execute(() => 0, props({ user: 'b' })), RateLimiterRejectedError);
});

test('partitioned: idle partitions are evicted, busy ones are kept; maxPartitions drops the least recently used', async () => {
  const time = new FakeTimeProvider();
  const { pipeline: p, strategy } = partitioned(time, { idleTimeout: '1m' });
  let release!: () => void;
  const busy = p.execute(() => new Promise<void>((r) => (release = r)), props({ user: 'busy' }));
  await p.execute(() => 0, props({ user: 'a' }));
  await p.execute(() => 0, props({ user: 'b' }));
  assert.equal(strategy.partitions, 3);
  await time.advance('1m');
  await p.execute(() => 0, props({ user: 'c' }));
  assert.equal(strategy.partitions, 2, 'a and b evicted, busy kept');
  release();
  await busy;

  const lru = partitioned(time, { maxPartitions: 2 });
  for (const user of ['a', 'b', 'a', 'c']) await lru.pipeline.execute(() => 0, props({ user })).catch(() => {});
  assert.equal(lru.strategy.partitions, 2);
  // b was least recently used, so it was dropped and starts with a fresh quota; c still holds its window.
  await lru.pipeline.execute(() => 0, props({ user: 'b' }));
  await assert.rejects(lru.pipeline.execute(() => 0, props({ user: 'c' })), RateLimiterRejectedError);
});

test('validation', () => {
  const time = new FakeTimeProvider();
  assert.throws(() => fixed(time, { permitLimit: 0, window: '1s' }), RangeError);
  assert.throws(() => fixed(time, { permitLimit: 1, window: 0 }), RangeError);
  assert.throws(() => sliding(time, { permitLimit: 1, window: '1s', segmentsPerWindow: 0 }), RangeError);
  assert.throws(() => partitioned(time, { maxPartitions: 0 }), RangeError);
});
