import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createContext, fail, ok, report } from '../src/core.ts';
import { ResiliencePipelineBuilder } from '../src/index.ts';
import { createStats } from '../src/stats.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const executed = (pipelineName: string, duration: number, success = true) =>
  report({ pipelineName, strategyName: undefined }, 'OnPipelineExecuted', createContext(), { outcome: success ? ok(1) : fail(new Error('x')), duration });
const event = (pipelineName: string, name: string) => report({ pipelineName, strategyName: 's' }, name, createContext(), {});

test('stats: counts from a real pipeline', async () => {
  const stats = createStats({ pipeline: 'orders', timeProvider: new FakeTimeProvider() });
  try {
    const pipeline = new ResiliencePipelineBuilder({ name: 'orders' }).addFallback({ fallbackAction: () => 'fb' }).addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
    await pipeline.execute(() => Promise.reject(Object.assign(new Error('x'), { status: 503 })));
    await pipeline.execute(() => 'fine');
    const s = stats.snapshot();
    assert.equal(s.executions, 2);
    assert.equal(s.successes, 2); // the fallback turned the failure into a success
    assert.equal(s.failures, 0);
    assert.equal(s.retries, 2);
    assert.equal(s.fallbacks, 1);
    assert.deepEqual(s.total.executions, 2);
  } finally {
    stats.dispose();
  }
});

test('stats: rejections, timeouts, cache and failures are counted', () => {
  const stats = createStats({ pipeline: 'p', timeProvider: new FakeTimeProvider() });
  try {
    for (const name of ['OnCircuitRejected', 'OnRateLimiterRejected', 'OnTimeout', 'OnCacheHit', 'OnCacheHit', 'OnCacheMiss', 'OnCoalesced', 'OnSomethingElse']) event('p', name);
    executed('p', 5, false);
    const s = stats.snapshot();
    assert.deepEqual(
      { r: s.rejections, t: s.timeouts, h: s.cacheHits, m: s.cacheMisses, c: s.coalesced, f: s.failures },
      { r: 2, t: 1, h: 2, m: 1, c: 1, f: 1 },
    );
  } finally {
    stats.dispose();
  }
});

test('stats: the window rolls one bucket at a time, totals keep everything', async () => {
  const time = new FakeTimeProvider(0);
  const stats = createStats({ window: '10s', timeProvider: time, pipeline: 'roll' });
  try {
    executed('roll', 100);
    await time.advance('5s');
    executed('roll', 1);
    executed('roll', 1);
    assert.equal(stats.snapshot().executions, 3);
    await time.advance('5s'); // t=10s: the bucket at t=0 has aged out
    let s = stats.snapshot();
    assert.equal(s.executions, 2);
    assert.equal(s.latency.max, 1);
    assert.equal(s.total.executions, 3);
    assert.equal(s.total.latency.max, 100);
    await time.advance('5s');
    s = stats.snapshot();
    assert.equal(s.executions, 0);
    assert.deepEqual(s.latency, { mean: 0, max: 0, p50: 0, p90: 0, p99: 0 });
  } finally {
    stats.dispose();
  }
});

test('stats: percentiles within 1% on known data', () => {
  const stats = createStats({ pipeline: 'lat', percentiles: [0.5, 0.9, 0.99, 0.999], timeProvider: new FakeTimeProvider() });
  try {
    for (let i = 1; i <= 10_000; i++) executed('lat', i / 10); // 0.1 … 1000 ms, uniform
    const { latency } = stats.snapshot();
    const near = (actual: number, expected: number) => assert.ok(Math.abs(actual - expected) / expected <= 0.01, `${actual} vs ${expected}`);
    near(latency.p50, 500);
    near(latency.p90, 900);
    near(latency.p99, 990);
    near(latency['p99.9'], 999);
    near(latency.mean, 500.05);
    assert.equal(latency.max, 1000);
  } finally {
    stats.dispose();
  }
});

test('stats: pipeline filter accepts one name or a list', () => {
  const one = createStats({ pipeline: 'a', timeProvider: new FakeTimeProvider() });
  const two = createStats({ pipeline: ['a', 'b'], timeProvider: new FakeTimeProvider() });
  try {
    executed('a', 1);
    executed('b', 1);
    executed('c', 1);
    assert.equal(one.snapshot().executions, 1);
    assert.equal(two.snapshot().executions, 2);
  } finally {
    one.dispose();
    two.dispose();
  }
});

test('stats: onSnapshot emits every interval until stopped; dispose stops everything', async () => {
  const time = new FakeTimeProvider(0);
  const stats = createStats({ pipeline: 'tick', timeProvider: time });
  const seen: number[] = [];
  const stop = stats.onSnapshot((s) => seen.push(s.executions), '1s');
  stats.onSnapshot(() => seen.push(-1), '10s');
  executed('tick', 1);
  await time.advance('3s');
  assert.deepEqual(seen, [1, 1, 1]);
  stop();
  await time.advance('3s');
  assert.deepEqual(seen, [1, 1, 1]);
  stats.dispose();
  assert.equal(time.pendingTimers, 0);
  executed('tick', 1);
  assert.equal(stats.snapshot().total.executions, 1);
});
