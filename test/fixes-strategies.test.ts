import assert from 'node:assert/strict';
import { test } from 'node:test';
import { CircuitBreakerManualControl, CircuitBreakerStateProvider, ResiliencePipelineBuilder, onTelemetry } from '../src/index.ts';
import { gradient2 } from '../src/adaptive-concurrency.ts';
import { MemoryCircuitStateStore, sharedCircuitState } from '../src/circuit-extensions.ts';
import { FakeTimeProvider } from '../src/testing.ts';

function storeFailures() {
  const ops: string[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnCacheStoreFailed' && ops.push((e.args as { operation: string }).operation)));
  return { ops, off };
}

test('cache: a synchronously throwing store.set does not hang coalesced callers', async () => {
  const { ops, off } = storeFailures();
  try {
    const store = { get: () => undefined, set: () => { throw new TypeError('circular JSON'); }, delete() {} };
    const p = new ResiliencePipelineBuilder().addCache({ ttl: '1m', key: () => 'k', store }).build();
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const calls = [1, 2].map(() => p.execute(() => gate.then(() => 42)));
    release();
    assert.deepEqual(await Promise.all(calls), [42, 42]);
  } finally {
    off();
  }
  assert.deepEqual(ops, ['set']);
});

test('cache: synchronously throwing store.get / store.delete are treated as a miss', async () => {
  const time = new FakeTimeProvider();
  const { ops, off } = storeFailures();
  try {
    const getThrows = { get: () => { throw new Error('get'); }, set() {}, delete() {} };
    const p1 = new ResiliencePipelineBuilder({ timeProvider: time }).addCache({ ttl: '1m', key: () => 'k', store: getThrows }).build();
    assert.equal(await p1.execute(() => 1), 1);
    const expired = { value: 0, storedAt: time.now() - 3_600_000 };
    const deleteThrows = { get: () => expired, set() {}, delete: () => { throw new Error('delete'); } };
    const p2 = new ResiliencePipelineBuilder({ timeProvider: time }).addCache({ ttl: '1m', key: () => 'k', store: deleteThrows }).build();
    assert.equal(await p2.execute(() => 2), 2);
  } finally {
    off();
  }
  assert.deepEqual(ops, ['get', 'delete']);
});

test('cache: a throwing shouldCache fails every coalesced caller instead of hanging', async () => {
  const p = new ResiliencePipelineBuilder().addCache({ ttl: '1m', key: () => 'k', shouldCache: () => { throw new Error('pred'); } }).build();
  const calls = [1, 2].map(() => p.execute(async () => 42));
  const results = await Promise.allSettled(calls);
  for (const r of results) assert.match(String(r.status === 'rejected' && (r.reason as Error).message), /pred/);
});

test('breaker: a throwing breakDurationGenerator falls back to breakDuration and keeps the original error', async () => {
  const time = new FakeTimeProvider();
  const st = new CircuitBreakerStateProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ consecutiveFailures: 1, breakDuration: 1000, stateProvider: st, breakDurationGenerator: () => { throw new Error('gen'); } })
    .build();
  await assert.rejects(p.execute(() => { throw new Error('original'); }), /original/);
  assert.equal(st.circuitState, 'open');
  assert.equal(st.snapshot().openUntil, time.now() + 1000);
  await time.advance(1000);
  assert.equal(await p.execute(() => 'ok'), 'ok');
});

test('breaker: trip() from an extension does not lift manual isolation', async () => {
  const time = new FakeTimeProvider();
  const mc = new CircuitBreakerManualControl();
  const st = new CircuitBreakerStateProvider();
  let handle!: { trip(reason?: unknown): Promise<void> };
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ manualControl: mc, stateProvider: st, breakDuration: 1000, extensions: [(h) => void (handle = h)] })
    .build();
  await mc.isolate();
  await handle.trip(new Error('health'));
  await time.advance(1500);
  assert.equal(st.circuitState, 'isolated');
  await assert.rejects(p.execute(() => 'ran'), { name: 'IsolatedCircuitError' });
});

test('breaker: a shared-state peer cannot close a manually isolated circuit', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryCircuitStateStore();
  const mc = new CircuitBreakerManualControl();
  const st = new CircuitBreakerStateProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ manualControl: mc, stateProvider: st, breakDuration: 1000, extensions: [sharedCircuitState({ store, key: 'k', syncInterval: 100 })] })
    .build();
  const release = await mc.isolate();
  await time.advance(10);
  store.set('k', { state: 'closed', openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, updatedAt: time.now() + 1 });
  await time.advance(200);
  assert.equal(st.circuitState, 'isolated');
  await assert.rejects(p.execute(() => 'ran'), { name: 'IsolatedCircuitError' });
  await release();
  assert.equal(await p.execute(() => 'ran'), 'ran');
});

test('gradient2: a first rtt of 0 ms does not pin the baseline', () => {
  const run = (first: number) => {
    const g = gradient2();
    let limit = g.onSample({ rtt: first, inflight: 100, dropped: false, limit: 100 });
    for (let i = 0; i < 2000; i++) limit = Math.min(1000, Math.max(1, g.onSample({ rtt: 10, inflight: Math.ceil(limit), dropped: false, limit })));
    return limit;
  };
  assert.ok(run(0) > 400, `limit ${run(0)}`);
  assert.ok(Math.abs(run(0) - run(10)) < 5);
});
