import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BrokenCircuitError, CircuitBreakerManualControl, CircuitBreakerStateProvider, IsolatedCircuitError, ResiliencePipelineBuilder } from '../src/index.ts';
import type { CircuitBreakerPolicy } from '../src/circuit-breaker.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const httpFail = (status: number) => (): never => {
  throw Object.assign(new Error(`HTTP ${status}`), { status });
};

test('breaker policy: a custom policy decides when to open and its history is snapshotted', async () => {
  // Opens after 2 failures in total, ever (a toy rule the built-ins can't express).
  let failures = 0;
  const policy: CircuitBreakerPolicy = {
    record: (failed) => void (failures += failed ? 1 : 0),
    shouldOpen: () => failures >= 2,
    reset: () => void (failures = 0),
    snapshot: () => failures,
    restore: (s) => void (failures = typeof s === 'number' ? s : 0),
  };
  const state = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder().addCircuitBreaker({ breaker: policy, stateProvider: state }).build();
  await assert.rejects(pipeline.execute(httpFail(500)));
  await pipeline.execute(() => 1);
  assert.equal(state.snapshot().window, 1);
  assert.deepEqual(state.metrics, { total: 0, failureCount: 0, failureRate: 0 }, 'no metrics() -> zeros');
  await assert.rejects(pipeline.execute(httpFail(500)));
  assert.equal(state.circuitState, 'open');

  // restore() receives initialState.window
  const resumed = new CircuitBreakerStateProvider();
  const policy2 = { ...policy };
  new ResiliencePipelineBuilder().addCircuitBreaker({ breaker: policy2, stateProvider: resumed, initialState: { state: 'closed', openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, window: 1 } }).build();
  assert.equal(failures, 1);
});

test('half-open success threshold: 0.8 of 5 probes must succeed', async () => {
  const time = new FakeTimeProvider();
  const run = async (probeFailures: number) => {
    const state = new CircuitBreakerStateProvider();
    const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
      .addCircuitBreaker({ stateProvider: state, failureRatio: 0.5, minimumThroughput: 1, breakDuration: '1s', halfOpenMaxCalls: 5, halfOpenSuccessThreshold: 0.8 })
      .build();
    await assert.rejects(pipeline.execute(httpFail(500)));
    await time.advance('1s');
    await Promise.allSettled([0, 1, 2, 3, 4].map((i) => pipeline.execute(i < probeFailures ? httpFail(500) : () => i)));
    return state.circuitState;
  };
  assert.equal(await run(1), 'closed', '4/5 succeeded');
  assert.equal(await run(2), 'open', '3/5 < 0.8 (default rule would close: 2/5 < failureRatio 0.5)');
});

test('manual control: isolations stack and close() lifts all of them', async () => {
  const control = new CircuitBreakerManualControl();
  const state = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder().addCircuitBreaker({ manualControl: control, stateProvider: state }).build();
  const releaseA = await control.isolate();
  const releaseB = await control.isolate();
  await releaseA();
  await releaseA(); // idempotent
  assert.equal(control.isIsolated, true);
  await assert.rejects(pipeline.execute(() => 1), IsolatedCircuitError);
  await releaseB();
  assert.equal(control.isIsolated, false);
  assert.equal(state.circuitState, 'closed');

  const releaseC = await control.isolate();
  await control.isolate();
  await control.close();
  assert.equal(state.circuitState, 'closed');
  await control.isolate();
  await releaseC(); // dropped by close(): must not lift the new isolation
  assert.equal(state.circuitState, 'isolated');
});

test('snapshot carries the count window across a restart (JSON round trip)', async () => {
  const opts = { samplingCount: 4, minimumThroughput: 4, failureRatio: 0.5 };
  const first = new CircuitBreakerStateProvider();
  const before = new ResiliencePipelineBuilder().addCircuitBreaker({ ...opts, stateProvider: first }).build();
  await before.execute(() => 1);
  await assert.rejects(before.execute(httpFail(500)));
  await before.execute(() => 1);
  const saved = JSON.parse(JSON.stringify(first.snapshot()));
  assert.deepEqual(saved.window, [0, 1, 0]);

  const resumed = new CircuitBreakerStateProvider();
  const after = new ResiliencePipelineBuilder().addCircuitBreaker({ ...opts, stateProvider: resumed, initialState: saved }).build();
  assert.equal(resumed.metrics.total, 3);
  await assert.rejects(after.execute(httpFail(500))); // 2/4 failed
  assert.equal(resumed.circuitState, 'open');
  await assert.rejects(after.execute(() => 1), BrokenCircuitError);
});

test('snapshot carries time-window buckets; expired and malformed ones are dropped', async () => {
  const time = new FakeTimeProvider();
  const opts = { samplingDuration: '10s', minimumThroughput: 3, failureRatio: 0.5 } as const;
  const first = new CircuitBreakerStateProvider();
  const before = new ResiliencePipelineBuilder({ timeProvider: time }).addCircuitBreaker({ ...opts, stateProvider: first }).build();
  await assert.rejects(before.execute(httpFail(500)));
  await before.execute(() => 1);
  const saved = JSON.parse(JSON.stringify(first.snapshot()));
  assert.deepEqual(saved.window, [[time.now(), 2, 1]]);

  const resumed = new CircuitBreakerStateProvider();
  const after = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ ...opts, stateProvider: resumed, initialState: { ...saved, window: [...saved.window, 'junk', [1, 1, 5]] } })
    .build();
  assert.equal(resumed.metrics.total, 2);
  await time.advance('10s');
  assert.equal(resumed.metrics.total, 0, 'restored bucket expires like a live one');

  // A count-window snapshot fed to a time window is ignored, not misread.
  const other = new CircuitBreakerStateProvider();
  new ResiliencePipelineBuilder({ timeProvider: time }).addCircuitBreaker({ ...opts, stateProvider: other, initialState: { ...saved, window: [1, 1, 1] } }).build();
  assert.equal(other.metrics.total, 0);
});
