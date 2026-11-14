import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readdirSync, readFileSync } from 'node:fs';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  CircuitBreakerStateProvider,
  NonRetryableError,
  ResiliencePipelineBuilder,
  TimeoutRejectedError,
  onTelemetry,
  retry,
  withTimeout,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const never = () => new Promise<never>(() => {});
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const httpFail = (status: number) => (): never => {
  throw Object.assign(new Error(`HTTP ${status}`), { status });
};

// --- FakeTimeProvider ---

test('FakeTimeProvider: 4 timeouts of 1s and 3 retry delays of 10s finish without waiting', async () => {
  const time = new FakeTimeProvider();
  const delays: number[] = [];
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
    .addRetry({ delay: '10s', maxRetryAttempts: 3, onRetry: (a) => void delays.push(a.retryDelay) })
    .addTimeout('1s')
    .build();
  const started = performance.now();
  const start = time.now();
  const result = assert.rejects(pipeline.execute(never), TimeoutRejectedError); // attach before running timers
  await time.runAll();
  await result;
  assert.deepEqual(delays, [10_000, 10_000, 10_000]);
  assert.equal(time.now() - start, 4 * 1000 + 3 * 10_000, 'virtual time moved 34s');
  assert.ok(performance.now() - started < 1000, 'real time stayed small');
});

test('FakeTimeProvider: advance() opens the half-open window of a circuit breaker', async () => {
  const time = new FakeTimeProvider();
  const state = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ stateProvider: state, failureRatio: 1, minimumThroughput: 1, breakDuration: '30s' })
    .build();
  await assert.rejects(pipeline.execute(httpFail(500)));
  await time.advance('29s');
  await assert.rejects(pipeline.execute(() => 1), BrokenCircuitError);
  await time.advance('1s');
  assert.equal(await pipeline.execute(() => 'probe'), 'probe');
  assert.equal(state.circuitState, 'closed');
});

// --- consecutive failures ---

test('circuit breaker: consecutiveFailures opens after N failures in a row; a success resets the count', async () => {
  const state = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder().addCircuitBreaker({ stateProvider: state, consecutiveFailures: 3, minimumThroughput: 1 }).build();
  await assert.rejects(pipeline.execute(httpFail(500)));
  await assert.rejects(pipeline.execute(httpFail(500)));
  await pipeline.execute(() => 'ok'); // reset
  await assert.rejects(pipeline.execute(httpFail(500)));
  await assert.rejects(pipeline.execute(httpFail(500)));
  assert.equal(state.circuitState, 'closed', 'ratio check is off when only consecutiveFailures is set');
  await assert.rejects(pipeline.execute(httpFail(500)));
  assert.equal(state.circuitState, 'open');
});

// --- snapshot / initialState ---

test('circuit breaker: a snapshot survives JSON and resumes an open circuit in a new process', async () => {
  const time = new FakeTimeProvider();
  const first = new CircuitBreakerStateProvider();
  const before = new ResiliencePipelineBuilder({ timeProvider: time }).addCircuitBreaker({ stateProvider: first, failureRatio: 1, minimumThroughput: 1, breakDuration: '1m' }).build();
  await assert.rejects(before.execute(httpFail(503)));
  const saved = JSON.stringify(first.snapshot()); // e.g. into Redis / KV on onOpened

  const resumed = new CircuitBreakerStateProvider();
  const after = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ stateProvider: resumed, failureRatio: 1, minimumThroughput: 1, breakDuration: '1m', initialState: JSON.parse(saved) })
    .build();
  assert.equal(resumed.circuitState, 'open');
  await assert.rejects(after.execute(() => 1), BrokenCircuitError);
  await time.advance('1m');
  assert.equal(await after.execute(() => 'probe'), 'probe');
});

// --- cooperative timeout ---

test('timeout: cooperative waits for the callback; the default gives up immediately', async () => {
  const time = new FakeTimeProvider();
  let finish = () => {};
  const ignoresSignal = () => new Promise<void>((resolve) => (finish = resolve));
  const aggressive = new ResiliencePipelineBuilder({ timeProvider: time }).addTimeout('10ms').build().executeOutcome(ignoresSignal);
  await time.advance(10);
  const first = await aggressive; // settles while the callback is still running
  assert.ok(!first.ok && first.error instanceof TimeoutRejectedError, 'aggressive');
  finish();

  let settled = false;
  const cooperative = new ResiliencePipelineBuilder({ timeProvider: time }).addTimeout({ timeout: '10ms', cooperative: true }).build().executeOutcome(ignoresSignal).finally(() => (settled = true));
  await time.advance('1h');
  assert.equal(settled, false, 'cooperative waits for the callback');
  finish();
  const second = await cooperative;
  assert.ok(!second.ok && second.error instanceof TimeoutRejectedError);
});

// --- NonRetryableError & function API ---

test('NonRetryableError stops retrying and the caller gets the original error', async () => {
  class CardDeclined extends Error {}
  let calls = 0;
  const err = await retry(
    () => {
      calls++;
      throw new NonRetryableError(new CardDeclined('declined'));
    },
    { delay: 0, maxRetryAttempts: 5 },
  ).catch((e) => e);
  assert.equal(calls, 1);
  assert.ok(err instanceof CardDeclined, 'unwrapped');
});

test('retry() and withTimeout() one-liners', async () => {
  let calls = 0;
  assert.equal(await retry(() => (++calls < 3 ? httpFail(503)() : 'ok'), { delay: 0 }), 'ok');
  assert.equal(calls, 3);
  await assert.rejects(withTimeout(({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason))), '10ms'), TimeoutRejectedError);
  const time = new FakeTimeProvider();
  const slow = assert.rejects(retry(httpFail(503), { delay: '1h', maxRetryAttempts: 2, timeProvider: time }), /503/);
  await time.runAll();
  await slow;
});

// --- telemetry listener ---

test('onTelemetry receives events in any runtime and can unsubscribe', async () => {
  const seen: string[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnRetry' && seen.push(`${e.source.pipelineName}:${e.event}`)));
  await assert.rejects(new ResiliencePipelineBuilder({ name: 'p' }).addRetry({ delay: 0, maxRetryAttempts: 1 }).build().execute(httpFail(500)));
  off();
  await assert.rejects(new ResiliencePipelineBuilder({ name: 'p' }).addRetry({ delay: 0, maxRetryAttempts: 1 }).build().execute(httpFail(500)));
  assert.deepEqual(seen, ['p:OnRetry']);
});

// --- runtime portability ---

test('core has no static node: imports (loads in browsers / edge runtimes)', () => {
  const offenders = readdirSync(new URL('../src/', import.meta.url))
    .filter((f) => f.endsWith('.ts') && f !== 'nestjs.ts')
    .filter((f) => /from ['"]node:/.test(readFileSync(new URL(`../src/${f}`, import.meta.url), 'utf8')));
  assert.deepEqual(offenders, []);
});

test('core works without Node builtins (simulated browser): no ambient context, still resilient', () => {
  const script = `
    delete globalThis.process.getBuiltinModule;
    const m = await import(${JSON.stringify(new URL('../src/index.ts', import.meta.url).href)});
    let n = 0;
    const r = await new m.ResiliencePipelineBuilder().addRetry({ delay: 0 }).addTimeout('1s').build().execute(() => (++n < 2 ? Promise.reject(new Error('x')) : 'ok'));
    console.log(JSON.stringify({ r, n, ambient: m.getResilienceSignal() === undefined }));
  `;
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' });
  assert.deepEqual(JSON.parse(out), { r: 'ok', n: 2, ambient: true });
});

// --- retry budget, warm-up ---

test('retry budget: retries are capped relative to traffic across pipelines sharing the budget', async () => {
  const { RetryBudget } = await import('../src/index.ts');
  const time = new FakeTimeProvider();
  const budget = new RetryBudget({ ratio: 0.5, minRetriesPerSecond: 0, window: '10s', timeProvider: time });
  let exhausted = 0;
  const make = () =>
    new ResiliencePipelineBuilder({ timeProvider: time }).addRetry({ delay: 0, maxRetryAttempts: 3, budget, onBudgetExhausted: () => void exhausted++ }).build();
  const [a, b] = [make(), make()];
  let calls = 0;
  const failing = () => (calls++, httpFail(503)());
  for (let i = 0; i < 4; i++) await assert.rejects((i % 2 ? a : b).execute(failing));
  // 4 requests × ratio 0.5 = 2 retries allowed in total, instead of 4 × 3 = 12.
  assert.equal(calls, 4 + 2);
  assert.ok(exhausted >= 2);
  assert.deepEqual(budget.stats(), { requests: 4, retries: 2 });
  await time.advance('10s');
  assert.deepEqual(budget.stats(), { requests: 0, retries: 0 }, 'window slid');
});

test('circuit breaker: warmUpDuration keeps it closed right after start', async () => {
  const time = new FakeTimeProvider();
  const state = new CircuitBreakerStateProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ stateProvider: state, failureRatio: 0.5, minimumThroughput: 1, warmUpDuration: '1m' })
    .build();
  for (let i = 0; i < 3; i++) await assert.rejects(p.execute(httpFail(500)));
  assert.equal(state.circuitState, 'closed', 'warming up');
  await time.advance('1m');
  await assert.rejects(p.execute(httpFail(500)));
  assert.equal(state.circuitState, 'open');
});
