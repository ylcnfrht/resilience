// Covers the items from tenace's API reference that resilience-pipeline was missing.
import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  Outcome,
  type PipelineErrors,
  ResiliencePipelineBuilder,
  TimeoutRejectedError,
  UnexpectedError,
  backoff,
  instrument,
  onTelemetry,
  registerResultAdapter,
  unregisterResultAdapter,
  waitFor,
} from '../src/index.ts';
import { errorType } from '../src/otel.ts';
import * as outcomeEntry from '../src/outcome.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
class NotFound extends Error {
  readonly code = 'NOT_FOUND' as const;
}
class PaymentUnavailable extends Error {
  readonly code = 'PAYMENT_UNAVAILABLE' as const;
}

test('Outcome.try / all / partition / unwrap / unwrapErr', async () => {
  assert.deepEqual(Outcome.try(() => 1), { ok: true, result: 1 });
  const thrown = Outcome.try(() => JSON.parse('{'));
  assert.equal(thrown.ok, false);
  const mapped = await Outcome.try({ try: () => Promise.reject(new Error('db')), catch: (e) => new NotFound(String(e)) });
  assert.ok(!mapped.ok && mapped.error instanceof NotFound);

  const all = Outcome.all([Outcome.fromResult(1), Outcome.fromResult('a')] as const);
  assert.deepEqual(all, { ok: true, result: [1, 'a'] });
  assert.equal(Outcome.all([Outcome.fromResult(1), Outcome.fromError(new NotFound('x'))]).ok, false);

  const { values, errors } = Outcome.partition([Outcome.fromResult(1), Outcome.fromError(new NotFound('x')), Outcome.fromResult(2)]);
  assert.deepEqual(values, [1, 2]);
  assert.equal(errors.length, 1);

  assert.equal(Outcome.unwrap(Outcome.fromResult(5)), 5);
  assert.throws(() => Outcome.unwrap(Outcome.fromError(new NotFound('gone'))), NotFound);
  const nf: Outcome<number, NotFound | TimeoutRejectedError> = Outcome.fromError(new NotFound('x'));
  assert.ok(Outcome.unwrapErr(nf, 'NOT_FOUND') instanceof NotFound);
  assert.throws(() => Outcome.unwrapErr(nf, 'TIMEOUT'), /expected code 'TIMEOUT'/);
});

test('foreign Result types via registerResultAdapter (neverthrow-like)', async () => {
  class Ok<T> {
    readonly value: T;
    constructor(value: T) {
      this.value = value;
    }
  }
  class Err<E> {
    readonly error: E;
    constructor(error: E) {
      this.error = error;
    }
  }
  registerResultAdapter('fake-neverthrow', {
    is: (v) => v instanceof Ok || v instanceof Err,
    toOutcome: (v) => (v instanceof Ok ? Outcome.fromResult(v.value) : Outcome.fromError((v as Err<unknown>).error)),
  });
  try {
    const p = new ResiliencePipelineBuilder().build();
    assert.deepEqual(await p.settle(async () => new Ok('v') as unknown as string), { ok: true, result: 'v' });
    const failed = await p.settle(async () => new Err(new NotFound('x')) as unknown as string);
    assert.ok(!failed.ok && failed.error instanceof NotFound, 'returned foreign error stays typed, not UnexpectedError');
    assert.deepEqual(Outcome.from(new Ok(3)), { ok: true, result: 3 });
  } finally {
    unregisterResultAdapter('fake-neverthrow');
  }
});

test('addFallbackOn removes only the listed codes; addMapError replaces inner errors; isError()', async () => {
  const p = new ResiliencePipelineBuilder().addFallbackOn(['TIMEOUT'], () => 'cached').addTimeout('10ms').addCircuitBreaker().build();
  const o = await p.settle(() => sleep(50).then(() => 'fresh'));
  // TIMEOUT is gone from the error type, CIRCUIT_* remain.
  assert.equal(Outcome.match(o, { ok: (v) => v, CIRCUIT_OPEN: () => 'x', CIRCUIT_ISOLATED: () => 'x', UNEXPECTED: () => 'bug' }), 'cached');
  // @ts-expect-error — CIRCUIT_OPEN is still possible
  assert.ok(Outcome.match(o, { ok: (v) => v, CIRCUIT_ISOLATED: () => 'x', UNEXPECTED: () => 'bug' }));

  const mapped = new ResiliencePipelineBuilder().addMapError((e) => new PaymentUnavailable('payments down', { cause: e })).addTimeout('10ms').build();
  const m = await mapped.settle(() => sleep(50));
  assert.equal(Outcome.match(m, { ok: () => 'ok', PAYMENT_UNAVAILABLE: (e) => (e.cause as Error).name, UNEXPECTED: () => 'bug' }), 'TimeoutRejectedError');
  assert.ok(!m.ok && mapped.isError(m.error));
  assert.equal(mapped.isError(new Error('random')), false);

  type Errors = PipelineErrors<typeof mapped>;
  const e: Errors = new PaymentUnavailable('x');
  assert.ok(e);
});

test('addStaleFallback serves the last good value on failure, never caches fresh calls', async () => {
  let calls = 0;
  let down = false;
  const p = new ResiliencePipelineBuilder().addStaleFallback({ maxAge: '1m', key: () => 'rates' }).build();
  const call = () => p.execute(() => (calls++, down ? Promise.reject(Object.assign(new Error('503'), { status: 503 })) : Promise.resolve(calls)));
  assert.equal(await call(), 1);
  assert.equal(await call(), 2, 'fresh call, not cached');
  down = true;
  assert.equal(await call(), 2, 'last good value served on failure');
});

test('per-execution name, attributes, clock; per-pipeline instrumentation and telemetry opt-out', async () => {
  const spans: string[] = [];
  const stop = instrument({ execution: (info, run) => (spans.push(`${info.context.operationName}:${JSON.stringify(info.context.attributes)}`), run()) });
  const own: string[] = [];
  try {
    const p = new ResiliencePipelineBuilder({ instrumentation: { attempt: (_i, run) => (own.push('attempt'), run()) } }).build();
    await p.execute(() => 1, { name: 'checkout', attributes: { tenant: 'acme' } });
    const off = new ResiliencePipelineBuilder({ instrumentation: false }).build();
    await off.execute(() => 1, { name: 'skipped' });
  } finally {
    stop();
  }
  assert.deepEqual(spans, ['checkout:{"tenant":"acme"}'], 'global instrumentation saw the named execution, not the opted-out pipeline');
  assert.deepEqual(own, ['attempt']);

  const events: string[] = [];
  const offTelemetry = onTelemetry((e) => void events.push(e.event));
  try {
    await new ResiliencePipelineBuilder({ telemetry: false }).addRetry({ delay: 0, maxRetryAttempts: 1 }).build().execute(() => Promise.reject(Object.assign(new Error('x'), { status: 503 }))).catch(() => {});
  } finally {
    offTelemetry();
  }
  assert.deepEqual(events, []);

  const time = new FakeTimeProvider();
  const p = new ResiliencePipelineBuilder().addRetry({ delay: '1h', maxRetryAttempts: 1 }).build();
  let calls = 0;
  const done = assert.rejects(p.execute(() => (calls++, Promise.reject(Object.assign(new Error('x'), { status: 503 }))), { timeProvider: time }));
  await time.runAll(); // an hour of retry delay, instantly: the per-execution clock was used
  await done;
  assert.equal(calls, 2);
});

test('backoff helpers, errorType, and the lightweight ./outcome entry', async () => {
  assert.deepEqual([0, 1, 2, 3].map(backoff.exponential({ delay: 100, maxDelay: 500 })), [100, 200, 400, 500]);
  assert.deepEqual([0, 1, 2].map(backoff.linear({ delay: '1s' })), [1000, 2000, 3000]);
  assert.equal(backoff.constant({ delay: 50 })({ attemptNumber: 7 }), 50);
  const full = backoff.exponential({ delay: 100, useJitter: 'full', randomizer: () => 0.5 });
  assert.equal(full(2), 200);

  const time = new FakeTimeProvider();
  let polls = 0;
  const waiting = waitFor(() => ++polls === 3, { interval: backoff.exponential({ delay: 10 }), timeout: '1m', timeProvider: time });
  await time.runAll();
  await waiting;

  assert.equal(errorType(new TimeoutRejectedError(1)), 'TIMEOUT');
  assert.equal(errorType(new RangeError('x')), 'RangeError');
  assert.equal(outcomeEntry.Outcome, Outcome);
  assert.equal(outcomeEntry.BrokenCircuitError, BrokenCircuitError);
  assert.equal(outcomeEntry.UnexpectedError, UnexpectedError);
});

test('opossum parity: OnPipelineExecuting at call start, breaker warmingUp, pipeline disposed', async () => {
  const { CircuitBreakerStateProvider, nextTelemetry } = await import('../src/index.ts');
  const time = new FakeTimeProvider();
  const state = new CircuitBreakerStateProvider();
  const p = new ResiliencePipelineBuilder({ name: 'orders', timeProvider: time }).addCircuitBreaker({ stateProvider: state, warmUpDuration: '1m' }).build();
  const breaker = p.getStrategy<import('../src/index.ts').CircuitBreakerStrategy<unknown>>('CircuitBreaker')!;

  const started = nextTelemetry((e) => e.event === 'OnPipelineExecuting');
  let release!: () => void;
  const running = p.execute(() => new Promise<void>((r) => (release = r)));
  assert.equal((await started).source.pipelineName, 'orders', 'reported while the call is still in flight');
  release();
  await running;

  assert.equal(breaker.warmingUp, true);
  assert.equal(state.isWarmingUp, true);
  await time.advance('1m');
  assert.equal(breaker.warmingUp, false);

  assert.equal(p.disposed, false);
  p.dispose();
  assert.equal(p.disposed, true);
});

test('cockatiel parity: fallbackValue, per-strategy success/failure events, backoff.sequence, minimumRps', async () => {
  const p = new ResiliencePipelineBuilder<string>().addFallback({ fallbackValue: 'default' }).build();
  assert.equal(await p.execute(() => Promise.reject(Object.assign(new Error('x'), { status: 503 }))), 'default');

  const seen: string[] = [];
  const off = onTelemetry((e) => {
    if (e.event !== 'OnStrategyExecuted') return;
    const { outcome, duration } = e.args as { outcome: { ok: boolean }; duration: number };
    seen.push(`${e.source.strategyName}:${outcome.ok ? 'success' : 'failure'}:${duration >= 0}`);
  });
  try {
    await new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 0, name: 'r' }).addTimeout({ timeout: '5ms', name: 't' }).build().execute(() => sleep(30)).catch(() => {});
  } finally {
    off();
  }
  assert.deepEqual(seen, ['t:failure:true', 'r:failure:true'], 'inner strategy reports first');

  assert.deepEqual([0, 1, 2, 5].map(backoff.sequence(['100ms', '1s', '5s'])), [100, 1000, 5000, 5000]);

  const { CircuitBreakerStateProvider } = await import('../src/index.ts');
  const time = new FakeTimeProvider();
  const state = new CircuitBreakerStateProvider();
  const quiet = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ stateProvider: state, failureRatio: 0.5, minimumThroughput: 2, samplingDuration: '10s', minimumRps: 1 })
    .build();
  const fail503 = () => Promise.reject(Object.assign(new Error('x'), { status: 503 }));
  for (let i = 0; i < 5; i++) await quiet.execute(fail503).catch(() => {});
  assert.equal(state.circuitState, 'closed', '5 calls in a 10 s window = 0.5 rps < 1: too quiet to judge');
  for (let i = 0; i < 5; i++) await quiet.execute(fail503).catch(() => {});
  assert.equal(state.circuitState, 'open', '10 calls = 1 rps: now the ratio counts');
  assert.throws(() => new ResiliencePipelineBuilder().addCircuitBreaker({ samplingCount: 10, minimumThroughput: 5, minimumRps: 1 }).build(), /minimumRps needs a time window/);
});
