import assert from 'node:assert/strict';
import diagnostics_channel from 'node:diagnostics_channel';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  CircuitBreakerManualControl,
  CircuitBreakerStateProvider,
  IsolatedCircuitError,
  Outcome,
  PredicateBuilder,
  RateLimiterRejectedError,
  ResiliencePipeline,
  ResiliencePipelineBuilder,
  ResiliencePipelineRegistry,
  TELEMETRY_CHANNEL,
  type TelemetryEvent,
  TimeoutRejectedError,
  computeDelay,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const boom = (): never => {
  throw new Error('boom');
};

// --- pipeline ---

test('pipeline: executeOutcome returns failures without throwing; empty pipeline passes through', async () => {
  const outcome = await ResiliencePipeline.empty.executeOutcome(boom);
  assert.equal(outcome.ok, false);
  assert.equal(await ResiliencePipeline.empty.execute(() => 42), 42);
});

test('pipeline: an already-aborted signal skips the callback', async () => {
  let called = false;
  const ac = new AbortController();
  ac.abort(new Error('cancelled'));
  await assert.rejects(new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build().execute(() => (called = true), { signal: ac.signal }), /cancelled/);
  assert.equal(called, false);
});

test('pipeline: invalid options throw on build', () => {
  assert.throws(() => new ResiliencePipelineBuilder().addRetry({ maxRetryAttempts: -1 }).build(), RangeError);
  assert.throws(() => new ResiliencePipelineBuilder().addCircuitBreaker({ failureRatio: 2 }).build(), RangeError);
});

test('pipeline: strategies added first are outermost (retry around timeout retries each timed-out attempt)', async () => {
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 2 }).addTimeout(20).build();
  const r = await p.execute(async () => {
    if (++calls < 3) await new Promise(() => {}); // never answers: only the timeout ends it
    return 'done';
  });
  assert.equal(r, 'done');
  assert.equal(calls, 3);
});

test('pipeline: addPipeline nests another pipeline', async () => {
  let calls = 0;
  const inner = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 1 }).build();
  const outer = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 1 }).addPipeline(inner).build();
  await assert.rejects(outer.execute(() => (calls++, boom())), /boom/);
  assert.equal(calls, 4);
});

// --- retry ---

test('retry: retries until success', async () => {
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build();
  assert.equal(await p.execute(() => (++calls < 3 ? boom() : 'ok')), 'ok');
  assert.equal(calls, 3);
});

test('retry: rethrows the last error after maxRetryAttempts', async () => {
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
  await assert.rejects(p.execute(() => (calls++, boom())), /boom/);
  assert.equal(calls, 3);
});

test('retry: handles results via PredicateBuilder with exponential delays', async () => {
  type Res = { status: number };
  const delays: number[] = [];
  const p = new ResiliencePipelineBuilder<Res>()
    .addRetry({
      delay: 1,
      backoffType: 'exponential',
      shouldHandle: new PredicateBuilder<Res>().handleResult((r) => r.status === 503),
      onRetry: (a) => void delays.push(a.retryDelay),
    })
    .build();
  let n = 0;
  const r = await p.execute(() => ({ status: ++n < 4 ? 503 : 200 }));
  assert.equal(r.status, 200);
  assert.deepEqual(delays, [1, 2, 4]);
});

test('retry: PredicateBuilder only handles the listed error types', async () => {
  class HttpError extends Error {
    status: number;
    constructor(status: number) {
      super(`HTTP ${status}`);
      this.status = status;
    }
  }
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, shouldHandle: new PredicateBuilder().handle(HttpError, (e) => e.status >= 500) }).build();
  await assert.rejects(p.execute(() => (calls++, Promise.reject(new HttpError(404)))), /404/);
  assert.equal(calls, 1);
});

test('retry: delayGenerator overrides the computed delay', async () => {
  const delays: number[] = [];
  const p = new ResiliencePipelineBuilder().addRetry({ maxRetryAttempts: 2, delayGenerator: () => 3, onRetry: (a) => void delays.push(a.retryDelay) }).build();
  await assert.rejects(p.execute(boom));
  assert.deepEqual(delays, [3, 3]);
});

test('retry: an outer abort stops waiting between attempts', async () => {
  let calls = 0;
  const ac = new AbortController();
  setTimeout(() => ac.abort(new Error('stop')), 20);
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 10_000, maxRetryAttempts: 5 }).build();
  await assert.rejects(p.execute(() => (calls++, boom()), { signal: ac.signal }), /stop/);
  assert.equal(calls, 1);
});

test('computeDelay: backoff types, maxDelay and jitter', () => {
  assert.equal(computeDelay('constant', false, 5, 100, undefined), 100);
  assert.equal(computeDelay('linear', false, 2, 100, undefined), 300);
  assert.equal(computeDelay('exponential', false, 3, 100, undefined), 800);
  assert.equal(computeDelay('exponential', false, 10, 100, 1000), 1000);
  for (let i = 0; i < 100; i++) {
    const d = computeDelay('constant', true, 0, 100, undefined);
    assert.ok(d >= 75 && d <= 125, `jitter out of range: ${d}`);
  }
  const state = { prev: 0 };
  const jittered = [0, 1, 2, 3, 4].map((n) => computeDelay('exponential', true, n, 100, undefined, state));
  assert.ok(jittered.every((d) => d > 0 && Number.isFinite(d)), String(jittered));
  assert.ok(jittered[4]! > jittered[0]!, 'decorrelated jitter should grow');
});

// --- timeout ---

test('timeout: rejects with TimeoutRejectedError and aborts the callback signal', async () => {
  let aborted = false;
  let timedOut = 0;
  const p = new ResiliencePipelineBuilder().addTimeout({ timeout: 20, onTimeout: (a) => void (timedOut = a.timeout) }).build();
  await assert.rejects(
    p.execute(async ({ signal }) => {
      signal.addEventListener('abort', () => (aborted = true));
      await new Promise(() => {});
    }),
    TimeoutRejectedError,
  );
  assert.ok(aborted);
  assert.equal(timedOut, 20);
});

test('timeout: an outer abort is reported as the abort, not as a timeout', async () => {
  const ac = new AbortController();
  setTimeout(() => ac.abort(new Error('user cancelled')), 10);
  const p = new ResiliencePipelineBuilder().addTimeout(1000).build();
  await assert.rejects(p.execute(({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason))), { signal: ac.signal }), /user cancelled/);
});

// --- circuit breaker ---

function breaker(extra: object = {}) {
  const events: string[] = [];
  const stateProvider = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder()
    .addCircuitBreaker({
      failureRatio: 0.5,
      minimumThroughput: 2,
      samplingDuration: 1000,
      breakDuration: 30,
      stateProvider,
      onOpened: () => void events.push('open'),
      onHalfOpened: () => void events.push('half-open'),
      onClosed: () => void events.push('closed'),
      ...extra,
    })
    .build();
  return { events, stateProvider, pipeline };
}

test('circuit breaker: opens, rejects, half-opens after the break and closes on a good probe', async () => {
  const { events, stateProvider, pipeline } = breaker();
  await assert.rejects(pipeline.execute(boom), /boom/);
  assert.equal(stateProvider.circuitState, 'closed', 'below minimum throughput');
  await assert.rejects(pipeline.execute(boom), /boom/);
  assert.equal(stateProvider.circuitState, 'open');
  const err = await pipeline.execute(() => 'never').catch((e) => e);
  assert.ok(err instanceof BrokenCircuitError && err.retryAfter! > 0);
  assert.match(String((err.cause as Error).message), /boom/);
  await sleep(40);
  assert.equal(await pipeline.execute(() => 'ok'), 'ok');
  assert.equal(stateProvider.circuitState, 'closed');
  assert.deepEqual(events, ['open', 'half-open', 'closed']);
});

test('circuit breaker: a failed probe re-opens; only one probe at a time', async () => {
  const { stateProvider, pipeline } = breaker();
  await assert.rejects(pipeline.execute(boom));
  await assert.rejects(pipeline.execute(boom));
  await sleep(40);
  const probe = pipeline.execute(async () => (await sleep(20), boom()));
  await assert.rejects(pipeline.execute(() => 'concurrent'), BrokenCircuitError);
  await assert.rejects(probe, /boom/);
  assert.equal(stateProvider.circuitState, 'open');
});

test('circuit breaker: manual isolate and close', async () => {
  const manualControl = new CircuitBreakerManualControl();
  const { stateProvider, pipeline } = breaker({ manualControl });
  await manualControl.isolate();
  assert.equal(stateProvider.circuitState, 'isolated');
  await assert.rejects(pipeline.execute(() => 1), IsolatedCircuitError);
  await manualControl.close();
  assert.equal(await pipeline.execute(() => 1), 1);
});

test('circuit breaker: a state provider can only be attached once', () => {
  const stateProvider = new CircuitBreakerStateProvider();
  new ResiliencePipelineBuilder().addCircuitBreaker({ stateProvider }).build();
  assert.throws(() => new ResiliencePipelineBuilder().addCircuitBreaker({ stateProvider }).build());
});

// --- fallback ---

test('fallback: substitutes a value for handled failures only', async () => {
  const p = new ResiliencePipelineBuilder<string>().addFallback({ fallbackAction: () => 'cached', shouldHandle: new PredicateBuilder().handle(TypeError) }).build();
  assert.equal(await p.execute(() => Promise.reject(new TypeError('x'))), 'cached');
  await assert.rejects(p.execute(boom), /boom/);
});

// --- hedging ---

test('hedging (latency mode): a fast hedged attempt wins and the slow primary is aborted', async () => {
  const time = new FakeTimeProvider();
  let n = 0;
  let primary: AbortSignal | undefined;
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10 }).build();
  const done = p.execute(async ({ signal }) => {
    if (n++ === 0) {
      primary = signal;
      await new Promise(() => {}); // never answers
    }
    return 'fast';
  });
  await time.advance(10);
  assert.equal(await done, 'fast');
  assert.equal(primary!.aborted, true);
});

test('hedging (fallback mode): next attempt only starts after a failure', async () => {
  const log: string[] = [];
  let n = 0;
  const p = new ResiliencePipelineBuilder().addHedging({ delay: -1, maxHedgedAttempts: 2 }).build();
  const r = await p.execute(async () => {
    const i = n++;
    log.push(`start ${i}`);
    await sleep(0);
    log.push(`end ${i}`);
    if (i < 2) throw new Error(`e${i}`);
    return i;
  });
  assert.equal(r, 2);
  assert.deepEqual(log, ['start 0', 'end 0', 'start 1', 'end 1', 'start 2', 'end 2']);
});

test('hedging (parallel mode) with actionGenerator; all failing returns the primary error', async () => {
  const started: number[] = [];
  const p = new ResiliencePipelineBuilder()
    .addHedging({
      delay: 0,
      maxHedgedAttempts: 2,
      actionGenerator: ({ attemptNumber }) => async () => {
        started.push(attemptNumber);
        throw new Error(`hedge${attemptNumber}`);
      },
    })
    .build();
  await assert.rejects(
    p.execute(async () => {
      started.push(0);
      await sleep(0); // the primary fails last
      throw new Error('primary');
    }),
    /primary/,
  );
  assert.deepEqual(started.sort(), [0, 1, 2]);
});

// --- rate limiting ---

test('concurrency limiter: runs permitLimit at once, queues queueLimit, rejects the rest', async () => {
  let active = 0;
  let peak = 0;
  const p = new ResiliencePipelineBuilder().addConcurrencyLimiter(1, 1).build();
  const work = () =>
    p.execute(async () => {
      peak = Math.max(peak, ++active);
      await sleep(20);
      active--;
      return 'ok';
    });
  const results = await Promise.allSettled([work(), work(), work()]);
  assert.deepEqual(
    results.map((r) => r.status),
    ['fulfilled', 'fulfilled', 'rejected'],
  );
  assert.ok((results[2] as PromiseRejectedResult).reason instanceof RateLimiterRejectedError);
  assert.equal(peak, 1);
});

test('concurrency limiter: an aborted waiter leaves the queue', async () => {
  const p = new ResiliencePipelineBuilder().addConcurrencyLimiter(1, 1).build();
  const ac = new AbortController();
  const first = p.execute(() => sleep(30));
  const queued = p.execute(() => 'never', { signal: ac.signal });
  ac.abort(new Error('gave up'));
  await assert.rejects(queued, /gave up/);
  await first;
  assert.equal(await p.execute(() => 'free'), 'free');
});

test('token bucket: rejects when empty with retryAfter, refills after the period', async () => {
  const p = new ResiliencePipelineBuilder().addRateLimiter({ tokenLimit: 2, replenishmentPeriod: 50 }).build();
  await p.execute(() => 1);
  await p.execute(() => 1);
  const err = await p.execute(() => 1).catch((e) => e);
  assert.ok(err instanceof RateLimiterRejectedError && err.retryAfter! > 0 && err.retryAfter! <= 50);
  await sleep(60);
  assert.equal(await p.execute(() => 'again'), 'again');
});

// --- chaos ---

test('chaos: fault, outcome and latency injection respect rate and enabled', async () => {
  const fault = new ResiliencePipelineBuilder().addChaosFault({ injectionRate: 1, fault: new Error('chaos') }).build();
  await assert.rejects(fault.execute(() => 1), /chaos/);

  const off = new ResiliencePipelineBuilder().addChaosFault({ injectionRate: 1, enabled: false, fault: new Error('chaos') }).build();
  assert.equal(await off.execute(() => 1), 1);

  const outcome = new ResiliencePipelineBuilder<number>().addChaosOutcome({ injectionRate: 1, outcome: Outcome.fromResult(99) }).build();
  assert.equal(await outcome.execute(() => 1), 99);

  const time = new FakeTimeProvider();
  const latency = new ResiliencePipelineBuilder({ timeProvider: time }).addChaosLatency({ injectionRate: 1, latency: 30 }).build();
  let settled = false;
  const delayed = latency.execute(() => 1).finally(() => (settled = true));
  await time.advance(29);
  assert.equal(settled, false, 'still waiting out the injected latency');
  await time.advance(1);
  assert.equal(await delayed, 1);
});

// --- registry & telemetry ---

test('registry: builds once per key and shares state', () => {
  const registry = new ResiliencePipelineRegistry();
  let builds = 0;
  const configure = (b: ResiliencePipelineBuilder) => (builds++, b.addRetry());
  assert.equal(registry.getOrAddPipeline('a', configure), registry.getOrAddPipeline('a', configure));
  assert.equal(builds, 1);
  assert.throws(() => registry.getPipeline('missing'));
  assert.ok(registry.tryAddBuilder('b', (b) => b.addTimeout(100)));
  assert.equal(registry.tryAddBuilder('b', (b) => b.addTimeout(100)), false);
  assert.ok(registry.getPipeline('b'));
});

test('telemetry: events are published on the diagnostics channel', async () => {
  const events: TelemetryEvent[] = [];
  const onMessage = (m: unknown) => void events.push(m as TelemetryEvent);
  diagnostics_channel.subscribe(TELEMETRY_CHANNEL, onMessage);
  try {
    const p = new ResiliencePipelineBuilder({ name: 'payments' }).addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
    await assert.rejects(p.execute(boom));
  } finally {
    diagnostics_channel.unsubscribe(TELEMETRY_CHANNEL, onMessage);
  }
  assert.deepEqual(events.map((e) => e.event), ['OnPipelineExecuting', 'OnExecutionAttempt', 'OnRetry', 'OnExecutionAttempt', 'OnRetry', 'OnExecutionAttempt', 'OnRetryExhausted', 'OnStrategyExecuted', 'OnPipelineExecuted']);
  assert.deepEqual(events[2]!.source, { pipelineName: 'payments', strategyName: 'Retry' });
  const executed = events.at(-1)!.args as { duration: number; outcome: { ok: boolean } };
  assert.equal(executed.outcome.ok, false);
  assert.ok(executed.duration >= 0);
});
