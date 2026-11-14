import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  CircuitBreakerStateProvider,
  PredicateBuilder,
  RateLimiterRejectedError,
  ResiliencePipelineBuilder,
  TimeoutRejectedError,
  computeDelay,
  getResilienceContext,
  getResilienceSignal,
  isClientError,
  toMs,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const defer = <T = void>() => {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
class HttpError extends Error {
  status: number;
  constructor(status: number) {
    super(`HTTP ${status}`);
    this.status = status;
  }
}
const httpFail = (status: number) => (): never => {
  throw new HttpError(status);
};

// --- error classification ---

test('isClientError: 4xx except 408/429, from status, statusCode or response.status', () => {
  assert.equal(isClientError(new HttpError(404)), true);
  assert.equal(isClientError({ statusCode: 422 }), true);
  assert.equal(isClientError({ response: { status: 400 } }), true); // axios
  assert.equal(isClientError(new HttpError(408)), false);
  assert.equal(isClientError(new HttpError(429)), false);
  assert.equal(isClientError(new HttpError(503)), false);
  assert.equal(isClientError(new Error('x')), false);
  assert.equal(isClientError(null), false);
});

test('retry (default): 4xx is not retried, 5xx / 429 / 408 are', async () => {
  for (const [status, expectedCalls] of [[404, 1], [400, 1], [503, 3], [429, 3], [408, 3]] as const) {
    let calls = 0;
    const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
    await assert.rejects(p.execute(() => (calls++, httpFail(status)())));
    assert.equal(calls, expectedCalls, `HTTP ${status}`);
  }
});

test('retry never retries an open circuit, even with a catch-all predicate', async () => {
  let calls = 0;
  const p = new ResiliencePipelineBuilder()
    .addRetry({ delay: 0, maxRetryAttempts: 5, shouldHandle: new PredicateBuilder().handle(Error) })
    .addCircuitBreaker({ failureRatio: 1, minimumThroughput: 1, breakDuration: 10_000 })
    .build();
  await assert.rejects(p.execute(() => (calls++, httpFail(503)())), BrokenCircuitError);
  assert.equal(calls, 1, 'first failure opened the circuit; the retry stopped there instead of hammering it');
});

test('retry (opt-in): a rate-limiter rejection can be retried after its retryAfter', async () => {
  const p = new ResiliencePipelineBuilder()
    .addRetry({
      maxRetryAttempts: 1,
      shouldHandle: new PredicateBuilder().handle(RateLimiterRejectedError),
      delayGenerator: ({ outcome }) => (outcome.ok ? undefined : (outcome.error as RateLimiterRejectedError).retryAfter),
    })
    .addRateLimiter({ tokenLimit: 1, replenishmentPeriod: '40ms' })
    .build();
  await p.execute(() => 1);
  assert.equal(await p.execute(() => 2), 2);
});

test('fallback (default) covers an open circuit but not a 4xx', async () => {
  const p = new ResiliencePipelineBuilder()
    .addFallback({ fallbackAction: () => 'fallback' })
    .addCircuitBreaker({ failureRatio: 1, minimumThroughput: 1, breakDuration: 10_000 })
    .build();
  assert.equal(await p.execute(httpFail(503)), 'fallback'); // failure, opens the circuit
  assert.equal(await p.execute(() => 'never'), 'fallback'); // BrokenCircuitError → fallback
  const q = new ResiliencePipelineBuilder().addFallback({ fallbackAction: () => 'fallback' }).build();
  await assert.rejects(q.execute(httpFail(404)), /404/);
});

// --- circuit breaker ---

function breaker(options: Parameters<ResiliencePipelineBuilder['addCircuitBreaker']>[0]) {
  const stateProvider = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder().addCircuitBreaker({ stateProvider, ...options }).build();
  return { stateProvider, pipeline };
}

test('circuit breaker: client errors and caller aborts are ignored, not counted as successes', async () => {
  const { stateProvider, pipeline } = breaker({ failureRatio: 0.5, minimumThroughput: 2, breakDuration: 1000 });
  await assert.rejects(pipeline.execute(httpFail(404)));
  const ac = new AbortController();
  ac.abort();
  await assert.rejects(pipeline.execute(() => 1, { signal: ac.signal }));
  assert.deepEqual(stateProvider.metrics, { total: 0, failureCount: 0, failureRate: 0 });
  await pipeline.execute(() => 'ok');
  await assert.rejects(pipeline.execute(httpFail(500)));
  assert.deepEqual(stateProvider.metrics, { total: 2, failureCount: 1, failureRate: 0.5 });
  assert.equal(stateProvider.circuitState, 'open');
});

test('circuit breaker: rejections from inner strategies are never recorded', async () => {
  const { stateProvider, pipeline } = (() => {
    const stateProvider = new CircuitBreakerStateProvider();
    const pipeline = new ResiliencePipelineBuilder()
      .addCircuitBreaker({ stateProvider, failureRatio: 0.1, minimumThroughput: 1, shouldHandle: new PredicateBuilder().handle(Error) })
      .addConcurrencyLimiter(1)
      .build();
    return { stateProvider, pipeline };
  })();
  const slow = pipeline.execute(() => sleep(30));
  await assert.rejects(pipeline.execute(() => 1), RateLimiterRejectedError); // limiter full, not the dependency's fault
  await slow;
  assert.equal(stateProvider.circuitState, 'closed');
  assert.deepEqual(stateProvider.metrics, { total: 1, failureCount: 0, failureRate: 0 });
});

test('circuit breaker: count-based window only looks at the last N calls', async () => {
  const { stateProvider, pipeline } = breaker({ samplingCount: 4, minimumThroughput: 4, failureRatio: 0.75, breakDuration: 1000 });
  await assert.rejects(pipeline.execute(httpFail(500)));
  await assert.rejects(pipeline.execute(httpFail(500)));
  for (let i = 0; i < 4; i++) await pipeline.execute(() => 'ok'); // pushes both failures out of the window
  assert.deepEqual(stateProvider.metrics, { total: 4, failureCount: 0, failureRate: 0 });
  for (let i = 0; i < 3; i++) await assert.rejects(pipeline.execute(httpFail(500)));
  assert.equal(stateProvider.circuitState, 'open', '3 of the last 4 failed');
});

test('circuit breaker: minimumThroughput larger than samplingCount is rejected at build', () => {
  assert.throws(() => breaker({ samplingCount: 5, minimumThroughput: 10 }), /can never be reached/);
});

test('circuit breaker: halfOpenMaxCalls lets N probes through and decides on their failure ratio', async () => {
  const time = new FakeTimeProvider();
  const stateProvider = new CircuitBreakerStateProvider();
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ stateProvider, failureRatio: 0.5, minimumThroughput: 1, breakDuration: 20, halfOpenMaxCalls: 3 })
    .build();
  await assert.rejects(pipeline.execute(httpFail(500)));
  await assert.rejects(pipeline.execute(() => 'early'), BrokenCircuitError, 'still open before breakDuration');
  await time.advance(20);
  const gate = defer<void>();
  const probes = [pipeline.execute(() => gate.promise.then(() => 'a')), pipeline.execute(() => gate.promise.then(() => 'b')), pipeline.execute(() => gate.promise.then(httpFail(500)))];
  assert.equal(stateProvider.circuitState, 'half-open');
  await assert.rejects(pipeline.execute(() => 'fourth'), BrokenCircuitError, 'only 3 probes allowed');
  gate.resolve();
  const settled = await Promise.allSettled(probes);
  assert.deepEqual(settled.map((r) => r.status), ['fulfilled', 'fulfilled', 'rejected']);
  assert.equal(stateProvider.circuitState, 'closed', '1 of 3 probes failed: 33% < 50%');
});

test('circuit breaker: a call admitted before the circuit opened cannot close it afterwards', async () => {
  const { stateProvider, pipeline } = breaker({ failureRatio: 0.5, minimumThroughput: 1, breakDuration: 10_000 });
  const slowSuccess = pipeline.execute(() => sleep(30).then(() => 'late'));
  await assert.rejects(pipeline.execute(httpFail(500))); // opens
  assert.equal(await slowSuccess, 'late');
  assert.equal(stateProvider.circuitState, 'open', 'stale success ignored');
});

test('circuit breaker: a timeout counts as a failure', async () => {
  const { stateProvider, pipeline } = (() => {
    const stateProvider = new CircuitBreakerStateProvider();
    const pipeline = new ResiliencePipelineBuilder().addCircuitBreaker({ stateProvider, failureRatio: 1, minimumThroughput: 1 }).addTimeout('10ms').build();
    return { stateProvider, pipeline };
  })();
  await assert.rejects(pipeline.execute(() => sleep(50)), TimeoutRejectedError);
  assert.equal(stateProvider.circuitState, 'open');
});

// --- jitter & durations ---

test('jitter: full is in [0, d), equal is in [d/2, d], both respect maxDelay', () => {
  for (let i = 0; i < 200; i++) {
    const full = computeDelay('exponential', 'full', 3, 100, undefined); // d = 800
    assert.ok(full >= 0 && full < 800, `full ${full}`);
    const equal = computeDelay('exponential', 'equal', 3, 100, undefined);
    assert.ok(equal >= 400 && equal <= 800, `equal ${equal}`);
    const capped = computeDelay('exponential', 'full', 10, 100, 1000);
    assert.ok(capped < 1000, `capped ${capped}`);
  }
  assert.equal(computeDelay('linear', 'full', 1, 100, undefined, undefined, () => 0.5), 100);
});

test('durations: strings are accepted everywhere a time is', async () => {
  assert.equal(toMs('250ms'), 250);
  assert.equal(toMs('1.5s'), 1500);
  assert.equal(toMs('2m'), 120_000);
  assert.equal(toMs('1h'), 3_600_000);
  assert.equal(toMs(42), 42);
  assert.throws(() => toMs('5 minutes' as never), /invalid duration/);
  assert.throws(() => new ResiliencePipelineBuilder().addTimeout('soon' as never).build(), RangeError);
  const p = new ResiliencePipelineBuilder().addRetry({ delay: '1ms', maxDelay: '1s' }).addTimeout('20ms').build();
  await assert.rejects(p.execute(() => sleep(100)), TimeoutRejectedError);
});

// --- limiters ---

test('concurrency limiter: queueTimeout rejects a waiter that waited too long', async () => {
  const time = new FakeTimeProvider();
  let rejected = 0;
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addConcurrencyLimiter({ permitLimit: 1, queueLimit: 5, queueTimeout: '20ms', onRejected: () => void rejected++ }).build();
  const gate = defer<void>();
  const busy = p.execute(() => gate.promise);
  let settled = false;
  const waiter = p.execute(() => 'never');
  void waiter.catch(() => {}).finally(() => (settled = true));
  await time.advance(19);
  assert.equal(settled, false, 'still queued before 20ms');
  await time.advance(1);
  await assert.rejects(waiter, RateLimiterRejectedError);
  assert.equal(rejected, 1);
  gate.resolve();
  await busy;
  assert.equal(await p.execute(() => 'free'), 'free');
});

test('token bucket: queueTimeout waits for the next token instead of rejecting', async () => {
  const time = new FakeTimeProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addRateLimiter({ tokenLimit: 1, replenishmentPeriod: '30ms', queueTimeout: '100ms' }).build();
  await p.execute(() => 1);
  let settled = false;
  const second = p.execute(() => 2).finally(() => (settled = true));
  await time.advance(29);
  assert.equal(settled, false, 'waits for the refill');
  await time.advance(1);
  assert.equal(await second, 2);
  const strict = new ResiliencePipelineBuilder({ timeProvider: time }).addRateLimiter({ tokenLimit: 1, replenishmentPeriod: '1s', queueTimeout: '10ms' }).build();
  await strict.execute(() => 1);
  await assert.rejects(strict.execute(() => 2), RateLimiterRejectedError, 'the token is further away than queueTimeout: rejected at once');
});

// --- ambient context / deadline propagation ---

test('nested pipelines inherit the outer signal: an outer timeout cancels inner calls without passing the signal', async () => {
  let innerAborted = false;
  const inner = new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build();
  const outer = new ResiliencePipelineBuilder().addTimeout('30ms').build();

  // A "service" deep in the call stack: it never sees the outer signal.
  const service = () =>
    inner.execute(async ({ signal }) => {
      signal.addEventListener('abort', () => (innerAborted = true));
      await sleep(200);
    });

  await assert.rejects(outer.execute(() => service()), TimeoutRejectedError);
  assert.ok(innerAborted, 'inner execution was aborted by the outer timeout');
});

test('getResilienceSignal / getResilienceContext work anywhere inside the callback', async () => {
  assert.equal(getResilienceSignal(), undefined);
  const p = new ResiliencePipelineBuilder().build();
  const deep = async () => {
    await sleep(1);
    return getResilienceContext()?.operationKey;
  };
  assert.equal(await p.execute(() => deep(), { operationKey: 'checkout' }), 'checkout');
  assert.ok((await p.execute(async () => getResilienceSignal())) instanceof AbortSignal);
});
