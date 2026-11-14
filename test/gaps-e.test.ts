import assert from 'node:assert/strict';
import { getEventListeners } from 'node:events';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  BulkheadRejectedError,
  type CircuitBreakerStrategy,
  MemoryCircuitStateStore,
  PredicateBuilder,
  RateLimiterRejectedError,
  ResiliencePipelineBuilder,
  ResiliencePipelineRegistry,
  TimeoutRejectedError,
  circuitHealthCheck,
  computeDelay,
  sharedCircuitState,
  getResilienceContext,
  isResilienceError,
  isTimeoutError,
  onTelemetry,
  setGlobalChaos,
  unrefSystemTime,
} from '../src/index.ts';
import { describeResilienceError, httpStatusFor } from '../src/http.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const never = () => new Promise<never>(() => {});
const httpFail = (status: number) => (): never => {
  throw Object.assign(new Error(`HTTP ${status}`), { status });
};
function events(filter: (name: string) => boolean = () => true) {
  const seen: string[] = [];
  const off = onTelemetry((e) => void (filter(e.event) && seen.push(e.event)));
  return { seen, off };
}

// --- E1 ---

test('E1: no listener piles up on a long-lived parent signal (AbortSignal.any leak fixed)', async () => {
  const longLived = new AbortController().signal;
  const p = new ResiliencePipelineBuilder().addTimeout('1s').addHedging({ delay: '1s' }).build();
  const outer = new ResiliencePipelineBuilder().build();
  for (let i = 0; i < 1000; i++) await outer.execute(() => p.execute(() => i, { signal: longLived }), { signal: longLived });
  assert.equal(getEventListeners(longLived, 'abort').length, 0);
});

test('E1: a full bulkhead is a BulkheadRejectedError (503), a rate limit stays 429', async () => {
  const p = new ResiliencePipelineBuilder().addConcurrencyLimiter({ permitLimit: 1, queueLimit: 1, queueTimeout: '20ms' }).build();
  const busy = p.execute(() => sleep(60));
  const timedOut = p.execute(() => 1).catch((e) => e);
  const full = await p.execute(() => 1).catch((e) => e);
  assert.ok(full instanceof BulkheadRejectedError && full.reason === 'full' && full.active === 1 && full.queued === 1);
  const queued = await timedOut;
  assert.ok(queued instanceof BulkheadRejectedError && queued.reason === 'queue-timeout');
  await busy;
  assert.equal(httpStatusFor(full), 503);
  assert.equal(httpStatusFor(new RateLimiterRejectedError(1000)), 429);
});

test('E1: a hanging half-open probe reopens the circuit after halfOpenMaxWait', async () => {
  const time = new FakeTimeProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addCircuitBreaker({ failureRatio: 1, minimumThroughput: 1, breakDuration: '10s', halfOpenMaxWait: '5s' })
    .build();
  const breaker = p.getStrategy<CircuitBreakerStrategy<unknown>>('CircuitBreaker')!;
  await assert.rejects(p.execute(httpFail(500)));
  await time.advance('10s');
  void p.execute(never).catch(() => {}); // the probe hangs
  await time.advance('1s');
  assert.equal(breaker.state, 'half-open');
  await time.advance('4s');
  assert.equal(breaker.state, 'open', 'probe timed out → open again instead of stuck half-open');
  await time.advance('10s');
  assert.equal(await p.execute(() => 'recovered'), 'recovered');
});

test('E1: hedged attempts get their own properties; the winner’s are merged back', async () => {
  let n = 0;
  const p = new ResiliencePipelineBuilder().addHedging({ delay: 0, maxHedgedAttempts: 1 }).build();
  const properties = new Map<string, unknown>([['shared', 'initial']]);
  await p.execute(
    async (ctx) => {
      const me = n++;
      ctx.properties.set('who', me);
      await sleep(me === 0 ? 50 : 5); // attempt 1 wins
      return me;
    },
    { properties },
  );
  assert.equal(properties.get('who'), 1, 'winner’s write, not overwritten by the loser');
  assert.equal(properties.get('shared'), 'initial');
});

test('E1: retry waits at least the error’s retryAfter (capped by maxDelay)', async () => {
  const time = new FakeTimeProvider();
  const delays: number[] = [];
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addRetry({ delay: 1, maxRetryAttempts: 1, shouldHandle: new PredicateBuilder().handle(RateLimiterRejectedError), onRetry: (a) => void delays.push(a.retryDelay) })
    .addRateLimiter({ tokenLimit: 1, replenishmentPeriod: '30s' })
    .build();
  await p.execute(() => 1);
  const second = p.execute(() => 2);
  await time.runAll();
  assert.equal(await second, 2);
  assert.equal(delays[0], 30_000);
});

test('E1: errors carry a code and the pipeline/strategy that raised them; guards work across package copies', async () => {
  const err = await new ResiliencePipelineBuilder({ name: 'payments' }).addTimeout({ timeout: '5ms', name: 'per-try' }).build().execute(never).catch((e) => e);
  assert.equal(err.code, 'TIMEOUT');
  assert.deepEqual(err.source, { pipelineName: 'payments', strategyName: 'per-try' });
  assert.deepEqual(JSON.parse(JSON.stringify(err)).code, 'TIMEOUT');
  // An error from a duplicate copy of the package: same brand symbol, different class identity.
  const foreign = { [Symbol.for('resilience-pipeline.error')]: true, code: 'TIMEOUT', message: 'x', retryAfter: undefined };
  assert.ok(isResilienceError(foreign) && isTimeoutError(foreign));
  assert.ok(!(foreign instanceof TimeoutRejectedError));
  assert.equal(describeResilienceError(foreign)?.httpStatus, 504);
});

// --- E2 ---

test('E2: the callback sees its attempt number (also via getResilienceContext deep inside)', async () => {
  const seen: number[] = [];
  const deep = () => getResilienceContext()!.attemptNumber;
  await new ResiliencePipelineBuilder()
    .addRetry({ delay: 0, maxRetryAttempts: 2 })
    .build()
    .execute((ctx) => {
      seen.push(ctx.attemptNumber, deep());
      if (ctx.attemptNumber < 2) httpFail(503)();
    });
  assert.deepEqual(seen, [0, 0, 1, 1, 2, 2]);
});

test('E2: events — OnExecutionAttempt per attempt, OnRetryExhausted, OnCircuitRejected', async () => {
  const { seen, off } = events((n) => ['OnExecutionAttempt', 'OnRetryExhausted', 'OnCircuitRejected'].includes(n));
  const rejected: string[] = [];
  try {
    const p = new ResiliencePipelineBuilder()
      .addRetry({ delay: 0, maxRetryAttempts: 1 })
      .addCircuitBreaker({ consecutiveFailures: 2, onRejected: (a) => void rejected.push(a.state) })
      .build();
    await assert.rejects(p.execute(httpFail(503)));
    await assert.rejects(p.execute(() => 1), BrokenCircuitError);
  } finally {
    off();
  }
  // The rejected call never ran the callback, so it is not an attempt.
  assert.deepEqual(seen, ['OnExecutionAttempt', 'OnExecutionAttempt', 'OnRetryExhausted', 'OnCircuitRejected']);
  assert.deepEqual(rejected, ['open']);
});

// --- E3: retry options ---

test('E3: idempotent:false never repeats a call that may have reached the dependency', async () => {
  let calls = 0;
  const retry = new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build();
  await assert.rejects(retry.execute(() => (calls++, httpFail(503)()), { idempotent: false }));
  assert.equal(calls, 1);
  let hedged = 0;
  const hedge = new ResiliencePipelineBuilder().addHedging({ delay: 0 }).build();
  await hedge.execute(async () => (hedged++, sleep(10)), { idempotent: false });
  assert.equal(hedged, 1);
});

test('E3: factor, initialDelay, maxRetryTime, shouldConsumeRetry, onFailedAttempt, onRetryExhausted', async () => {
  assert.deepEqual([0, 1, 2].map((n) => computeDelay('exponential', false, n, 100, undefined, undefined, undefined, 3)), [100, 300, 900]);

  const time = new FakeTimeProvider();
  const start = time.now();
  const left: number[] = [];
  let exhausted = 0;
  let calls = 0;
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addRetry({
      delay: '1s',
      maxRetryAttempts: 2,
      initialDelay: '500ms',
      shouldConsumeRetry: ({ outcome }) => (outcome.ok ? true : (outcome.error as Error).message !== 'token expired'),
      onFailedAttempt: (a) => void left.push(a.retriesLeft),
      onRetryExhausted: () => void exhausted++,
    })
    .build();
  const run = assert.rejects(
    p.execute(() => {
      calls++;
      if (calls === 1) throw Object.assign(new Error('token expired'), { status: 503 });
      httpFail(503)();
    }),
  );
  await time.runAll();
  await run;
  assert.equal(calls, 4, '1 free retry + 2 counted');
  assert.deepEqual(left, [2, 1, 0, 0]);
  assert.equal(exhausted, 1);
  assert.equal(time.now() - start, 500 + 3 * 1000);

  const capped = new ResiliencePipelineBuilder({ timeProvider: time }).addRetry({ delay: '1s', maxRetryAttempts: 10, maxRetryTime: '2500ms' }).build();
  let tries = 0;
  const done = assert.rejects(capped.execute(() => (tries++, httpFail(503)())));
  await time.runAll();
  await done;
  assert.equal(tries, 3, 'stopped scheduling once 2.5 s would be exceeded');
});

test('E3: PredicateBuilder.handleInner matches the cause chain', async () => {
  class DbError extends Error {}
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 1, shouldHandle: new PredicateBuilder().handleInner(DbError) }).build();
  await assert.rejects(p.execute(() => (calls++, Promise.reject(new Error('wrapped', { cause: new DbError('deadlock') })))));
  assert.equal(calls, 2);
});

// --- E3: pipeline features ---

test('E3: wrap(), kill switch, describe(), dispose()', async () => {
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, name: 'r' }).addTimeout('10ms').build();
  const double = p.wrap((x: number) => x * 2);
  assert.equal(await double(21), 42);
  assert.deepEqual(p.describe().map((d) => [d.name, d.kind]), [['r', 'RetryStrategy'], ['Timeout', 'TimeoutStrategy']]);

  p.disable();
  assert.equal(await p.execute(async () => (await sleep(30), 'no timeout while disabled')), 'no timeout while disabled');
  p.enable();
  await assert.rejects(p.execute(() => sleep(30)), TimeoutRejectedError);

  p.dispose();
  await assert.rejects(p.execute(() => 1), /disposed/);
  assert.equal(new ResiliencePipelineBuilder({ unrefTimers: true }).timeProvider, unrefSystemTime);
});

test('E3: circuit breaker trip(), reset(), lastFailure, health check, shared state store', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryCircuitStateStore();
  let healthy = true;
  const make = () =>
    new ResiliencePipelineBuilder({ timeProvider: time })
      .addCircuitBreaker({
        consecutiveFailures: 1,
        breakDuration: '1m',
        extensions: [sharedCircuitState({ store, key: 'payments', syncInterval: '1s' }), circuitHealthCheck({ interval: '10s', check: () => healthy })],
      })
      .build();
  const a = make();
  const b = make();
  const breakerA = a.getStrategy<CircuitBreakerStrategy<unknown>>('CircuitBreaker')!;
  const breakerB = b.getStrategy<CircuitBreakerStrategy<unknown>>('CircuitBreaker')!;

  await assert.rejects(a.execute(httpFail(500)));
  assert.equal(breakerA.state, 'open');
  assert.equal((breakerA.lastFailure as { error: Error }).error.message, 'HTTP 500');
  await time.advance('1s');
  assert.equal(breakerB.state, 'open', 'instance B adopted A’s open circuit from the store');

  await breakerA.reset();
  await time.advance('1s');
  assert.equal(breakerB.state, 'closed');

  healthy = false;
  await time.advance('10s');
  assert.equal(breakerA.state, 'open', 'failing health check opened it');

  await breakerB.reset();
  await breakerB.trip();
  assert.equal(breakerB.state, 'open');
  a.dispose();
  b.dispose();
  assert.equal(time.pendingTimers, 0, 'dispose stopped the background timers');
});

test('E3: registry reload keeps the reference, lists breakers and limiters, disposes', async () => {
  const registry = new ResiliencePipelineRegistry();
  registry.tryAddBuilder('api', (b) => b.addRetry({ delay: 0, maxRetryAttempts: 0 }).addCircuitBreaker().addConcurrencyLimiter(5));
  const p = registry.getPipeline('api');
  let calls = 0;
  await assert.rejects(p.execute(() => (calls++, httpFail(503)())));
  assert.equal(calls, 1);
  registry.reload('api', (b) => b.addRetry({ delay: 0, maxRetryAttempts: 2 }));
  calls = 0;
  await assert.rejects(p.execute(() => (calls++, httpFail(503)())));
  assert.equal(calls, 3, 'same reference, new configuration');

  registry.reload('api', (b) => b.addCircuitBreaker().addConcurrencyLimiter(5));
  assert.deepEqual(registry.circuitBreakers().map((d) => d.key), ['api']);
  assert.deepEqual(registry.limiters().map((d) => d.kind), ['ConcurrencyLimiterStrategy']);
  registry.dispose();
  await assert.rejects(p.execute(() => 1), /disposed/);
});

test('E3: setGlobalChaos injects into every pipeline until turned off', async () => {
  const p = new ResiliencePipelineBuilder().build();
  const off = setGlobalChaos({ injectionRate: 1, fault: new Error('game day') });
  try {
    await assert.rejects(p.execute(() => 1), /game day/);
  } finally {
    off();
  }
  assert.equal(await p.execute(() => 1), 1);
});

test('E3: cache reports coalesced joins and store failures', async () => {
  const { seen, off } = events((n) => n === 'OnCoalesced' || n === 'OnCacheStoreFailed');
  try {
    const broken = { get: () => Promise.reject(new Error('redis down')), set: () => {}, delete: () => {} };
    const p = new ResiliencePipelineBuilder().addCache({ ttl: '1m', store: broken }).build();
    await Promise.all([1, 2, 3].map(() => p.execute(() => sleep(10).then(() => 'v'), { operationKey: 'k' })));
  } finally {
    off();
  }
  assert.equal(seen.filter((n) => n === 'OnCacheStoreFailed').length, 3);
  assert.ok(seen.includes('OnCoalesced'));
});

test('attempt events come from every pipeline, not only ones with retry (hedging-only here)', async () => {
  const attempts: number[] = [];
  const off = onTelemetry((e) => void (e.event === 'OnExecutionAttempt' && attempts.push((e.args as { attemptNumber: number }).attemptNumber)));
  try {
    let n = 0;
    await new ResiliencePipelineBuilder().addHedging({ delay: 0 }).build().execute(async () => sleep(n++ === 0 ? 30 : 5));
    await sleep(40); // the losing attempt still finishes (and reports) after the winner returned
  } finally {
    off();
  }
  assert.deepEqual(attempts.sort(), [0, 1]);
});
