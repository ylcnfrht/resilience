import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import { delay } from '../src/core.ts';
import {
  BrokenCircuitError,
  NonRetryableError,
  Outcome,
  PredicateBuilder,
  RateLimiterRejectedError,
  ResiliencePipeline,
  ResiliencePipelineBuilder,
  ResiliencePipelineRegistry,
  RetryBudget,
  TimeoutRejectedError,
  UnexpectedError,
  backoff,
  mapErrorStrategy,
  PipelineBuilder,
  nextTelemetry,
  report,
  resilient,
  unrefSystemTime,
  withTimeout,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

test('core without node builtins (browsers): no ambient context, no diagnostics channel, still works', () => {
  // A child process, so this copy of core.ts loads with getBuiltinModule throwing (as in a browser bundle).
  const script = `
    process.getBuiltinModule = () => { throw new Error('not here'); };
    const core = await import(${JSON.stringify(new URL('../src/core.ts', import.meta.url).href)});
    const seen = [];
    const ambient = core.ambient.run(core.createContext(), () => core.getResilienceContext());
    const enabled = core.telemetryEnabled();
    core.report({ pipelineName: 'p' }, 'X', core.createContext(), {});
    const off = core.onTelemetry((e) => seen.push(e.event));
    core.report({ pipelineName: 'p' }, 'Y', core.createContext(), {});
    off();
    console.log(JSON.stringify({ ambient: ambient ?? null, signal: core.getResilienceSignal() ?? null, enabled, seen }));`;
  const out = execFileSync(process.execPath, ['--input-type=module', '-e', script], { encoding: 'utf8' });
  assert.deepEqual(JSON.parse(out), { ambient: null, signal: null, enabled: false, seen: ['Y'] });
});

test('Outcome helpers and error classes: the less common paths', () => {
  assert.throws(() => Outcome.unwrapErr(Outcome.fromResult(1)), /succeeded/);
  assert.throws(() => Outcome.unwrap(Outcome.fromError(new Error('e'))), /e/);

  const limited = new RateLimiterRejectedError(500);
  assert.deepEqual(limited.toJSON(), { name: 'RateLimiterRejectedError', code: 'RATE_LIMITED', message: limited.message, retryAfter: 500 });
  const open = new BrokenCircuitError();
  open.source = { pipelineName: 'p', strategyName: 'cb' };
  assert.deepEqual(open.toJSON(), { name: 'BrokenCircuitError', code: 'CIRCUIT_OPEN', message: open.message, source: { pipelineName: 'p', strategyName: 'cb' } });

  const fromString = new NonRetryableError('invalid card');
  assert.equal(fromString.message, 'invalid card');
  assert.ok(fromString.cause instanceof Error);
  const unexpected = new UnexpectedError(42);
  assert.equal(unexpected.message, '42');
  assert.equal(unexpected.cause, 42);
});

test('PredicateBuilder.handleInner: walks the cause chain, stops at its end and at depth 10', () => {
  const p = new PredicateBuilder().handleInner(RangeError, (e) => e.message === 'deep').build();
  const ctx = {} as never;
  const wrap = (cause: unknown) => new Error('outer', { cause });
  assert.equal(p({ outcome: Outcome.fromError(wrap(wrap(new RangeError('deep')))), context: ctx }), true);
  assert.equal(p({ outcome: Outcome.fromError(wrap(new RangeError('other'))), context: ctx }), false);
  assert.equal(p({ outcome: Outcome.fromResult(1), context: ctx }), false);
  let tooDeep: unknown = new RangeError('deep');
  for (let i = 0; i < 10; i++) tooDeep = wrap(tooDeep);
  assert.equal(p({ outcome: Outcome.fromError(tooDeep), context: ctx }), false);
});

test('nextTelemetry: an abort signal rejects it, before and after subscribing', async () => {
  await assert.rejects(nextTelemetry(undefined, AbortSignal.abort(new Error('already'))), /already/);
  const ac = new AbortController();
  const waiting = nextTelemetry(() => false, ac.signal);
  ac.abort(new Error('later'));
  await assert.rejects(waiting, /later/);
  // A matching event resolves it and detaches from the signal.
  const ok = new AbortController();
  const next = nextTelemetry((e) => e.event === 'Z', ok.signal);
  report({ pipelineName: undefined, strategyName: undefined }, 'Z', {} as never, 1);
  assert.equal((await next).event, 'Z');
  ok.abort(); // no unhandled rejection
});

test('delay: an aborted signal rejects at once; an infinite delay only ends on abort', async () => {
  await assert.rejects(delay(10, AbortSignal.abort(new Error('stop'))), /stop/);
  const ac = new AbortController();
  const forever = delay(Infinity, ac.signal);
  ac.abort(new Error('finally'));
  await assert.rejects(forever, /finally/);
});

test('unrefSystemTime: timers fire and can be cleared', async () => {
  let fired = 0;
  const cleared = unrefSystemTime.setTimeout(() => fired++, 1);
  unrefSystemTime.clearTimeout(cleared);
  // An unref'd timer alone doesn't keep the process alive (Node 22 exits mid-test); a ref'd one holds it open.
  const keepAlive = setTimeout(() => {}, 1000);
  await new Promise<void>((resolve) => unrefSystemTime.setTimeout(resolve, 1));
  clearTimeout(keepAlive);
  assert.equal(fired, 0);
  assert.ok(Math.abs(unrefSystemTime.now() - Date.now()) < 1000);
});

test('@resilient refuses to decorate something that is not a method', () => {
  const decorate = resilient(ResiliencePipeline.empty);
  assert.throws(() => decorate({}, 'field', { value: 42 }), TypeError);
  assert.throws(() => decorate({}, 'field', undefined), TypeError);
});

test('fallback: onFallback runs before the substitute; mapError passes aborts through', async () => {
  const log: string[] = [];
  const p = new ResiliencePipelineBuilder().addFallback({ fallbackValue: 'cached', onFallback: () => void log.push('onFallback') }).build();
  assert.equal(await p.execute(() => Promise.reject(new Error('x'))), 'cached');
  assert.deepEqual(log, ['onFallback']);

  const mapped = new PipelineBuilder().add(mapErrorStrategy(() => new Error('mapped'))).build();
  const abortError = new DOMException('aborted', 'AbortError');
  const o1 = await mapped.executeOutcome(() => Promise.reject(abortError));
  assert.ok(!o1.ok && o1.error === abortError);
  const ac = new AbortController();
  ac.abort(new Error('caller left'));
  const o2 = await mapped.executeOutcome(() => Promise.reject(new Error('x')), { signal: ac.signal });
  assert.ok(!o2.ok && (o2.error as Error).message === 'caller left');
  assert.deepEqual(await mapped.executeOutcome(() => 1), { ok: true, result: 1 });
});

test('withTimeout accepts full timeout options', async () => {
  const time = new FakeTimeProvider();
  let timedOut = 0;
  const done = withTimeout((ctx) => new Promise((_, reject) => ctx.signal.addEventListener('abort', () => reject(ctx.signal.reason))), { timeout: '1s', onTimeout: (a) => void (timedOut = a.timeout) }, { timeProvider: time });
  const settled = assert.rejects(done, TimeoutRejectedError);
  await time.advance('1s');
  await settled;
  assert.equal(timedOut, 1000);
});

test('pipeline: describe() names a prototype-less strategy, getStrategy() misses, dispose() twice is fine', async () => {
  const bare = Object.assign(Object.create(null) as object, { execute: (next: (c: unknown) => unknown, ctx: unknown) => next(ctx) });
  const p = new PipelineBuilder().addStrategy(() => bare as never, 'bare').build();
  assert.deepEqual(
    p.describe().map((d) => d.kind),
    ['Strategy'],
  );
  assert.equal(p.getStrategy('missing'), undefined);
  p.dispose();
  p.dispose();
  assert.equal(p.disposed, true);
  await assert.rejects(p.execute(() => 1), /disposed/);
});

test('executeStream: a stream cached by a cache strategy is refused on reuse', async () => {
  const p = new ResiliencePipelineBuilder().addCache({ ttl: '1m', key: () => 'k' }).build();
  const read = async () => {
    const got: number[] = [];
    for await (const c of p.executeStream(async function* () {
      yield 1;
    }))
      got.push(c);
    return got;
  };
  assert.deepEqual(await read(), [1]);
  await assert.rejects(read(), /cannot cache streams/);
});

test('executeStream: a ReadableStream-like source without Symbol.asyncIterator is read through its reader', async () => {
  const chunks = ['a', 'b'];
  let cancelled = false;
  const source = {
    getReader: () => ({
      read: async () => (chunks.length ? { done: false, value: chunks.shift()! } : { done: true, value: undefined }),
      cancel: async () => void (cancelled = true),
    }),
  };
  const got: string[] = [];
  for await (const c of ResiliencePipeline.empty.executeStream(() => source as unknown as ReadableStream<string>)) got.push(c);
  assert.deepEqual(got, ['a', 'b']);
  assert.equal(cancelled, true);
});

test('registry: keys(), reload of an unknown key is a no-op, no new pipelines after dispose', () => {
  const registry = new ResiliencePipelineRegistry();
  registry.getOrAddPipeline('a', (b) => b.addRetry());
  assert.deepEqual(registry.keys(), ['a']);
  registry.reload('nope');
  assert.deepEqual(registry.keys(), ['a']);
  registry.tryAddBuilder('later', (b) => b.addRetry());
  registry.dispose();
  assert.throws(() => registry.getPipeline('later'), /disposed/);
});

test('builder shortcuts for the less common strategies build the right strategies', () => {
  const p = new ResiliencePipelineBuilder()
    .addAdaptiveConcurrencyLimiter()
    .addFixedWindowRateLimiter({ permitLimit: 5, window: '1s' })
    .addSlidingWindowRateLimiter({ permitLimit: 5, window: '1s' })
    .addCoalesce()
    .build();
  assert.deepEqual(
    p.describe().map((d) => d.name),
    ['AdaptiveConcurrencyLimiter', 'FixedWindowRateLimiter', 'SlidingWindowRateLimiter', 'Coalesce'],
  );
  p.dispose();
});

test('backoff.sequence takes attempt objects too; RetryBudget takes every option', () => {
  assert.equal(backoff.sequence(['1s', '5s'])({ attemptNumber: 9 }), 5000);
  const time = new FakeTimeProvider();
  const budget = new RetryBudget({ ratio: 0.5, minRetriesPerSecond: 0, window: '1s', timeProvider: time });
  budget.recordRequest();
  budget.recordRequest();
  assert.equal(budget.tryRetry(), true);
  assert.equal(budget.tryRetry(), false, '50% of 2 requests is 1 retry');
  assert.throws(() => new RetryBudget({ ratio: -1 }), RangeError);
  assert.deepEqual(new RetryBudget().stats(), { requests: 0, retries: 0 });
});
