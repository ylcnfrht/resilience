import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  Outcome,
  PipelineBuilder,
  ResiliencePipelineBuilder,
  ResiliencePipelineRegistry,
  ResiliencePropertyKey,
  TimeoutRejectedError,
  UnexpectedError,
  allSettled,
  chaosFaultStrategy,
  nextTelemetry,
  onTelemetry,
  retryStrategy,
  timeoutStrategy,
  toMs,
  waitFor,
  weighted,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

class NotFound extends Error {
  readonly code = 'NOT_FOUND' as const;
}

// --- typed error channel ---

test('settle(): typed errors from strategies + returned errors; Outcome.match is exhaustive', async () => {
  const pipeline = new ResiliencePipelineBuilder().addTimeout('20ms').addCircuitBreaker({ consecutiveFailures: 10 }).build();

  const found = await pipeline.settle(async () => 'ada');
  const missing = await pipeline.settle<string, NotFound>(async () => Outcome.fromError(new NotFound('user 7')));
  const slow = await pipeline.settle((): Promise<string> => new Promise(() => {})); // never answers
  const bug = await pipeline.settle((): string => {
    throw new RangeError('oops');
  });

  const describe = (o: Outcome<string, NotFound | TimeoutRejectedError | BrokenCircuitError | UnexpectedError>) =>
    Outcome.match(o, {
      ok: (name) => `hello ${name}`,
      NOT_FOUND: (e) => `missing: ${e.message}`,
      TIMEOUT: (e) => `slow (${e.timeout}ms)`,
      CIRCUIT_OPEN: () => 'down',
      CIRCUIT_ISOLATED: () => 'isolated',
      UNEXPECTED: (e) => `bug: ${(e.cause as Error).message}`,
    });
  assert.equal(describe(found), 'hello ada');
  assert.equal(describe(missing), 'missing: user 7');
  assert.equal(describe(slow), 'slow (20ms)');
  assert.equal(describe(bug), 'bug: oops');
  assert.ok(!bug.ok && bug.error instanceof UnexpectedError);

  // Type-level: forgetting a handler for an error the pipeline can raise doesn't compile.
  // @ts-expect-error — TIMEOUT, CIRCUIT_OPEN, CIRCUIT_ISOLATED and UNEXPECTED are missing
  assert.throws(() => Outcome.match(slow, { ok: () => 1, NOT_FOUND: () => 2 }));
});

test('settle(): a catch-all fallback removes the errors of the strategies inside it', async () => {
  const p = new ResiliencePipelineBuilder<string>().addFallback({ fallbackAction: () => 'cached' }).addTimeout('10ms').addCircuitBreaker().build();
  const outcome = await p.settle((): Promise<string> => new Promise(() => {}));
  // Only `ok` and UNEXPECTED are required now: TIMEOUT / CIRCUIT_* can't escape the fallback.
  assert.equal(Outcome.match(outcome, { ok: (v) => v, UNEXPECTED: () => 'bug' }), 'cached');

  const lean = new PipelineBuilder().add(retryStrategy({ delay: 0 })).add(timeoutStrategy('5ms')).build();
  const r = await lean.settle(() => new Promise(() => {}));
  assert.ok(!r.ok && r.error instanceof TimeoutRejectedError);
});

test('returned errors count as failures for strategies (retried) and reach the caller typed', async () => {
  let calls = 0;
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
  const o = await p.settle(async () => (++calls < 3 ? Outcome.fromError(new NotFound('x')) : 'found'));
  assert.deepEqual(o, { ok: true, result: 'found' });
  assert.equal(calls, 3);
});

// --- streaming ---

async function* chunks(fail: { before?: boolean; after?: boolean }, log: string[]) {
  log.push('open');
  try {
    if (fail.before) throw Object.assign(new Error('connect failed'), { status: 503 });
    yield 'a';
    if (fail.after) throw new Error('stream broke');
    yield 'b';
  } finally {
    log.push('closed');
  }
}

test('executeStream: retried before the first chunk, committed after it', async () => {
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 3 }).build();
  let attempt = 0;
  const log: string[] = [];
  const got: string[] = [];
  for await (const c of p.executeStream(() => chunks({ before: attempt++ < 2 }, log))) got.push(c);
  assert.deepEqual(got, ['a', 'b']);
  assert.equal(attempt, 3, 'two failed connects were retried');

  attempt = 0;
  const partial: string[] = [];
  await assert.rejects(async () => {
    for await (const c of p.executeStream(() => (attempt++, chunks({ after: true }, [])))) partial.push(c);
  }, /stream broke/);
  assert.deepEqual(partial, ['a']);
  assert.equal(attempt, 1, 'no retry once a chunk was delivered');
});

test('executeStream: breaking out closes the source; ReadableStream works too', async () => {
  const p = new ResiliencePipelineBuilder().build();
  const log: string[] = [];
  for await (const c of p.executeStream(() => chunks({}, log))) {
    assert.equal(c, 'a');
    break;
  }
  assert.deepEqual(log, ['open', 'closed']);

  const stream = () => new ReadableStream<number>({ start: (c) => (c.enqueue(1), c.enqueue(2), c.close()) });
  const nums: number[] = [];
  for await (const n of p.executeStream(stream)) nums.push(n);
  assert.deepEqual(nums, [1, 2]);
});

// --- small E4 items ---

test('abortOnReturn aborts leftover work; keepLateResult keeps a late success', async () => {
  let leftoverAborted = false;
  await new ResiliencePipelineBuilder().build().execute(
    async ({ signal }) => {
      signal.addEventListener('abort', () => (leftoverAborted = true));
      return 1;
    },
    { abortOnReturn: true },
  );
  assert.ok(leftoverAborted);

  const late = new ResiliencePipelineBuilder().addTimeout({ timeout: '10ms', cooperative: true, keepLateResult: true }).build();
  assert.equal(await late.execute(() => sleep(30).then(() => 'late but kept')), 'late but kept');
});

test('typed property keys, tags in telemetry, nextTelemetry, registry pipeline names', async () => {
  const TENANT = new ResiliencePropertyKey<string>('tenant');
  const p = new ResiliencePipelineBuilder({ name: 'billing', tags: { group: 'payments' } }).addRetry({ delay: 0, maxRetryAttempts: 1 }).build();
  const event = nextTelemetry((e) => e.event === 'OnRetry');
  const properties = new Map<string, unknown>();
  let seenTenant: string | undefined;
  await assert.rejects(
    p.execute(
      (ctx) => {
        seenTenant = TENANT.get(ctx);
        throw Object.assign(new Error('x'), { status: 503 });
      },
      { properties: (TENANT.set({ properties }, 'acme'), properties) },
    ),
  );
  assert.equal(seenTenant, 'acme');
  assert.deepEqual((await event).source.tags, { group: 'payments' });

  const registry = new ResiliencePipelineRegistry<{ service: string }>({ pipelineName: (k) => `svc:${k.service}` });
  assert.equal(registry.getOrAddPipeline({ service: 'orders' }, (b) => b.addRetry()).name, 'svc:orders');
});

test('week durations and delays longer than setTimeout’s ~24.8-day limit', async () => {
  assert.equal(toMs('2w'), 14 * 86_400_000);
  const time = new FakeTimeProvider();
  const start = time.now();
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addRetry({ delay: '30d', maxRetryAttempts: 1 }).build();
  let calls = 0;
  const done = assert.rejects(p.execute(() => (calls++, Promise.reject(Object.assign(new Error('x'), { status: 503 })))));
  await time.advance('25d');
  assert.equal(calls, 1, 'did not fire early (a single setTimeout of 30 days would fire immediately)');
  await time.runAll();
  await done;
  assert.equal(calls, 2);
  assert.equal(time.now() - start, toMs('30d'));
});

test('allSettled, waitFor with backoff interval, weighted chaos with Polly event names', async () => {
  const [a, b] = await allSettled([() => 1, () => Promise.reject(new Error('no'))]);
  assert.deepEqual(a, { ok: true, result: 1 });
  assert.equal(b.ok, false);

  const time = new FakeTimeProvider();
  const start = time.now();
  let polls = 0;
  const waiting = waitFor(() => ++polls === 4, { interval: (n) => 100 * 2 ** n, timeout: '1m', timeProvider: time });
  await time.runAll();
  await waiting;
  assert.equal(time.now() - start, 200 + 400 + 800);

  const pick = weighted([{ weight: 3, value: 'a' }, { weight: 1, value: () => 'b' }], (() => { const seq = [0.1, 0.8]; return () => seq.shift()!; })());
  assert.deepEqual([pick(), pick()], ['a', 'b']);

  const names: string[] = [];
  const off = onTelemetry((e) => void (e.event.startsWith('Chaos.') && names.push(e.event)));
  try {
    const p = new PipelineBuilder().add(chaosFaultStrategy({ injectionRate: 1, faultGenerator: weighted([{ weight: 1, value: () => new BrokenCircuitError() }]) })).build();
    await assert.rejects(p.execute(() => 1), BrokenCircuitError);
  } finally {
    off();
  }
  assert.deepEqual(names, ['Chaos.OnFault']);
});

test('executeStream + hedging: a late losing attempt that ignores the signal cannot take over the stream', async () => {
  const time = new FakeTimeProvider();
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10, maxHedgedAttempts: 1 }).build();
  let attempt = 0;
  const closed: string[] = [];
  let releasePrimary = () => {};
  const primaryGate = new Promise<void>((resolve) => (releasePrimary = resolve));
  async function* source(name: string, gate: Promise<void>) {
    try {
      await gate; // ignores the abort signal on purpose
      yield `${name}-1`;
      yield `${name}-2`;
    } finally {
      closed.push(name);
    }
  }
  const got: string[] = [];
  const consume = (async () => {
    for await (const c of p.executeStream(() => (attempt++ === 0 ? source('primary', primaryGate) : source('hedge', Promise.resolve())))) got.push(c);
  })();
  await time.advance(10); // the primary is slow: the hedge starts and wins
  await consume;
  assert.deepEqual(got, ['hedge-1', 'hedge-2']);
  releasePrimary(); // the loser's first chunk arrives after the race is decided
  for (let i = 0; i < 10 && !closed.includes('primary'); i++) await new Promise((r) => setImmediate(r));
  assert.ok(closed.includes('primary'), 'the late loser is closed');
});

test('executeStream + fallback: a fallback value is one chunk, a fallback stream is iterated', async () => {
  const fail = () => chunks({ before: true }, []);
  const one = new ResiliencePipelineBuilder<unknown>().addFallback({ fallbackAction: () => 'cached' }).build();
  const a: unknown[] = [];
  for await (const c of one.executeStream(fail)) a.push(c);
  assert.deepEqual(a, ['cached']);

  const many = new ResiliencePipelineBuilder<unknown>().addFallback({ fallbackAction: async () => chunks({}, []) }).build();
  const b: unknown[] = [];
  for await (const c of many.executeStream(fail)) b.push(c);
  assert.deepEqual(b, ['a', 'b']);
});
