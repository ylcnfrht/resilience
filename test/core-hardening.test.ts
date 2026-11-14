import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  FallbackStrategy,
  HedgingStrategy,
  type OnHedgingArgs,
  type OnRetryArgs,
  ResiliencePipelineBuilder,
  RetryStrategy,
  TimeoutRejectedError,
  TimeoutStrategy,
  backoff,
  computeDelay,
  linkSignals,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const never = () => new Promise<never>(() => {});
const untilAborted = (signal: AbortSignal) => new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));

test('index exports the retry, timeout, hedging and fallback strategy classes', () => {
  const p = new ResiliencePipelineBuilder().addRetry().addTimeout(100).addHedging().addFallback({ fallbackValue: 1 }).build();
  assert.deepEqual(
    p.describe().map((d) => d.strategy.constructor),
    [RetryStrategy, TimeoutStrategy, HedgingStrategy, FallbackStrategy],
  );
});

// --- attempt numbers ---

test('retry inside hedging: attempt numbers are unique across hedged branches', async () => {
  const seen: number[] = [];
  const pipeline = new ResiliencePipelineBuilder().addHedging({ delay: 0, maxHedgedAttempts: 1 }).addRetry({ delay: 0, maxRetryAttempts: 2 }).build();
  const outcome = await pipeline.executeOutcome(async (ctx) => {
    seen.push(ctx.attemptNumber);
    await tick();
    throw new Error('x');
  });
  assert.equal(outcome.ok, false);
  assert.deepEqual([...seen].sort((a, b) => a - b), [0, 1, 2, 3, 4, 5]);
});

test('a strategy run on a hand-made context still numbers attempts uniquely', async () => {
  const time = new FakeTimeProvider();
  const retry = new RetryStrategy({ delay: 0, maxRetryAttempts: 2 }, { pipelineName: undefined, strategyName: 'r', timeProvider: time });
  const seen: number[] = [];
  const context = { signal: new AbortController().signal, operationKey: undefined, properties: new Map(), attemptNumber: 4, idempotent: true };
  await retry.execute(async (ctx) => (seen.push(ctx.attemptNumber), { ok: false, error: new Error('x') }), context);
  assert.deepEqual(seen, [4, 5, 6]);
});

// --- discarded stream results are closed ---

function streams() {
  const closed: string[] = [];
  let n = 0;
  const factory = async function* () {
    const name = n++ === 0 ? 'bad' : 'good';
    try {
      yield name;
      yield `${name}-2`;
    } finally {
      closed.push(name);
    }
  };
  // shouldHandle sees the attempt object executeStream builds: its first chunk is `first.value`.
  const isBad = ({ outcome }: { outcome: { ok: boolean; result?: unknown } }) => outcome.ok && (outcome.result as { first: IteratorResult<string> }).first.value === 'bad';
  return { closed, factory, isBad };
}

const drain = async (it: AsyncIterable<string>) => {
  const got: string[] = [];
  for await (const c of it) got.push(c);
  return got;
};

test('executeStream + retry: a successful stream that shouldHandle retries is closed', async () => {
  const { closed, factory, isBad } = streams();
  const p = new ResiliencePipelineBuilder().addRetry({ delay: 0, shouldHandle: isBad }).build();
  assert.deepEqual(await drain(p.executeStream(factory)), ['good', 'good-2']);
  assert.deepEqual(closed, ['bad', 'good']);
});

test('executeStream + hedging: a handled stream is closed; the one returned at the end is not', async () => {
  const { closed, factory, isBad } = streams();
  const p = new ResiliencePipelineBuilder().addHedging({ delay: -1, shouldHandle: isBad }).build();
  assert.deepEqual(await drain(p.executeStream(factory)), ['good', 'good-2']);
  assert.deepEqual(closed, ['bad', 'good']);

  // Every attempt handled: the primary's stream is returned (and stays open until consumed), the other one is closed.
  const all = streams();
  const every = new ResiliencePipelineBuilder().addHedging({ delay: -1, shouldHandle: () => true }).build();
  assert.deepEqual(await drain(every.executeStream(all.factory)), ['bad', 'bad-2']);
  assert.deepEqual(all.closed, ['good', 'bad']);
});

test('executeStream + fallback: a handled successful stream replaced by the fallback is closed', async () => {
  const { closed, factory, isBad } = streams();
  const p = new ResiliencePipelineBuilder().addFallback({ shouldHandle: isBad, fallbackValue: 'cached' }).build();
  assert.deepEqual(await drain(p.executeStream(factory)), ['cached']);
  assert.deepEqual(closed, ['bad']);
});

test('executeStream + cooperative timeout: a stream that ignored the deadline is closed', async () => {
  const time = new FakeTimeProvider();
  const closed: string[] = [];
  let release = () => {};
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addTimeout({ timeout: 10, cooperative: true }).build();
  const done = drain(
    p.executeStream(async function* () {
      try {
        await new Promise<void>((resolve) => (release = resolve)); // ignores the signal
        yield 'late';
      } finally {
        closed.push('late');
      }
    }),
  ).catch((e) => e);
  await time.advance(10);
  release();
  assert.ok((await done) instanceof TimeoutRejectedError);
  assert.deepEqual(closed, ['late']);
});

// --- timeout ---

test('timeoutGenerator: per-execution timeout; undefined keeps the default, Infinity and 0 disable it', async () => {
  const time = new FakeTimeProvider();
  const timeouts: number[] = [];
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addTimeout({ timeout: '1s', timeoutGenerator: ({ context }) => context.properties.get('t') as never, onTimeout: (a) => void timeouts.push(a.timeout) })
    .build();
  const run = (t?: unknown) => p.executeOutcome((ctx) => untilAborted(ctx.signal), { properties: new Map([['t', t]]) });

  const custom = run('50ms');
  await time.advance(50);
  const o1 = await custom;
  assert.ok(!o1.ok && o1.error instanceof TimeoutRejectedError && o1.error.timeout === 50);

  const fallback = run(undefined);
  await time.advance(1000);
  assert.ok(!(await fallback).ok);
  assert.deepEqual(timeouts, [50, 1000]);

  for (const off of [Infinity, 0]) {
    let settled = false;
    const unlimited = p.executeOutcome(async () => 'done', { properties: new Map([['t', off]]) }).finally(() => (settled = true));
    assert.equal(time.pendingTimers, 0);
    assert.deepEqual(await unlimited, { ok: true, result: 'done' });
    assert.ok(settled);
  }
  assert.throws(() => new ResiliencePipelineBuilder().addTimeout(0).build(), RangeError);
});

test('timeout: a caller abort wins over the timeout; keepLateResult only keeps successes', async () => {
  const time = new FakeTimeProvider();
  const ac = new AbortController();
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addTimeout({ timeout: 10, cooperative: true, keepLateResult: true }).build();
  let finish: (v: unknown) => void = () => {};
  const late = p.executeOutcome(() => new Promise((_, reject) => (finish = reject)));
  await time.advance(10);
  finish(new Error('late failure'));
  const o = await late;
  assert.ok(!o.ok && o.error instanceof TimeoutRejectedError, 'a late failure becomes the timeout');

  const aborted = p.executeOutcome((ctx) => untilAborted(ctx.signal), { signal: ac.signal });
  ac.abort(new Error('user'));
  const a = await aborted;
  assert.ok(!a.ok && (a.error as Error).message === 'user');
});

// --- hedging ---

test('onHedging: called for every hedged attempt with its number and elapsed time', async () => {
  const time = new FakeTimeProvider();
  const calls: OnHedgingArgs[] = [];
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10, maxHedgedAttempts: 2, onHedging: (a) => void calls.push(a) }).build();
  const ac = new AbortController();
  const done = p.executeOutcome((ctx) => untilAborted(ctx.signal), { operationKey: 'k', signal: ac.signal });
  await time.advance(10);
  await time.advance(10);
  assert.deepEqual(
    calls.map((c) => [c.attemptNumber, c.duration, c.context.operationKey]),
    [
      [1, 10, 'k'],
      [2, 20, 'k'],
    ],
  );
  // No more hedges left: it waits for the running attempts; the caller's abort ends them.
  assert.equal(time.pendingTimers, 0);
  ac.abort(new Error('done'));
  assert.equal((await done).ok, false);
});

test('parallel hedging: the first success wins and every loser is aborted', async () => {
  const signals: AbortSignal[] = [];
  let release = () => {};
  const gate = new Promise<void>((resolve) => (release = resolve));
  const p = new ResiliencePipelineBuilder().addHedging({ delay: 0, maxHedgedAttempts: 2 }).build();
  const done = p.execute(async (ctx) => {
    signals[ctx.attemptNumber] = ctx.signal;
    if (ctx.attemptNumber === 1) {
      await gate;
      return 'winner';
    }
    return untilAborted(ctx.signal);
  });
  while (signals.filter(Boolean).length < 3) await tick(); // all three start at once
  release();
  assert.equal(await done, 'winner');
  assert.deepEqual(
    signals.map((s) => s.aborted),
    [true, false, true],
  );
});

test('hedging failure paths', async (t) => {
  await t.test('every attempt fails: the primary error is returned', async () => {
    let n = 0;
    const p = new ResiliencePipelineBuilder().addHedging({ delay: -1, maxHedgedAttempts: 2 }).build();
    const o = await p.executeOutcome(async () => {
      throw new Error(`e${n++}`);
    });
    assert.ok(!o.ok && (o.error as Error).message === 'e0');
    assert.equal(n, 3);
  });

  await t.test('an unhandled failure is returned right away', async () => {
    let n = 0;
    const p = new ResiliencePipelineBuilder().addHedging({ delay: -1, shouldHandle: () => false }).build();
    const o = await p.executeOutcome(async () => {
      n++;
      throw new Error('final');
    });
    assert.ok(!o.ok && (o.error as Error).message === 'final');
    assert.equal(n, 1);
  });

  await t.test('non-idempotent: a started failure is not hedged, a rejection (never started) is', async () => {
    const time = new FakeTimeProvider();
    let n = 0;
    const p = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10 }).build();
    const o = await p.executeOutcome(async () => {
      n++;
      throw new Error('charged?');
    }, { idempotent: false });
    assert.equal(o.ok, false);
    assert.equal(n, 1);
    assert.equal(time.pendingTimers, 0, 'no hedging timer for a non-idempotent call');

    let hedges = 0;
    const limited = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: -1, shouldHandle: () => true, onHedging: () => void hedges++ }).addConcurrencyLimiter(1).build();
    const busy = limited.execute(() => never());
    let calls = 0;
    const rejected = await limited.executeOutcome(() => ++calls, { idempotent: false });
    // The rejection said nothing was called, so the second attempt ran; it was rejected too.
    assert.ok(!rejected.ok);
    assert.equal(calls, 0);
    assert.equal(hedges, 1);
    void busy;
  });

  await t.test('a caller abort stops hedging', async () => {
    const ac = new AbortController();
    let n = 0;
    const p = new ResiliencePipelineBuilder().addHedging({ delay: -1, maxHedgedAttempts: 3 }).build();
    const o = await p.executeOutcome(async () => {
      n++;
      ac.abort(new Error('gone'));
      throw new Error('fail');
    }, { signal: ac.signal });
    assert.equal(o.ok, false);
    assert.equal(n, 1);
  });

  await t.test('delayGenerator decides each wait; actionGenerator returning undefined repeats the callback', async () => {
    const time = new FakeTimeProvider();
    const waits: number[] = [];
    let n = 0;
    const p = new ResiliencePipelineBuilder({ timeProvider: time })
      .addHedging({ delay: 999, maxHedgedAttempts: 2, delayGenerator: ({ attemptNumber }) => (waits.push(attemptNumber), attemptNumber === 1 ? '5ms' : undefined), actionGenerator: () => undefined })
      .build();
    const done = p.execute(async (ctx) => (n++ === 2 ? 'third' : untilAborted(ctx.signal)));
    await time.advance(5);
    assert.equal(n, 2);
    await time.advance(999);
    assert.equal(await done, 'third');
    assert.deepEqual(waits, [1, 2]);
  });

  await t.test('actionGenerator callback runs the rest of the pipeline and throws its failure', async () => {
    const p = new ResiliencePipelineBuilder()
      .addHedging({ delay: -1, actionGenerator: ({ callback }) => (ctx) => callback(ctx).catch((e: Error) => `recovered ${e.message}`) })
      .build();
    let n = 0;
    assert.equal(
      await p.execute(async () => {
        throw new Error(`e${n++}`);
      }),
      'recovered e1',
    );
  });

  assert.throws(() => new ResiliencePipelineBuilder().addHedging({ maxHedgedAttempts: 0 }).build(), RangeError);
});

// --- retry delays ---

async function retryDelays(options: Parameters<ResiliencePipelineBuilder['addRetry']>[0], error: unknown = new Error('x')) {
  const time = new FakeTimeProvider();
  const delays: number[] = [];
  const p = new ResiliencePipelineBuilder({ timeProvider: time })
    .addRetry({ maxRetryAttempts: 3, ...options, onRetry: (a: OnRetryArgs<unknown>) => void delays.push(a.retryDelay) })
    .build();
  const done = p.executeOutcome(() => Promise.reject(error));
  await time.runAll();
  await done;
  return delays;
}

test('useJitter: true is ±25% for constant and linear', async () => {
  assert.deepEqual(await retryDelays({ delay: 100, useJitter: true, randomizer: () => 0 }), [75, 75, 75]);
  assert.deepEqual(await retryDelays({ delay: 100, useJitter: true, randomizer: () => 1 }), [125, 125, 125]);
  assert.deepEqual(await retryDelays({ delay: 100, backoffType: 'linear', useJitter: true, randomizer: () => 0.5 }), [100, 200, 300]);
  // The ±25% is capped by maxDelay too.
  assert.deepEqual(await retryDelays({ delay: 100, maxDelay: 110, useJitter: true, randomizer: () => 1 }), [110, 110, 110]);
});

test("useJitter: 'full' spreads over [0, d), 'equal' over [d/2, d)", async () => {
  assert.deepEqual(await retryDelays({ delay: 100, backoffType: 'exponential', useJitter: 'full', randomizer: () => 0.5 }), [50, 100, 200]);
  assert.deepEqual(await retryDelays({ delay: 100, backoffType: 'exponential', useJitter: 'equal', randomizer: () => 0.5 }), [75, 150, 300]);
  assert.deepEqual(await retryDelays({ delay: 100, backoffType: 'exponential', maxDelay: 100, useJitter: 'full', randomizer: () => 0.5 }), [50, 50, 50]);
});

test('useJitter: true with exponential is decorrelated jitter V2 (ignores factor, capped by maxDelay)', async () => {
  const expected: number[] = [];
  let prev = 0;
  for (let attempt = 0; attempt < 3; attempt++) {
    const t = attempt + 0.5;
    const next = 2 ** t * Math.tanh(Math.sqrt(4 * t));
    expected.push(((next - prev) / 1.4) * 100);
    prev = next;
  }
  const opts = { delay: 100, backoffType: 'exponential' as const, useJitter: true, randomizer: () => 0.5 };
  const got = await retryDelays(opts);
  got.forEach((d, i) => assert.ok(Math.abs(d - expected[i]!) < 1e-9, `${d} vs ${expected[i]}`));
  assert.deepEqual(await retryDelays({ ...opts, factor: 5 }), got, 'factor is ignored');
  assert.deepEqual(await retryDelays({ ...opts, maxDelay: 150 }), got.map((d) => Math.min(d, 150)));
  // Far attempts overflow to Infinity instead of NaN, then the cap applies.
  assert.equal(computeDelay('exponential', true, 2000, 100, 1000), 1000);
  assert.equal(computeDelay('exponential', true, 2000, 100, undefined), Infinity);
});

test('backoff helpers take useJitter like retry does', () => {
  assert.equal(backoff.constant({ delay: 100, useJitter: 'equal', randomizer: () => 0 })(3), 50);
  assert.equal(backoff.linear({ delay: 100, useJitter: 'full', randomizer: () => 0.5 })({ attemptNumber: 1 }), 100);
  assert.equal(backoff.exponential({ delay: 100, useJitter: false })(2), 400);
  assert.throws(() => backoff.exponential({ delay: 100, factor: 0.5 }), RangeError);
  assert.throws(() => backoff.sequence([]), RangeError);
});

test('respectRetryAfter: default waits for the hint (capped by maxDelay); false ignores it', async () => {
  const limited = Object.assign(new Error('slow down'), { retryAfter: 5000 });
  assert.deepEqual(await retryDelays({ delay: 10, maxRetryAttempts: 1 }, limited), [5000]);
  assert.deepEqual(await retryDelays({ delay: 10, maxRetryAttempts: 1, maxDelay: 1000 }, limited), [1000]);
  assert.deepEqual(await retryDelays({ delay: 10, maxRetryAttempts: 1, respectRetryAfter: false }, limited), [10]);
  // A hint shorter than the computed delay doesn't shorten it.
  assert.deepEqual(await retryDelays({ delay: 10, maxRetryAttempts: 1 }, Object.assign(new Error(), { retryAfter: 1 })), [10]);
});

// --- linkSignals ---

test('linkSignals: follows its parents until disposed, then detaches', () => {
  const a = new AbortController();
  const b = new AbortController();
  const link = linkSignals(a.signal, undefined, b.signal);
  b.abort('b');
  assert.equal(link.signal.aborted, true);
  assert.equal(link.signal.reason, 'b');

  const detached = linkSignals(a.signal);
  detached.dispose();
  a.abort('a');
  assert.equal(detached.signal.aborted, false, 'a disposed link no longer follows its parent');

  const already = linkSignals(AbortSignal.abort('early'), new AbortController().signal);
  assert.equal(already.signal.reason, 'early');
});
