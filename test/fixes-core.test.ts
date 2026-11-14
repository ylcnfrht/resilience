import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ResiliencePipelineBuilder, ResiliencePipelineRegistry, TimeoutRejectedError, onTelemetry, toMs } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const tick = () => new Promise<void>((resolve) => setImmediate(resolve));
const untilAborted = (signal: AbortSignal) => new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));

test('a throwing telemetry listener never fails or retries the call', async () => {
  const off = onTelemetry(() => {
    throw new Error('listener bug');
  });
  try {
    let calls = 0;
    const pipeline = new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build();
    const outcome = await pipeline.executeOutcome(async () => (calls++, 'charged'));
    assert.deepEqual(outcome, { ok: true, result: 'charged' });
    assert.equal(calls, 1);
  } finally {
    off();
  }
});

for (const hook of ['actionGenerator', 'onHedging', 'delayGenerator'] as const) {
  test(`hedging: a throwing ${hook} aborts the running attempt`, async () => {
    const time = new FakeTimeProvider();
    const boom = () => {
      throw new Error('boom');
    };
    const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10, [hook]: boom }).build();
    let primary: AbortSignal | undefined;
    const done = pipeline.executeOutcome((ctx) => ((primary = ctx.signal), untilAborted(ctx.signal)));
    await time.advance(10);
    const outcome = await done;
    assert.equal(outcome.ok, false);
    assert.equal(primary!.aborted, true);
  });
}

test('hedging + executeStream: the losing stream is closed when both first chunks arrive together', async () => {
  const time = new FakeTimeProvider();
  const closed: number[] = [];
  let open!: () => void;
  const gate = new Promise<void>((resolve) => (open = resolve));
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addHedging({ delay: 10 }).build();
  const seen: number[] = [];
  const consume = (async () => {
    for await (const chunk of pipeline.executeStream(async function* (ctx) {
      try {
        await gate; // both attempts get their first chunk in the same tick
        yield ctx.attemptNumber;
        yield ctx.attemptNumber;
      } finally {
        closed.push(ctx.attemptNumber);
      }
    }))
      seen.push(chunk);
  })();
  await time.advance(10); // starts the hedged attempt
  open();
  await consume;
  await tick();
  assert.equal(seen.length, 2);
  assert.deepEqual(closed.sort(), [0, 1]);
});

test('retry around hedging: attempt numbers are unique across the execution', async () => {
  const seen: number[] = [];
  const pipeline = new ResiliencePipelineBuilder().addRetry({ delay: 0, maxRetryAttempts: 2 }).addHedging({ delay: -1, maxHedgedAttempts: 1 }).build();
  await pipeline.executeOutcome(async (ctx) => {
    seen.push(ctx.attemptNumber);
    throw new Error('x');
  });
  assert.deepEqual(seen, [0, 1, 2, 3, 4, 5]);
});

test('timeout above ~24.8 days still fires (chained timers)', async () => {
  const time = new FakeTimeProvider();
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addTimeout('30d').build();
  let settled = false;
  const done = pipeline.executeOutcome((ctx) => untilAborted(ctx.signal)).finally(() => (settled = true));
  await time.advance(0);
  assert.equal(time.pendingTimers, 1);
  await time.advance('29d');
  assert.equal(settled, false);
  await time.advance('1d');
  const outcome = await done;
  assert.ok(!outcome.ok && outcome.error instanceof TimeoutRejectedError);
  assert.equal(time.pendingTimers, 0);
});

test('toMs accepts every number syntax the Duration type allows and rejects NaN', () => {
  assert.equal(toMs('1e3ms'), 1000);
  assert.equal(toMs('.5s'), 500);
  assert.equal(toMs('+5s'), 5000);
  assert.equal(toMs('0x10ms'), 16);
  assert.equal(toMs('1.s'), 1000);
  assert.equal(toMs('-5s'), -5000);
  assert.equal(toMs('2w'), 1_209_600_000);
  assert.throws(() => toMs(NaN), RangeError);
  assert.throws(() => toMs('NaNms' as never), RangeError);
  assert.throws(() => toMs('5 minutes' as never), RangeError);
  assert.throws(() => new ResiliencePipelineBuilder().addHedging({ delay: NaN }).build(), RangeError);
});

test('registry: pipeline-level timing uses the registry clock, and the inner pipeline kill switch works', async () => {
  const time = new FakeTimeProvider();
  const registry = new ResiliencePipelineRegistry({ timeProvider: time });
  const pipeline = registry.getOrAddPipeline('k', (b) => b.addRetry({ delay: 0, maxRetryAttempts: 3 }));
  let duration: unknown;
  const off = onTelemetry((e) => {
    if (e.event === 'OnPipelineExecuted') duration = (e.args as { duration: number }).duration;
  });
  try {
    await pipeline.execute(() => time.advance('5s'));
  } finally {
    off();
  }
  assert.equal(duration, 5000);

  (pipeline as unknown as { current: { disable(): void } }).current.disable();
  let calls = 0;
  await pipeline.executeOutcome(async () => {
    calls++;
    throw new Error('x');
  });
  assert.equal(calls, 1);
});
