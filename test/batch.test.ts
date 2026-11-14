import assert from 'node:assert/strict';
import { test } from 'node:test';
import { all, map, waitFor } from '../src/batch.ts';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { TimeoutRejectedError } from '../src/core.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const defer = <T = void>() => {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

test('all: results in task order, typed as a tuple', async () => {
  const [a, b] = await all([async () => (await sleep(10), 1), () => 'two'] as const);
  assert.equal(a, 1);
  assert.equal(b, 'two');
});

test('all: the first failure aborts the other tasks and rejects', async () => {
  let abortedWith: unknown;
  const boom = new Error('boom');
  await assert.rejects(
    all([
      ({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject((abortedWith = signal.reason)))),
      async () => {
        await sleep(5);
        throw boom;
      },
    ]),
    /boom/,
  );
  assert.equal(abortedWith, boom);
});

test('map: input order, concurrency cap respected', async () => {
  let running = 0;
  let max = 0;
  const result = await map(
    [30, 10, 20, 5, 15],
    async (ms, i) => {
      max = Math.max(max, ++running);
      await sleep(ms);
      running--;
      return `${i}:${ms}`;
    },
    { concurrency: 2 },
  );
  assert.deepEqual(result, ['0:30', '1:10', '2:20', '3:5', '4:15']);
  assert.equal(max, 2);
});

test('map: stops on the first error, aborts running items and skips the rest', async () => {
  const started: number[] = [];
  let aborted = false;
  await assert.rejects(
    map(
      [0, 1, 2, 3, 4],
      (i, _index, { signal }) => {
        started.push(i);
        if (i === 1) throw new Error('item 1');
        return new Promise((resolve, reject) => {
          signal.addEventListener('abort', () => ((aborted = true), reject(signal.reason)));
          setTimeout(resolve, 50);
        });
      },
      { concurrency: 2 },
    ),
    /item 1/,
  );
  await sleep(10);
  assert.deepEqual(started, [0, 1]);
  assert.equal(aborted, true);
});

test('map: stopOnError false returns an outcome per item', async () => {
  const outcomes = await map(
    [1, 2, 3],
    (n) => {
      if (n === 2) throw new Error('two');
      return n * 10;
    },
    { stopOnError: false, concurrency: 1 },
  );
  assert.deepEqual(outcomes[0], { ok: true, result: 10 });
  assert.equal(outcomes[1]!.ok, false);
  assert.match(String(!outcomes[1]!.ok && outcomes[1]!.error), /two/);
  assert.deepEqual(outcomes[2], { ok: true, result: 30 });
});

test('map: the pipeline runs per item (each item gets its own retries)', async () => {
  const attempts = new Map<number, number>();
  const pipeline = new ResiliencePipelineBuilder().addRetry({ maxRetryAttempts: 2, delay: 0 }).build();
  const result = await map(
    [1, 2, 3],
    (n) => {
      const a = (attempts.get(n) ?? 0) + 1;
      attempts.set(n, a);
      if (a < 3) throw new Error('transient');
      return n;
    },
    { pipeline, concurrency: 3 },
  );
  assert.deepEqual(result, [1, 2, 3]);
  assert.deepEqual([...attempts.values()], [3, 3, 3]);
});

test('map: a caller abort rejects', async () => {
  const controller = new AbortController();
  const started: number[] = [];
  const entered = defer<void>();
  const run = map(
    [1, 2, 3],
    (n, _i, { signal }) => {
      started.push(n);
      entered.resolve();
      return new Promise((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));
    },
    { concurrency: 1, signal: controller.signal },
  );
  await entered.promise;
  controller.abort(new Error('stop'));
  await assert.rejects(run, /stop/);
  assert.deepEqual(started, [1], 'the items after the abort never start');
});

test('waitFor: resolves once the condition is true', async () => {
  const time = new FakeTimeProvider();
  let checks = 0;
  const done = waitFor(async () => ++checks === 3, { interval: '1s', timeout: '10s', timeProvider: time });
  await time.advance('2s');
  await done;
  assert.equal(checks, 3);
  assert.equal(time.pendingTimers, 0);
});

test('waitFor: rejects with TimeoutRejectedError, also when the condition hangs', async () => {
  const time = new FakeTimeProvider();
  const polling = assert.rejects(waitFor(() => false, { interval: '1s', timeout: '5s', timeProvider: time }), TimeoutRejectedError);
  await time.advance('5s');
  await polling;

  let signalAborted = false;
  const hanging = assert.rejects(
    waitFor(() => new Promise<boolean>(() => {}), { interval: 10, timeout: '1s', timeProvider: time }),
    TimeoutRejectedError,
  );
  const probe = waitFor(
    (signal) => new Promise<boolean>((resolve) => signal.addEventListener('abort', () => ((signalAborted = true), resolve(false)))),
    { interval: 10, timeout: '1s', timeProvider: time },
  ).catch(() => {});
  await time.advance('1s');
  await hanging;
  await probe;
  assert.equal(signalAborted, true, 'the condition got an aborted signal');
});
