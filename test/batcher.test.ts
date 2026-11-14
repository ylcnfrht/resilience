import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createBatcher } from '../src/batcher.ts';
import { ResiliencePipelineBuilder } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

test('batcher: flushes after maxWait and maps results back by index', async () => {
  const time = new FakeTimeProvider();
  const calls: number[][] = [];
  const b = createBatcher({ maxSize: 10, maxWait: '10ms', timeProvider: time, run: async (items: number[]) => (calls.push(items), items.map((n) => n * 2)) });
  const all = Promise.all([b.load(1), b.load(2), b.load(3)]);
  await time.advance(9);
  assert.equal(calls.length, 0);
  await time.advance(1);
  assert.deepEqual(await all, [2, 4, 6]);
  assert.deepEqual(calls, [[1, 2, 3]]);
});

test('batcher: flushes at maxSize without waiting', async () => {
  const time = new FakeTimeProvider();
  const calls: number[][] = [];
  const b = createBatcher({ maxSize: 2, maxWait: '1h', timeProvider: time, run: async (items: number[]) => (calls.push(items), items) });
  const first = Promise.all([b.load(1), b.load(2)]);
  const third = b.load(3);
  assert.deepEqual(await first, [1, 2]);
  assert.deepEqual(calls, [[1, 2]]);
  await time.advance('1h');
  assert.equal(await third, 3);
  assert.equal(time.pendingTimers, 0);
});

test('batcher: dedupes by key, Map results, per-item Error and missing key reject only that item', async () => {
  const time = new FakeTimeProvider();
  const calls: string[][] = [];
  const b = createBatcher({
    maxSize: 10,
    maxWait: 5,
    timeProvider: time,
    key: (id: string) => id,
    run: async (ids: string[]) => (calls.push(ids), new Map<string, string | Error>([['a', 'A'], ['bad', new Error('nope')]])),
  });
  const results = Promise.allSettled([b.load('a'), b.load('a'), b.load('bad'), b.load('missing')]);
  await time.advance(5);
  const [a1, a2, bad, missing] = await results;
  assert.deepEqual(calls, [['a', 'bad', 'missing']]);
  assert.deepEqual([a1, a2], [{ status: 'fulfilled', value: 'A' }, { status: 'fulfilled', value: 'A' }]);
  assert.equal(bad.status === 'rejected' && bad.reason.message, 'nope');
  assert.match(missing.status === 'rejected' ? missing.reason.message : '', /no result for key missing/);
});

test('batcher: caller abort (explicit or ambient) drops the item before the batch starts', async () => {
  const time = new FakeTimeProvider();
  const calls: number[][] = [];
  const b = createBatcher({ maxSize: 10, maxWait: 5, timeProvider: time, run: async (items: number[]) => (calls.push(items), items) });
  const explicit = new AbortController();
  const ambient = new AbortController();
  const kept = b.load(1);
  const dropped = b.load(2, { signal: explicit.signal });
  // Inside a caller's pipeline, load() follows that execution's signal (deadline propagation).
  const inner = new ResiliencePipelineBuilder().build().execute(() => b.load(3), { signal: ambient.signal });
  explicit.abort(new Error('gone'));
  ambient.abort(new Error('deadline'));
  await assert.rejects(dropped, /gone/);
  await assert.rejects(inner, /deadline/);
  await time.advance(5);
  assert.equal(await kept, 1);
  assert.deepEqual(calls, [[1]]);
});

test('batcher: the batch runs through the pipeline as a unit (retried together), and a failing batch rejects every item', async () => {
  const time = new FakeTimeProvider();
  let attempts = 0;
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addRetry({ maxRetryAttempts: 1, delay: 0 }).build();
  const b = createBatcher({
    maxSize: 2,
    maxWait: 5,
    timeProvider: time,
    pipeline,
    run: async (items: number[]) => {
      if (++attempts === 1) throw new Error('flaky');
      return items;
    },
  });
  assert.deepEqual(await Promise.all([b.load(1), b.load(2)]), [1, 2]);
  assert.equal(attempts, 2);

  const broken = createBatcher({ maxSize: 2, maxWait: 5, timeProvider: time, run: async () => [1] });
  const r = Promise.allSettled([broken.load(1), broken.load(2)]);
  assert.deepEqual((await r).map((x) => x.status), ['rejected', 'rejected']);
});

test("batcher: a started batch isn't cancelled by the caller whose execution triggered the flush", async () => {
  let batchSignal: AbortSignal | undefined;
  let release!: () => void;
  const b = createBatcher({ maxSize: 1, maxWait: 5, run: async (items: number[], { signal }) => ((batchSignal = signal), await new Promise<void>((r) => (release = r)), items) });
  const caller = new AbortController();
  const inner = new ResiliencePipelineBuilder().build().execute(() => b.load(1), { signal: caller.signal });
  await new Promise((r) => setImmediate(r));
  caller.abort(new Error('caller gone'));
  await assert.rejects(inner, /caller gone/);
  assert.equal(batchSignal?.aborted, false);
  release();
});
