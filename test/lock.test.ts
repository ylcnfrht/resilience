import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { LOCK_TOKEN, LockLostError, LockNotAcquiredError, type LockOptions, LockStrategy, MemoryLockStore, getLockToken } from '../src/lock.ts';
import { FakeTimeProvider } from '../src/testing.ts';

function setup(options: Partial<LockOptions> = {}) {
  const time = new FakeTimeProvider();
  const store = new MemoryLockStore({ timeProvider: time });
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new LockStrategy({ key: () => 'job', store, ...options }, s)).build();
  return { time, store, pipeline };
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

test('lock: mutual exclusion; a waiter runs after the holder releases', async () => {
  const { time, pipeline } = setup({ wait: '10s' });
  const gate = deferred();
  let running = 0;
  let maxRunning = 0;
  const order: string[] = [];
  const job = (name: string, until?: Promise<void>) =>
    pipeline.execute(async () => {
      maxRunning = Math.max(maxRunning, ++running);
      order.push(name);
      await until;
      running--;
    });
  const first = job('a', gate.promise);
  const second = job('b');
  await time.advance('2s');
  assert.deepEqual(order, ['a']);
  gate.resolve();
  await first;
  await time.advance('2s');
  await second;
  assert.deepEqual(order, ['a', 'b']);
  assert.equal(maxRunning, 1);
});

test('lock: fail fast by default; waiting gives up after `wait`', async () => {
  const { time, pipeline } = setup();
  const gate = deferred();
  const holder = pipeline.execute(() => gate.promise);
  await time.advance(0);
  await assert.rejects(pipeline.execute(() => 'x'), LockNotAcquiredError);

  const waiting = setup({ wait: '1s' });
  const hold = waiting.pipeline.execute(() => gate.promise);
  const late = assert.rejects(waiting.pipeline.execute(() => 'x'), (e: unknown) => e instanceof LockNotAcquiredError && e.key === 'job');
  await waiting.time.advance('999ms');
  let settled = false;
  void late.then(() => (settled = true));
  await waiting.time.advance(0);
  assert.equal(settled, false, 'still waiting before 1s');
  await waiting.time.advance('1ms');
  await late;
  gate.resolve();
  await Promise.all([holder, hold]);
});

test('lock: released when the callback throws', async () => {
  const { pipeline } = setup();
  await assert.rejects(
    pipeline.execute(() => {
      throw new Error('boom');
    }),
    /boom/,
  );
  assert.equal(await pipeline.execute(() => 'next'), 'next');
});

test('lock: renewal keeps the lease alive past ttl', async () => {
  const { time, store, pipeline } = setup({ ttl: '3s' });
  const gate = deferred();
  const run = pipeline.execute(() => gate.promise);
  await time.advance('10s');
  assert.equal(await store.acquire('job', 'intruder', 1000), undefined, 'lease still held after 10s');
  gate.resolve();
  await run;
  assert.ok(await store.acquire('job', 'intruder', 1000), 'free after release');
});

test('lock: a lost lease aborts the callback with LockLostError and calls onLockLost', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryLockStore({ timeProvider: time });
  store.renew = async () => false; // someone else took over
  const lost: string[] = [];
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
    .addStrategy((s) => new LockStrategy({ key: () => 'job', store, ttl: '3s', onLockLost: (a) => void lost.push(a.key) }, s))
    .build();
  let reason: unknown;
  const run = assert.rejects(
    pipeline.execute(({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject((reason = signal.reason))))),
    LockLostError,
  );
  await time.advance('1s');
  await run;
  assert.ok(reason instanceof LockLostError);
  assert.deepEqual(lost, ['job']);
  assert.equal(time.pendingTimers, 0, 'renewal stopped');
});

test('lock: store errors during renewal are tolerated until the lease expires', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryLockStore({ timeProvider: time });
  store.renew = async () => {
    throw new Error('redis down');
  };
  const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addStrategy((s) => new LockStrategy({ key: () => 'job', store, ttl: '3s' }, s)).build();
  let abortedAt: number | undefined;
  const start = time.now();
  const run = assert.rejects(
    pipeline.execute(({ signal }) => new Promise((_, reject) => signal.addEventListener('abort', () => reject((abortedAt = time.now() - start, signal.reason))))),
    LockLostError,
  );
  await time.advance('5s');
  await run;
  assert.equal(abortedAt, 3000, 'aborted when the last lease ran out, not at the first failed renewal');
});

test('lock: fencing tokens increase per key and are readable in the callback', async () => {
  const { pipeline } = setup();
  const tokens: Array<number | undefined> = [];
  for (let i = 0; i < 3; i++) await pipeline.execute((ctx) => void tokens.push(getLockToken(), ctx.properties.get(LOCK_TOKEN) as number));
  assert.deepEqual(tokens, [1, 1, 2, 2, 3, 3]);
  assert.equal(getLockToken(), undefined, 'outside an execution');
});

test('lock: aborting while waiting rejects with the abort reason and never runs the callback', async () => {
  const { time, pipeline } = setup({ wait: '1m' });
  const gate = deferred();
  const holder = pipeline.execute(() => gate.promise);
  const controller = new AbortController();
  let ran = false;
  const waiting = assert.rejects(
    pipeline.execute(() => void (ran = true), { signal: controller.signal }),
    (e: unknown) => e === 'cancelled',
  );
  await time.advance('5s');
  controller.abort('cancelled');
  await waiting;
  gate.resolve();
  await holder;
  assert.equal(ran, false);
});
