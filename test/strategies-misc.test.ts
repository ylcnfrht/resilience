import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BrokenCircuitError, IsolatedCircuitError, createContext, fail, ok, onTelemetry } from '../src/core.ts';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { FakeTimeProvider } from '../src/testing.ts';
import {
  type CircuitBreakerHandle,
  type CircuitSnapshot,
  type OnCircuitOpenedArgs,
  CircuitBreakerManualControl,
  CircuitBreakerStateProvider,
  CircuitBreakerStrategy,
} from '../src/circuit-breaker.ts';
import { MemoryCircuitStateStore, circuitHealthCheck, sharedCircuitState } from '../src/circuit-extensions.ts';
import { LOCK_TOKEN, LockLostError, LockNotAcquiredError, LockStrategy, MemoryLockStore, getLockToken } from '../src/lock.ts';
import { CacheStrategy, MemoryCacheStore, coalesceStrategy } from '../src/cache.ts';
import { allSettled, map, waitFor } from '../src/batch.ts';
import { createBatcher } from '../src/batcher.ts';
import { createStats } from '../src/stats.ts';

const defer = <T = void>() => {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
};
const src = (time: FakeTimeProvider) => ({ pipelineName: 'p', strategyName: 's', timeProvider: time });
const value = <T>(v: T) => async () => ok(v);
const boom = (message = 'boom') => async () => fail(Object.assign(new Error(message), { status: 503 }));
const err = (o: { ok: boolean; error?: unknown }) => (assert.equal(o.ok, false), o.error);
const events = (name: string) => {
  const seen: unknown[] = [];
  const off = onTelemetry((e) => void (e.event === name && seen.push(e.args)));
  return { seen, off };
};

// --- circuit breaker ---

test('breakDurationGenerator: string / undefined / throwing results, halfOpenAttempts grows, calls stay blocked while it runs', async () => {
  const time = new FakeTimeProvider();
  const seen: Array<[number, number]> = [];
  const opened: number[] = [];
  let pause: ReturnType<typeof defer<void>> | undefined;
  const cb = new CircuitBreakerStrategy<string>(
    {
      consecutiveFailures: 1,
      breakDuration: 100,
      breakDurationGenerator: async ({ halfOpenAttempts, failureCount }) => {
        seen.push([halfOpenAttempts, failureCount]);
        if (pause) await pause.promise;
        if (halfOpenAttempts === 0) return '2s' as const;
        if (halfOpenAttempts === 1) return undefined as never; // keep breakDuration
        throw new Error('generator broke');
      },
      onOpened: (a) => void opened.push(a.breakDuration),
    },
    src(time),
  );
  pause = defer();
  const opening = cb.execute(boom(), createContext());
  await time.advance(0);
  assert.equal(cb.state, 'open');
  const blocked = err(await cb.execute(value('x'), createContext()));
  assert.ok(blocked instanceof BrokenCircuitError && blocked.retryAfter === Infinity, 'blocked while the generator is pending');
  pause.resolve();
  pause = undefined;
  await opening;
  await time.advance(1999);
  assert.ok(err(await cb.execute(value('x'), createContext())) instanceof BrokenCircuitError);
  await time.advance(1);
  await cb.execute(boom(), createContext()); // failed probe: attempt 1
  await time.advance(100);
  await cb.execute(boom(), createContext()); // failed probe: attempt 2, generator throws
  assert.deepEqual(opened, [2000, 100, 100]);
  assert.deepEqual(seen, [[0, 1], [1, 1], [2, 1]], 'metrics: the closed window, kept until the circuit closes (probes are not recorded)');
  await time.advance(100);
  assert.deepEqual(await cb.execute(value('ok'), createContext()), ok('ok'));
  assert.equal(cb.state, 'closed');
});

test('breakDurationGenerator: a sync exponential generator', async () => {
  const time = new FakeTimeProvider();
  const opened: number[] = [];
  const cb = new CircuitBreakerStrategy({ consecutiveFailures: 1, breakDurationGenerator: ({ halfOpenAttempts }) => 1000 * 2 ** halfOpenAttempts, onOpened: (a) => void opened.push(a.breakDuration) }, src(time));
  await cb.execute(boom(), createContext());
  for (const wait of [1000, 2000, 4000]) {
    await time.advance(wait);
    await cb.execute(boom(), createContext());
  }
  assert.deepEqual(opened, [1000, 2000, 4000, 8000]);
});

test('isolation beats trip and adopt; releases stack; adopt takes only newer remote state', async () => {
  const time = new FakeTimeProvider();
  const control = new CircuitBreakerManualControl();
  const opened: Array<OnCircuitOpenedArgs<unknown>> = [];
  let closed = 0;
  let handle!: CircuitBreakerHandle;
  const cb = new CircuitBreakerStrategy({ manualControl: control, extensions: [(h) => void (handle = h)], onOpened: (a) => void opened.push(a), onClosed: () => void closed++ }, src(time));
  const remote = (state: CircuitSnapshot['state'], updatedAt?: number): CircuitSnapshot => ({ state, openUntil: time.now() + 60_000, consecutiveFailures: 0, halfOpenAttempts: 0, ...(updatedAt !== undefined && { updatedAt }) });

  const release = await control.isolate();
  await cb.trip();
  await handle.trip(new Error('health'));
  assert.equal(handle.adopt(remote('closed', time.now() + 1e6)), false, 'an isolated breaker ignores remote state');
  assert.equal(cb.state, 'isolated');
  assert.equal(opened.length, 1, 'only the isolation itself opened it');
  assert.ok(err(await cb.execute(value(1), createContext())) instanceof IsolatedCircuitError);
  await release();
  await release();
  assert.equal(cb.state, 'closed');
  assert.equal(closed, 1, 'a second release does nothing');

  assert.equal(handle.adopt(remote('open', 1)), false, 'older than ours');
  assert.equal(handle.adopt(remote('open')), false, 'no updatedAt counts as 0');
  assert.equal(handle.adopt(remote('half-open', time.now() + 10)), true);
  assert.equal(cb.state, 'open');
  assert.equal(cb.snapshot().openUntil, 0, 'a remote half-open resumes as open-and-due');
  assert.deepEqual(await cb.execute(value('probe'), createContext()), ok('probe'));
  assert.equal(cb.state, 'closed');

  const reason = new Error('health check');
  await handle.trip(reason);
  await cb.reset();
  await handle.trip();
  assert.deepEqual(opened.slice(1).map((a) => (a.outcome && !a.outcome.ok ? a.outcome.error : a.outcome)), [reason, undefined]);
});

test('manual control created isolated: the breaker starts isolated until close()', async () => {
  const time = new FakeTimeProvider();
  const control = new CircuitBreakerManualControl(true);
  assert.equal(control.isIsolated, true);
  const cb = new CircuitBreakerStrategy({ manualControl: control }, src(time));
  assert.equal(cb.state, 'isolated');
  assert.ok(err(await cb.execute(value(1), createContext())) instanceof IsolatedCircuitError);
  await control.close();
  assert.deepEqual(await cb.execute(value(2), createContext()), ok(2));
});

test('state provider: defaults before it is attached', () => {
  const p = new CircuitBreakerStateProvider();
  assert.equal(p.circuitState, 'closed');
  assert.equal(p.isWarmingUp, false);
  assert.deepEqual(p.metrics, { total: 0, failureCount: 0, failureRate: 0 });
  assert.deepEqual(p.snapshot(), { state: 'closed', openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, updatedAt: 0 });
});

test('half-open: an ignored probe (client error) frees its slot; a stale probe timer does nothing after a reset', async () => {
  const time = new FakeTimeProvider();
  const timedOut = events('OnHalfOpenProbeTimedOut');
  try {
    const cb = new CircuitBreakerStrategy({ consecutiveFailures: 1, breakDuration: 100, halfOpenMaxWait: '1s' }, src(time));
    await cb.execute(boom(), createContext());
    await time.advance(100);
    await cb.execute(async () => fail(Object.assign(new Error('not found'), { status: 404 })), createContext());
    assert.equal(cb.state, 'half-open', 'a 404 says nothing about health');
    assert.deepEqual(await cb.execute(value('probe'), createContext()), ok('probe'), 'the slot was freed');
    assert.equal(cb.state, 'closed');

    await cb.execute(boom(), createContext());
    await time.advance(100);
    const hang = defer<void>();
    const probe = cb.execute(async () => (await hang.promise, ok('late')), createContext());
    assert.equal(cb.state, 'half-open');
    await cb.reset();
    await time.advance('1s');
    assert.equal(cb.state, 'closed', 'the probe timer of an earlier generation is ignored');
    assert.equal(timedOut.seen.length, 0);
    hang.resolve();
    await probe;
  } finally {
    timedOut.off();
  }
});

test('initialState: a malformed window is ignored by both window kinds', () => {
  const time = new FakeTimeProvider();
  const initialState = { state: 'closed', openUntil: 0, consecutiveFailures: 0, halfOpenAttempts: 0, window: 'junk' } as const;
  for (const o of [{ samplingCount: 10, minimumThroughput: 5 }, { samplingDuration: '10s' as const }]) {
    const cb = new CircuitBreakerStrategy({ ...o, initialState: initialState as never }, src(time));
    assert.deepEqual(cb.metrics, { total: 0, failureCount: 0, failureRate: 0 });
  }
});

// --- circuit extensions ---

test('health check: skipped while not closed; stopping during a running check schedules nothing more', async () => {
  const time = new FakeTimeProvider();
  let checks = 0;
  let hang: ReturnType<typeof defer<void>> | undefined;
  const cb = new CircuitBreakerStrategy(
    {
      consecutiveFailures: 1,
      breakDuration: '1h',
      extensions: [circuitHealthCheck({ interval: '1s', timeout: '10s', check: async () => (checks++, await hang?.promise) })],
    },
    src(time),
  );
  await cb.execute(boom(), createContext());
  await time.advance('3s');
  assert.equal(checks, 0, 'open: no checks');
  await cb.reset();
  hang = defer();
  await time.advance('1s');
  assert.equal(checks, 1);
  cb.dispose();
  hang.resolve();
  await time.advance(0);
  assert.equal(time.pendingTimers, 0, 'nothing rescheduled after dispose');
});

test('shared state: memory store keeps the newest; a throwing store.get is reported; syncInterval defaults to 5 s', async () => {
  const store = new MemoryCircuitStateStore();
  const snap = (updatedAt?: number): CircuitSnapshot => ({ state: 'open', openUntil: 1, consecutiveFailures: 0, halfOpenAttempts: 0, ...(updatedAt !== undefined && { updatedAt }) });
  store.set('k', snap());
  store.set('k', snap(5));
  store.set('k', snap(3));
  store.set('k', snap());
  assert.equal(store.get('k')?.updatedAt, 5);

  const time = new FakeTimeProvider();
  const errors = events('OnCircuitStoreError');
  try {
    let gets = 0;
    const broken = { get: () => (gets++, Promise.reject(new Error('down'))), set: () => {} };
    const cb = new CircuitBreakerStrategy({ extensions: [sharedCircuitState({ store: broken, key: 'k' })] }, src(time));
    await time.advance(4999);
    assert.equal(gets, 1, 'pulled once on start');
    await time.advance(1);
    assert.equal(gets, 2);
    assert.equal(errors.seen.length, 2);
    cb.dispose();
  } finally {
    errors.off();
  }
});

// --- lock ---

test('lock: the default store follows the pipeline clock: shared on one clock, separate on another', async () => {
  const time = new FakeTimeProvider();
  const other = new FakeTimeProvider();
  const lock = (t: FakeTimeProvider) => new ResiliencePipelineBuilder({ timeProvider: t }).addLock({ key: () => 'job', ttl: '1s' }).build();
  const hold = defer<void>();
  const holder = lock(time).execute(() => hold.promise.then(() => getLockToken()));
  await time.advance(0);
  await assert.rejects(lock(time).execute(() => 'second'), LockNotAcquiredError);
  assert.equal(await lock(other).execute(() => getLockToken()), 1, 'another clock has its own store');
  await time.advance('10s'); // renewals run on the fake clock and keep the lease
  await assert.rejects(lock(time).execute(() => 'third'), LockNotAcquiredError);
  hold.resolve();
  assert.equal(await holder, 1);
  assert.equal(await lock(time).execute(() => getLockToken()), 2, 'released, next token');
});

test('MemoryLockStore: leases expire on its clock; only the owner renews', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryLockStore({ timeProvider: time });
  assert.deepEqual(await store.acquire('k', 'a', 100), { fencingToken: 1 });
  assert.equal(await store.renew('k', 'b', 100), false);
  assert.equal(await store.acquire('k', 'b', 100), undefined);
  await time.advance(100);
  assert.deepEqual(await store.acquire('k', 'b', 100), { fencingToken: 2 });
  const real = new MemoryLockStore();
  assert.deepEqual(await real.acquire('k', 'a', 60_000), { fencingToken: 1 });
  assert.equal(await real.acquire('k', 'b', 60_000), undefined, 'system clock: still live');
});

test('lock: nested locks restore the outer fencing token; a renewal answer after the end is ignored', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryLockStore({ timeProvider: time });
  const outer = new LockStrategy<unknown>({ key: () => 'outer', store }, src(time));
  const inner = new LockStrategy<unknown>({ key: () => 'inner', store }, src(time));
  await store.acquire('inner', 'x', 1); // bump inner's token to 1, so it differs from outer's
  await time.advance(1);
  const ctx = createContext();
  const seen: unknown[] = [];
  await outer.execute(async (c) => {
    seen.push(c.properties.get(LOCK_TOKEN));
    await inner.execute(async (c2) => (seen.push(c2.properties.get(LOCK_TOKEN)), ok(0)), c);
    seen.push(c.properties.get(LOCK_TOKEN));
    return ok(0);
  }, ctx);
  assert.deepEqual(seen, [1, 2, 1]);
  assert.equal(ctx.properties.has(LOCK_TOKEN), false);

  const renew = defer<boolean>();
  const slow = { acquire: async () => ({ fencingToken: 1 }), renew: () => renew.promise, release: async () => {} };
  let lost = 0;
  const s = new LockStrategy<string>({ key: () => 'k', store: slow, ttl: 300, onLockLost: () => void lost++ }, src(time));
  const done = defer<void>();
  const run = s.execute(async () => (await done.promise, ok('done')), createContext());
  await time.advance(100); // renewal sent
  done.resolve();
  assert.deepEqual(await run, ok('done'));
  renew.resolve(false); // would mean "lost", but the execution is over
  await time.advance(1000);
  assert.equal(lost, 0);
});

test('lock: throwing stores', async () => {
  const time = new FakeTimeProvider();
  const down = new Error('store down');
  const acquireThrows = { acquire: () => Promise.reject(down), renew: async () => true, release: async () => {} };
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addLock({ key: () => 'k', store: acquireThrows }).build();
  await assert.rejects(p.execute(() => 1), (e) => e === down);

  const releaseThrows = { acquire: async () => ({ fencingToken: 7 }), renew: async () => true, release: () => Promise.reject(down) };
  const p2 = new ResiliencePipelineBuilder({ timeProvider: time }).addLock({ key: () => 'k', store: releaseThrows }).build();
  assert.equal(await p2.execute(() => getLockToken()), 7, 'a failing release keeps the outcome');

  let lost = 0;
  const renewThrows = { acquire: async () => ({ fencingToken: 1 }), renew: () => Promise.reject(down), release: async () => {} };
  const s = new LockStrategy<string>({ key: () => 'k', store: renewThrows, ttl: 300, onLockLost: () => void lost++ }, src(time));
  let aborted: unknown;
  const run = s.execute(
    ({ signal }) => new Promise((resolve) => signal.addEventListener('abort', () => ((aborted = signal.reason), resolve(fail(signal.reason))))),
    createContext(),
  );
  await time.advance(200);
  assert.equal(lost, 0, 'store errors are retried while the lease is still valid');
  await time.advance(100);
  assert.ok(err(await run) instanceof LockLostError);
  assert.ok(aborted instanceof LockLostError);
  assert.equal(lost, 1);
});

// --- cache ---

test('cache: an aborted caller fails before the store is read; onCacheMiss is called; clear(); coalesceStrategy', async () => {
  const time = new FakeTimeProvider();
  const store = new MemoryCacheStore();
  let gets = 0;
  const counting = { get: (k: string) => (gets++, store.get(k)), set: store.set.bind(store), delete: store.delete.bind(store) };
  const misses: string[] = [];
  const s = new CacheStrategy<number>({ ttl: '1m', key: () => 'k', store: counting, onCacheMiss: ({ key }) => void misses.push(key) }, src(time));
  const reason = new Error('gone');
  assert.equal(err(await s.execute(value(1), createContext({ signal: AbortSignal.abort(reason) }))), reason);
  assert.equal(gets, 0);
  assert.deepEqual(await s.execute(value(1), createContext()), ok(1));
  assert.deepEqual(await s.execute(value(2), createContext()), ok(1), 'cached');
  store.clear();
  assert.deepEqual(await s.execute(value(3), createContext()), ok(3), 'cleared');
  assert.deepEqual(misses, ['k', 'k']);

  const def = coalesceStrategy<number>({ key: () => 'k' });
  assert.equal(def.name, 'Coalesce');
  const coalesce = def.create(src(time));
  const g = defer<void>();
  let calls = 0;
  const shared = async () => (calls++, await g.promise, ok(calls));
  const both = [coalesce.execute(shared, createContext()), coalesce.execute(shared, createContext())];
  g.resolve();
  assert.deepEqual(await Promise.all(both), [ok(1), ok(1)]);
  assert.deepEqual(await coalesce.execute(value(9), createContext()), ok(9), 'nothing cached');
});

// --- batch / batcher / stats ---

test('allSettled rejects on caller abort; map runs everything at once by default; waitFor on the system clock; a throwing condition rejects', async () => {
  const controller = new AbortController();
  controller.abort(new Error('stop'));
  await assert.rejects(allSettled([() => 1], { signal: controller.signal }), /stop/);

  let running = 0;
  let peak = 0;
  const g = defer<void>();
  const run = map([1, 2, 3, 4], async (n) => (running++, (peak = Math.max(peak, running)), await g.promise, running--, n));
  await new Promise((r) => setImmediate(r));
  g.resolve();
  assert.deepEqual(await run, [1, 2, 3, 4]);
  assert.equal(peak, 4);

  await waitFor(() => true, { interval: 1, timeout: '1s' });
  const broken = new Error('check failed');
  await assert.rejects(waitFor(() => { throw broken; }, { interval: 1, timeout: '1s' }), (e) => e === broken);
});

test('batcher: per-caller signal is detached on success and on a failed batch', async () => {
  const time = new FakeTimeProvider();
  let fail = false;
  const b = createBatcher({ maxSize: 10, maxWait: 5, timeProvider: time, run: async (items: number[]) => { if (fail) throw new Error('batch'); return items; } });
  const controller = new AbortController();
  const a = b.load(1, { signal: controller.signal });
  await time.advance(5);
  assert.equal(await a, 1);
  fail = true;
  const withSignal = assert.rejects(b.load(2, { signal: controller.signal }), /batch/);
  const without = assert.rejects(b.load(3), /batch/);
  await time.advance(5);
  await Promise.all([withSignal, without]);
  controller.abort(new Error('late')); // listeners were removed: nothing left to reject
});

test('stats: a single pipeline name filters events; the default clock works', async () => {
  const stats = createStats({ pipeline: 'orders' });
  try {
    const pipeline = (name: string) => new ResiliencePipelineBuilder({ name }).build();
    await pipeline('orders').execute(() => 1);
    await pipeline('other').execute(() => 2);
    assert.equal(stats.snapshot().executions, 1);
  } finally {
    stats.dispose();
  }
  const both = createStats({ pipeline: ['a', 'b'], timeProvider: new FakeTimeProvider() });
  try {
    for (const name of ['a', 'b', 'c']) await new ResiliencePipelineBuilder({ name }).build().execute(() => 1);
    assert.equal(both.snapshot().executions, 2);
  } finally {
    both.dispose();
  }
  const every = createStats({ timeProvider: new FakeTimeProvider() });
  try {
    for (const name of ['a', 'b']) await new ResiliencePipelineBuilder({ name }).build().execute(() => 1);
    assert.equal(every.snapshot().executions, 2, 'no pipeline filter: all of them');
  } finally {
    every.dispose();
  }
});
