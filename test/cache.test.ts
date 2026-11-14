import assert from 'node:assert/strict';
import { test } from 'node:test';
import { ResiliencePipelineBuilder, onTelemetry } from '../src/index.ts';
import { type CacheOptions, CacheStrategy, MemoryCacheStore } from '../src/cache.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const httpError = (status: number) => Object.assign(new Error(`HTTP ${status}`), { status });

function build(opts: CacheOptions<any>, time = new FakeTimeProvider()) {
  const pipeline = new ResiliencePipelineBuilder({ name: 'p', timeProvider: time }).addStrategy((s) => new CacheStrategy(opts, s), 'Cache').build();
  return { pipeline, time };
}

function counter(result: (n: number) => unknown = (n) => n) {
  const fn = async () => {
    fn.calls++;
    return result(fn.calls);
  };
  fn.calls = 0;
  return fn;
}

test('cache: miss then hit, until ttl expires', async () => {
  const { pipeline, time } = build({ ttl: '10s' });
  const fn = counter();
  assert.equal(await pipeline.execute(fn, { operationKey: 'a' }), 1);
  assert.equal(await pipeline.execute(fn, { operationKey: 'a' }), 1);
  assert.equal(await pipeline.execute(fn, { operationKey: 'b' }), 2, 'other key misses');
  await time.advance('9999ms');
  assert.equal(await pipeline.execute(fn, { operationKey: 'a' }), 1);
  await time.advance(1);
  assert.equal(await pipeline.execute(fn, { operationKey: 'a' }), 3, 'expired at exactly ttl');
});

test('cache: undefined key bypasses the cache; custom key and shouldCache', async () => {
  const fn = counter();
  const { pipeline } = build({ ttl: '1m' });
  await pipeline.execute(fn);
  await pipeline.execute(fn);
  assert.equal(fn.calls, 2);

  const r = build({ ttl: '1m', key: (c) => c.properties.get('user') as string, shouldCache: ({ outcome }) => outcome.ok && outcome.result !== 1 });
  const g = counter();
  const opts = () => ({ properties: new Map([['user', 'u1']]) });
  assert.equal(await r.pipeline.execute(g, opts()), 1, 'not cached by shouldCache');
  assert.equal(await r.pipeline.execute(g, opts()), 2);
  assert.equal(await r.pipeline.execute(g, opts()), 2);
});

test('cache: errors are not cached', async () => {
  const { pipeline } = build({ ttl: '1m' });
  let calls = 0;
  const fn = async () => {
    if (++calls === 1) throw httpError(500);
    return 'ok';
  };
  await assert.rejects(pipeline.execute(fn, { operationKey: 'k' }), /HTTP 500/);
  assert.equal(await pipeline.execute(fn, { operationKey: 'k' }), 'ok');
  assert.equal(calls, 2);
});

test('MemoryCacheStore: evicts the least recently used entry', async () => {
  const store = new MemoryCacheStore({ maxEntries: 2 });
  const { pipeline } = build({ ttl: '1m', store });
  const fn = counter();
  await pipeline.execute(fn, { operationKey: 'a' }); // 1
  await pipeline.execute(fn, { operationKey: 'b' }); // 2
  await pipeline.execute(fn, { operationKey: 'a' }); // hit: a is now most recent
  await pipeline.execute(fn, { operationKey: 'c' }); // 3, evicts b
  assert.equal(store.size, 2);
  assert.equal(await pipeline.execute(fn, { operationKey: 'a' }), 1);
  assert.equal(await pipeline.execute(fn, { operationKey: 'b' }), 4);
  assert.throws(() => new MemoryCacheStore({ maxEntries: 0 }), RangeError);
});

test('cache: stale value hides a 5xx inside the stale window, not a 4xx, not after it', async () => {
  const stale: number[] = [];
  const { pipeline, time } = build({ ttl: '10s', staleTtl: '1m', onStaleServed: (a) => void stale.push(a.age) });
  assert.equal(await pipeline.execute(async () => 'v1', { operationKey: 'k' }), 'v1');
  await time.advance('30s');
  assert.equal(
    await pipeline.execute(() => Promise.reject(httpError(503)), { operationKey: 'k' }),
    'v1',
  );
  assert.deepEqual(stale, [30_000]);
  await assert.rejects(pipeline.execute(() => Promise.reject(httpError(404)), { operationKey: 'k' }), /HTTP 404/);
  await time.advance('40s'); // 70s > ttl + staleTtl
  await assert.rejects(pipeline.execute(() => Promise.reject(httpError(503)), { operationKey: 'k' }), /HTTP 503/);

  const custom = build({ ttl: 0, staleTtl: '1m', shouldServeStale: () => true });
  await custom.pipeline.execute(async () => 'v', { operationKey: 'k' });
  assert.equal(await custom.pipeline.execute(() => Promise.reject(httpError(400)), { operationKey: 'k' }), 'v', 'ttl 0 = stale-if-error only');
});

test('cache: concurrent misses share one call', async () => {
  const { pipeline } = build({ ttl: 0 });
  let calls = 0;
  let release!: (v: string) => void;
  const fn = () => {
    calls++;
    return new Promise<string>((r) => (release = r));
  };
  const all = Promise.all(Array.from({ length: 5 }, () => pipeline.execute(fn, { operationKey: 'k' })));
  await new Promise((r) => setTimeout(r, 0));
  release('shared');
  assert.deepEqual(await all, Array(5).fill('shared'));
  assert.equal(calls, 1);
  await pipeline.execute(async () => calls++, { operationKey: 'k' });
  assert.equal(calls, 2, 'ttl 0: deduped, not cached');

  const solo = build({ ttl: 0, coalesce: false });
  let n = 0;
  await Promise.all([1, 2, 3].map(() => solo.pipeline.execute(async () => n++, { operationKey: 'k' })));
  assert.equal(n, 3);
});

test('cache: one coalesced caller aborting does not cancel the shared call; all aborting does', async () => {
  const { pipeline } = build({ ttl: 0 });
  let sharedSignal!: AbortSignal;
  let release!: (v: string) => void;
  const fn = ({ signal }: { signal: AbortSignal }) => {
    sharedSignal = signal;
    return new Promise<string>((r) => (release = r));
  };
  const a = new AbortController();
  const b = new AbortController();
  const first = pipeline.execute(fn, { operationKey: 'k', signal: a.signal });
  const second = pipeline.execute(fn, { operationKey: 'k', signal: b.signal });
  await new Promise((r) => setTimeout(r, 0));
  a.abort(new Error('a left'));
  await assert.rejects(first, /a left/);
  assert.equal(sharedSignal.aborted, false);
  release('done');
  assert.equal(await second, 'done');

  const c = new AbortController();
  const d = new AbortController();
  const p1 = pipeline.execute(fn, { operationKey: 'k', signal: c.signal });
  const p2 = pipeline.execute(fn, { operationKey: 'k', signal: d.signal });
  await new Promise((r) => setTimeout(r, 0));
  c.abort(new Error('c left'));
  d.abort(new Error('d left'));
  await assert.rejects(p1, /c left/);
  await assert.rejects(p2, /d left/);
  assert.equal(sharedSignal.aborted, true);
  // A newcomer starts a fresh call rather than joining the aborted one.
  const p3 = pipeline.execute(fn, { operationKey: 'k' });
  await new Promise((r) => setTimeout(r, 0));
  assert.equal(sharedSignal.aborted, false);
  release('fresh');
  assert.equal(await p3, 'fresh');
});

test('cache: telemetry events and validation', async () => {
  const seen: string[] = [];
  const off = onTelemetry((e) => void (e.source.strategyName === 'Cache' && e.event !== 'OnStrategyExecuted' && seen.push(`${e.source.strategyName}:${e.event}:${(e.args as { key: string }).key}`)));
  const { pipeline } = build({ ttl: '1m', staleTtl: '1m' });
  await pipeline.execute(async () => 1, { operationKey: 'k' });
  await pipeline.execute(async () => 1, { operationKey: 'k' });
  off();
  assert.deepEqual(seen, ['Cache:OnCacheMiss:k', 'Cache:OnCacheHit:k']);

  const time = new FakeTimeProvider();
  const s = { pipelineName: 'p', strategyName: 'Cache', timeProvider: time };
  assert.throws(() => new CacheStrategy({ ttl: -1 }, s), RangeError);
  assert.throws(() => new CacheStrategy({ ttl: '1x' as any }, s), RangeError);
});
