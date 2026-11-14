// Branches of the outbound clients (axios, fetch), rate-limiter-flexible adapter, Redis store defaults and FakeTimeProvider.
import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import axios from 'axios';
import { fromRateLimiterFlexible } from '../src/adapters.ts';
import { attachResilience } from '../src/axios.ts';
import { createResilientFetch, isTransientHttpOutcome, parseRetryAfter, standardHttpPipeline, toUrl } from '../src/fetch.ts';
import { BrokenCircuitError, Outcome, TimeoutRejectedError } from '../src/index.ts';
import { RedisCacheStore, RedisCircuitStateStore, RedisLockStore, RedisRateLimiter } from '../src/redis.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const noDelay = { retry: { delay: 0 } } as const;

// --- axios ---

/** An axios-like instance whose adapter is a plain function (the "existing function adapter" path). */
function fakeAxios(adapter: (config: any) => Promise<any>) {
  const configs: any[] = [];
  const instance = { defaults: { adapter: (config: any) => (configs.push(config), adapter(config)) as unknown } };
  return { instance, configs, call: (config: any) => (instance.defaults.adapter as (c: any) => Promise<any>)(config) };
}

test('axios: an existing function adapter is wrapped; method defaults to GET; eject restores it', async () => {
  let n = 0;
  const f = fakeAxios(async () => (++n < 2 ? Promise.reject({ response: { status: 503, headers: {}, data: null } }) : { status: 200, headers: {}, data: 'ok' }));
  const original = f.instance.defaults.adapter;
  const resilience = attachResilience(f.instance, noDelay);
  assert.notEqual(f.instance.defaults.adapter, original);
  assert.equal((await f.call({ url: 'http://h/' })).data, 'ok');
  assert.equal(n, 2, 'no method: treated as GET and retried; a null response body is discarded safely');
  resilience.eject();
  assert.equal(f.instance.defaults.adapter, original);
  // Eject does not clobber an adapter someone else installed meanwhile.
  const again = attachResilience(f.instance);
  const other = async () => ({});
  f.instance.defaults.adapter = other;
  again.eject();
  assert.equal(f.instance.defaults.adapter, other);
});

test('axios: Retry-After read from AxiosHeaders-like get(), plain objects and numbers; missing values ignored', async () => {
  const time = new FakeTimeProvider();
  const delays: number[] = [];
  const replies = [
    { status: 503, headers: { get: (name: string) => (name === 'retry-after' ? '3' : null) } },
    { status: 503, headers: { 'retry-after': 2 } },
    { status: 503, headers: { 'retry-after': null } },
    { status: 503 },
    { status: 200, headers: {}, data: 'done' },
  ];
  const f = fakeAxios(async () => {
    const r = replies.shift()!;
    if (r.status >= 400) throw { response: r };
    return r;
  });
  attachResilience(f.instance, { timeProvider: time, retry: { maxRetryAttempts: 4, useJitter: false, delay: 7, backoffType: 'constant', onRetry: (a) => void delays.push(a.retryDelay) } });
  const result = f.call({ url: 'http://h/', method: 'get' });
  await time.runAll();
  assert.equal((await result).data, 'done');
  assert.deepEqual(delays, [3000, 2000, 7, 7]);
});

test('axios: only network codes and timeouts are transient; a stream body of a discarded response is destroyed', async () => {
  for (const error of [null, { code: 'EWHATEVER' }, new Error('no code')]) {
    let calls = 0;
    const f = fakeAxios(async () => (calls++, Promise.reject(error)));
    attachResilience(f.instance, noDelay);
    await assert.rejects(f.call({ url: 'http://h/', method: 'GET' }), (e) => e === error);
    assert.equal(calls, 1, `${JSON.stringify(error)} is not retried`);
  }
  let calls = 0;
  const f = fakeAxios(async () => (++calls < 2 ? Promise.reject(new TimeoutRejectedError(1)) : { status: 200, headers: {} }));
  attachResilience(f.instance, noDelay);
  await f.call({ url: 'http://h/' });
  assert.equal(calls, 2, 'a timeout is retried');

  let destroyed = 0;
  let n = 0;
  const s = fakeAxios(async () => (++n < 2 ? { status: 503, headers: {}, data: { destroy: () => destroyed++ } } : { status: 200, headers: {}, data: 'ok' }));
  attachResilience(s.instance, noDelay);
  assert.equal((await s.call({ url: 'http://h/' })).data, 'ok');
  assert.equal(destroyed, 1);
});

test('axios: onBeforeAttempt can replace the config per attempt or leave it', async () => {
  const f = fakeAxios(async (config) => (config.headers.attempt === '0' ? Promise.reject({ response: { status: 503, headers: {} } }) : { status: 200, headers: {}, data: config.headers }));
  const seen: number[] = [];
  attachResilience(f.instance, {
    ...noDelay,
    onBeforeAttempt: (config, { attemptNumber }) => {
      seen.push(attemptNumber);
      if (attemptNumber === 2) return; // keep as is
      return { ...config, headers: { ...(config.headers as object), attempt: String(attemptNumber) } };
    },
  });
  const res = await f.call({ url: 'http://h/', headers: { a: 'b' } });
  assert.deepEqual(res.data, { a: 'b', attempt: '1' });
  assert.deepEqual(seen, [0, 1]);
  assert.ok(f.configs.every((c) => c.signal instanceof AbortSignal), 'each attempt gets the attempt signal');
});

test('axios: adapter names are resolved by import("axios") for an instance without getAdapter, or by options.getAdapter', async () => {
  const s = http.createServer((_req, res) => res.end('hi'));
  await new Promise<void>((ok) => s.listen(0, '127.0.0.1', ok));
  const url = `http://127.0.0.1:${(s.address() as AddressInfo).port}/`;
  try {
    const created = axios.create();
    assert.equal((created as { getAdapter?: unknown }).getAdapter, undefined, 'only the default export has getAdapter');
    attachResilience(created);
    assert.equal((await created.get(url)).data, 'hi');

    const resolved: unknown[] = [];
    const given = axios.create();
    attachResilience(given, { getAdapter: (names) => (resolved.push(names), axios.getAdapter(names)) });
    assert.equal((await given.get(url)).data, 'hi');
    assert.deepEqual(resolved, [axios.defaults.adapter]);
  } finally {
    s.close();
  }
});

// --- fetch ---

const respond = (status: number, headers?: Record<string, string>) => new Response(status === 204 ? null : 'body', { status, headers });

test('fetch: isTransientHttpOutcome, parseRetryAfter edge cases, toUrl', () => {
  const context = { signal: new AbortController().signal };
  assert.equal(isTransientHttpOutcome({ outcome: Outcome.fromResult(respond(503)), context } as never), true);
  assert.equal(isTransientHttpOutcome({ outcome: Outcome.fromResult(respond(404)), context } as never), false);
  assert.equal(isTransientHttpOutcome({ outcome: Outcome.fromError(new TypeError('fetch failed')), context } as never), true);
  assert.equal(parseRetryAfter('soon'), undefined);
  assert.equal(parseRetryAfter(new Date(10_000).toUTCString(), 4000), 6000);
  assert.equal(parseRetryAfter(new Date(0).toUTCString(), 4000), 0, 'a past date is no delay');
  assert.equal(toUrl('/relative'), undefined, 'no base outside a browser');
  assert.equal(toUrl('/x', 'http://h')?.href, 'http://h/x');
  (globalThis as { location?: unknown }).location = { href: 'https://site.test/app/' }; // a browser page
  try {
    assert.equal(toUrl('x')?.href, 'https://site.test/app/x');
  } finally {
    delete (globalThis as { location?: unknown }).location;
  }
});

test('fetch: unresolvable url shares one keyless pipeline; named per-key pipelines; concurrency limit per key', async () => {
  const names: string[] = [];
  const { onTelemetry } = await import('../src/index.ts');
  const off = onTelemetry((e) => void (e.source.pipelineName && names.push(e.source.pipelineName)));
  try {
    let inFlight = 0;
    let max = 0;
    const f = createResilientFetch({
      name: 'svc',
      concurrency: 1,
      retry: false,
      fetch: async () => {
        max = Math.max(max, ++inFlight);
        await new Promise((r) => setImmediate(r));
        inFlight--;
        return respond(200);
      },
    });
    const results = await Promise.allSettled([f('/relative-1'), f('/relative-2')]);
    assert.equal(results.filter((r) => r.status === 'fulfilled').length, 1, 'both relative urls share the "" key and its limit of 1');
    assert.equal(max, 1);
    assert.ok(names.includes('svc/'), JSON.stringify([...new Set(names)]));
  } finally {
    off();
  }
});

test('fetch: respectRetryAfter false; retryOn predicate gets error / response; retries false; onBeforeAttempt keeps the request', async () => {
  const time = new FakeTimeProvider();
  const delays: number[] = [];
  let n = 0;
  const f = createResilientFetch({
    timeProvider: time,
    retry: { respectRetryAfter: false, delay: 5, useJitter: false, backoffType: 'constant', onRetry: (a) => void delays.push(a.retryDelay) },
    fetch: async () => (++n === 1 ? respond(429, { 'retry-after': '60' }) : respond(200)),
  });
  const r = f('http://h/');
  await time.runAll();
  assert.equal((await r).status, 200);
  assert.deepEqual(delays, [5], 'Retry-After ignored');

  const seen: unknown[] = [];
  let calls = 0;
  const g = createResilientFetch({
    retry: { delay: 0 },
    retryOn: ({ response, error }) => (seen.push([response?.status, (error as Error | undefined)?.message]), response === undefined),
    fetch: async () => (++calls === 1 ? Promise.reject(new Error('net')) : respond(500)),
    onBeforeAttempt: () => undefined,
  });
  assert.equal((await g('http://h/')).status, 500);
  assert.deepEqual(seen, [[undefined, 'net'], [500, undefined]]);

  let single = 0;
  const h = createResilientFetch({ retry: { delay: 0 }, fetch: async () => (single++, respond(503)) });
  assert.equal((await h('http://h/', { resilience: { retries: false } })).status, 503);
  assert.equal(single, 1);
});

test('fetch: standardHttpPipeline is the built-in pipeline for your own client', async () => {
  let calls = 0;
  const retrying = standardHttpPipeline({ retry: { delay: 0 }, circuitBreaker: false });
  assert.equal((await retrying.execute(async () => (calls++, respond(503)))).status, 503);
  assert.equal(calls, 4, '3 retries by default');
  retrying.dispose();
  const breaking = standardHttpPipeline({ name: 'std', retry: false, circuitBreaker: { minimumThroughput: 2, samplingCount: 2 } });
  const call = () => breaking.execute(async () => respond(503));
  await call();
  await call();
  await assert.rejects(call(), BrokenCircuitError, 'without a url every call shares one breaker');
  breaking.dispose();
  const plain = standardHttpPipeline();
  assert.equal((await plain.execute(async () => respond(204))).status, 204);
  plain.dispose();
});

// --- rate-limiter-flexible ---

test('rate-limiter-flexible: a rejection without msBeforeNext retries after 1 ms; points are rewarded back', async () => {
  const rewarded: number[] = [];
  const algorithm = fromRateLimiterFlexible({ points: 3, consume: () => Promise.reject({ remainingPoints: 0 }), reward: async (_k, p = 0) => void rewarded.push(p) }, 'k');
  assert.deepEqual(await algorithm.acquire(2, 0), { ok: false, retryAfter: 1 });
  assert.deepEqual(rewarded, [2]);
});

// --- Redis store defaults (fake client: no Redis needed) ---

test('redis stores: default prefix, missing updatedAt, permits beyond the limit', async () => {
  const calls: Array<Array<string | number>> = [];
  let reply: unknown = null;
  const client = { eval: async (_script: string, _n: number, ...rest: Array<string | number>) => (calls.push(rest), reply) };
  await new RedisCacheStore({ client }).get('k');
  await new RedisLockStore({ client }).release('job', 'me');
  await new RedisCircuitStateStore({ client }).set('svc', { state: 'closed' } as never);
  reply = 0;
  assert.deepEqual(await new RedisRateLimiter({ client, key: 'api', permitLimit: 1, window: '1s' }).acquire(1), { ok: true });
  reply = 250;
  assert.deepEqual(await new RedisRateLimiter({ client, key: 'api', permitLimit: 1, window: '1s' }).acquire(1), { ok: false, retryAfter: 250 });
  reply = -1;
  await assert.rejects(Promise.resolve(new RedisRateLimiter({ client, key: 'api', permitLimit: 1, window: '1s' }).acquire(2)), RangeError);
  assert.equal(calls[0]![0], 'rp:k');
  assert.match(String(calls[1]![0]), /^rp:/);
  assert.deepEqual(calls[2]!.slice(0, 1).concat(calls[2]!.slice(2)), ['rp:cb:svc', 0], 'missing updatedAt compares as 0');
  assert.equal(calls[3]![0], 'rp:rl:api');
});

// --- FakeTimeProvider ---

test('FakeTimeProvider: Date start, runAll guards against timers that reschedule forever', async () => {
  const time = new FakeTimeProvider(new Date(5000));
  assert.equal(time.now(), 5000);
  const loop = () => void time.setTimeout(loop, 1);
  loop();
  await assert.rejects(time.runAll(3), /more than 3 timers fired/);
  assert.equal(new FakeTimeProvider().now(), Date.UTC(2026, 0, 1));
});
