import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import { BrokenCircuitError, TimeoutRejectedError } from '../src/index.ts';
import { createResilientFetch, retryAfterMs } from '../src/fetch.ts';

type Reply = { status?: number; hang?: boolean; headers?: Record<string, string> };

async function server(reply: (n: number, req: http.IncomingMessage) => Reply) {
  let n = 0;
  const s = http.createServer((req, res) => {
    const r = reply(++n, req);
    if (r.hang) return; // never answers: only a timeout or an abort ends the call
    if (!res.destroyed) res.writeHead(r.status ?? 200, r.headers).end(`ok ${n}`);
  });
  await new Promise<void>((ok) => s.listen(0, ok));
  return { url: `http://localhost:${(s.address() as AddressInfo).port}`, get calls() { return n; }, close: () => (s.closeAllConnections(), s.close()) };
}

const fast = { retry: { delay: 1 } } as const;

test('fetch: retries 503 and returns the final good response', async () => {
  const s = await server((n) => (n < 3 ? { status: 503 } : {}));
  try {
    const res = await createResilientFetch(fast)(s.url);
    assert.equal(res.status, 200);
    assert.equal(await res.text(), 'ok 3');
  } finally {
    s.close();
  }
});

test('fetch: 404 is returned as-is without retrying', async () => {
  const s = await server(() => ({ status: 404 }));
  try {
    assert.equal((await createResilientFetch(fast)(s.url)).status, 404);
    assert.equal(s.calls, 1);
  } finally {
    s.close();
  }
});

test('fetch: POST is not retried unless it has an Idempotency-Key', async () => {
  const s = await server(() => ({ status: 503 }));
  try {
    const http = createResilientFetch(fast);
    assert.equal((await http(s.url, { method: 'POST', body: 'x' })).status, 503);
    assert.equal(s.calls, 1, 'unsafe POST: single attempt');
    await http(s.url, { method: 'POST', body: 'x', headers: { 'Idempotency-Key': 'k1' } });
    assert.equal(s.calls, 1 + 4, 'idempotent POST: retried');
  } finally {
    s.close();
  }
});

test('fetch: honours Retry-After', async () => {
  const s = await server((n) => (n === 1 ? { status: 429, headers: { 'retry-after': '0.2' } } : {}));
  const delays: number[] = [];
  try {
    const res = await createResilientFetch({ retry: { onRetry: (a) => void delays.push(a.retryDelay) } })(s.url);
    assert.equal(res.status, 200);
    assert.deepEqual(delays, [200]);
  } finally {
    s.close();
  }
  assert.equal(retryAfterMs(new Response(null, { headers: { 'retry-after': new Date(Date.now() + 5000).toUTCString() } }))! > 3000, true);
});

test('fetch: attempt timeout, then circuit opens on a dead dependency', async () => {
  const s = await server(() => ({ hang: true }));
  try {
    const http = createResilientFetch({ attemptTimeout: '30ms', retry: { delay: 1, maxRetryAttempts: 1 }, circuitBreaker: { minimumThroughput: 2, samplingCount: 2 } });
    await assert.rejects(http(s.url), TimeoutRejectedError);
    await assert.rejects(http(s.url), BrokenCircuitError);
  } finally {
    s.close();
  }
});

test('fetch: the caller signal cancels everything', async () => {
  const ac = new AbortController();
  const s = await server(() => (ac.abort(new Error('user left')), { hang: true })); // abort once the request is in flight
  try {
    await assert.rejects(createResilientFetch(fast)(s.url, { signal: ac.signal }), /user left/);
  } finally {
    s.close();
  }
});

test('fetch: is still a typeof fetch', () => {
  const f: typeof fetch = createResilientFetch();
  assert.equal(typeof f, 'function');
});

test('fetch: retryOn status list replaces the default statuses', async () => {
  const s = await server((n) => (n === 1 ? { status: 409 } : { status: 503 }));
  try {
    const http = createResilientFetch({ ...fast, retryOn: [409] });
    assert.equal((await http(s.url)).status, 503);
    assert.equal(s.calls, 2, '409 retried, 503 not');
  } finally {
    s.close();
  }
});

test('fetch: per-request retries, retryOn, idempotent and timeout', async () => {
  const s = await server((n, req) => (req.url === '/slow' ? { hang: true } : n < 100 ? { status: 503 } : {}));
  try {
    const http = createResilientFetch({ ...fast, circuitBreaker: false });
    await http(s.url, { resilience: { retries: 0 } });
    assert.equal(s.calls, 1);
    await http(s.url, { resilience: { retries: 5 } });
    assert.equal(s.calls, 1 + 6);
    const seen: number[] = [];
    await http(s.url, { resilience: { retryOn: ({ response, attemptNumber }) => (seen.push(response!.status), attemptNumber < 1) } });
    assert.deepEqual(seen, [503, 503]);
    assert.equal(s.calls, 7 + 2);
    await http(s.url, { method: 'POST', body: 'x', resilience: { idempotent: true, retries: 1 } });
    assert.equal(s.calls, 9 + 2, 'POST forced idempotent');
    await assert.rejects(http(`${s.url}/slow`, { resilience: { timeout: '50ms' } }), TimeoutRejectedError);
  } finally {
    s.close();
  }
});

test('fetch: one circuit breaker per host; pipelineKey can share one', async () => {
  const dead = await server(() => ({ status: 503 }));
  const alive = await server(() => ({}));
  const cb = { minimumThroughput: 2, samplingCount: 2 };
  try {
    const perHost = createResilientFetch({ retry: false, circuitBreaker: cb });
    await perHost(dead.url);
    await perHost(dead.url);
    await assert.rejects(perHost(dead.url), BrokenCircuitError);
    assert.equal((await perHost(alive.url)).status, 200, 'other host unaffected');

    const shared = createResilientFetch({ retry: false, circuitBreaker: cb, pipelineKey: () => 'all' });
    await shared(dead.url);
    await shared(dead.url);
    await assert.rejects(shared(alive.url), BrokenCircuitError);
  } finally {
    dead.close();
    alive.close();
  }
});

test('fetch: least recently used host pipelines are dropped beyond maxPipelines', async () => {
  const a = await server(() => ({ status: 503 }));
  const b = await server(() => ({}));
  try {
    const http = createResilientFetch({ retry: false, circuitBreaker: { minimumThroughput: 1, samplingCount: 1 }, maxPipelines: 1 });
    await http(a.url);
    await assert.rejects(http(a.url), BrokenCircuitError);
    await http(b.url); // evicts a's pipeline, and its open circuit
    assert.equal((await http(a.url)).status, 503);
  } finally {
    a.close();
    b.close();
  }
});

test('fetch: onBeforeAttempt rebuilds the request per attempt (Request input, body re-sent)', async () => {
  const seen: Array<string | undefined> = [];
  const s = await server((n, req) => (seen.push(req.headers.authorization), n === 1 ? { status: 503 } : {}));
  try {
    const http = createResilientFetch({
      ...fast,
      onBeforeAttempt: (req, ctx) => new Request(req, { headers: { ...Object.fromEntries(req.headers), authorization: `token-${ctx.attemptNumber}` } }),
    });
    const res = await http(new Request(s.url, { method: 'PUT', body: 'payload' }));
    assert.equal(res.status, 200);
    assert.deepEqual(seen, ['token-0', 'token-1']);
  } finally {
    s.close();
  }
});
