import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import axios, { AxiosError } from 'axios';
import { BrokenCircuitError, TimeoutRejectedError } from '../src/index.ts';
import { attachResilience } from '../src/axios.ts';

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

function client(options: Parameters<typeof attachResilience>[1] = fast) {
  const instance = axios.create();
  attachResilience(instance, options);
  return instance;
}

test('axios: retries 503 and returns the final good response', async () => {
  const s = await server((n) => (n < 3 ? { status: 503 } : {}));
  try {
    const res = await client().get(s.url);
    assert.equal(res.data, 'ok 3');
  } finally {
    s.close();
  }
});

test('axios: 404 is not retried; the AxiosError comes through', async () => {
  const s = await server(() => ({ status: 404 }));
  try {
    const error = await client().get(s.url).catch((e: unknown) => e);
    assert.ok(error instanceof AxiosError);
    assert.equal(error.response?.status, 404);
    assert.equal(s.calls, 1);
  } finally {
    s.close();
  }
});

test('axios: network errors (ECONNREFUSED) are retried', async () => {
  const s = await server(() => ({}));
  s.close();
  const codes: string[] = [];
  const error: AxiosError = await client({ retry: { delay: 1, maxRetryAttempts: 2, onRetry: (a) => void codes.push((a.outcome as { error: AxiosError }).error.code!) } })
    .get(s.url)
    .then(() => assert.fail('should reject'), (e: AxiosError) => e);
  assert.equal(error.code, 'ECONNREFUSED');
  assert.deepEqual(codes, ['ECONNREFUSED', 'ECONNREFUSED']);
});

test('axios: honours Retry-After (default export, then eject)', async () => {
  const s = await server((n) => (n === 1 ? { status: 429, headers: { 'retry-after': '0.2' } } : {}));
  const delays: number[] = [];
  const resilience = attachResilience(axios, { retry: { onRetry: (a) => void delays.push(a.retryDelay) } });
  try {
    assert.equal((await axios.get(s.url)).status, 200);
    assert.deepEqual(delays, [200]);
  } finally {
    resilience.eject();
    s.close();
  }
  assert.ok(Array.isArray(axios.defaults.adapter), 'eject restored the default adapter list');
});

test('axios: POST is not retried unless it has an Idempotency-Key', async () => {
  const s = await server(() => ({ status: 503 }));
  try {
    const api = client();
    await assert.rejects(api.post(s.url, 'x'), (e: AxiosError) => e.response?.status === 503);
    assert.equal(s.calls, 1);
    await assert.rejects(api.post(s.url, 'x', { headers: { 'Idempotency-Key': 'k1' } }));
    assert.equal(s.calls, 1 + 4);
  } finally {
    s.close();
  }
});

test('axios: per-request override via config.resilience', async () => {
  const s = await server((n) => (n < 3 ? { status: 503 } : {}));
  try {
    const api = client();
    await assert.rejects(api.get(s.url, { resilience: { retries: 0 } }));
    assert.equal(s.calls, 1);
    assert.equal((await api.get(s.url, { resilience: { retries: 1 } })).data, 'ok 3');
  } finally {
    s.close();
  }
});

test('axios: one circuit breaker per host', async () => {
  const dead = await server(() => ({ status: 503 }));
  const alive = await server(() => ({}));
  try {
    const api = client({ retry: false, circuitBreaker: { minimumThroughput: 2, samplingCount: 2 } });
    await assert.rejects(api.get(dead.url));
    await assert.rejects(api.get(dead.url));
    await assert.rejects(api.get(dead.url), BrokenCircuitError);
    assert.equal(dead.calls, 2);
    assert.equal((await api.get(alive.url)).status, 200);
  } finally {
    dead.close();
    alive.close();
  }
});

test('axios: attempt timeout', async () => {
  const s = await server(() => ({ hang: true }));
  try {
    let retries = 0;
    await assert.rejects(client({ attemptTimeout: '30ms', retry: { delay: 1, maxRetryAttempts: 1, onRetry: () => void retries++ } }).get(s.url), TimeoutRejectedError);
    assert.equal(retries, 1, 'the timed-out attempt was retried once, then the second timed out too');
  } finally {
    s.close();
  }
});

test('axios: the caller signal cancels everything', async () => {
  const ac = new AbortController();
  const s = await server(() => (ac.abort(), { hang: true })); // abort once the request is in flight
  try {
    await assert.rejects(client().get(s.url, { signal: ac.signal }), (e) => axios.isCancel(e));
    assert.equal(s.calls, 1);
  } finally {
    s.close();
  }
});
