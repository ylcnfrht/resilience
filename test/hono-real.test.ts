// honoResilience against a real Hono app (the other Hono tests use a fake context).
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Hono } from 'hono';
import { BrokenCircuitError, ResiliencePipelineBuilder, getResilienceSignal } from '../src/index.ts';
import { honoResilience } from '../src/http.ts';

const never = (signal: AbortSignal) => new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason)));

test('hono: passes through, and the request budget reaches pipelines inside the handler (504)', async () => {
  let innerAborted = false;
  const app = new Hono();
  app.use(honoResilience({ timeout: '30ms' }));
  app.get('/ok', (c) => c.text('ok'));
  app.get('/slow', async (c) => {
    await new ResiliencePipelineBuilder().build().execute(({ signal }) => {
      signal.addEventListener('abort', () => (innerAborted = true));
      return never(signal);
    });
    return c.text('late');
  });
  const ok = await app.request('/ok');
  assert.equal(ok.status, 200);
  assert.equal(await ok.text(), 'ok');
  const slow = await app.request('/slow');
  assert.equal(slow.status, 504);
  assert.equal(((await slow.json()) as { error: string }).error, 'TimeoutRejectedError');
  assert.ok(innerAborted, 'the downstream call was cancelled by the request deadline');
});

test('hono: rate limit answers 429 with Retry-After and RateLimit headers', async () => {
  const app = new Hono();
  app.use(honoResilience({ rateLimit: { permitLimit: 2, window: '1m' } }));
  app.get('/', (c) => c.text('ok'));
  const statuses = [];
  for (let i = 0; i < 3; i++) statuses.push(await app.request('/'));
  assert.deepEqual(statuses.map((r) => r.status), [200, 200, 429]);
  assert.ok(statuses[0].headers.get('ratelimit-limit'), 'RateLimit headers on admitted responses');
  assert.ok(Number(statuses[2].headers.get('retry-after')) > 0);
});

test('hono: concurrency limit sheds load while requests are in flight', async () => {
  const app = new Hono();
  let release!: () => void;
  const gate = new Promise<void>((r) => (release = r));
  app.use(honoResilience({ concurrency: { permitLimit: 1, queueLimit: 0 } }));
  app.get('/', async (c) => (await gate, c.text('done')));
  const first = app.request('/');
  await new Promise((r) => setTimeout(r, 5));
  const second = await app.request('/');
  assert.ok([429, 503].includes(second.status), `shed with ${second.status}`);
  release();
  assert.equal((await first).status, 200);
});

test('hono: a resilience error from a downstream pipeline goes to app.onError unchanged', async () => {
  const app = new Hono();
  app.use(honoResilience({ timeout: '1s' }));
  app.onError((error, c) => c.text(`handled ${(error as Error).name}`, 502));
  app.get('/', () => {
    throw new BrokenCircuitError();
  });
  const res = await app.request('/');
  assert.equal(res.status, 502);
  assert.equal(await res.text(), 'handled BrokenCircuitError');
});

test('hono: the client disconnecting aborts the request signal seen by the handler', async () => {
  const app = new Hono();
  let aborted = false;
  app.use(honoResilience({ timeout: '5s' }));
  app.get('/', async (c) => {
    const signal = getResilienceSignal()!;
    signal.addEventListener('abort', () => (aborted = true));
    await never(signal).catch(() => {});
    return c.text('gone');
  });
  const client = new AbortController();
  const pending = Promise.resolve(app.request('/', { signal: client.signal }));
  await new Promise((r) => setTimeout(r, 5));
  client.abort();
  await pending.catch(() => {});
  assert.ok(aborted);
});
