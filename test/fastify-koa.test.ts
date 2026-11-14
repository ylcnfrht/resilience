import assert from 'node:assert/strict';
import { AsyncResource } from 'node:async_hooks';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import Fastify from 'fastify';
import Koa from 'koa';
import { ResiliencePipelineBuilder } from '../src/index.ts';
import { fastifyResilience } from '../src/fastify.ts';
import { type InboundOptions } from '../src/http.ts';
import { koaResilience } from '../src/koa.ts';

// Each app serves: GET / → 'ok', GET /slow → 'ok' once `probe.slow()` resolves, POST /nested → a downstream pipeline call
// that only ends when it is aborted (no wall-clock race between the request deadline and the handler).
interface Probe {
  innerAborted: boolean;
  aborted?: Promise<void>;
  slow?: () => Promise<void>;
}

async function fastifyApp(options: InboundOptions, probe: Probe) {
  const app = Fastify();
  await app.register(fastifyResilience, options);
  // Like a callback-style auth/pool library: resumes the request from an async resource created at startup, which drops
  // AsyncLocalStorage. The plugin's onRoute wrapper must still give the handler the request's context.
  const pool = new AsyncResource('pool');
  app.addHook('preHandler', (_req, _reply, done) => void setTimeout(() => pool.runInAsyncScope(done), 1));
  app.get('/', async () => 'ok');
  app.get('/slow', async () => (await probe.slow?.(), 'ok'));
  app.post('/nested', async () => nested(probe));
  await app.listen({ port: 0, host: '127.0.0.1' });
  return { url: `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`, close: () => app.close() };
}

async function koaApp(options: InboundOptions, probe: Probe) {
  const app = new Koa();
  app.use(koaResilience(options));
  app.use(async (ctx) => {
    if (ctx.path === '/slow') await probe.slow?.();
    ctx.body = ctx.path === '/nested' ? await nested(probe) : 'ok';
  });
  const server = app.listen(0, '127.0.0.1');
  await new Promise((ok) => server.once('listening', ok));
  return { url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, close: () => (server.closeAllConnections(), server.close()) };
}

async function nested(probe: Probe): Promise<string> {
  const downstream = new ResiliencePipelineBuilder().build(); // unaware of the request
  let aborted!: () => void;
  probe.aborted = new Promise((r) => (aborted = r));
  return downstream
    .execute(async ({ signal }: { signal: AbortSignal }) => {
      await new Promise<void>((r) => signal.addEventListener('abort', () => r()));
      probe.innerAborted = true;
      aborted();
      return 'late';
    })
    .catch(() => 'aborted');
}

const gate = () => {
  let open!: () => void;
  const opened = new Promise<void>((r) => (open = r));
  return { opened, open };
}

for (const [name, make] of [
  ['fastify', fastifyApp],
  ['koa', koaApp],
] as const) {
  test(`${name}: normal request untouched`, async () => {
    const s = await make({ timeout: '1s', concurrency: { permitLimit: 10 } }, { innerAborted: false });
    try {
      const res = await fetch(s.url);
      assert.equal(res.status, 200);
      assert.equal(await res.text(), 'ok');
      assert.equal(res.headers.get('ratelimit-limit'), null);
    } finally {
      await s.close();
    }
  });

  test(`${name}: timeout → 504 and the nested pipeline in the handler is aborted`, async () => {
    const probe: Probe = { innerAborted: false };
    const s = await make({ timeout: '30ms' }, probe);
    try {
      const res = await fetch(s.url + '/nested', { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{"a":1}' });
      assert.equal(res.status, 504);
      assert.equal(((await res.json()) as { error: string }).error, 'TimeoutRejectedError');
      await probe.aborted;
      assert.ok(probe.innerAborted, 'downstream call was cancelled by the request deadline');
    } finally {
      await s.close();
    }
  });

  test(`${name}: load shedding → 503`, async () => {
    const entered = gate();
    const release = gate();
    const probe: Probe = { innerAborted: false, slow: () => (entered.open(), release.opened) };
    const s = await make({ concurrency: { permitLimit: 1, queueLimit: 0 } }, probe);
    try {
      const first = fetch(s.url + '/slow');
      await entered.opened; // the first request holds the only permit
      assert.equal((await fetch(s.url + '/slow')).status, 503);
      release.open();
      assert.equal((await first).status, 200);
    } finally {
      await s.close();
    }
  });

  test(`${name}: per-key rate limit with RateLimit-* headers, 429 + Retry-After beyond it`, async () => {
    const key = (req: { headers: Record<string, string | string[] | undefined> }) => String(req.headers['x-user']);
    const s = await make({ rateLimit: { key, permitLimit: 2, window: '1h' } }, { innerAborted: false });
    try {
      const get = (user: string) => fetch(s.url, { headers: { 'x-user': user } });
      const first = await get('a');
      assert.equal(first.status, 200);
      assert.equal(first.headers.get('ratelimit-limit'), '2');
      assert.equal(first.headers.get('ratelimit-remaining'), '1');
      assert.equal((await get('a')).status, 200);
      const limited = await get('a');
      assert.equal(limited.status, 429);
      assert.equal(limited.headers.get('ratelimit-remaining'), '0');
      assert.ok(Number(limited.headers.get('retry-after')) > 0);
      assert.equal((await get('b')).status, 200, 'other keys have their own limit');
    } finally {
      await s.close();
    }
  });
}
