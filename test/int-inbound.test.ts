// Edge paths of the inbound integrations (Express, Hono, Fastify, Koa): errors before / after the handler starts, client
// disconnects, headers already sent. Real servers where the path is reachable end to end, fakes for the guards.
import assert from 'node:assert/strict';
import { EventEmitter, once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import Fastify from 'fastify';
import Koa from 'koa';
import { fail } from '../src/core.ts';
import { fastifyResilience } from '../src/fastify.ts';
import { HTTP_REQUEST, describeResilienceError, honoResilience, resilienceMiddleware, retryAfterSeconds } from '../src/http.ts';
import { CallRejectedError, RateLimiterRejectedError, ResiliencePipelineBuilder, TimeoutRejectedError, getResilienceContext, getResilienceSignal, setGlobalChaos } from '../src/index.ts';
import { koaResilience } from '../src/koa.ts';

/** A pipeline that runs the callback, then fails with `error` (an error that arrives after the handler started). */
const failAfter = (error: unknown) =>
  new ResiliencePipelineBuilder().addStrategy(() => ({ execute: async (next, context) => (await next(context), fail(error)) })).build();

const waitAbort = (signal: AbortSignal) => new Promise<unknown>((resolve) => signal.addEventListener('abort', () => resolve(signal.reason), { once: true }));
const deferred = () => {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
};

test('describeResilienceError: unknown resilience codes map to 503; retryAfterSeconds rounds up', () => {
  assert.deepEqual(describeResilienceError(new CallRejectedError('nope')), { code: 'REJECTED', httpStatus: 503, retryAfter: undefined, message: 'nope', source: undefined });
  assert.equal(retryAfterSeconds(new RateLimiterRejectedError(1500)), 2);
  assert.equal(retryAfterSeconds(new Error('x')), undefined);
});

// --- Express (fake req/res) ---

function fakeNode() {
  const res = Object.assign(new EventEmitter(), {
    headersSent: false,
    writableFinished: false,
    statusCode: 200,
    headers: {} as Record<string, unknown>,
    body: undefined as string | undefined,
    setHeader(name: string, value: unknown) {
      res.headers[name] = value;
    },
    end(body?: string) {
      res.body = body;
    },
  });
  return { req: new EventEmitter(), res };
}

test('express: no response once headers are sent or the client is gone; a disconnect aborts the handler', async () => {
  // Headers already sent by the handler when the late timeout arrives.
  {
    const { req, res } = fakeNode();
    const handled = deferred();
    resilienceMiddleware({ pipeline: failAfter(new TimeoutRejectedError(1)) })(req, res, () => handled.resolve());
    await handled.promise;
    res.headersSent = true;
    res.writableFinished = true;
    res.emit('finish');
    res.emit('close');
    await new Promise((r) => setImmediate(r));
    assert.equal(res.body, undefined);
    assert.equal(res.statusCode, 200);
  }
  // Client disconnects mid-handler: the request's signal aborts and nothing is written.
  {
    const { req, res } = fakeNode();
    const aborted = deferred();
    let reason: unknown;
    resilienceMiddleware({ pipeline: failAfter(new TimeoutRejectedError(1)) })(req, res, () => {
      void waitAbort(getResilienceSignal()!).then((r) => ((reason = r), aborted.resolve()));
      res.emit('close'); // not finished: a disconnect
    });
    await aborted.promise;
    assert.match(String((reason as Error).message), /Client disconnected/);
    await new Promise((r) => setImmediate(r));
    assert.equal(res.body, undefined);
  }
});

test('express: rate limit headers in legacy, both and false modes', async () => {
  for (const [mode, expected] of [
    ['legacy', ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset']],
    ['both', ['RateLimit-Limit', 'RateLimit-Remaining', 'RateLimit-Reset', 'X-RateLimit-Limit', 'X-RateLimit-Remaining', 'X-RateLimit-Reset']],
    [false, []],
  ] as const) {
    const { req, res } = fakeNode();
    const handled = deferred();
    resilienceMiddleware({ rateLimit: { permitLimit: 5, window: '1h' }, rateLimitHeaders: mode })(req, res, () => handled.resolve());
    await handled.promise;
    assert.deepEqual(Object.keys(res.headers), expected, String(mode));
    res.writableFinished = true;
    res.emit('finish');
  }
});

// --- Hono (fake context) ---

test('hono: rate limit headers on success, 429 with Retry-After, non-resilience pipeline errors rethrown', async () => {
  const mw = honoResilience({ rateLimit: { key: (c: { id: string }) => c.id, permitLimit: 1, window: '1h' } });
  const set: Record<string, string> = {};
  const c = (id: string) => ({ id, req: { raw: new Request('http://x/') }, json: (body: unknown, status: number, headers?: Record<string, string>) => ({ body, status, headers }), header: (n: string, v: string) => void (set[n] = v) });
  assert.equal(await mw(c('a') as never, async () => {}), undefined);
  assert.deepEqual(set, { 'RateLimit-Limit': '1', 'RateLimit-Remaining': '0', 'RateLimit-Reset': '3600' });
  const limited = (await mw(c('a') as never, async () => {})) as unknown as { status: number; headers: Record<string, string>; body: { error: string } };
  assert.equal(limited.status, 429);
  assert.equal(limited.body.error, 'RateLimiterRejectedError');
  assert.ok(Number(limited.headers['retry-after']) > 0);
  // Without rate limit headers the 504 carries no header object at all.
  const timedOut = (await honoResilience({ pipeline: failAfter(new TimeoutRejectedError(1)) })(c('b') as never, async () => {})) as unknown as { status: number; headers: unknown };
  assert.deepEqual([timedOut.status, timedOut.headers], [504, undefined]);

  const stop = setGlobalChaos({ injectionRate: 1, fault: new Error('chaos') });
  try {
    let called = false;
    await assert.rejects(honoResilience()(c('c') as never, async () => void (called = true)), /chaos/);
    assert.equal(called, false);
  } finally {
    stop();
  }
});

// --- Fastify ---

test('fastify (real): a non-resilience error before the handler goes to Fastify error handling; a disconnect aborts the handler', async () => {
  const app = Fastify();
  await app.register(fastifyResilience, { timeout: '1m' });
  const entered = deferred();
  let handlerAborted: Promise<unknown> | undefined;
  let handled = 0;
  app.get('/', async () => (handled++, 'ok'));
  app.get('/hang', async () => {
    handlerAborted = waitAbort(getResilienceSignal()!);
    entered.resolve();
    await handlerAborted;
    return 'gone';
  });
  await app.listen({ port: 0, host: '127.0.0.1' });
  const url = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`;
  try {
    const stop = setGlobalChaos({ injectionRate: 1, fault: new Error('chaos before handler') });
    let res: Response;
    try {
      res = await fetch(url);
    } finally {
      stop();
    }
    assert.equal(res.status, 500);
    assert.equal(((await res.json()) as { message: string }).message, 'chaos before handler');
    assert.equal(handled, 0);

    const client = new AbortController();
    const pending = fetch(url + '/hang', { signal: client.signal }).catch((e: unknown) => e);
    await entered.promise;
    client.abort();
    assert.match(String(((await handlerAborted) as Error).message), /Client disconnected/);
    await pending;
  } finally {
    await app.close();
  }
});

function fakeFastify(options: Parameters<typeof fastifyResilience>[1]) {
  const hooks: Record<string, (...args: any[]) => any> = {};
  let registered = false;
  fastifyResilience({ addHook: (name: string, fn: (...args: any[]) => any) => (hooks[name] = fn) }, options, () => (registered = true));
  assert.ok(registered);
  const request = () => {
    const raw = Object.assign(new EventEmitter(), { headersSent: false, writableFinished: false });
    const sent: { status?: number; headers: Record<string, string>; body?: unknown } = { headers: {} };
    const reply = {
      raw,
      code: (s: number) => ((sent.status = s), reply),
      headers: (h: Record<string, string>) => (Object.assign(sent.headers, h), reply),
      send: (b: unknown) => ((sent.body = b), reply),
    };
    const req: Record<string | symbol, unknown> = {};
    const nexts: unknown[][] = [];
    const started = new Promise<void>((resolve) => hooks.onRequest!(req, reply, (...args: unknown[]) => (nexts.push(args), resolve())));
    return { raw, sent, req, nexts, started };
  };
  return { hooks, request };
}

test('fastify (fake): errors after the handler started, headers already sent, disconnect, and the route wrapper', async () => {
  const tick = () => new Promise((r) => setImmediate(r));
  // A non-resilience error after the handler started: next() is not called a second time, nothing is sent.
  {
    const r = fakeFastify({ pipeline: failAfter(new Error('late')) }).request();
    await r.started;
    r.raw.writableFinished = true;
    r.raw.emit('finish');
    await tick();
    assert.deepEqual(r.nexts, [[]]);
    assert.equal(r.sent.status, undefined);
  }
  // A resilience error once headers are out: no second response.
  {
    const r = fakeFastify({ pipeline: failAfter(new TimeoutRejectedError(1)) }).request();
    await r.started;
    r.raw.headersSent = true;
    r.raw.emit('finish');
    await tick();
    assert.equal(r.sent.status, undefined);
  }
  // A resilience error after the client went away: nothing sent; the request context was aborted.
  {
    const f = fakeFastify({ pipeline: failAfter(new TimeoutRejectedError(1)) });
    const r = f.request();
    await r.started;
    r.raw.emit('close');
    await tick();
    assert.equal(r.sent.status, undefined);
    // The route wrapper re-enters the request's context around the handler.
    const route: { handler: (this: unknown, request: unknown, reply: unknown) => unknown } = { handler: function (this: unknown) { return [this, getResilienceContext()?.signal.aborted]; } };
    f.hooks.onRoute!(route);
    const self = {};
    assert.deepEqual(route.handler.call(self, r.req, {}), [self, true]);
    // Without a context (a request the onRequest hook never saw) the handler runs as is.
    assert.deepEqual(route.handler.call(self, {}, {}), [self, undefined]);
  }
  // A normal finish closes without aborting.
  {
    const f = fakeFastify({ pipeline: failAfter(new TimeoutRejectedError(1)) });
    const r = f.request();
    await r.started;
    r.raw.writableFinished = true;
    r.raw.emit('close');
    await tick();
    assert.equal(r.sent.status, 504, 'not a disconnect: the rejection is sent');
    assert.equal((r.sent.body as { error: string }).error, 'TimeoutRejectedError');
  }
});

// --- Koa ---

test('koa (real): a non-resilience error before the handler reaches Koa error handling; a disconnect aborts the handler', async () => {
  const app = new Koa();
  app.silent = true;
  const errors: unknown[] = [];
  app.on('error', (e) => errors.push(e));
  app.use(koaResilience({ timeout: '1m' }));
  const entered = deferred();
  let handlerAborted: Promise<unknown> | undefined;
  let handled = 0;
  app.use(async (ctx) => {
    if (ctx.path === '/hang') {
      handlerAborted = waitAbort(getResilienceSignal()!);
      entered.resolve();
      await handlerAborted;
    }
    handled++;
    ctx.body = 'ok';
  });
  const server = app.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    const stop = setGlobalChaos({ injectionRate: 1, fault: new Error('chaos before handler') });
    let res: Response;
    try {
      res = await fetch(url);
    } finally {
      stop();
    }
    assert.equal(res.status, 500);
    assert.equal(handled, 0);
    assert.equal((errors[0] as Error).message, 'chaos before handler');

    const client = new AbortController();
    const pending = fetch(url + '/hang', { signal: client.signal }).catch((e: unknown) => e);
    await entered.promise;
    client.abort();
    assert.match(String(((await handlerAborted) as Error).message), /Client disconnected/);
    await pending;
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test('koa (fake): a non-resilience error after the handler is rethrown; a normal close does not abort; key gets ctx', async () => {
  const fakeCtx = () => {
    const res = Object.assign(new EventEmitter(), { writableFinished: false });
    return { res, status: 404, body: undefined as unknown, headers: {} as Record<string, string>, set(h: Record<string, string>) { Object.assign(this.headers, h); } };
  };
  const ctx = fakeCtx();
  let ran = false;
  await assert.rejects(koaResilience({ pipeline: failAfter(new Error('late')) })(ctx, async () => void (ran = true)), /late/);
  assert.ok(ran);
  assert.equal(ctx.status, 404, 'left to Koa');

  const done = fakeCtx();
  let signal: AbortSignal | undefined;
  await koaResilience()(done, async () => {
    signal = getResilienceSignal();
    done.res.writableFinished = true;
    done.res.emit('close');
  });
  assert.equal(signal?.aborted, false);

  const keyed = fakeCtx();
  const seen: unknown[] = [];
  const mw = koaResilience({ rateLimit: { key: (c) => (seen.push(c), 'k'), permitLimit: 1, window: '1h' }, rateLimitHeaders: 'legacy' });
  await mw(keyed, async () => {});
  assert.equal(seen[0], keyed);
  assert.equal(keyed.headers['X-RateLimit-Remaining'], '0');
  const limited = fakeCtx();
  await mw(limited, async () => assert.fail('not called'));
  assert.equal(limited.status, 429);
  assert.equal((limited.body as { statusCode: number }).statusCode, 429);
  assert.ok(Number(limited.headers['retry-after']) > 0);
  // The request is in context.properties for custom limiters.
  let request: unknown;
  await koaResilience({ pipeline: new ResiliencePipelineBuilder().addStrategy(() => ({ execute: (next, c) => ((request = c.properties.get(HTTP_REQUEST)), next(c)) })).build() })(keyed, async () => {});
  assert.equal(request, keyed);
});
