import 'reflect-metadata';
import assert from 'node:assert/strict';
import http from 'node:http';
import { Readable } from 'node:stream';
import { test } from 'node:test';
import { Controller, Injectable, Module, Scope } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import axios from 'axios';
import Koa from 'koa';
import { attachResilience } from '../src/axios.ts';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { BrokenCircuitError } from '../src/core.ts';
import { createResilientFetch } from '../src/fetch.ts';
import { honoResilience } from '../src/http.ts';
import { koaResilience } from '../src/koa.ts';
import { ResilienceModule, Retry } from '../src/nestjs.ts';
import { RedisLockStore } from '../src/redis.ts';

// A server whose first request gets 503, later ones 200; records every body it received.
async function flakyServer() {
  const bodies: string[] = [];
  const server = http.createServer((req, res) => {
    let body = '';
    req.on('data', (c) => (body += c)).on('end', () => {
      bodies.push(body);
      res.statusCode = bodies.length === 1 ? 503 : 200;
      res.end();
    });
  });
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  return { url: `http://127.0.0.1:${(server.address() as { port: number }).port}/`, bodies, close: () => new Promise<void>((r) => server.close(() => r())) };
}

test('axios: a stream body is not retried (it would be resent empty); the 503 reaches the caller', async () => {
  // try/finally rather than `await using`: Node 22 can't parse explicit resource management.
  const server = await flakyServer();
  try {
    const api = axios.create();
    attachResilience(api, { retry: { delay: 1 } });
    await assert.rejects(api.put(server.url, Readable.from(['hello'])), (e: { response?: { status: number } }) => e.response?.status === 503);
    assert.deepEqual(server.bodies, ['hello']);
  } finally {
    await server.close();
  }
});

test('fetch: a ReadableStream body is not retried; the 503 response is returned', async () => {
  const server = await flakyServer();
  try {
    const f = createResilientFetch({ retry: { delay: 1 } });
    const body = new ReadableStream({ start: (c) => (c.enqueue(new TextEncoder().encode('hello')), c.close()) });
    const res = await f(server.url, { method: 'PUT', body, duplex: 'half' } as RequestInit);
    assert.equal(res.status, 503);
    assert.deepEqual(server.bodies, ['hello']);
    // A replayable body is still retried.
    assert.equal((await f(server.url, { method: 'PUT', body: 'again' })).status, 200);
  } finally {
    await server.close();
  }
});

test("fetch: a Request's own signal cancels the call", async () => {
  const controller = new AbortController();
  // Settles only through its signal; aborts the caller's Request once the call is in flight.
  const hanging = ((_input: unknown, init: RequestInit) =>
    new Promise((_resolve, reject) => {
      init.signal!.addEventListener('abort', () => reject(init.signal!.reason));
      controller.abort(new Error('caller aborted'));
    })) as typeof fetch;
  const f = createResilientFetch({ fetch: hanging, retry: false });
  await assert.rejects(f(new Request('http://127.0.0.1/', { signal: controller.signal })), /caller aborted/);
});

// Applies method decorators the way TypeScript does (bottom-up, keeping a returned descriptor).
function decorate(cls: { prototype: object }, key: string, ...decorators: MethodDecorator[]) {
  let d = Object.getOwnPropertyDescriptor(cls.prototype, key)!;
  for (const dec of [...decorators].reverse()) d = (dec(cls.prototype, key, d) as typeof d | undefined) ?? d;
  Object.defineProperty(cls.prototype, key, d);
}

test('nestjs: @Retry under another wrapping decorator, and on a controller; request-scoped is refused', async () => {
  const Log = ((_t: object, _k: string | symbol, d: PropertyDescriptor) => {
    const fn = d.value as (...a: unknown[]) => unknown;
    return { ...d, value: function (this: unknown, ...a: unknown[]) { return fn.apply(this, a); } };
  }) as MethodDecorator;
  class Svc {
    calls = 0;
    async flaky() {
      if (++this.calls < 2) throw new Error('flaky');
      return 'ok';
    }
  }
  decorate(Svc, 'flaky', Log, Retry({ delay: 0 }));
  Injectable()(Svc);
  class Ctl {
    calls = 0;
    async get() {
      if (++this.calls < 2) throw new Error('flaky');
      return 'ok';
    }
  }
  decorate(Ctl, 'get', Retry({ delay: 0 }));
  Controller('x')(Ctl);
  class AppModule {}
  Module({ imports: [ResilienceModule.forRoot()], providers: [Svc], controllers: [Ctl] })(AppModule);
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  try {
    assert.equal(await app.get(Svc).flaky(), 'ok');
    assert.equal(await app.get(Ctl).get(), 'ok');
  } finally {
    await app.close();
  }

  class Scoped {
    async run() {}
  }
  decorate(Scoped, 'run', Retry());
  Injectable({ scope: Scope.REQUEST })(Scoped);
  class ScopedModule {}
  Module({ imports: [ResilienceModule.forRoot()], providers: [Scoped] })(ScopedModule);
  await assert.rejects(NestFactory.createApplicationContext(ScopedModule, { logger: false, abortOnError: false }), /request-scoped/);
});

// An outbound pipeline with an open circuit, called from a route handler: its error is the handler's, not the middleware's.
async function openOutbound() {
  const outbound = new ResiliencePipelineBuilder().addCircuitBreaker({ consecutiveFailures: 1, breakDuration: '1m' }).build();
  await outbound.execute(() => Promise.reject(new Error('db down'))).catch(() => {});
  return () => outbound.execute(() => 'x');
}

test('koa: a downstream resilience error goes to Koa error handling unchanged', async () => {
  const call = await openOutbound();
  const app = new Koa();
  let seen: unknown;
  app.use(async (ctx, next) => {
    try {
      await next();
    } catch (error) {
      seen = error;
      ctx.status = 500;
    }
  });
  app.use(koaResilience({ timeout: '5s' }));
  app.use(async (ctx) => void (ctx.body = await call()));
  const server = app.listen(0);
  await new Promise((r) => server.once('listening', r));
  try {
    const res = await fetch(`http://127.0.0.1:${(server.address() as { port: number }).port}/`);
    assert.equal(res.status, 500);
    assert.ok(seen instanceof BrokenCircuitError);
  } finally {
    server.close();
  }
});

test('hono: a downstream resilience error is rethrown unchanged', async () => {
  const call = await openOutbound();
  const c = { req: { raw: new Request('http://x/') }, json: () => new Response(null, { status: 503 }), header: () => {} };
  await assert.rejects(honoResilience({ timeout: '5s' })(c, async () => void (await call())), BrokenCircuitError);
});

// Redis Cluster key slot (CRC16/XMODEM of the hash tag, or the whole key when the tag is missing or empty).
function slot(key: string) {
  const s = key.indexOf('{');
  const e = key.indexOf('}', s + 1);
  if (s >= 0 && e > s + 1) key = key.slice(s + 1, e);
  let crc = 0;
  for (const b of Buffer.from(key)) {
    crc ^= b << 8;
    for (let i = 0; i < 8; i++) crc = crc & 0x8000 ? ((crc << 1) ^ 0x1021) & 0xffff : (crc << 1) & 0xffff;
  }
  return crc % 16384;
}

test('redis lock: lease and fence keys share a Cluster slot, also for the empty key', async () => {
  assert.equal(slot('123456789'), 12739); // CRC16/XMODEM check value 0x31C3
  for (const key of ['', 'job', '}x', 'a{b}c']) {
    let keys: string[] = [];
    const client = { eval: async (_s: string, n: number, ...rest: Array<string | number>) => ((keys = rest.slice(0, n).map(String)), 1) };
    await new RedisLockStore({ client }).acquire(key, 'owner', 1000);
    assert.equal(keys.length, 2);
    assert.equal(slot(keys[0]!), slot(keys[1]!), `key ${JSON.stringify(key)}: ${keys.join(' / ')}`);
  }
});
