// The inbound deadline re-thrown by a nested pipeline inside the handler is still the middleware's own 504
// (it once surfaced as the framework's 500 in Fastify and Hono).
import assert from 'node:assert/strict';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import Fastify from 'fastify';
import Koa from 'koa';
import { BrokenCircuitError, ResiliencePipelineBuilder } from '../src/index.ts';
import { fastifyResilience } from '../src/fastify.ts';
import { koaResilience } from '../src/koa.ts';

const nestedCallTillAborted = () =>
  new ResiliencePipelineBuilder().build().execute(({ signal }) => new Promise<never>((_, reject) => signal.addEventListener('abort', () => reject(signal.reason))));

test('fastify: the request deadline thrown from a nested pipeline answers 504, other errors stay the handler’s', async () => {
  const app = Fastify();
  await app.register(fastifyResilience, { timeout: '30ms' });
  app.get('/slow', async () => nestedCallTillAborted());
  app.get('/broken', async () => {
    throw new BrokenCircuitError();
  });
  app.get('/sync', (_req, reply) => void reply.send('ok')); // callback-style handlers are left untouched
  try {
    const slow = await app.inject('/slow');
    assert.equal(slow.statusCode, 504);
    assert.equal(slow.json().error, 'TimeoutRejectedError');
    assert.equal((await app.inject('/broken')).statusCode, 500, 'a downstream error goes to Fastify unchanged');
    assert.equal((await app.inject('/sync')).body, 'ok');
  } finally {
    await app.close();
  }
});

test('koa: the request deadline thrown from a nested pipeline answers 504', async () => {
  const app = new Koa();
  app.silent = true;
  app.use(koaResilience({ timeout: '30ms' }));
  app.use(async () => {
    await nestedCallTillAborted();
  });
  const server = app.listen(0);
  try {
    await new Promise((r) => server.once('listening', r));
    const res = await fetch(`http://127.0.0.1:${(server.address() as AddressInfo).port}/`);
    assert.equal(res.status, 504);
    assert.equal(((await res.json()) as { error: string }).error, 'TimeoutRejectedError');
  } finally {
    server.closeAllConnections();
    server.close();
  }
});
