import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import { ResiliencePipelineBuilder, getResilienceSignal } from '../src/index.ts';
import { resilient } from '../src/decorators.ts';
import { honoResilience, httpStatusFor, resilienceMiddleware } from '../src/http.ts';
import { BrokenCircuitError, RateLimiterRejectedError, TimeoutRejectedError } from '../src/index.ts';

// --- @resilient ---

test('@resilient works as a legacy (experimentalDecorators) and a standard (TC39) decorator', async () => {
  const pipeline = new ResiliencePipelineBuilder().addRetry({ delay: 0 }).build();
  class Client {
    calls = 0;
    async get() {
      if (++this.calls < 3) throw new Error('flaky');
      return getResilienceSignal() instanceof AbortSignal ? 'ok' : 'no signal';
    }
  }
  // legacy: (prototype, key, descriptor)
  const descriptor = Object.getOwnPropertyDescriptor(Client.prototype, 'get')!;
  Object.defineProperty(Client.prototype, 'get', resilient(pipeline)(Client.prototype, 'get', descriptor));
  const a = new Client();
  assert.equal(await a.get(), 'ok');
  assert.equal(a.calls, 3);

  // standard: (method, { kind: 'method' }) → replacement; per-instance pipeline via a function
  class Other {
    pipeline = new ResiliencePipelineBuilder().addTimeout('10ms').build();
    async slow() {
      await new Promise(() => {}); // never settles: only the timeout can end it
    }
  }
  Other.prototype.slow = resilient((self: Other) => self.pipeline)(Other.prototype.slow, { kind: 'method', name: 'slow' });
  await assert.rejects(new Other().slow(), TimeoutRejectedError);
});

// --- HTTP ---

test('httpStatusFor maps resilience errors', () => {
  assert.equal(httpStatusFor(new TimeoutRejectedError(1)), 504);
  assert.equal(httpStatusFor(new RateLimiterRejectedError(1000)), 429);
  assert.equal(httpStatusFor(new BrokenCircuitError()), 503);
  assert.equal(httpStatusFor(new Error('x')), undefined);
});

async function serve(middleware: ReturnType<typeof resilienceMiddleware>, handler: (req: http.IncomingMessage, res: http.ServerResponse) => void) {
  const server = http.createServer((req, res) => middleware(req, res, () => handler(req, res)));
  await new Promise<void>((ok) => server.listen(0, ok));
  return { url: `http://localhost:${(server.address() as AddressInfo).port}`, close: () => (server.closeAllConnections(), server.close()) };
}

test('resilienceMiddleware: request timeout → 504, and the budget propagates to pipelines deep in the handler', async () => {
  let innerAborted = false;
  let aborted!: () => void;
  const abortSeen = new Promise<void>((r) => (aborted = r));
  const downstream = new ResiliencePipelineBuilder().build(); // e.g. a payment client, unaware of the request
  const s = await serve(resilienceMiddleware({ timeout: '30ms' }), (_req, res) => {
    downstream
      .execute(async ({ signal }) => {
        await new Promise<void>((r) => signal.addEventListener('abort', () => r())); // ends only when aborted
        innerAborted = true;
        aborted();
      })
      .catch(() => {})
      .finally(() => !res.headersSent && res.end('late'));
  });
  try {
    const res = await fetch(s.url);
    assert.equal(res.status, 504);
    assert.equal(((await res.json()) as { error: string }).error, 'TimeoutRejectedError');
    await abortSeen;
    assert.ok(innerAborted, 'downstream call was cancelled by the request deadline');
  } finally {
    s.close();
  }
});

test('resilienceMiddleware: load shedding rejects with 503 beyond the queue (bulkhead full)', async () => {
  let entered!: () => void;
  let release!: () => void;
  const inHandler = new Promise<void>((r) => (entered = r));
  const released = new Promise<void>((r) => (release = r));
  const s = await serve(resilienceMiddleware({ concurrency: { permitLimit: 1, queueLimit: 0 } }), (_req, res) => (entered(), void released.then(() => res.end('ok'))));
  try {
    const first = fetch(s.url);
    await inHandler; // the first request holds the only permit
    assert.equal((await fetch(s.url)).status, 503);
    release();
    assert.equal((await first).status, 200);
  } finally {
    s.close();
  }
});

test('honoResilience: returns 504 JSON on timeout', async () => {
  const mw = honoResilience({ timeout: '10ms' });
  const c = { req: { raw: new Request('http://x/') }, json: (body: unknown, status: number) => new Response(JSON.stringify(body), { status }) };
  const res = (await mw(c, () => new Promise<void>(() => {}))) as Response; // a handler that never finishes
  assert.equal(res.status, 504);
});

test('resilienceMiddleware: a non-resilience error before the handler goes to next(error) instead of hanging', async () => {
  const { setGlobalChaos } = await import('../src/index.ts');
  const stop = setGlobalChaos({ injectionRate: 1, fault: new Error('chaos') });
  try {
    const res = { headersSent: false, writableFinished: false, setHeader() {}, end() {}, once() {} };
    const forwarded = await new Promise<unknown>((resolve) => resilienceMiddleware()({ headers: {} } as never, res as never, resolve));
    assert.equal((forwarded as Error)?.message, 'chaos');
  } finally {
    stop();
  }
});
