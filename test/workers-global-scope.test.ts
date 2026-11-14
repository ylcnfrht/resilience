// Cloudflare Workers reject timers, AbortControllers, I/O and random values at module scope ("Disallowed operation
// called within global scope"); 0.1.0 once failed to load there for one `new AbortController()` in core.ts.
// This file runs in its own process (node --test), so the modules below are loaded here for the first time.
import assert from 'node:assert/strict';
import { test } from 'node:test';

test('no public entry point does Workers-forbidden work at import time', async () => {
  const forbidden = ['AbortController', 'setTimeout', 'setInterval', 'fetch'] as const;
  const saved = forbidden.map((name) => [name, globalThis[name]] as const);
  const savedRandom = crypto.getRandomValues;
  const calls: string[] = [];
  for (const name of forbidden) {
    (globalThis as Record<string, unknown>)[name] = new Proxy(globalThis[name] as object, {
      apply: () => calls.push(name),
      construct: () => (calls.push(name), {}),
    });
  }
  crypto.getRandomValues = (() => calls.push('crypto.getRandomValues')) as typeof crypto.getRandomValues;
  try {
    for (const entry of ['index', 'outcome', 'testing', 'fetch', 'axios', 'otel', 'prometheus', 'http', 'fastify', 'koa', 'hystrix', 'redis', 'adapters', 'nestjs']) {
      await import(`../src/${entry}.ts`);
    }
  } finally {
    for (const [name, value] of saved) (globalThis as Record<string, unknown>)[name] = value;
    crypto.getRandomValues = savedRandom;
  }
  assert.deepEqual(calls, []);
});
