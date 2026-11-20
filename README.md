<div align="center">

# @ylcnfrht/resilience

**Failures are inevitable. Outages are optional.**

Retry, circuit breaker, timeout, hedging, rate limiting and typed errors for TypeScript,<br>
composed into one pipeline. A [Polly v8](https://www.pollydocs.org/) API for Node, Bun, Deno, Workers and browsers.

[![npm](https://img.shields.io/npm/v/@ylcnfrht/resilience?color=5eead4&label=npm)](https://www.npmjs.com/package/@ylcnfrht/resilience)
[![CI](https://github.com/ylcnfrht/resilience/actions/workflows/ci.yml/badge.svg)](https://github.com/ylcnfrht/resilience/actions/workflows/ci.yml)
[![zero dependencies](https://img.shields.io/badge/dependencies-0-5eead4)](package.json)
[![types](https://img.shields.io/npm/types/@ylcnfrht/resilience?color=5eead4)](https://www.npmjs.com/package/@ylcnfrht/resilience)
[![license](https://img.shields.io/npm/l/@ylcnfrht/resilience?color=5eead4)](LICENSE)
[![Buy me a coffee](https://img.shields.io/badge/Buy%20me%20a%20coffee-ffdd00?logo=buymeacoffee&logoColor=black)](https://buymeacoffee.com/ylcnfrht)

[**Documentation**](https://resilience.ferhatyalcin.dev) · [Quick start](https://resilience.ferhatyalcin.dev/start/quick-start/) · [Playground](https://resilience.ferhatyalcin.dev/playground/)

</div>

---

```sh
npm i @ylcnfrht/resilience
```

```ts
import { ResiliencePipelineBuilder } from '@ylcnfrht/resilience';

const payments = new ResiliencePipelineBuilder({ name: 'payments' })
  .addFallback({ fallbackAction: () => queueForLater(order) }) // same order.id key: the charge may already have gone through
  .addRetry({ maxRetryAttempts: 3, backoffType: 'exponential', useJitter: 'full' })
  .addCircuitBreaker({ failureRatio: 0.5, minimumThroughput: 20 })
  .addTimeout('2s')
  .build();

const receipt = await payments.execute(({ signal }) => charge(order, { signal, idempotencyKey: order.id }));
```

Strategies run in the order you add them, the first one outermost: here every retry attempt gets its own 2 s timeout and passes the circuit breaker, and if everything fails the fallback answers. Pass `signal` to your I/O: timeouts, hedging and caller cancellation all work by aborting it.

## Why @ylcnfrht/resilience

- **One composable pipeline.** Retry, circuit breaker, timeout, fallback, hedging, bulkhead, rate limiting, cache, lock and chaos, in any order, through one builder. If you know Polly, you already know the API.
- **Strategies the other Node libraries don't ship.** Hedging, adaptive concurrency limits (gradient2, vegas, aimd), retry budgets, fixed/sliding/per-key rate limits and Redis-shared state.
- **Typed errors, if you want them.** `settle()` returns an outcome whose error type is built from the strategies you added; `Outcome.match` makes you handle each one, so forgetting a circuit-open branch is a compile error.
- **Deadlines that propagate.** A pipeline executed inside another inherits its signal. An inbound request timeout cancels every nested call, with no plumbing.
- **Safe by default.** 4xx errors are never retried, open circuits never retried, `idempotent: false` calls never repeated once they may have reached the dependency.
- **Testable in milliseconds.** `FakeTimeProvider` drives every delay, timeout and break duration: test a 30-second outage instantly.
- **Small and everywhere.** Zero dependencies, tree-shakeable, `retry()` from 4.1 KB min+gzip. ESM, CommonJS and a script tag; Node ≥ 20.16, Bun, Deno, Cloudflare Workers, browsers.

## A quick tour

**One-liners**, when you don't need a pipeline:

```ts
import { NonRetryableError, retry, withTimeout } from '@ylcnfrht/resilience';

const user = await retry(() => api.getUser(id), { maxRetryAttempts: 5, backoffType: 'exponential', useJitter: 'full' });
const page = await withTimeout(({ signal }) => fetch(url, { signal }), '2s');
// throw new NonRetryableError(err) inside the callback to stop retrying
```

**HTTP clients**: per-host breakers, retries that honour `Retry-After`, idempotency-aware POSTs.

```ts
import { createResilientFetch } from '@ylcnfrht/resilience/fetch';

const http = createResilientFetch({ name: 'github', retryOn: [429, 503] });
const res = await http('https://api.github.com/repos/nodejs/node', { resilience: { timeout: '3s' } });
```

**Typed errors**: the compiler knows every way a call can fail.

```ts
import { Outcome, ResiliencePipelineBuilder } from '@ylcnfrht/resilience';

const pipeline = new ResiliencePipelineBuilder().addTimeout('2s').addCircuitBreaker().build();

const user = Outcome.match(await pipeline.settle(() => db.findUser(id)), {
  ok: (u) => u,
  TIMEOUT: () => cachedUser(id),
  CIRCUIT_OPEN: () => cachedUser(id),
  CIRCUIT_ISOLATED: () => cachedUser(id),
  UNEXPECTED: (e) => { throw e.cause; },
});
```

**Inbound protection** for Express, Fastify, Koa and Hono: a request budget that propagates, load shedding and rate limits.

```ts
import { resilienceMiddleware } from '@ylcnfrht/resilience/http';

app.use(resilienceMiddleware({
  timeout: '5s',
  concurrency: { permitLimit: 200, queueLimit: 100 },
  rateLimit: { key: (req) => req.ip, permitLimit: 100, window: '1m' },
}));
```

**Streaming**: retried until the first chunk, committed after it.

```ts
for await (const token of pipeline.executeStream(({ signal }) => llm.stream(prompt, { signal }))) render(token);
```

**Tests without waiting**:

```ts
import { FakeTimeProvider } from '@ylcnfrht/resilience/testing';

const time = new FakeTimeProvider();
const pipeline = new ResiliencePipelineBuilder({ timeProvider: time })
  .addRetry({ delay: '10s', maxRetryAttempts: 3 })
  .addTimeout('1s')
  .build();

const done = assert.rejects(pipeline.execute(() => new Promise(() => {})));
await time.runAll(); // 34 virtual seconds, instantly
await done;
```

## Strategies

| Strategy | Builder method | What it does |
|---|---|---|
| Retry | `addRetry` | Constant, linear or exponential backoff with jitter, `Retry-After`, retry budgets |
| Circuit breaker | `addCircuitBreaker` | Failure ratio over a time or count window, half-open probes, manual isolation, health checks |
| Timeout | `addTimeout` | Aborts the attempt's signal; nested pipelines inherit the deadline |
| Fallback | `addFallback`, `addFallbackOn`, `addStaleFallback` | Answer anyway, for all or selected error codes, or with the last good value |
| Hedging | `addHedging` | Race a second attempt against a slow one; the first good result wins, the rest are aborted |
| Bulkhead | `addConcurrencyLimiter` | Limit parallel calls, with a queue and priorities |
| Adaptive concurrency | `addAdaptiveConcurrencyLimiter` | A limit that finds itself from latency (gradient2, vegas, aimd) |
| Rate limiting | `addRateLimiter`, `addFixedWindowRateLimiter`, `addSlidingWindowRateLimiter`, `addPartitionedRateLimiter` | Token bucket, fixed and sliding windows, per user or tenant |
| Cache | `addCache`, `addCoalesce` | TTL + stale-if-error cache, single-flight for concurrent identical calls |
| Lock | `addLock` | Distributed lock with lease renewal and fencing tokens |
| Chaos | `addChaosFault`, `addChaosLatency`, `addChaosOutcome`, `addChaosBehavior` | Inject failures and latency for game days |

Every strategy is tree-shakeable: `PipelineBuilder` with `retryStrategy()`, `timeoutStrategy()`, … bundles only what you add. See [Strategies](https://resilience.ferhatyalcin.dev/strategies/retry/).

## Integrations

Each is a subpath of the same package, with optional peer dependencies.

| Import | For |
|---|---|
| `@ylcnfrht/resilience/fetch` | `createResilientFetch`: per-host pipelines for `fetch` |
| `@ylcnfrht/resilience/axios` | `attachResilience(axios)` |
| `@ylcnfrht/resilience/http` | Express / Connect and Hono middleware, `describeResilienceError` |
| `@ylcnfrht/resilience/fastify`, `/koa` | Fastify plugin, Koa middleware |
| `@ylcnfrht/resilience/nestjs` | `@Retry`, `@CircuitBreaker`, `@Timeout`, `@UsePipeline` … for providers and controllers |
| `@ylcnfrht/resilience/redis` | Shared circuit state, rate limits, locks and cache across instances (ioredis, node-redis, Cluster) |
| `@ylcnfrht/resilience/otel`, `/prometheus` | OpenTelemetry spans and metrics, Prometheus metrics |
| `@ylcnfrht/resilience/adapters` | rate-limiter-flexible as a rate limiter backend |
| `@ylcnfrht/resilience/hystrix` | A Hystrix-dashboard event stream |
| `@ylcnfrht/resilience/testing` | `FakeTimeProvider` |

## Runtimes

| Runtime | Status |
|---|---|
| Node.js ≥ 20.16 | Supported |
| Bun | Supported, tested |
| Deno 2 | Supported, tested (deadline propagation and `diagnostics_channel` on Deno ≥ 2.1) |
| Cloudflare Workers | Supported, tested (enable `nodejs_compat` for deadline propagation) |
| Browsers (ES2022) | Supported; also as a script tag: `unpkg.com/@ylcnfrht/resilience`, global `resilience` |

Details in [Runtimes](https://resilience.ferhatyalcin.dev/concepts/runtimes/).

## Bundle size

| What you import (min+gzip, browser) | Size |
|---|---|
| `retry()` one-liner | 4.1 KB |
| `PipelineBuilder` + retry + timeout | 4.8 KB |
| `PipelineBuilder` + retry + circuit breaker + timeout | 7.1 KB |
| `createResilientFetch()` | 9.1 KB |
| `ResiliencePipelineBuilder` (every strategy) | 15.1 KB |

## Migrating

Coming from another library? Step-by-step guides for [cockatiel](https://resilience.ferhatyalcin.dev/migration/cockatiel/), [opossum](https://resilience.ferhatyalcin.dev/migration/opossum/), [p-retry](https://resilience.ferhatyalcin.dev/migration/p-retry/), [axios-retry](https://resilience.ferhatyalcin.dev/migration/axios-retry/), [tenace](https://resilience.ferhatyalcin.dev/migration/tenace/), [@nestjs/resilience](https://resilience.ferhatyalcin.dev/migration/nestjs-resilience/), [Polly](https://resilience.ferhatyalcin.dev/migration/polly/).

## Documentation

Everything else (concepts, every strategy option, integrations, guides for databases, serverless, streaming, multi-instance setups and game days, telemetry events and error codes) is on **[resilience.ferhatyalcin.dev](https://resilience.ferhatyalcin.dev)**, with interactive explainers that run the library in your browser. An index for coding agents is at [`/llms.txt`](https://resilience.ferhatyalcin.dev/llms.txt).

## Contributing

```sh
npm install
npm test              # node --test; the Redis tests run when Redis listens on 127.0.0.1:6379, else they skip
npm run typecheck
npm run check:size    # bundle-size budgets
```

`REDIS_CLUSTER=host:port,…` also runs the Redis Cluster test. The docs site lives in [`site/`](site/) (`npm run dev` there); every code sample in it is type-checked at build time.

@ylcnfrht/resilience is an independent TypeScript implementation inspired by Polly. It is not affiliated with the Polly project or the .NET Foundation.

## Support

If @ylcnfrht/resilience saves you an outage, you can [buy me a coffee](https://buymeacoffee.com/ylcnfrht) ☕. Issues, ideas and pull requests are just as welcome.

## License

[MIT](LICENSE) © [ylcnfrht](https://github.com/ylcnfrht)
