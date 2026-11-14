// 6) How an app calls an external API: resilience lives in the client layer, business code never sees it.
import { randomUUID } from 'node:crypto';
import { PredicateBuilder, ResiliencePipelineBuilder, TimeoutRejectedError, type ResiliencePipeline } from '../src/index.ts';
import { log, startServer } from './flaky-server.ts';

// ─── 1. Infrastructure (written once, e.g. src/lib/http.ts) ────────────────────────────────

/** Errors worth retrying: network failures, timeouts, 5xx, 408, 429. A 400/404 is a bug, retrying won't help. */
const transient = new PredicateBuilder<Response>()
  .handle(TypeError) // fetch throws TypeError on DNS/connection errors
  .handle(TimeoutRejectedError)
  .handleResult((r) => r.status >= 500 || r.status === 408 || r.status === 429);

/** Same layering as .NET's AddStandardResilienceHandler. */
function standardHttpPipeline(name: string) {
  return new ResiliencePipelineBuilder<Response>({ name })
    .addTimeout(5_000) // total budget for the call, all retries included
    .addRetry({
      maxRetryAttempts: 3,
      delay: 100,
      backoffType: 'exponential',
      useJitter: true,
      shouldHandle: transient,
      delayGenerator: ({ outcome }) => {
        const s = outcome.ok ? outcome.result.headers.get('retry-after') : null;
        return s ? Number(s) * 1000 : undefined;
      },
      onRetry: async ({ outcome, attemptNumber, retryDelay }) => {
        // Drain the failed response, or undici keeps the socket busy.
        if (outcome.ok) await outcome.result.body?.cancel();
        log(`   ↻ [${name}] attempt ${attemptNumber} failed (${outcome.ok ? 'HTTP ' + outcome.result.status : (outcome.error as Error).name}), retrying in ${Math.round(retryDelay)}ms`);
      },
    })
    .addCircuitBreaker({ shouldHandle: transient, failureRatio: 0.5, minimumThroughput: 10, samplingDuration: 30_000, breakDuration: 5_000 })
    .addTimeout(500) // per attempt
    .build();
}

/** Drop-in `fetch` with resilience. Unsafe methods (POST/PATCH) are retried only with an Idempotency-Key. */
function resilientFetch(pipeline: ResiliencePipeline<Response>) {
  return (url: string, init: RequestInit = {}) => {
    const method = (init.method ?? 'GET').toUpperCase();
    const safe = ['GET', 'HEAD', 'OPTIONS', 'PUT', 'DELETE'].includes(method) || new Headers(init.headers).has('idempotency-key');
    const call = (signal: AbortSignal) => fetch(url, { ...init, signal });
    // A POST without an idempotency key gets a single attempt: retrying it could charge twice.
    if (!safe) return call(init.signal ? AbortSignal.any([init.signal, AbortSignal.timeout(5_000)]) : AbortSignal.timeout(5_000));
    return pipeline.execute(({ signal }) => call(signal), { signal: init.signal ?? undefined });
  };
}

// ─── 2. API client for one external service (e.g. src/clients/payment-client.ts) ───────────

class PaymentApiError extends Error {
  readonly status: number;
  constructor(status: number) {
    super(`Payment API responded ${status}`);
    this.status = status;
  }
}

class PaymentClient {
  // One pipeline per external service: its circuit breaker reflects only that service's health.
  readonly #http = resilientFetch(standardHttpPipeline('payment-api'));
  readonly #baseUrl: string;
  constructor(baseUrl: string) {
    this.#baseUrl = baseUrl;
  }

  async getBalance(userId: string): Promise<{ balance: number }> {
    const res = await this.#http(`${this.#baseUrl}/users/${userId}/balance`);
    if (!res.ok) throw new PaymentApiError(res.status);
    return res.json() as Promise<{ balance: number }>;
  }

  async charge(userId: string, amount: number): Promise<{ chargeId: string }> {
    const res = await this.#http(`${this.#baseUrl}/charges`, {
      method: 'POST',
      // One key per business operation, reused by every retry → the provider charges once.
      headers: { 'content-type': 'application/json', 'idempotency-key': randomUUID() },
      body: JSON.stringify({ userId, amount }),
    });
    if (!res.ok) throw new PaymentApiError(res.status);
    return res.json() as Promise<{ chargeId: string }>;
  }
}

// ─── 3. Business code: no retry/timeout/circuit logic here ─────────────────────────────────

async function checkout(payments: PaymentClient, userId: string, amount: number) {
  const { balance } = await payments.getBalance(userId);
  if (balance < amount) throw new Error('insufficient funds');
  return payments.charge(userId, amount);
}

// ─── Demo: a flaky payment provider ─────────────────────────────────────────────────────────

const charges = new Map<string, string>(); // idempotency-key → chargeId (what Stripe-like APIs do)
let balanceCalls = 0;
let chargeCalls = 0;
const provider = await startServer((_, req) => {
  if (req.url!.endsWith('/balance')) {
    // 1st call: overloaded. 2nd: connection-level 429. 3rd: fine.
    return ++balanceCalls === 1 ? { status: 503 } : balanceCalls === 2 ? { status: 429, headers: { 'retry-after': '0.2' } } : { body: '{"balance":500}' };
  }
  // POST /charges: the 1st request is processed but its response is "lost" (too slow → our timeout fires).
  const key = req.headers['idempotency-key'] as string;
  chargeCalls++;
  const replay = charges.has(key);
  if (!replay) charges.set(key, `ch_${charges.size + 1}`);
  const body = JSON.stringify({ chargeId: charges.get(key) });
  return chargeCalls === 1 ? { delay: 1000, body } : { body };
});

const payments = new PaymentClient(provider.url);
log('checkout(user-1, 120) ...');
const result = await checkout(payments, 'user-1', 120);
log(`✓ checkout done: ${JSON.stringify(result)}`);
log(`provider saw ${balanceCalls} balance calls and ${chargeCalls} charge requests, but created ${charges.size} charge(s)`);
provider.close();
