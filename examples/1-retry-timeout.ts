// 1) A typical HTTP call: retry on network errors / 5xx / 429, with a per-attempt timeout.
import { PredicateBuilder, ResiliencePipelineBuilder, TimeoutRejectedError } from '../src/index.ts';
import { log, startServer } from './flaky-server.ts';

// Request 1: 503, request 2: hangs 5 s, request 3: 429 + Retry-After, request 4: 200
const server = await startServer((n) =>
  n === 1 ? { status: 503 } : n === 2 ? { delay: 5000 } : n === 3 ? { status: 429, headers: { 'retry-after': '1' } } : { body: '{"balance":42}' },
);

const pipeline = new ResiliencePipelineBuilder<Response>()
  .addRetry({
    maxRetryAttempts: 4,
    delay: 100,
    backoffType: 'exponential',
    useJitter: true,
    shouldHandle: new PredicateBuilder<Response>()
      .handle(TypeError) // network error from fetch
      .handle(TimeoutRejectedError) // our own timeout below
      .handleResult((r) => r.status >= 500 || r.status === 429),
    // Honour Retry-After when the server sends it; otherwise fall back to exponential backoff.
    delayGenerator: ({ outcome }) => {
      const s = outcome.ok ? outcome.result.headers.get('retry-after') : null;
      return s ? Number(s) * 1000 : undefined;
    },
    onRetry: ({ attemptNumber, retryDelay, outcome }) =>
      log(`attempt ${attemptNumber} failed (${outcome.ok ? 'HTTP ' + outcome.result.status : (outcome.error as Error).name}) → retry in ${Math.round(retryDelay)}ms`),
  })
  .addTimeout({ timeout: 300, onTimeout: () => log('attempt timed out after 300ms') }) // per attempt
  .build();

const res = await pipeline.execute(({ signal }) => fetch(`${server.url}/balance`, { signal }));
log(`final: HTTP ${res.status}`, await res.text(), `after ${server.requests} requests`);
server.close();
