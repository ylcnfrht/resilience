// 4) Don't overload a dependency: max 2 calls in flight, 2 more may queue, the rest are rejected fast.
import { RateLimiterRejectedError, ResiliencePipelineBuilder } from '../src/index.ts';
import { log, startServer } from './flaky-server.ts';

let inFlight = 0;
const server = await startServer(() => ({ delay: 200 }));

const pipeline = new ResiliencePipelineBuilder()
  .addConcurrencyLimiter({ permitLimit: 2, queueLimit: 2, onRejected: () => log('rejected: bulkhead full') })
  .build();

const results = await Promise.allSettled(
  Array.from({ length: 6 }, (_, i) =>
    pipeline.execute(async () => {
      log(`job ${i} started (in flight: ${++inFlight})`);
      await fetch(server.url);
      inFlight--;
      return i;
    }),
  ),
);
log('results:', results.map((r) => (r.status === 'fulfilled' ? `✓${r.value}` : r.reason instanceof RateLimiterRejectedError ? '✗rejected' : '✗')).join(' '));

// Token bucket: 3 calls per second for an external API quota.
const quota = new ResiliencePipelineBuilder().addRateLimiter({ tokenLimit: 3, replenishmentPeriod: 1000 }).build();
for (let i = 1; i <= 4; i++) {
  await quota.execute(() => 'ok').then(
    () => log(`quota call ${i}: ok`),
    (e: RateLimiterRejectedError) => log(`quota call ${i}: rejected, retry after ${e.retryAfter}ms`),
  );
}
server.close();
