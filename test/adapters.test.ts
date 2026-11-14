import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RateLimiterMemory } from 'rate-limiter-flexible';
import { fromRateLimiterFlexible } from '../src/adapters.ts';
import { ResiliencePipelineBuilder } from '../src/builder.ts';
import { RateLimiterRejectedError } from '../src/core.ts';
import { RateLimiterStrategy } from '../src/window-rate-limiter.ts';

test('rate-limiter-flexible: admits up to points, maps msBeforeNext, gives back rejected points', async () => {
  const rlf = new RateLimiterMemory({ points: 2, duration: 1 });
  const algorithm = fromRateLimiterFlexible(rlf, 'k');
  assert.equal(algorithm.permitLimit, 2);
  assert.deepEqual(await algorithm.acquire(2, 0), { ok: true });
  const rejected = await algorithm.acquire(1, 0);
  assert.ok(!rejected.ok && rejected.retryAfter > 0 && rejected.retryAfter <= 1000, JSON.stringify(rejected));
  assert.equal((await rlf.get('k'))?.consumedPoints, 2, 'rejected point was rewarded back');
  assert.deepEqual(await fromRateLimiterFlexible(rlf, 'other').acquire(1, 0), { ok: true });

  const failing = fromRateLimiterFlexible({ points: 1, consume: () => Promise.reject(new Error('store down')), reward: async () => {} }, 'k');
  await assert.rejects(Promise.resolve(failing.acquire(1, 0)), /store down/);

  const pipeline = new ResiliencePipelineBuilder().addStrategy((s) => new RateLimiterStrategy(fromRateLimiterFlexible(new RateLimiterMemory({ points: 1, duration: 1 }), 'p'), {}, s)).build();
  assert.equal(await pipeline.execute(() => 'ok'), 'ok');
  await assert.rejects(pipeline.execute(() => 'ok'), (e: unknown) => e instanceof RateLimiterRejectedError && e.retryAfter! <= 1000);
});
