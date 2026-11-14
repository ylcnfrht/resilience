// 3) Cut tail latency: if the primary replica is slow, also ask a second one; first answer wins.
import { ResiliencePipelineBuilder } from '../src/index.ts';
import { log, startServer } from './flaky-server.ts';

const slow = await startServer(() => ({ delay: 2000, body: 'from replica A (slow)' }));
const fast = await startServer(() => ({ delay: 50, body: 'from replica B' }));

const pipeline = new ResiliencePipelineBuilder<string>()
  .addHedging({
    delay: 200, // wait 200ms for the primary before hedging
    maxHedgedAttempts: 1,
    // Hedged attempts go to a different replica.
    actionGenerator: () => async ({ signal }) => (await fetch(fast.url, { signal })).text(),
    onHedging: ({ duration }) => log(`primary still running after ${Math.round(duration)}ms → asking replica B`),
  })
  .build();

const t = performance.now();
const body = await pipeline.execute(async ({ signal }) => {
  signal.addEventListener('abort', () => log('replica A request aborted (lost the race)'));
  return (await fetch(slow.url, { signal })).text();
});
log(`answer: "${body}" in ${Math.round(performance.now() - t)}ms (replica A alone would take 2000ms)`);
slow.close();
fast.close();
