// 2) Protect a struggling dependency: circuit breaker stops hammering it, fallback serves cached data.
import { CircuitBreakerStateProvider, ResiliencePipelineBuilder } from '../src/index.ts';
import { log, startServer } from './flaky-server.ts';

let down = true;
const server = await startServer(() => (down ? { status: 500 } : { body: 'fresh price: 105' }));
const state = new CircuitBreakerStateProvider();
let cached = 'cached price: 100';

const getPrice = new ResiliencePipelineBuilder<string>()
  .addFallback({
    // Outermost: whatever happens inside (5xx, open circuit), answer from cache.
    fallbackAction: ({ outcome }) => `${cached}  (fallback: ${outcome.ok ? '' : (outcome.error as Error).name})`,
  })
  .addCircuitBreaker({
    failureRatio: 0.5,
    minimumThroughput: 3,
    samplingDuration: 10_000,
    breakDuration: 500,
    stateProvider: state,
    onOpened: ({ breakDuration }) => log(`🔴 circuit OPEN for ${breakDuration}ms — not calling the service`),
    onHalfOpened: () => log('🟡 circuit HALF-OPEN — sending one probe'),
    onClosed: () => log('🟢 circuit CLOSED — back to normal'),
  })
  .build();

const fetchPrice = async () => {
  const r = await fetch(`${server.url}/price`);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const fresh = await r.text();
  cached = fresh.replace('fresh', 'cached'); // refresh the cache on success
  return fresh;
};

for (let i = 1; i <= 6; i++) log(`call ${i}:`, await getPrice.execute(fetchPrice), `| state=${state.circuitState}`);
log(`service received only ${server.requests} requests out of 6 calls`);

down = false; // service recovers
await new Promise((r) => setTimeout(r, 600));
log('call 7:', await getPrice.execute(fetchPrice), `| state=${state.circuitState}`);
server.close();
