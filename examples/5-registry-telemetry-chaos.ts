// 5) App-wide setup: one shared pipeline per dependency, metrics via diagnostics_channel, chaos in staging.
import diagnostics_channel from 'node:diagnostics_channel';
import { ResiliencePipelineRegistry, TELEMETRY_CHANNEL, type TelemetryEvent } from '../src/index.ts';
import { log } from './flaky-server.ts';

// Telemetry: plug into Prometheus/OpenTelemetry/logs here.
const counts: Record<string, number> = {};
diagnostics_channel.subscribe(TELEMETRY_CHANNEL, (m) => {
  const { event, source } = m as TelemetryEvent;
  const key = `${source.pipelineName}/${event}`;
  counts[key] = (counts[key] ?? 0) + 1;
});

const isStaging = true; // e.g. process.env.NODE_ENV === 'staging'
const registry = new ResiliencePipelineRegistry();

// Usually in one module at startup:
registry.tryAddBuilder('inventory-db', (b) =>
  b
    .addRetry({ maxRetryAttempts: 3, delay: 10 })
    .addTimeout(1000)
    // Chaos: in staging, fail 30% of calls to verify the retry really covers them.
    .addChaosFault({ enabled: isStaging, injectionRate: 0.3, faultGenerator: () => new Error('chaos: injected DB failure') }),
);

// Anywhere in the app: same instance every time (shared circuit breakers / limiters).
const db = registry.getPipeline('inventory-db');
let ok = 0;
for (let i = 0; i < 20; i++) {
  await db.execute(async () => 'row').then(() => ok++, () => {});
}
log(`${ok}/20 queries succeeded despite injected faults`);
log('telemetry:', counts);
