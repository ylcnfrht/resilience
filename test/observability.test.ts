import assert from 'node:assert/strict';
import { AsyncLocalStorage } from 'node:async_hooks';
import { test } from 'node:test';
import { type Context, type ContextManager, ROOT_CONTEXT, SpanStatusCode, context, trace } from '@opentelemetry/api';
import { AggregationTemporality, InMemoryMetricExporter, MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { Registry } from 'prom-client';
import { CircuitBreakerManualControl, ResiliencePipelineBuilder } from '../src/index.ts';
import { instrumentOpenTelemetry } from '../src/otel.ts';
import { instrumentPrometheus } from '../src/prometheus.ts';

const flaky = () => {
  let n = 0;
  return () => (++n < 3 ? Promise.reject(Object.assign(new Error('x'), { status: 503 })) : 'ok');
};

// Test-only stand-in for @opentelemetry/context-async-hooks (not installed).
class AlsContextManager implements ContextManager {
  readonly #als = new AsyncLocalStorage<Context>();
  active = () => this.#als.getStore() ?? ROOT_CONTEXT;
  with = ((ctx: Context, fn: (...args: unknown[]) => unknown, thisArg?: unknown, ...args: unknown[]) => this.#als.run(ctx, () => fn.apply(thisArg, args))) as ContextManager['with'];
  bind = <T>(_ctx: Context, target: T) => target;
  enable = () => this;
  disable = () => {
    this.#als.disable();
    return this;
  };
}

async function withMeter(fn: (meter: ReturnType<MeterProvider['getMeter']>, collect: () => Promise<any[]>) => Promise<void>) {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
  const provider = new MeterProvider({ readers: [reader] });
  try {
    await fn(provider.getMeter('test'), async () => {
      await reader.forceFlush();
      return exporter.getMetrics().at(-1)?.scopeMetrics.flatMap((s) => s.metrics) ?? [];
    });
  } finally {
    await provider.shutdown();
  }
}

test('OpenTelemetry: events counter, duration histograms in seconds, enricher', async () => {
  await withMeter(async (meter, collect) => {
    const stop = instrumentOpenTelemetry({ meter, traces: false, attributes: ({ source }) => ({ team: source.pipelineName === 'payments' ? 'billing' : 'other' }) });
    try {
      await new ResiliencePipelineBuilder({ name: 'payments' }).addRetry({ delay: 0 }).build().execute(flaky());
      const metrics = await collect();
      const events = metrics.find((m) => m.descriptor.name === 'resilience.strategy.events')!;
      const retries = events.dataPoints.find((d: any) => d.attributes['event.name'] === 'OnRetry')!;
      assert.equal(retries.value, 2);
      assert.equal(retries.attributes['pipeline.name'], 'payments');
      assert.equal(retries.attributes.team, 'billing');
      const duration = metrics.find((m) => m.descriptor.name === 'resilience.pipeline.duration')!;
      assert.equal(duration.descriptor.unit, 's');
      assert.equal(duration.dataPoints[0]!.attributes.outcome, 'success');
      assert.equal(duration.dataPoints[0]!.attributes.team, 'billing');
      const attempts = metrics.find((m) => m.descriptor.name === 'resilience.attempt.duration')!;
      assert.equal(attempts.descriptor.unit, 's');
      const points = attempts.dataPoints.map((d: any) => [d.attributes['attempt.number'], d.attributes.outcome, d.value.count]);
      assert.deepEqual(points.sort(), [
        [0, 'failure', 1],
        [1, 'failure', 1],
        [2, 'success', 1],
      ]);
    } finally {
      stop();
    }
  });
});

test('OpenTelemetry: circuit state gauge follows transitions', async () => {
  await withMeter(async (meter, collect) => {
    const stop = instrumentOpenTelemetry({ meter, traces: false });
    const manualControl = new CircuitBreakerManualControl();
    const pipeline = new ResiliencePipelineBuilder({ name: 'cb' }).addCircuitBreaker({ failureRatio: 1, minimumThroughput: 1, breakDuration: 10_000, manualControl }).build();
    const state = async () => (await collect()).find((m) => m.descriptor.name === 'resilience.circuit.state')?.dataPoints.map((d: any) => [d.attributes['pipeline.name'], d.attributes['strategy.name'], d.value]);
    try {
      assert.equal(await state(), undefined);
      await pipeline.execute(() => Promise.reject(new Error('down'))).catch(() => {});
      assert.deepEqual(await state(), [['cb', 'CircuitBreaker', 2]]);
      await manualControl.isolate();
      assert.deepEqual(await state(), [['cb', 'CircuitBreaker', 3]]);
      await manualControl.close();
      assert.deepEqual(await state(), [['cb', 'CircuitBreaker', 0]]);
    } finally {
      stop();
      pipeline.dispose();
    }
  });
});

test('OpenTelemetry: execution span with active attempt spans; user spans nest under the attempt', async () => {
  const manager = new AlsContextManager();
  context.setGlobalContextManager(manager);
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  const tracer = provider.getTracer('test');
  const stop = instrumentOpenTelemetry({ tracer, metrics: false, attributes: () => ({ region: 'eu' }) });
  try {
    const call = flaky();
    await new ResiliencePipelineBuilder({ name: 'payments' })
      .addRetry({ delay: 0 })
      .build()
      .execute(() => tracer.startActiveSpan('db.query', (span) => Promise.resolve(call()).finally(() => span.end())), { operationKey: 'charge' });
    const spans = exporter.getFinishedSpans();
    const parentOf = (s: (typeof spans)[number]) => s.parentSpanContext?.spanId;
    const pipeline = spans.find((s) => s.name === 'resilience.pipeline payments')!;
    assert.equal(pipeline.attributes['pipeline.name'], 'payments');
    assert.equal(pipeline.attributes['operation.key'], 'charge');
    assert.equal(pipeline.attributes.region, 'eu');
    assert.equal(pipeline.status.code, SpanStatusCode.UNSET);
    assert.equal(pipeline.events.filter((e) => e.name === 'resilience.OnRetry').length, 2);

    const attempts = spans.filter((s) => s.name === 'resilience.attempt');
    assert.deepEqual(attempts.map((a) => a.attributes['attempt.number']), [0, 1, 2]);
    assert.ok(attempts.every((a) => parentOf(a) === pipeline.spanContext().spanId));
    assert.deepEqual(attempts.map((a) => a.status.code), [SpanStatusCode.ERROR, SpanStatusCode.ERROR, SpanStatusCode.UNSET]);
    assert.equal(attempts[0]!.attributes['error.type'], 'Error');
    assert.equal(attempts[0]!.events[0]!.name, 'exception');

    const queries = spans.filter((s) => s.name === 'db.query');
    assert.deepEqual(queries.map(parentOf), attempts.map((a) => a.spanContext().spanId));

    await new ResiliencePipelineBuilder({ name: 'broken' }).build().execute(() => Promise.reject(new TypeError('bad'))).catch(() => {});
    const broken = exporter.getFinishedSpans().find((s) => s.name === 'resilience.pipeline broken')!;
    assert.equal(broken.status.code, SpanStatusCode.ERROR);
    assert.equal(broken.attributes['error.type'], 'TypeError');
  } finally {
    stop();
    await provider.shutdown();
    context.disable();
  }
  assert.equal(trace.getActiveSpan(), undefined);
});

test('OpenTelemetry: metrics: false records nothing', async () => {
  await withMeter(async (meter, collect) => {
    const stop = instrumentOpenTelemetry({ meter, metrics: false, traces: false });
    try {
      await new ResiliencePipelineBuilder({ name: 'quiet' }).addRetry({ delay: 0 }).build().execute(flaky());
      assert.ok((await collect()).every((m) => m.dataPoints.length === 0));
    } finally {
      stop();
    }
  });
});

test('Prometheus: counters and histogram in the registry', async () => {
  const registry = new Registry();
  const stop = instrumentPrometheus({ registry });
  try {
    await new ResiliencePipelineBuilder({ name: 'inventory' }).addRetry({ delay: 0 }).build().execute(flaky());
    const text = await registry.metrics();
    assert.match(text, /resilience_strategy_events_total\{pipeline="inventory",strategy="Retry",event="OnRetry"\} 2/);
    assert.match(text, /resilience_pipeline_duration_seconds_count\{pipeline="inventory",outcome="success"\} 1/);
    assert.match(text, /resilience_attempt_duration_seconds_count\{pipeline="inventory",attempt="0",outcome="failure"\} 1/);
    assert.match(text, /resilience_attempt_duration_seconds_count\{pipeline="inventory",attempt="2",outcome="success"\} 1/);
  } finally {
    stop();
  }
});

test('Prometheus: circuit state gauge, extra labels, durations off', async () => {
  const registry = new Registry();
  const stop = instrumentPrometheus({ registry, durations: false, labelNames: ['team'], labels: () => ({ team: 'core' }) });
  const manualControl = new CircuitBreakerManualControl();
  const pipeline = new ResiliencePipelineBuilder({ name: 'cb' }).addCircuitBreaker({ failureRatio: 1, minimumThroughput: 1, breakDuration: 10_000, manualControl }).build();
  try {
    await pipeline.execute(() => Promise.reject(new Error('down'))).catch(() => {});
    let text = await registry.metrics();
    assert.match(text, /resilience_circuit_state\{pipeline="cb",strategy="CircuitBreaker"\} 2/);
    assert.match(text, /resilience_strategy_events_total\{team="core",pipeline="cb",strategy="CircuitBreaker",event="OnCircuitOpened"\} 1/);
    assert.doesNotMatch(text, /duration_seconds/);
    await manualControl.isolate();
    assert.match(await registry.metrics(), /resilience_circuit_state\{pipeline="cb",strategy="CircuitBreaker"\} 3/);
    await manualControl.close();
    text = await registry.metrics();
    assert.match(text, /resilience_circuit_state\{pipeline="cb",strategy="CircuitBreaker"\} 0/);
  } finally {
    stop();
    pipeline.dispose();
  }
});
