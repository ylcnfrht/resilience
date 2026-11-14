import assert from 'node:assert/strict';
import { test } from 'node:test';
import { SpanStatusCode, metrics, trace } from '@opentelemetry/api';
import { AggregationTemporality, InMemoryMetricExporter, MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { BasicTracerProvider, InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base';
import { Registry, register } from 'prom-client';
import { report } from '../src/core.ts';
import { createHystrixStream } from '../src/hystrix.ts';
import { CircuitBreakerManualControl, ResiliencePipelineBuilder } from '../src/index.ts';
import { errorType, instrumentOpenTelemetry } from '../src/otel.ts';
import { instrumentPrometheus } from '../src/prometheus.ts';
import { FakeTimeProvider } from '../src/testing.ts';

const ctx = { properties: new Map() } as never;
const circuitEvent = (pipelineName: string | undefined, event: string, args: unknown = {}) => report({ pipelineName, strategyName: undefined } as never, event, ctx, args);

function meterKit(temporality = AggregationTemporality.CUMULATIVE) {
  const exporter = new InMemoryMetricExporter(temporality);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const collect = async (name: string) => {
    await reader.forceFlush();
    return (exporter.getMetrics().at(-1)?.scopeMetrics.flatMap((s) => s.metrics) ?? []).find((m) => m.descriptor.name === name)?.dataPoints ?? [];
  };
  return { provider, collect };
}

function tracerKit() {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  return { provider, exporter };
}

test('errorType: code, class name, typeof (also for null)', () => {
  assert.equal(errorType(Object.assign(new Error('x'), { code: 'ECONNRESET' })), 'ECONNRESET');
  assert.equal(errorType(new TypeError('x')), 'TypeError');
  assert.equal(errorType({ code: 42 }), 'object');
  assert.equal(errorType(null), 'object');
  assert.equal(errorType('boom'), 'string');
});

test('OpenTelemetry: global tracer and meter by default; span names for unnamed and named operations; non-Error failures', async () => {
  const t = tracerKit();
  const m = meterKit();
  trace.setGlobalTracerProvider(t.provider);
  metrics.setGlobalMeterProvider(m.provider);
  const stop = instrumentOpenTelemetry({ spanEvents: false });
  try {
    await new ResiliencePipelineBuilder().build().execute(() => Promise.reject('plain string')).catch(() => {});
    await new ResiliencePipelineBuilder({ name: 'p' }).build().execute(() => 1, { name: 'charge', operationKey: 'k1' });
    const spans = t.exporter.getFinishedSpans();
    const unnamed = spans.find((s) => s.name === 'resilience.pipeline')!;
    assert.equal(unnamed.attributes['pipeline.name'], '');
    assert.equal(unnamed.status.code, SpanStatusCode.ERROR);
    assert.equal(unnamed.status.message, 'plain string');
    assert.equal(unnamed.attributes['error.type'], 'string');
    assert.equal(unnamed.events.length, 0, 'no exception recorded for a non-Error');
    const named = spans.find((s) => s.name === 'resilience.pipeline charge')!;
    assert.equal(named.attributes['operation.name'], 'charge');
    assert.equal(named.attributes['operation.key'], 'k1');
    const durations = await m.collect('resilience.pipeline.duration');
    assert.deepEqual(durations.map((d: any) => [d.attributes['pipeline.name'], d.attributes.outcome, d.attributes['operation.name']]).sort(), [
      ['', 'failure', undefined],
      ['p', 'success', 'charge'],
    ]);
  } finally {
    stop();
    trace.disable();
    metrics.disable();
    await t.provider.shutdown();
    await m.provider.shutdown();
  }
});

test('OpenTelemetry: an instrumentation error inside the span ends it with ERROR and is rethrown', async () => {
  const t = tracerKit();
  const stop = instrumentOpenTelemetry({ tracer: t.provider.getTracer('t'), metrics: false });
  try {
    const p = new ResiliencePipelineBuilder({ name: 'inst', instrumentation: { execution: () => Promise.reject(new Error('instrumentation broke')) } }).build();
    await assert.rejects(p.execute(() => 1), /instrumentation broke/);
    const span = t.exporter.getFinishedSpans().find((s) => s.name === 'resilience.pipeline inst')!;
    assert.equal(span.status.code, SpanStatusCode.ERROR);
    assert.equal(span.status.message, 'instrumentation broke');
    assert.equal(span.events[0]!.name, 'exception');
  } finally {
    stop();
    await t.provider.shutdown();
  }
});

test('OpenTelemetry: attempt.number is capped at 10 on the histogram, exact on spans', async () => {
  const m = meterKit();
  const t = tracerKit();
  const stop = instrumentOpenTelemetry({ meter: m.provider.getMeter('t'), tracer: t.provider.getTracer('t') });
  try {
    let n = 0;
    await new ResiliencePipelineBuilder({ name: 'many' }).addRetry({ delay: 0, maxRetryAttempts: 12 }).build().execute(() => (++n < 13 ? Promise.reject(new Error('x')) : 'ok'));
    const points = (await m.collect('resilience.attempt.duration')).map((d: any) => [d.attributes['attempt.number'], d.attributes.outcome, d.value.count]);
    const at10 = points.filter((p) => p[0] === 10);
    assert.deepEqual(at10.sort(), [[10, 'failure', 2], [10, 'success', 1]]);
    assert.equal(Math.max(...points.map((p) => p[0])), 10);
    const spanNumbers = t.exporter.getFinishedSpans().filter((s) => s.name === 'resilience.attempt').map((s) => s.attributes['attempt.number']);
    assert.equal(Math.max(...(spanNumbers as number[])), 12);
  } finally {
    stop();
    await m.provider.shutdown();
    await t.provider.shutdown();
  }
});

test('OpenTelemetry: circuit gauge covers half-open and synced states and keeps at most 1000 circuits', async () => {
  const m = meterKit(AggregationTemporality.DELTA); // cumulative export would replay every attribute set ever observed
  const stop = instrumentOpenTelemetry({ meter: m.provider.getMeter('t'), traces: false });
  const states = async () => new Map((await m.collect('resilience.circuit.state')).map((d: any) => [d.attributes['pipeline.name'], d.value]));
  try {
    circuitEvent('h', 'OnCircuitHalfOpened');
    circuitEvent('s', 'OnCircuitStateSynced', { snapshot: { state: 'isolated' } });
    circuitEvent(undefined, 'OnCircuitClosed');
    let s = await states();
    assert.equal(s.get('h'), 1);
    assert.equal(s.get('s'), 3);
    assert.equal(s.get(''), 0, 'unnamed pipeline and strategy report as empty strings');

    circuitEvent('h', 'OnCircuitOpened'); // re-touched: now the most recent
    for (let i = 0; i < 999; i++) circuitEvent(`host-${i}`, 'OnCircuitOpened');
    s = await states();
    assert.equal(s.size, 1000);
    assert.equal(s.has('s'), false, 'least recently changed circuit dropped');
    assert.equal(s.has(''), false);
    assert.equal(s.get('h'), 2);
    assert.equal(s.get('host-998'), 2);
  } finally {
    stop();
    await m.provider.shutdown();
  }
});

test('Prometheus: attempt label capped at 10; circuit series capped at 1000; half-open / synced; unnamed sources', async () => {
  const registry = new Registry();
  const stop = instrumentPrometheus({ registry, prefix: 'rp_', buckets: [1] });
  try {
    let n = 0;
    await new ResiliencePipelineBuilder({ name: 'many' }).addRetry({ delay: 0, maxRetryAttempts: 12 }).build().execute(() => (++n < 13 ? Promise.reject(new Error('x')) : 'ok'));
    let text = await registry.metrics();
    assert.match(text, /rp_attempt_duration_seconds_count\{pipeline="many",attempt="10",outcome="failure"\} 2/);
    assert.match(text, /rp_attempt_duration_seconds_count\{pipeline="many",attempt="10",outcome="success"\} 1/);
    assert.doesNotMatch(text, /attempt="11"/);
    assert.match(text, /rp_attempt_duration_seconds_bucket\{le="1",/, 'custom buckets');
    await new ResiliencePipelineBuilder({ name: 'bad' }).build().execute(() => Promise.reject(new Error('x'))).catch(() => {});
    assert.match(await registry.metrics(), /rp_pipeline_duration_seconds_count\{pipeline="bad",outcome="failure"\} 1/);

    circuitEvent('h', 'OnCircuitHalfOpened');
    circuitEvent('s', 'OnCircuitStateSynced', { snapshot: { state: 'open' } });
    circuitEvent(undefined, 'OnCircuitOpened', { isManual: true });
    circuitEvent('s', 'OnCircuitStateSynced', { snapshot: { state: 'bogus' } }); // unknown state: ignored, the counter still counts it
    text = await registry.metrics();
    assert.match(text, /rp_circuit_state\{pipeline="h",strategy=""\} 1/);
    assert.match(text, /rp_circuit_state\{pipeline="s",strategy=""\} 2/);
    assert.match(text, /rp_circuit_state\{pipeline="",strategy=""\} 3/);
    assert.match(text, /rp_strategy_events_total\{pipeline="s",strategy="",event="OnCircuitStateSynced"\} 2/);

    circuitEvent('h', 'OnCircuitClosed');
    for (let i = 0; i < 998; i++) circuitEvent(`host-${i}`, 'OnCircuitOpened');
    text = await registry.metrics();
    const series = text.split('\n').filter((l) => l.startsWith('rp_circuit_state{'));
    assert.equal(series.length, 1000);
    assert.doesNotMatch(text, /rp_circuit_state\{pipeline="s"/, 'least recently changed series removed');
    assert.match(text, /rp_circuit_state\{pipeline="h",strategy=""\} 0/);
  } finally {
    stop();
  }
});

test('Prometheus: registers in prom-client\'s global registry by default', async () => {
  const stop = instrumentPrometheus({ prefix: 'rp_global_test_' });
  try {
    await new ResiliencePipelineBuilder({ name: 'g' }).build().execute(() => 1);
    assert.match(await register.metrics(), /rp_global_test_pipeline_duration_seconds_count\{pipeline="g",outcome="success"\} 1/);
  } finally {
    stop();
    for (const name of ['strategy_events_total', 'circuit_state', 'pipeline_duration_seconds', 'attempt_duration_seconds']) register.removeSingleMetric(`rp_global_test_${name}`);
  }
});

test('hystrix: default clock and interval, ignores other pipelines, closes the circuit, dispose stops open streams', async () => {
  const stream = createHystrixStream({ pipelines: ['hx'] });
  try {
    const manualControl = new CircuitBreakerManualControl();
    const p = new ResiliencePipelineBuilder({ name: 'hx' }).addCircuitBreaker({ manualControl }).build();
    await new ResiliencePipelineBuilder({ name: 'not-reported' }).addCircuitBreaker({ consecutiveFailures: 1 }).build().execute(() => Promise.reject(new Error('x'))).catch(() => {});
    assert.deepEqual(stream.commands().map((c) => c.name), ['hx']);
    await manualControl.isolate();
    assert.equal(stream.commands()[0]!.isCircuitBreakerOpen, true);
    await manualControl.close();
    assert.equal(stream.commands()[0]!.isCircuitBreakerOpen, false);
    assert.equal(stream.commands()[0]!.group, 'resilience-pipeline');
    assert.equal(stream.commands()[0]!.propertyValue_metricsRollingStatisticalWindowInMilliseconds, 10_000);
    p.dispose();
  } finally {
    stream.dispose();
  }

  const time = new FakeTimeProvider();
  const timed = createHystrixStream({ pipelines: ['hy'], group: 'g', timeProvider: time });
  const written: string[] = [];
  timed.handler({ once: () => {} }, { writeHead: () => {}, write: (c) => written.push(c) });
  await time.advance('1s'); // default interval
  assert.equal(written.length, 2);
  assert.match(written[0]!, /"group":"g"/);
  timed.dispose(); // the client never closed: dispose stops its timer
  assert.equal(time.pendingTimers, 0);
});
