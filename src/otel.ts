import { type Attributes, type Meter, type ObservableResult, type Span, SpanStatusCode, type Tracer, metrics, trace } from '@opentelemetry/api';
import { type ExecutionInfo, type Outcome, type TelemetryEvent, instrument, onTelemetry } from './core.ts';

export interface OpenTelemetryOptions {
  /** Default: `metrics.getMeter('@nodelibraries/resilience')` from the global MeterProvider. */
  meter?: Meter;
  /** Default: `trace.getTracer('@nodelibraries/resilience')` from the global TracerProvider. */
  tracer?: Tracer;
  /** Record metrics. Default true. */
  metrics?: boolean;
  /** One span per pipeline execution and one child span per attempt. Default true. */
  traces?: boolean;
  /** Also add each strategy event to the active span (retries, timeouts, circuit changes show up in traces). Default true. */
  spanEvents?: boolean;
  /** Extra attributes for every metric data point and span, e.g. `({ context }) => ({ tenant: context.properties.get('tenant') })`. */
  attributes?: (input: TelemetryEvent | ExecutionInfo) => Attributes | undefined;
}

/** The `error.type` attribute value for an error: its `code` if it has one, else its class name, else its typeof. */
export const errorType = (error: unknown): string => {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'string' ? code : error instanceof Error ? error.name : typeof error;
};
const seconds = (ms: number) => ms / 1000;
// OTel's default buckets are meant for milliseconds; these are the semantic-conventions ones for seconds.
const advice = { explicitBucketBoundaries: [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10, 30] };
const CIRCUIT_STATE = { closed: 0, 'half-open': 1, open: 2, isolated: 3 } as const;
/** Attempts from this number on share one `attempt.number` value, so huge `maxRetryAttempts` don't create unbounded series. */
const MAX_ATTEMPT_LABEL = 10;
// shortcut: circuits gauge keeps the most recently changed 1000; pipelines disposed without a telemetry event (e.g. fetch's evicted per-host ones) age out instead of being removed.
const MAX_CIRCUITS = 1000;

/**
 * Exports every pipeline's telemetry to OpenTelemetry.
 *
 * Traces: span `resilience.pipeline <name>` per execution, with a child `resilience.attempt` span per call of the
 * callback (made active, so spans you start in the callback nest under it). Failures set ERROR status and record the
 * exception; strategy events (retry, timeout, circuit rejected…) are added as span events. Nesting needs a context
 * manager, which every OTel Node SDK registers.
 *
 * Metrics:
 * - `resilience.strategy.events` (counter): retries, timeouts, circuit transitions, rejections…
 * - `resilience.pipeline.duration` (histogram, s): one sample per execution, with `outcome` and `error.type`.
 * - `resilience.attempt.duration` (histogram, s): one sample per attempt of a retry strategy, with `attempt.number`
 *   (capped at 10: 10 means the 10th retry or later), `outcome`, `error.type`. Emitted for every attempt (any pipeline). Its count is the number of attempts.
 * - `resilience.circuit.state` (gauge): 0 closed, 1 half-open, 2 open, 3 isolated, per `pipeline.name` + `strategy.name`
 *   (the 1000 most recently changed circuits).
 *
 * Call once at startup; returns a function that stops it.
 */
export function instrumentOpenTelemetry(options: OpenTelemetryOptions = {}): () => void {
  const enrich = options.attributes ?? (() => undefined);
  const spanEvents = options.spanEvents ?? true;
  const stops: Array<() => void> = [];

  if (options.traces ?? true) {
    const tracer = options.tracer ?? trace.getTracer('@nodelibraries/resilience');
    const failed = (span: Span, error: unknown) => {
      span.setAttribute('error.type', errorType(error));
      if (error instanceof Error) span.recordException(error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: error instanceof Error ? error.message : String(error) });
    };
    const traced =
      (name: (info: ExecutionInfo) => string, attributes: (info: ExecutionInfo) => Attributes) =>
      <R>(info: ExecutionInfo, run: () => Promise<R>): Promise<R> =>
        tracer.startActiveSpan(name(info), { attributes: { ...attributes(info), ...enrich(info) } }, async (span) => {
          try {
            // Pipelines pass outcomes around instead of throwing.
            const result = await run();
            const outcome = result as Outcome<unknown> | undefined;
            if (outcome?.ok === false) failed(span, outcome.error);
            return result;
          } catch (error) {
            failed(span, error);
            throw error;
          } finally {
            span.end();
          }
        });
    const base = ({ source, context }: ExecutionInfo): Attributes => ({
      'pipeline.name': source.pipelineName ?? '',
      ...(context.operationKey !== undefined && { 'operation.key': context.operationKey }),
      ...(context.operationName !== undefined && { 'operation.name': context.operationName }),
      ...source.tags,
      ...context.attributes,
    });
    stops.push(
      instrument({
        execution: traced(({ source, context }) => {
          const name = context.operationName ?? source.pipelineName;
          return name ? `resilience.pipeline ${name}` : 'resilience.pipeline';
        }, base),
        attempt: traced(() => 'resilience.attempt', (info) => ({ ...base(info), 'attempt.number': info.context.attemptNumber })),
      }),
    );
  }

  const record = options.metrics ?? true;
  const meter = options.meter ?? metrics.getMeter('@nodelibraries/resilience');
  const events = meter.createCounter('resilience.strategy.events', { description: 'Resilience strategy events (retry, timeout, circuit state changes, rejections).' });
  const duration = meter.createHistogram('resilience.pipeline.duration', { unit: 's', description: 'Duration of resilience pipeline executions, retries included.', advice });
  const attemptDuration = meter.createHistogram('resilience.attempt.duration', { unit: 's', description: 'Duration of each attempt made by a retry strategy.', advice });
  const circuitGauge = meter.createObservableGauge('resilience.circuit.state', { description: 'Circuit breaker state: 0 closed, 1 half-open, 2 open, 3 isolated.' });
  const circuits = new Map<string, { attributes: Attributes; state: number }>();
  const observeCircuits = (result: ObservableResult) => circuits.forEach((c) => result.observe(c.state, c.attributes));
  if (record) {
    circuitGauge.addCallback(observeCircuits);
    stops.push(() => circuitGauge.removeCallback(observeCircuits));
  }

  stops.push(
    onTelemetry((e: TelemetryEvent) => {
      const pipeline = e.source.pipelineName ?? '';
      const strategy = e.source.strategyName ?? '';
      const base: Attributes = { 'pipeline.name': pipeline, 'operation.key': e.context.operationKey ?? '', ...(e.context.operationName !== undefined && { 'operation.name': e.context.operationName }), ...e.source.tags, ...e.context.attributes, ...enrich(e) };
      const outcomeAttributes = (outcome: Outcome<unknown>): Attributes => ({ outcome: outcome.ok ? 'success' : 'failure', ...(!outcome.ok && { 'error.type': errorType(outcome.error) }) });

      if (e.event === 'OnPipelineExecuted') {
        const { outcome, duration: ms } = e.args as { outcome: Outcome<unknown>; duration: number };
        if (record) duration.record(seconds(ms), { ...base, ...outcomeAttributes(outcome) });
        return;
      }
      if (e.event === 'OnPipelineExecuting' || e.event === 'OnStrategyExecuted') return; // the execution span already marks the start
      // Per-attempt measurements are a histogram, not a strategy event: no counter, no span event (the attempt has its own span).
      if (e.event === 'OnExecutionAttempt') {
        const { outcome, duration: ms, attemptNumber } = e.args as { outcome: Outcome<unknown>; duration: number; attemptNumber: number };
        if (record) attemptDuration.record(seconds(ms), { ...base, 'attempt.number': Math.min(attemptNumber, MAX_ATTEMPT_LABEL), ...outcomeAttributes(outcome) });
        return;
      }
      const attributes: Attributes = { ...base, 'strategy.name': strategy, 'event.name': e.event };
      if (spanEvents) trace.getActiveSpan()?.addEvent(`resilience.${e.event}`, attributes);
      if (!record) return;
      events.add(1, attributes);

      const state =
        e.event === 'OnCircuitClosed' ? 0
        : e.event === 'OnCircuitHalfOpened' ? 1
        : e.event === 'OnCircuitOpened' ? ((e.args as { isManual?: boolean }).isManual ? 3 : 2)
        : e.event === 'OnCircuitStateSynced' ? CIRCUIT_STATE[(e.args as { snapshot: { state: keyof typeof CIRCUIT_STATE } }).snapshot.state]
        : undefined;
      if (state === undefined) return;
      const key = `${pipeline}\0${strategy}`;
      circuits.delete(key); // re-insert: Map order is the least-recently-changed order
      circuits.set(key, { attributes: { 'pipeline.name': pipeline, 'strategy.name': strategy }, state });
      if (circuits.size > MAX_CIRCUITS) circuits.delete(circuits.keys().next().value!);
    }),
  );

  return () => stops.forEach((stop) => stop());
}
