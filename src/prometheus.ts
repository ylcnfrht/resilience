import { Counter, Gauge, Histogram, type Registry, register as defaultRegistry } from 'prom-client';
import { type Outcome, type TelemetryEvent, onTelemetry } from './core.ts';

export interface PrometheusOptions {
  /** Default: prom-client's global registry. */
  registry?: Registry;
  /** Metric name prefix. Default `resilience_`. */
  prefix?: string;
  /** Histogram buckets in seconds. */
  buckets?: number[];
  /** Record the duration histograms (pipeline and attempt). Default true. */
  durations?: boolean;
  /** Names of extra labels returned by `labels` (prom-client needs them up front). */
  labelNames?: string[];
  /** Extra label values for the event counter and duration histograms, e.g. `({ context }) => ({ tenant: String(context.properties.get('tenant')) })`. */
  labels?: (event: TelemetryEvent) => Record<string, string | number>;
}

const CIRCUIT_STATE: Record<string, number> = { closed: 0, 'half-open': 1, open: 2, isolated: 3 };
/** Attempts from this number on share one `attempt` label value, so huge `maxRetryAttempts` don't create unbounded series. */
const MAX_ATTEMPT_LABEL = 10;
// shortcut: keeps the 1000 most recently changed circuits; pipelines disposed without a telemetry event (e.g. fetch's evicted per-host ones) age out instead of being removed.
const MAX_CIRCUITS = 1000;

/**
 * Exposes pipeline telemetry as Prometheus metrics (via prom-client):
 * - `<prefix>strategy_events_total{pipeline,strategy,event}`
 * - `<prefix>pipeline_duration_seconds{pipeline,outcome}`
 * - `<prefix>attempt_duration_seconds{pipeline,attempt,outcome}`: every call of the callback (retries and hedged attempts included);
 *   `attempt` is capped at 10 (10 means the 10th retry or later)
 * - `<prefix>circuit_state{pipeline,strategy}`: 0 closed, 1 half-open, 2 open, 3 isolated (the 1000 most recently changed circuits)
 * Call once at startup; returns a function that stops it (metrics stay registered).
 */
export function instrumentPrometheus(options: PrometheusOptions = {}): () => void {
  const registry = options.registry ?? defaultRegistry;
  const prefix = options.prefix ?? 'resilience_';
  const extra = options.labelNames ?? [];
  const labelsOf = options.labels ?? (() => ({}));
  const buckets = options.buckets ?? [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];
  const durations = options.durations ?? true;
  const events = new Counter({ name: `${prefix}strategy_events_total`, help: 'Resilience strategy events (retry, timeout, circuit state changes, rejections).', labelNames: ['pipeline', 'strategy', 'event', ...extra], registers: [registry] });
  const circuit = new Gauge({ name: `${prefix}circuit_state`, help: 'Circuit breaker state: 0 closed, 1 half-open, 2 open, 3 isolated.', labelNames: ['pipeline', 'strategy'], registers: [registry] });
  const duration = durations
    ? new Histogram({ name: `${prefix}pipeline_duration_seconds`, help: 'Duration of resilience pipeline executions, retries included.', labelNames: ['pipeline', 'outcome', ...extra], buckets, registers: [registry] })
    : undefined;
  const attemptDuration = durations
    ? new Histogram({ name: `${prefix}attempt_duration_seconds`, help: 'Duration of every attempt (each call of the callback).', labelNames: ['pipeline', 'attempt', 'outcome', ...extra], buckets, registers: [registry] })
    : undefined;
  const circuits = new Map<string, { pipeline: string; strategy: string }>(); // Map order: least recently changed first

  return onTelemetry((e: TelemetryEvent) => {
    const pipeline = e.source.pipelineName ?? '';
    const strategy = e.source.strategyName ?? '';
    if (e.event === 'OnPipelineExecuted') {
      const { outcome, duration: ms } = e.args as { outcome: Outcome<unknown>; duration: number };
      duration?.observe({ ...labelsOf(e), pipeline, outcome: outcome.ok ? 'success' : 'failure' }, ms / 1000);
      return;
    }
    if (e.event === 'OnPipelineExecuting' || e.event === 'OnStrategyExecuted') return; // not a strategy event
    if (e.event === 'OnExecutionAttempt') {
      const { outcome, duration: ms, attemptNumber } = e.args as { outcome: Outcome<unknown>; duration: number; attemptNumber: number };
      attemptDuration?.observe({ ...labelsOf(e), pipeline, attempt: Math.min(attemptNumber, MAX_ATTEMPT_LABEL), outcome: outcome.ok ? 'success' : 'failure' }, ms / 1000);
      return;
    }
    events.inc({ ...labelsOf(e), pipeline, strategy, event: e.event });
    const state =
      e.event === 'OnCircuitClosed' ? 0
      : e.event === 'OnCircuitHalfOpened' ? 1
      : e.event === 'OnCircuitOpened' ? ((e.args as { isManual?: boolean }).isManual ? 3 : 2)
      : e.event === 'OnCircuitStateSynced' ? CIRCUIT_STATE[(e.args as { snapshot: { state: string } }).snapshot.state]
      : undefined;
    if (state === undefined) return;
    const key = `${pipeline}\0${strategy}`;
    circuits.delete(key);
    circuits.set(key, { pipeline, strategy });
    circuit.set({ pipeline, strategy }, state);
    if (circuits.size > MAX_CIRCUITS) {
      const [oldest, labels] = circuits.entries().next().value!;
      circuits.delete(oldest);
      circuit.remove(labels);
    }
  });
}
