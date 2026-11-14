import { useEffect, useMemo, useReducer, useRef, useState } from 'preact/hooks';
import { CircuitBreakerStateProvider, PipelineBuilder, circuitBreakerStrategy, isBrokenCircuitError } from '@nodelibraries/resilience';
import Range from './Range.tsx';
import { seeded, sleep } from './sim.ts';
import './demo.css';
import './BreakerStateMachine.css';

export interface BreakerStateMachineProps {
  /** How long the circuit stays open, ms. Default 3000. */
  breakDuration?: number;
  /** Failure ratio that opens the circuit. Default 0.5. */
  failureRatio?: number;
  /** Count-based window size. Default 10. */
  samplingCount?: number;
  /** Calls needed in the window before the ratio counts. Default 4. */
  minimumThroughput?: number;
}

const CALL_MS = 300; // each simulated call takes this long, so the half-open probe is visible

/** A real circuit breaker running in the page; the diagram reflects its live state. */
export default function BreakerStateMachine({ breakDuration = 3000, failureRatio = 0.5, samplingCount = 10, minimumThroughput = 4 }: BreakerStateMachineProps) {
  const [, render] = useReducer((n: number) => n + 1, 0);
  const [failPct, setFailPct] = useState(60);
  const [auto, setAuto] = useState(false);
  const [log, setLog] = useState<string[]>([]);
  const counts = useRef({ ok: 0, failed: 0, rejected: 0 });
  const random = useMemo(() => seeded(42), []);

  const [{ pipeline, provider }, setBreaker] = useState(() => build());
  function build() {
    const provider = new CircuitBreakerStateProvider();
    const note = (s: string) => setLog((l) => [`${new Date().toLocaleTimeString()} ${s}`, ...l].slice(0, 5));
    const pipeline = new PipelineBuilder({ name: 'docs-breaker-demo' })
      .add(
        circuitBreakerStrategy({
          failureRatio,
          samplingCount,
          minimumThroughput,
          breakDuration,
          stateProvider: provider,
          onOpened: () => note(`opened for ${breakDuration / 1000} s`),
          onHalfOpened: () => note('half-open: letting one probe through'),
          onClosed: () => note('closed: probe succeeded'),
        }),
      )
      .build();
    return { pipeline, provider };
  }

  async function send(fail: boolean) {
    const pending = pipeline.executeOutcome(async () => {
      await sleep(CALL_MS);
      if (fail) throw new Error('503');
      return 'ok';
    });
    render(0); // show half-open as soon as the probe is admitted
    const outcome = await pending;
    const c = counts.current;
    if (outcome.ok) c.ok++;
    else if (isBrokenCircuitError(outcome.error)) c.rejected++;
    else c.failed++;
    render(0);
  }

  // Auto traffic, plus a steady refresh for the open-state countdown.
  const failRef = useRef(failPct);
  failRef.current = failPct;
  useEffect(() => {
    const id = setInterval(() => {
      if (auto) void send(random() * 100 < failRef.current);
      render(0);
    }, auto ? 350 : 200);
    return () => clearInterval(id);
  }, [auto, pipeline]);

  const state = provider.circuitState;
  const m = provider.metrics;
  const left = state === 'open' ? Math.max(0, provider.snapshot().openUntil - Date.now()) : 0;
  const c = counts.current;
  const reset = () => {
    setAuto(false);
    counts.current = { ok: 0, failed: 0, rejected: 0 };
    setLog([]);
    setBreaker(build());
  };

  return (
    <div class="rp-demo rp-bsm">
      <svg class="rp-bsm-svg" viewBox="0 0 570 250" role="img" aria-label={`Circuit state machine, current state: ${state}`}>
        <defs>
          <marker id="rp-bsm-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
            <path d="M0 0L10 5L0 10z" class="rp-bsm-head" />
          </marker>
        </defs>
        <Edge d="M177 50 L382 50" label={`failure rate ≥ ${failureRatio * 100}%`} x={280} y={40} />
        <Edge d="M500 82 Q 500 200 358 200" label={`after ${breakDuration / 1000} s`} x={506} y={150} anchor="start" />
        <Edge d="M335 168 L430 84" label="probe fails" x={360} y={130} anchor="end" />
        <Edge d="M220 168 L125 84" label="probe succeeds" x={150} y={140} anchor="end" />
        <Node x={100} y={50} id="closed" label="Closed" sub="calls flow" current={state} />
        <Node x={460} y={50} id="open" label="Open" sub={state === 'open' ? `${(left / 1000).toFixed(1)} s left` : 'calls rejected'} current={state} />
        <Node x={280} y={200} id="half-open" label="Half-open" sub="one probe" current={state} />
      </svg>

      <div class="rp-row rp-bsm-actions">
        <button type="button" class="rp-btn" onClick={() => void send(false)}>
          Send success
        </button>
        <button type="button" class="rp-btn" onClick={() => void send(true)}>
          Send failure
        </button>
        <label class="rp-check">
          <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.currentTarget.checked)} />
          Auto traffic
        </label>
        <button type="button" class="rp-btn" onClick={reset}>
          Reset
        </button>
      </div>
      <div class="rp-controls rp-bsm-slider">
        <Range label="Auto traffic failure rate" value={failPct} min={0} max={100} step={5} fmt={(v) => `${v}%`} set={setFailPct} />
      </div>

      <p class="rp-sr" aria-live="polite">
        Circuit {state}. {c.ok} succeeded, {c.failed} failed, {c.rejected} rejected.
      </p>
      <div class="rp-stats">
        <div class="rp-stat">
          <span>State</span>
          <b class={`rp-chip rp-state-${state}`}>{state}</b>
        </div>
        <div class="rp-stat">
          <span>Window failure rate</span>
          <b>{Math.round(m.failureRate * 100)}%</b>
        </div>
        <div class="rp-stat">
          <span>Calls in window</span>
          <b>
            {m.total}/{samplingCount}
          </b>
        </div>
        <div class="rp-stat">
          <span>Half-open in</span>
          <b>{state === 'open' ? `${(left / 1000).toFixed(1)} s` : '–'}</b>
        </div>
        <div class="rp-stat">
          <span>Succeeded / failed</span>
          <b>
            {c.ok} / {c.failed}
          </b>
        </div>
        <div class="rp-stat">
          <span>Rejected (open)</span>
          <b>{c.rejected}</b>
        </div>
      </div>
      <p class="rp-bsm-rule">
        Opens when at least {failureRatio * 100}% of the last {samplingCount} calls failed (and at least {minimumThroughput} calls were recorded). While open, calls fail fast with{' '}
        <code>BrokenCircuitError</code>; after {breakDuration / 1000} s the next call is let through as a probe.
      </p>
      {log.length > 0 && (
        <ol class="rp-bsm-log" aria-label="Transitions">
          {log.map((l, i) => (
            <li key={log.length - i}>{l}</li>
          ))}
        </ol>
      )}
    </div>
  );
}

function Node({ x, y, id, label, sub, current }: { x: number; y: number; id: string; label: string; sub: string; current: string }) {
  const active = current === id;
  return (
    <g class={`rp-bsm-node rp-state-${id}`} data-active={active || undefined} transform={`translate(${x} ${y})`}>
      <rect x={-75} y={-30} width={150} height={60} rx={14} />
      <text y={-3} text-anchor="middle" class="rp-bsm-label">
        {label}
      </text>
      <text y={16} text-anchor="middle" class="rp-bsm-sub">
        {sub}
      </text>
    </g>
  );
}

function Edge({ d, label, x, y, anchor = 'middle' }: { d: string; label: string; x: number; y: number; anchor?: string }) {
  return (
    <g class="rp-bsm-edge">
      <path d={d} marker-end="url(#rp-bsm-arrow)" />
      <text x={x} y={y} text-anchor={anchor}>
        {label}
      </text>
    </g>
  );
}
