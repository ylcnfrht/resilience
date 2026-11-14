import { useEffect, useMemo, useReducer, useRef, useState } from 'preact/hooks';
import { BulkheadRejectedError, PipelineBuilder, concurrencyLimiterStrategy } from '@nodelibraries/resilience';
import Range from './Range.tsx';
import { fmtMs, seeded, sleep } from './sim.ts';
import './demo.css';
import './BulkheadSimulator.css';

type Result = 'done' | 'full' | 'queue-timeout' | 'dropped';
interface Req {
  id: number;
  arrived: number;
  started?: number;
  ms: number;
}

const LABELS: Record<Result, string> = { done: 'completed', full: 'rejected · full', 'queue-timeout': 'rejected · queue-timeout', dropped: 'rejected · dropped' };
const HISTORY = 40;

export interface BulkheadSimulatorProps {
  permitLimit?: number;
  queueLimit?: number;
}

/** A real concurrency limiter running in the page; slots, queue and rejections are read from the calls it lets through. */
export default function BulkheadSimulator(props: BulkheadSimulatorProps) {
  const [, render] = useReducer((n: number) => n + 1, 0);
  const [permitLimit, setPermitLimit] = useState(props.permitLimit ?? 3);
  const [queueLimit, setQueueLimit] = useState(props.queueLimit ?? 3);
  const [queueTimeout, setQueueTimeout] = useState(1000);
  const [overflow, setOverflow] = useState<'reject-new' | 'drop-oldest'>('reject-new');
  const [rate, setRate] = useState(3);
  const [callMs, setCallMs] = useState(1500);
  const [auto, setAuto] = useState(() => typeof matchMedia === 'undefined' || !matchMedia('(prefers-reduced-motion: reduce)').matches);
  const random = useMemo(() => seeded(9), []);

  // A fresh limiter (and empty board) whenever its options change.
  const world = useMemo(() => {
    const pipeline = new PipelineBuilder({ name: 'docs-bulkhead-demo' })
      .add(concurrencyLimiterStrategy({ permitLimit, queueLimit, queueTimeout: queueTimeout || undefined, overflow }))
      .build();
    return { pipeline, nextId: 1, waiting: [] as Req[], slots: new Array<Req | undefined>(permitLimit).fill(undefined), history: [] as { id: number; r: Result }[], counts: { done: 0, full: 0, 'queue-timeout': 0, dropped: 0 } as Record<Result, number> };
  }, [permitLimit, queueLimit, queueTimeout, overflow]);

  const live = useRef({ callMs, rate });
  live.current = { callMs, rate };

  function send() {
    const w = world;
    const req: Req = { id: w.nextId++, arrived: Date.now(), ms: live.current.callMs * (0.7 + random() * 0.6) };
    w.waiting.push(req);
    void w.pipeline
      .executeOutcome(async () => {
        w.waiting.splice(w.waiting.indexOf(req), 1);
        req.started = Date.now();
        w.slots[w.slots.indexOf(undefined)] = req;
        try {
          await sleep(req.ms);
        } finally {
          w.slots[w.slots.indexOf(req)] = undefined;
        }
      })
      .then((outcome) => {
        let r: Result = 'done';
        if (!outcome.ok) {
          if (!(outcome.error instanceof BulkheadRejectedError)) throw outcome.error;
          r = outcome.error.reason;
          const i = w.waiting.indexOf(req);
          if (i >= 0) w.waiting.splice(i, 1);
        }
        w.counts[r]++;
        w.history = [{ id: req.id, r }, ...w.history].slice(0, HISTORY);
        render(0);
      });
  }

  // Arrivals at roughly `rate` per second, plus a steady redraw for the progress bars.
  useEffect(() => {
    let carry = 0;
    const id = setInterval(() => {
      if (auto) {
        carry += live.current.rate * 0.1 * (0.4 + random() * 1.2);
        for (; carry >= 1; carry--) send();
      }
      render(0);
    }, 100);
    return () => clearInterval(id);
  }, [auto, world]);

  const now = Date.now();
  const c = world.counts;
  const rejected = c.full + c['queue-timeout'] + c.dropped;

  return (
    <div class="rp-demo rp-bhs">
      <div class="rp-controls">
        <Range label="permitLimit" value={permitLimit} min={1} max={8} step={1} fmt={String} set={setPermitLimit} />
        <Range label="queueLimit" value={queueLimit} min={0} max={8} step={1} fmt={String} set={setQueueLimit} />
        <Range label="queueTimeout" value={queueTimeout} min={0} max={5000} step={250} fmt={(v) => (v ? fmtMs(v) : 'none')} set={setQueueTimeout} />
        <label class="rp-field">
          overflow
          <select value={overflow} onChange={(e) => setOverflow(e.currentTarget.value as 'reject-new' | 'drop-oldest')}>
            <option value="reject-new">'reject-new'</option>
            <option value="drop-oldest">'drop-oldest'</option>
          </select>
        </label>
        <Range label="Arrivals" value={rate} min={0.5} max={10} step={0.5} fmt={(v) => `${v}/s`} set={setRate} />
        <Range label="Call duration" value={callMs} min={250} max={4000} step={250} fmt={fmtMs} set={setCallMs} />
      </div>

      <div class="rp-bhs-board">
        <section>
          <h3>
            Running <span class="rp-bhs-count">{world.slots.filter(Boolean).length}/{permitLimit}</span>
          </h3>
          <ol class="rp-bhs-cells" aria-label="Permits">
            {world.slots.map((r, i) => (
              <li key={i} class="rp-bhs-cell" data-busy={r ? true : undefined}>
                {r ? (
                  <>
                    <span>#{r.id}</span>
                    <i class="rp-bhs-bar is-run" style={{ width: `${Math.min(100, ((now - r.started!) / r.ms) * 100)}%` }} />
                  </>
                ) : (
                  <span class="rp-bhs-free">free</span>
                )}
              </li>
            ))}
          </ol>
        </section>
        <section>
          <h3>
            Queue <span class="rp-bhs-count">{world.waiting.length}/{queueLimit}</span>
          </h3>
          {queueLimit === 0 ? (
            <p class="rp-bhs-none">No queue: a call that finds every permit taken is rejected at once.</p>
          ) : (
            <ol class="rp-bhs-cells" aria-label="Queue, oldest first">
              {Array.from({ length: queueLimit }, (_, i) => {
                const r = world.waiting[i];
                return (
                  <li key={i} class="rp-bhs-cell" data-busy={r ? true : undefined} data-queue>
                    {r ? (
                      <>
                        <span>#{r.id}</span>
                        {queueTimeout > 0 && <i class="rp-bhs-bar is-wait" style={{ width: `${Math.min(100, ((now - r.arrived) / queueTimeout) * 100)}%` }} />}
                      </>
                    ) : (
                      <span class="rp-bhs-free">empty</span>
                    )}
                  </li>
                );
              })}
            </ol>
          )}
        </section>
      </div>

      <div class="rp-row rp-bhs-actions">
        <label class="rp-check">
          <input type="checkbox" checked={auto} onChange={(e) => setAuto(e.currentTarget.checked)} />
          Auto traffic
        </label>
        <button type="button" class="rp-btn" onClick={() => send()}>
          Send one
        </button>
        <button type="button" class="rp-btn" onClick={() => Array.from({ length: 8 }, send)}>
          Burst of 8
        </button>
      </div>

      <div class="rp-bhs-history" aria-hidden="true">
        {world.history.map((h) => (
          <i key={h.id} class={`rp-bhs-dot is-${h.r}`} title={`#${h.id} ${LABELS[h.r]}`} />
        ))}
      </div>
      <ul class="rp-bhs-legend">
        {(Object.keys(LABELS) as Result[]).map((k) => (
          <li key={k}>
            <i class={`rp-bhs-dot is-${k}`} /> {LABELS[k]}
          </li>
        ))}
      </ul>

      <p class="rp-sr" aria-live="polite">
        {c.done} completed, {rejected} rejected: {c.full} full, {c['queue-timeout']} queue timeout, {c.dropped} dropped.
      </p>
      <div class="rp-stats rp-bhs-stats">
        {(Object.keys(LABELS) as Result[]).map((k) => (
          <div class="rp-stat" key={k}>
            <span>{LABELS[k]}</span>
            <b class={`rp-bhs-n is-${k}`}>{c[k]}</b>
          </div>
        ))}
      </div>
      <code class="rp-bhs-code">{`addConcurrencyLimiter({ permitLimit: ${permitLimit}, queueLimit: ${queueLimit}${queueTimeout ? `, queueTimeout: '${queueTimeout % 1000 ? `${queueTimeout}ms` : `${queueTimeout / 1000}s`}'` : ''}${overflow === 'reject-new' ? '' : `, overflow: '${overflow}'`} })`}</code>
    </div>
  );
}
