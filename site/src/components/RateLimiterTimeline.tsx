import { useMemo, useState } from 'preact/hooks';
import { TokenBucket, WindowCounter } from '@nodelibraries/resilience';
import Range from './Range.tsx';
import { type Mark, type Pattern, requestStream, runLimiter, worstSpan } from './limiter-sims.ts';
import './demo.css';
import './RateLimiterTimeline.css';

const WINDOW = 1000;
const DURATION = 3000;

export interface RateLimiterTimelineProps {
  permitLimit?: number;
  pattern?: Pattern;
}

/** One request stream through the library's TokenBucket and WindowCounter (fixed = 1 segment, sliding = n segments), no queue. */
export default function RateLimiterTimeline(props: RateLimiterTimelineProps) {
  const [limit, setLimit] = useState(props.permitLimit ?? 8);
  const [pattern, setPattern] = useState<Pattern>(props.pattern ?? 'edge');
  const [segments, setSegments] = useState(5);
  const [seed, setSeed] = useState(1);

  const lanes = useMemo(() => {
    const times = requestStream(pattern, limit, WINDOW, DURATION, seed);
    const period = Math.round(WINDOW / limit);
    const lane = (name: string, code: string, marks: Mark[], grid: number[]) => ({ name, code, marks, grid, worst: worstSpan(marks, WINDOW) });
    return [
      lane('Token bucket', `addRateLimiter({ tokenLimit: ${limit}, replenishmentPeriod: '${period}ms', tokensPerPeriod: 1 })`, runLimiter(new TokenBucket(limit, period, 1, 0), times), []),
      lane('Fixed window', `addFixedWindowRateLimiter({ permitLimit: ${limit}, window: '1s' })`, runLimiter(new WindowCounter(limit, WINDOW, 1, 0), times), [1000, 2000]),
      lane(
        'Sliding window',
        `addSlidingWindowRateLimiter({ permitLimit: ${limit}, window: '1s', segmentsPerWindow: ${segments} })`,
        runLimiter(new WindowCounter(limit, WINDOW, segments, 0), times),
        Array.from({ length: 3 * segments - 1 }, (_, i) => ((i + 1) * WINDOW) / segments),
      ),
    ];
  }, [limit, pattern, segments, seed]);

  return (
    <div class="rp-demo rp-rlt">
      <div class="rp-controls">
        <label class="rp-field">
          Request stream
          <select value={pattern} onChange={(e) => setPattern(e.currentTarget.value as Pattern)}>
            <option value="edge">burst across a window edge</option>
            <option value="steady">steady, 1.5× the limit</option>
            <option value="random">random clumps</option>
          </select>
        </label>
        <Range label="Limit per second" value={limit} min={4} max={12} step={1} fmt={String} set={setLimit} />
        <Range label="Sliding segments" value={segments} min={2} max={10} step={1} fmt={String} set={setSegments} />
      </div>

      {lanes.map((l) => {
        const over = l.worst.count > limit;
        return (
          <div class="rp-rlt-lane" key={l.name}>
            <div class="rp-rlt-head">
              <h3>{l.name}</h3>
              <span class="rp-rlt-sum" data-over={over || undefined}>
                {l.marks.filter((m) => m.ok).length}/{l.marks.length} admitted · worst 1 s: <b>{l.worst.count}</b>
              </span>
            </div>
            <svg viewBox={`0 0 ${DURATION} 40`} preserveAspectRatio="none" aria-hidden="true">
              {l.worst.count > 0 && <rect class="rp-rlt-span" data-over={over || undefined} x={l.worst.start} width={WINDOW} y={0} height={40} />}
              {l.grid.map((x) => (
                <line key={x} class={x % WINDOW === 0 ? 'rp-rlt-edge' : 'rp-rlt-seg'} x1={x} x2={x} y1={0} y2={40} />
              ))}
              <line class="rp-rlt-base" x1={0} x2={DURATION} y1={24} y2={24} />
              {l.marks.map((m, i) => (
                <line key={i} class={m.ok ? 'rp-rlt-ok' : 'rp-rlt-no'} x1={m.t} x2={m.t} y1={m.ok ? 4 : 27} y2={m.ok ? 22 : 37} />
              ))}
            </svg>
            <code class="rp-rlt-code">{l.code}</code>
          </div>
        );
      })}

      <div class="rp-rlt-axis" aria-hidden="true">
        {[0, 1, 2, 3].map((s) => (
          <span key={s} style={{ left: `${(s * 1000 * 100) / DURATION}%` }}>
            {s} s
          </span>
        ))}
      </div>

      <div class="rp-row rp-rlt-foot">
        <ul class="rp-rlt-legend">
          <li>
            <i class="rp-rlt-key is-ok" /> admitted
          </li>
          <li>
            <i class="rp-rlt-key is-no" /> rejected
          </li>
          <li>
            <i class="rp-rlt-key is-span" /> busiest 1 s span
          </li>
          <li>
            <i class="rp-rlt-key is-edge" /> window boundary
          </li>
        </ul>
        {pattern === 'random' && (
          <button type="button" class="rp-btn" onClick={() => setSeed((s) => s + 1)}>
            New stream
          </button>
        )}
      </div>
      <p class="rp-rlt-note">
        Each request asks for one permit and nothing waits (no queue). The decisions come from the library's <code>TokenBucket</code> and <code>WindowCounter</code>. A red
        span means more than {limit} requests got through inside one second.
      </p>

      <table class="rp-sr">
        <caption>Requests admitted per algorithm</caption>
        <thead>
          <tr>
            <th scope="col">Algorithm</th>
            <th scope="col">Admitted</th>
            <th scope="col">Rejected</th>
            <th scope="col">Most admitted in any 1 s</th>
          </tr>
        </thead>
        <tbody>
          {lanes.map((l) => (
            <tr key={l.name}>
              <td>{l.name}</td>
              <td>{l.marks.filter((m) => m.ok).length}</td>
              <td>{l.marks.filter((m) => !m.ok).length}</td>
              <td>{l.worst.count}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
