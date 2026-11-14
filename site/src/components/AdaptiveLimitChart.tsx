import { useMemo, useState } from 'preact/hooks';
import { aimd, gradient2, vegas } from '@nodelibraries/resilience';
import { type AdaptivePoint, runAdaptive } from './limiter-sims.ts';
import './demo.css';
import './AdaptiveLimitChart.css';

const ALGORITHMS = { gradient2, vegas, aimd };
type Algorithm = keyof typeof ALGORITHMS;

const DURATION = 30_000;
const SLOW: [number, number] = [10_000, 20_000];
const MAX_LIMIT = 100;
const DEP = { rate: 1500, base: 40, capacity: 30, slowCapacity: 10, timeout: 120 };

export interface AdaptiveLimitChartProps {
  algorithm?: Algorithm;
}

/** The library's limit algorithms driven by a simulated dependency whose capacity drops for 10 s. */
export default function AdaptiveLimitChart(props: AdaptiveLimitChartProps) {
  const [algorithm, setAlgorithm] = useState<Algorithm>(props.algorithm ?? 'gradient2');
  const [slow, setSlow] = useState(true);

  const run = useMemo(
    () => runAdaptive(ALGORITHMS[algorithm](), { ...DEP, slow: slow ? SLOW : undefined }, { duration: DURATION, initialLimit: 20, minLimit: 1, maxLimit: MAX_LIMIT }),
    [algorithm, slow],
  );
  const avg = (from: number, to: number) => {
    const ps = run.points.filter((p) => p.t >= from && p.t < to);
    return Math.round(ps.reduce((s, p) => s + p.limit, 0) / ps.length);
  };
  const before = avg(5000, 10_000);
  const during = avg(15_000, 20_000);
  const after = avg(25_000, 30_000);

  const W = 640, L = 40, R = 10;
  const T1 = 10, H1 = 170; // limit chart
  const T2 = T1 + H1 + 26, H2 = 70; // latency chart
  const H = T2 + H2 + 24;
  const pw = W - L - R;
  const x = (t: number) => L + (t / DURATION) * pw;
  const y1 = (v: number) => T1 + H1 - (v / MAX_LIMIT) * H1;
  const LAT_MAX = 150;
  const y2 = (v: number) => T2 + H2 - (Math.min(v, LAT_MAX) / LAT_MAX) * H2;
  const step = (get: (p: AdaptivePoint) => number, y: (v: number) => number) =>
    run.points.map((p, i) => `${i ? `H${x(p.t).toFixed(1)}V` : `M${x(p.t).toFixed(1)} `}${y(get(p)).toFixed(1)}`).join('');
  const limitPath = step((p) => p.limit, y1);
  const inflightArea = `${step((p) => p.inflight, y1)}V${y1(0)}H${x(0)}Z`;
  const capPath = step((p) => p.capacity, y1);
  const latPath = run.points.map((p, i) => `${i ? 'L' : 'M'}${x(p.t).toFixed(1)} ${y2(p.latency).toFixed(1)}`).join('');

  return (
    <div class="rp-demo rp-alc">
      <div class="rp-controls">
        <label class="rp-field">
          algorithm
          <select value={algorithm} onChange={(e) => setAlgorithm(e.currentTarget.value as Algorithm)}>
            <option value="gradient2">'gradient2' (default)</option>
            <option value="vegas">'vegas'</option>
            <option value="aimd">'aimd'</option>
          </select>
        </label>
        <label class="rp-check rp-alc-toggle">
          <input type="checkbox" checked={slow} onChange={(e) => setSlow(e.currentTarget.checked)} />
          Slow down the dependency (10–20 s)
        </label>
      </div>

      <figure class="rp-alc-fig">
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Concurrency limit over 30 seconds with ${algorithm}: about ${before} before the slowdown, ${during} during it, ${after} after.`}>
          {slow && <rect class="rp-alc-slow" x={x(SLOW[0])} width={x(SLOW[1]) - x(SLOW[0])} y={T1} height={T2 + H2 - T1} />}
          {[0, 25, 50, 75, 100].map((v) => (
            <g key={v}>
              <line class="rp-alc-grid" x1={L} x2={W - R} y1={y1(v)} y2={y1(v)} />
              <text class="rp-alc-axis" x={L - 6} y={y1(v) + 4} text-anchor="end">
                {v}
              </text>
            </g>
          ))}
          <path class="rp-alc-inflight" d={inflightArea} />
          <path class="rp-alc-cap" d={capPath} />
          <path class="rp-alc-limit" d={limitPath} />
          {run.drops.map((t, i) => (
            <line key={i} class="rp-alc-drop" x1={x(t)} x2={x(t)} y1={y1(0) - 7} y2={y1(0)} />
          ))}
          {slow && (
            <text class="rp-alc-axis rp-alc-slow-label" x={(x(SLOW[0]) + x(SLOW[1])) / 2} y={T1 + 14} text-anchor="middle">
              dependency slowed
            </text>
          )}

          <text class="rp-alc-axis" x={L} y={T2 - 6}>
            latency (ms)
          </text>
          {[0, 60, 120].map((v) => (
            <g key={v}>
              <line class={v === DEP.timeout ? 'rp-alc-timeout' : 'rp-alc-grid'} x1={L} x2={W - R} y1={y2(v)} y2={y2(v)} />
              <text class="rp-alc-axis" x={L - 6} y={y2(v) + 4} text-anchor="end">
                {v}
              </text>
            </g>
          ))}
          <text class="rp-alc-axis rp-alc-timeout-label" x={W - R} y={y2(DEP.timeout) - 4} text-anchor="end">
            timeout
          </text>
          <path class="rp-alc-lat" d={latPath} />
          {[0, 10, 20, 30].map((s) => (
            <text key={s} class="rp-alc-axis" x={x(s * 1000)} y={H - 6} text-anchor={s === 0 ? 'start' : s === 30 ? 'end' : 'middle'}>
              {s} s
            </text>
          ))}
        </svg>
        <figcaption>
          <ul class="rp-alc-legend">
            <li>
              <i class="rp-alc-key is-limit" /> limit
            </li>
            <li>
              <i class="rp-alc-key is-inflight" /> in flight
            </li>
            <li>
              <i class="rp-alc-key is-cap" /> dependency capacity
            </li>
            <li>
              <i class="rp-alc-key is-lat" /> latency
            </li>
            <li>
              <i class="rp-alc-key is-drop" /> drop (timeout)
            </li>
          </ul>
        </figcaption>
      </figure>

      <p class="rp-sr" aria-live="polite">
        {algorithm}: limit {before} before, {during} during and {after} after the slowdown; {run.drops.length} drops.
      </p>
      <div class="rp-stats rp-alc-stats">
        <div class="rp-stat">
          <span>Limit, 5–10 s</span>
          <b>{before}</b>
        </div>
        <div class="rp-stat">
          <span>Limit, 15–20 s</span>
          <b>{during}</b>
        </div>
        <div class="rp-stat">
          <span>Limit, 25–30 s</span>
          <b>{after}</b>
        </div>
        <div class="rp-stat">
          <span>Drops counted</span>
          <b>{run.drops.length}</b>
        </div>
      </div>
      <p class="rp-alc-note">
        Simulated dependency: {DEP.base} ms per call while up to {DEP.capacity} run at once ({DEP.slowCapacity} while slowed); above that calls queue inside it and latency grows. Offered load
        is {DEP.rate} req/s, so the limit, not the traffic, decides how many run. Each new limit comes from the library's <code>{algorithm}()</code>, with the strategy's
        clamping, app-limited and one-cut-per-round-trip rules.
      </p>
      <code class="rp-alc-code">{`addAdaptiveConcurrencyLimiter({ algorithm: '${algorithm}', initialLimit: 20, maxLimit: ${MAX_LIMIT} }).addTimeout('${DEP.timeout}ms')`}</code>
    </div>
  );
}
