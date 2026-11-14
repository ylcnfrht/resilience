import { useMemo, useState } from 'preact/hooks';
import { type BackoffType, type Jitter, computeDelay } from 'resilience-pipeline';
import { fmtMs, seeded } from './sim.ts';
import Range from './Range.tsx';
import './demo.css';
import './BackoffChart.css';

const RUNS = 16;
const JITTERS: Record<string, Jitter> = { none: false, true: true, full: 'full', equal: 'equal' };

export interface BackoffChartProps {
  type?: BackoffType;
  delay?: number;
  factor?: number;
  /** 0 = no cap */
  maxDelay?: number;
  jitter?: 'none' | 'true' | 'full' | 'equal';
  attempts?: number;
}

/** Retry delays per attempt, computed by the library's own `computeDelay`, with jittered runs as dots. */
export default function BackoffChart(props: BackoffChartProps) {
  const [type, setType] = useState<BackoffType>(props.type ?? 'exponential');
  const [base, setBase] = useState(props.delay ?? 200);
  const [factor, setFactor] = useState(props.factor ?? 2);
  const [maxDelay, setMaxDelay] = useState(props.maxDelay ?? 0);
  const [jitter, setJitter] = useState<string>(props.jitter ?? 'none');
  const [attempts, setAttempts] = useState(props.attempts ?? 6);
  const [seed, setSeed] = useState(1);

  const runs = useMemo(() => {
    const j = JITTERS[jitter]!;
    const n = j === false ? 1 : RUNS;
    return Array.from({ length: n }, (_, r) => {
      const state = { prev: 0 };
      const random = seeded(seed * 7919 + r);
      return Array.from({ length: attempts }, (_, i) => Math.min(computeDelay(type, j, i, base, maxDelay || undefined, state, random, factor), 3_600_000));
    });
  }, [type, base, factor, maxDelay, jitter, attempts, seed]);

  const means = Array.from({ length: attempts }, (_, i) => runs.reduce((s, r) => s + r[i]!, 0) / runs.length);
  const totals = runs.map((r) => r.reduce((a, b) => a + b, 0));
  const meanTotal = totals.reduce((a, b) => a + b, 0) / totals.length;
  const yMax = niceMax(Math.max(1, ...runs.flat()));

  const W = 640, H = 240, L = 56, R = 8, T = 10, B = 26;
  const pw = W - L - R, ph = H - T - B;
  const slot = pw / attempts;
  const y = (v: number) => T + ph - (v / yMax) * ph;
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * yMax);

  const code = `retryStrategy({ maxRetryAttempts: ${attempts}, backoffType: '${type}', delay: ${base}${type === 'exponential' && factor !== 2 ? `, factor: ${factor}` : ''}${maxDelay ? `, maxDelay: ${maxDelay}` : ''}${jitter === 'none' ? '' : `, useJitter: ${jitter === 'true' ? 'true' : `'${jitter}'`}`} })`;

  return (
    <div class="rp-demo rp-backoff">
      <div class="rp-controls">
        <label class="rp-field">
          Backoff
          <select value={type} onChange={(e) => setType(e.currentTarget.value as BackoffType)}>
            <option value="constant">constant</option>
            <option value="linear">linear</option>
            <option value="exponential">exponential</option>
          </select>
        </label>
        <label class="rp-field">
          Jitter (useJitter)
          <select value={jitter} onChange={(e) => setJitter(e.currentTarget.value)}>
            <option value="none">none (false)</option>
            <option value="true">true (Polly)</option>
            <option value="full">'full'</option>
            <option value="equal">'equal'</option>
          </select>
        </label>
        <Range label="Base delay" value={base} min={50} max={5000} step={50} fmt={fmtMs} set={setBase} />
        <Range label="Factor" value={factor} min={1} max={4} step={0.1} fmt={(v) => `×${v}`} set={setFactor} disabled={type !== 'exponential'} />
        <Range label="maxDelay" value={maxDelay} min={0} max={30000} step={500} fmt={(v) => (v ? fmtMs(v) : 'off')} set={setMaxDelay} />
        <Range label="Retries" value={attempts} min={1} max={10} step={1} fmt={String} set={setAttempts} />
      </div>

      <figure class="rp-backoff-fig">
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`Bar chart of ${attempts} retry delays, from ${fmtMs(means[0]!)} to ${fmtMs(means[attempts - 1]!)}. Total wait about ${fmtMs(meanTotal)}.`}>
          {ticks.map((v) => (
            <g key={v}>
              <line class="rp-grid" x1={L} x2={W - R} y1={y(v)} y2={y(v)} />
              <text class="rp-axis" x={L - 6} y={y(v) + 4} text-anchor="end">
                {fmtMs(v)}
              </text>
            </g>
          ))}
          {means.map((m, i) => {
            const x = L + i * slot;
            return (
              <g key={i}>
                <rect class="rp-bar" x={x + slot * 0.2} width={slot * 0.6} y={y(m)} height={Math.max(0, T + ph - y(m))} rx={3} />
                <text class="rp-axis" x={x + slot / 2} y={H - 8} text-anchor="middle">
                  {i + 1}
                </text>
              </g>
            );
          })}
          {runs.length > 1 &&
            runs.map((r, ri) =>
              r.map((v, i) => <circle key={`${ri}-${i}`} class="rp-dot" cx={L + i * slot + slot * (0.28 + (0.44 * ri) / (runs.length - 1))} cy={y(v)} r={3} />),
            )}
        </svg>
        <figcaption>
          Bars: {runs.length > 1 ? `mean of ${RUNS} runs` : 'delay'} before retry 1…{attempts}
          {runs.length > 1 && '; dots: individual jittered runs'}.
        </figcaption>
      </figure>

      <div class="rp-row rp-backoff-foot">
        <p aria-live="polite" class="rp-backoff-total">
          Total wait: <b>{fmtMs(meanTotal)}</b>
          {runs.length > 1 && (
            <>
              {' '}
              (range {fmtMs(Math.min(...totals))} – {fmtMs(Math.max(...totals))})
            </>
          )}
        </p>
        {runs.length > 1 && (
          <button type="button" class="rp-btn" onClick={() => setSeed((s) => s + 1)}>
            Resample jitter
          </button>
        )}
      </div>
      <code class="rp-backoff-code">{code}</code>

      <table class="rp-sr">
        <caption>Delay before each retry</caption>
        <thead>
          <tr>
            <th scope="col">Retry</th>
            <th scope="col">Delay</th>
          </tr>
        </thead>
        <tbody>
          {means.map((m, i) => (
            <tr key={i}>
              <td>{i + 1}</td>
              <td>{fmtMs(m)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function niceMax(v: number): number {
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * p).find((n) => n >= v)!;
}
