import type { ComponentChildren } from 'preact';
import { useEffect, useMemo, useReducer, useRef, useState } from 'preact/hooks';
import {
  type BackoffType,
  type Outcome,
  type Stats,
  type StrategyDefinition,
  CircuitBreakerStateProvider,
  PipelineBuilder,
  cacheStrategy,
  circuitBreakerStrategy,
  concurrencyLimiterStrategy,
  createStats,
  fallbackStrategy,
  hedgingStrategy,
  isRejection,
  isTimeoutError,
  retryStrategy,
  timeoutStrategy,
} from '@nodelibraries/resilience';
import Range from './Range.tsx';
import { type ServiceConfig, flakyService, fmtMs, seeded } from './sim.ts';
import './demo.css';
import './Playground.css';

type Status = 'pending' | 'success' | 'recovered' | 'cache' | 'fallback' | 'timeout' | 'rejected' | 'failed';
interface Req {
  id: number;
  status: Status;
  attempts: number;
  ms: number;
  /** Why the fallback was used. */
  cause?: Status;
}

interface Config {
  retry: boolean;
  retries: number;
  backoff: BackoffType;
  retryDelay: number;
  timeout: boolean;
  timeoutMs: number;
  breaker: boolean;
  breakerMode: 'consecutive' | 'ratio';
  consecutive: number;
  ratio: number;
  breakMs: number;
  hedging: boolean;
  hedgeDelay: number;
  bulkhead: boolean;
  permits: number;
  fallback: boolean;
  cache: boolean;
  ttl: number;
}

const DEFAULTS: Config = {
  retry: true,
  retries: 3,
  backoff: 'exponential',
  retryDelay: 100,
  timeout: true,
  timeoutMs: 500,
  breaker: true,
  breakerMode: 'ratio',
  consecutive: 5,
  ratio: 50,
  breakMs: 5000,
  hedging: false,
  hedgeDelay: 300,
  bulkhead: false,
  permits: 10,
  fallback: true,
  cache: false,
  ttl: 1000,
};

const MAX_ROWS = 240;
const FALLBACK = 'cached user #42';
const LABELS: Record<Exclude<Status, 'pending'>, string> = {
  success: 'success',
  recovered: 'recovered (retry / hedge)',
  cache: 'cache hit',
  fallback: 'fallback',
  timeout: 'timeout',
  rejected: 'rejected (circuit / bulkhead)',
  failed: 'failed',
};
// Approximate min+gzip cost of each strategy on top of the builder core (2.4 KB), measured with esbuild from the source.
const SIZE = { base: 2.44, retry: 1.6, timeout: 0.75, breaker: 3.0, hedging: 1.05, bulkhead: 1.32, fallback: 0.46, cache: 1.3 };

/** Strategy definitions and the code that creates them, outermost first. */
function definitions(c: Config, provider: CircuitBreakerStateProvider, random: () => number) {
  const defs: Array<[StrategyDefinition<string, any>, string, keyof typeof SIZE]> = [];
  const breakerOpts = c.breakerMode === 'consecutive' ? { consecutiveFailures: c.consecutive } : { failureRatio: c.ratio / 100, minimumThroughput: 10, samplingDuration: '10s' as const };
  const breakerCode = c.breakerMode === 'consecutive' ? `consecutiveFailures: ${c.consecutive}` : `failureRatio: ${c.ratio / 100}, minimumThroughput: 10, samplingDuration: '10s'`;
  if (c.fallback)
    defs.push([
      fallbackStrategy<string>({
        fallbackAction: ({ outcome, context }) => {
          context.properties.set('cause', classify(outcome, 1));
          return FALLBACK;
        },
      }),
      `fallbackStrategy({ fallbackValue: cachedUser })`,
      'fallback',
    ]);
  if (c.cache) defs.push([cacheStrategy<string>({ ttl: c.ttl, key: () => 'user:42' }), `cacheStrategy({ ttl: ${c.ttl}, key: () => 'user:42' })`, 'cache']);
  if (c.bulkhead) defs.push([concurrencyLimiterStrategy<string>(c.permits), `concurrencyLimiterStrategy(${c.permits})`, 'bulkhead']);
  if (c.retry)
    defs.push([
      retryStrategy<string>({ maxRetryAttempts: c.retries, backoffType: c.backoff, delay: c.retryDelay, useJitter: true, randomizer: random }),
      `retryStrategy({ maxRetryAttempts: ${c.retries}, backoffType: '${c.backoff}', delay: ${c.retryDelay}, useJitter: true })`,
      'retry',
    ]);
  if (c.breaker)
    defs.push([circuitBreakerStrategy<string>({ ...breakerOpts, breakDuration: c.breakMs, stateProvider: provider }), `circuitBreakerStrategy({ ${breakerCode}, breakDuration: ${c.breakMs} })`, 'breaker']);
  if (c.hedging) defs.push([hedgingStrategy<string>({ delay: c.hedgeDelay }), `hedgingStrategy({ delay: ${c.hedgeDelay} })`, 'hedging']);
  if (c.timeout) defs.push([timeoutStrategy<string>(c.timeoutMs), `timeoutStrategy(${c.timeoutMs})`, 'timeout']);
  return defs;
}

function classify(outcome: Outcome<string>, attempts: number): Status {
  if (outcome.ok) return outcome.result === FALLBACK ? 'fallback' : attempts === 0 ? 'cache' : attempts > 1 ? 'recovered' : 'success';
  return isTimeoutError(outcome.error) ? 'timeout' : isRejection(outcome.error) ? 'rejected' : 'failed';
}

function generateCode(snippets: string[]) {
  const fns = [...new Set(snippets.map((s) => s.slice(0, s.indexOf('('))))];
  const imports = ['PipelineBuilder', ...fns].join(', ');
  return [
    `import { ${imports} } from '@nodelibraries/resilience';`,
    '',
    '// Strategies run in the order they are added: the first is the outermost.',
    'const pipeline = new PipelineBuilder<User>()',
    ...snippets.map((s) => `  .add(${s})`),
    '  .build();',
    '',
    'const user = await pipeline.execute(({ signal }) => fetchUser(42, { signal }));',
  ].join('\n');
}

export default function Playground() {
  const [, render] = useReducer((n: number) => n + 1, 0);
  const [cfg, setCfg] = useState(DEFAULTS);
  const [sim, setSim] = useState<ServiceConfig>({ failureRate: 20, latency: 120, jitter: 200, outage: false, slowdown: false });
  const [rps, setRps] = useState(10);
  const [seed, setSeed] = useState(42);
  const [running, setRunning] = useState(false);
  const [epoch, setEpoch] = useState(0); // bumped by Reset: fresh pipeline, stats and random stream
  const [copied, setCopied] = useState(false);

  const name = useMemo(() => `playground-${Math.random().toString(36).slice(2, 8)}`, []);
  const simRef = useRef(sim);
  simRef.current = sim;
  const reqs = useRef<Req[]>([]);
  const seq = useRef(0);
  const inflight = useRef(0);
  const stats = useRef<Stats | undefined>(undefined);
  const announce = useRef({ t: 0, text: '' });

  const rng = useMemo(() => seeded(seed), [seed, epoch]);
  const service = useMemo(() => flakyService(() => simRef.current, rng), [rng]);
  const provider = useMemo(() => new CircuitBreakerStateProvider(), [cfg, epoch]);
  const defs = useMemo(() => definitions(cfg, provider, seeded(seed + 1)), [provider]);
  const pipeline = useMemo(() => {
    const b = new PipelineBuilder<string, any, any>({ name });
    for (const [d] of defs) b.add(d);
    return b.build();
  }, [defs]);

  // Stats only in the browser (it subscribes to telemetry); recreated on Reset.
  useEffect(() => {
    stats.current = createStats({ pipeline: name, window: '10s' });
    return () => stats.current?.dispose();
  }, [epoch]);

  // rAF-batched re-render, at most ~12 times a second.
  const frame = useRef({ queued: false, last: 0 });
  const dirty = () => {
    const f = frame.current;
    if (f.queued) return;
    f.queued = true;
    requestAnimationFrame(function tick(now) {
      if (now - f.last < 80) return void requestAnimationFrame(tick);
      f.queued = false;
      f.last = now;
      render(0);
    });
  };

  const fire = async () => {
    const r: Req = { id: ++seq.current, status: 'pending', attempts: 0, ms: 0 };
    const rows = reqs.current;
    rows.push(r);
    if (rows.length > MAX_ROWS) rows.splice(0, rows.length - MAX_ROWS);
    inflight.current++;
    dirty();
    const properties = new Map<string, unknown>();
    const started = performance.now();
    const outcome = await pipeline.executeOutcome(
      async ({ signal }) => {
        r.attempts++;
        return service(signal);
      },
      { properties },
    );
    r.ms = performance.now() - started;
    r.status = classify(outcome, r.attempts);
    r.cause = properties.get('cause') as Status | undefined;
    inflight.current--;
    dirty();
  };

  useEffect(() => {
    if (!running) return;
    const id = setInterval(() => void fire(), 1000 / rps);
    // Keep the breaker countdown and the rolling window moving even when no request finishes.
    const refresh = setInterval(dirty, 500);
    return () => {
      clearInterval(id);
      clearInterval(refresh);
    };
  }, [running, rps, pipeline, service]);

  const reset = () => {
    setRunning(false);
    reqs.current = [];
    seq.current = 0;
    inflight.current = 0;
    announce.current = { t: 0, text: '' };
    setEpoch((e) => e + 1);
  };

  const set = <K extends keyof Config>(k: K) => (v: Config[K]) => setCfg((c) => ({ ...c, [k]: v }));
  const setS = <K extends keyof ServiceConfig>(k: K) => (v: ServiceConfig[K]) => setSim((s) => ({ ...s, [k]: v }));

  const snap = stats.current?.snapshot();
  const success = snap && snap.executions ? (snap.successes / snap.executions) * 100 : undefined;
  const state = cfg.breaker ? provider.circuitState : undefined;
  const openLeft = state === 'open' ? Math.max(0, provider.snapshot().openUntil - Date.now()) : 0;
  const code = generateCode(defs.map((d) => d[1]));
  const kb = SIZE.base + defs.reduce((s, d) => s + SIZE[d[2]], 0) * 0.75; // gzip shares the core, so marginals overlap ~25%

  const now = Date.now();
  if (snap && now - announce.current.t > 5000 && snap.executions) {
    announce.current = { t: now, text: `Last 10 seconds: ${Math.round(success!)}% success, p99 ${fmtMs(snap.latency.p99 ?? 0)}, ${snap.rejections} rejected.` };
  }
  const recent = reqs.current.filter((r) => r.status !== 'pending').slice(-6).reverse();

  return (
    <div class="rp-demo rp-pg">
      <div class="rp-row rp-pg-bar">
        <button type="button" class="rp-btn" data-primary onClick={() => setRunning((r) => !r)} aria-pressed={running}>
          {running ? 'Pause' : 'Start'}
        </button>
        <button type="button" class="rp-btn" onClick={reset}>
          Reset
        </button>
        <button type="button" class="rp-btn" onClick={() => void fire()}>
          Send one
        </button>
        <label class="rp-field rp-pg-seed">
          Seed
          <input type="number" value={seed} min={0} onChange={(e) => setSeed(+e.currentTarget.value || 0)} />
        </label>
        <div class="rp-pg-rps">
          <Range label="Requests / s" value={rps} min={1} max={50} step={1} fmt={String} set={setRps} />
        </div>
      </div>

      <div class="rp-pg-grid">
        <div class="rp-pg-side">
          <fieldset class="rp-pg-box">
            <legend>Flaky dependency</legend>
            <Range label="Failure rate" value={sim.failureRate} min={0} max={100} step={1} fmt={(v) => `${v}%`} set={setS('failureRate')} />
            <Range label="Base latency" value={sim.latency} min={0} max={1000} step={10} fmt={fmtMs} set={setS('latency')} />
            <Range label="Latency jitter" value={sim.jitter} min={0} max={2000} step={10} fmt={(v) => `+0…${fmtMs(v)}`} set={setS('jitter')} />
            <div class="rp-row">
              <label class="rp-check">
                <input type="checkbox" checked={sim.outage} onChange={(e) => setS('outage')(e.currentTarget.checked)} /> Outage
              </label>
              <label class="rp-check">
                <input type="checkbox" checked={sim.slowdown} onChange={(e) => setS('slowdown')(e.currentTarget.checked)} /> Slowdown ×5
              </label>
            </div>
          </fieldset>

          <Strategy label="Fallback" on={cfg.fallback} toggle={set('fallback')} />
          <Strategy label="Cache" on={cfg.cache} toggle={set('cache')}>
            <Range label="ttl" value={cfg.ttl} min={100} max={10000} step={100} fmt={fmtMs} set={set('ttl')} />
          </Strategy>
          <Strategy label="Bulkhead (concurrency limiter)" on={cfg.bulkhead} toggle={set('bulkhead')}>
            <Range label="Permits" value={cfg.permits} min={1} max={50} step={1} fmt={String} set={set('permits')} />
          </Strategy>
          <Strategy label="Retry" on={cfg.retry} toggle={set('retry')}>
            <Range label="maxRetryAttempts" value={cfg.retries} min={0} max={6} step={1} fmt={String} set={set('retries')} />
            <Range label="delay" value={cfg.retryDelay} min={0} max={2000} step={10} fmt={fmtMs} set={set('retryDelay')} />
            <label class="rp-field">
              backoffType
              <select value={cfg.backoff} onChange={(e) => set('backoff')(e.currentTarget.value as BackoffType)}>
                <option value="constant">constant</option>
                <option value="linear">linear</option>
                <option value="exponential">exponential</option>
              </select>
            </label>
          </Strategy>
          <Strategy label="Circuit breaker" on={cfg.breaker} toggle={set('breaker')}>
            <label class="rp-field">
              Opens on
              <select value={cfg.breakerMode} onChange={(e) => set('breakerMode')(e.currentTarget.value as Config['breakerMode'])}>
                <option value="ratio">failure ratio (10 s window)</option>
                <option value="consecutive">consecutive failures</option>
              </select>
            </label>
            {cfg.breakerMode === 'ratio' ? (
              <Range label="failureRatio" value={cfg.ratio} min={5} max={100} step={5} fmt={(v) => `${v}%`} set={set('ratio')} />
            ) : (
              <Range label="consecutiveFailures" value={cfg.consecutive} min={1} max={20} step={1} fmt={String} set={set('consecutive')} />
            )}
            <Range label="breakDuration" value={cfg.breakMs} min={1000} max={15000} step={500} fmt={fmtMs} set={set('breakMs')} />
          </Strategy>
          <Strategy label="Hedging" on={cfg.hedging} toggle={set('hedging')}>
            <Range label="delay" value={cfg.hedgeDelay} min={0} max={2000} step={10} fmt={fmtMs} set={set('hedgeDelay')} />
          </Strategy>
          <Strategy label="Timeout (per attempt)" on={cfg.timeout} toggle={set('timeout')}>
            <Range label="timeout" value={cfg.timeoutMs} min={50} max={5000} step={50} fmt={fmtMs} set={set('timeoutMs')} />
          </Strategy>
          <p class="rp-pg-note">Changing a strategy rebuilds the pipeline (breaker state starts closed again).</p>
        </div>

        <div class="rp-pg-main">
          <div class="rp-row rp-pg-status">
            <span>Circuit:</span>
            {state ? <span class={`rp-chip rp-state-${state}`}>{state}{state === 'open' && ` · ${(openLeft / 1000).toFixed(1)} s`}</span> : <span class="rp-chip">no breaker</span>}
            <span class="rp-pg-inflight">{inflight.current} in flight</span>
          </div>
          <p class="rp-sr" aria-live="polite">
            {state ? `Circuit ${state}.` : ''}
          </p>
          <p class="rp-sr" aria-live="polite">
            {announce.current.text}
          </p>

          <div class="rp-stats">
            <Stat label={cfg.fallback ? "OK incl. fallback (10 s)" : "Success (10 s)"} value={success === undefined ? '–' : `${success.toFixed(1)}%`} />
            <Stat label="p50 latency" value={snap?.executions ? fmtMs(snap.latency.p50 ?? 0) : '–'} />
            <Stat label="p99 latency" value={snap?.executions ? fmtMs(snap.latency.p99 ?? 0) : '–'} />
            <Stat label="Retries" value={snap?.retries ?? 0} />
            <Stat label="Timeouts" value={snap?.timeouts ?? 0} />
            <Stat label="Rejections" value={snap?.rejections ?? 0} />
            <Stat label="Fallbacks" value={snap?.fallbacks ?? 0} />
            <Stat label="Total calls" value={snap?.total.executions ?? 0} />
          </div>

          <h3 class="rp-pg-h">Timeline (last {MAX_ROWS} requests)</h3>
          <div class="rp-pg-timeline" aria-hidden="true">
            {reqs.current.map((r) => (
              <i key={r.id} class={`rp-pg-cell is-${r.status}`} title={`#${r.id} ${r.status}${r.cause ? ` after ${r.cause}` : ''} · ${r.attempts} attempt(s) · ${fmtMs(r.ms)}`} />
            ))}
          </div>
          <ul class="rp-pg-legend">
            {Object.entries(LABELS).map(([k, v]) => (
              <li key={k}>
                <i class={`rp-pg-cell is-${k}`} /> {v}
              </li>
            ))}
          </ul>

          <table class="rp-pg-recent">
            <caption class="rp-sr">Most recent finished requests</caption>
            <thead>
              <tr>
                <th scope="col">#</th>
                <th scope="col">Result</th>
                <th scope="col">Attempts</th>
                <th scope="col">Latency</th>
              </tr>
            </thead>
            <tbody>
              {recent.map((r) => (
                <tr key={r.id}>
                  <td>{r.id}</td>
                  <td>
                    <i class={`rp-pg-cell is-${r.status}`} /> {r.status}
                    {r.cause && <span class="rp-pg-cause"> after {r.cause}</span>}
                  </td>
                  <td>{r.attempts}</td>
                  <td>{fmtMs(r.ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div class="rp-pg-code">
            <div class="rp-row rp-pg-codebar">
              <h3 class="rp-pg-h">Your pipeline</h3>
              <span class="rp-pg-size">≈ {kb.toFixed(1)} KB min+gzip (estimate)</span>
              <button
                type="button"
                class="rp-btn"
                onClick={() =>
                  void navigator.clipboard?.writeText(code).then(() => {
                    setCopied(true);
                    setTimeout(() => setCopied(false), 1500);
                  })
                }
              >
                {copied ? 'Copied' : 'Copy'}
              </button>
              <span class="rp-sr" aria-live="polite">
                {copied ? 'Code copied' : ''}
              </span>
            </div>
            <pre>
              <code>{code}</code>
            </pre>
          </div>
        </div>
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div class="rp-stat">
      <b>{value}</b>
      <span>{label}</span>
    </div>
  );
}

function Strategy(p: { label: string; on: boolean; toggle: (v: boolean) => void; children?: ComponentChildren }) {
  return (
    <fieldset class="rp-pg-box" disabled={!p.on} data-on={p.on || undefined}>
      <legend>
        <label class="rp-check">
          <input type="checkbox" checked={p.on} onChange={(e) => p.toggle(e.currentTarget.checked)} /> {p.label}
        </label>
      </legend>
      {p.children}
    </fieldset>
  );
}
