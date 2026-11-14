import { useEffect, useRef, useState } from 'preact/hooks';
import { PipelineBuilder, circuitBreakerStrategy, fallbackStrategy, retryStrategy, timeoutStrategy } from '@nodelibraries/resilience';
import { type ServiceConfig, flakyService, seeded, sleep } from './sim.ts';
import './demo.css';
import './HeroDemo.css';

type Tick = 'pending' | 'ok' | 'retried' | 'fallback' | 'slow' | 'failed';
const N = 20;
const FALLBACK = 'fallback';
const SERVICE: ServiceConfig = { failureRate: 35, latency: 60, jitter: 120, outage: false, slowdown: false, hang: 0.08, hangMs: 2000 };

/** Landing-page widget: the same flaky service hit by 20 requests, without and with a pipeline. */
export default function HeroDemo({ seed = 7 }: { seed?: number }) {
  const [plain, setPlain] = useState<Tick[]>([]);
  const [piped, setPiped] = useState<Tick[]>([]);
  const [round, setRound] = useState(0);
  const root = useRef<HTMLDivElement>(null);

  // Auto-play once, when scrolled into view.
  useEffect(() => {
    const io = new IntersectionObserver(([e]) => {
      if (e?.isIntersecting) {
        io.disconnect();
        setRound(1);
      }
    });
    io.observe(root.current!);
    return () => io.disconnect();
  }, []);

  useEffect(() => {
    if (!round) return;
    let live = true;
    const pipeline = new PipelineBuilder<string>({ telemetry: false })
      .add(fallbackStrategy({ fallbackValue: FALLBACK }))
      .add(retryStrategy({ maxRetryAttempts: 3, delay: 50, useJitter: true }))
      .add(circuitBreakerStrategy({ consecutiveFailures: 5, breakDuration: 2000 }))
      .add(timeoutStrategy(400))
      .build();
    const a: Tick[] = Array(N).fill('pending');
    const b: Tick[] = Array(N).fill('pending');
    setPlain([...a]);
    setPiped([...b]);
    const stagger = matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 110;
    const s = seed * 1000 + round;
    for (let i = 0; i < N; i++) {
      void (async () => {
        await sleep(i * stagger);
        // Same random stream per request on both sides: the first attempt meets the same fate.
        const call = flakyService(() => SERVICE, seeded(s + i * 31));
        const call2 = flakyService(() => SERVICE, seeded(s + i * 31));
        const t0 = performance.now();
        const [x, y] = await Promise.all([
          call().then(
            () => (performance.now() - t0 > 1000 ? 'slow' : 'ok') as Tick,
            () => 'failed' as const,
          ),
          (async () => {
            let attempts = 0;
            const o = await pipeline.executeOutcome(({ signal }) => (attempts++, call2(signal)));
            return !o.ok ? 'failed' : o.result === FALLBACK ? 'fallback' : attempts > 1 ? 'retried' : 'ok';
          })(),
        ]);
        if (!live) return;
        a[i] = x;
        b[i] = y;
        setPlain([...a]);
        setPiped([...b]);
      })();
    }
    return () => {
      live = false;
    };
  }, [round]);

  return (
    <div class="rp-demo rp-hero" ref={root}>
      <Column title="Without pipeline" ticks={plain} />
      <Column title="With retry + timeout + breaker + fallback" ticks={piped} />
      <div class="rp-row rp-hero-foot">
        <span class="rp-hero-legend">
          <i class="rp-hero-tick is-ok" /> ok <i class="rp-hero-tick is-retried" /> ok after retry <i class="rp-hero-tick is-fallback" /> fallback <i class="rp-hero-tick is-slow" /> slow (&gt; 1 s) <i class="rp-hero-tick is-failed" /> error
        </span>
        <button type="button" class="rp-btn" onClick={() => setRound((r) => r + 1)}>
          Replay
        </button>
      </div>
    </div>
  );
}

function Column({ title, ticks }: { title: string; ticks: Tick[] }) {
  const done = ticks.length > 0 && !ticks.includes('pending');
  const answered = ticks.filter((t) => t !== 'failed' && t !== 'slow' && t !== 'pending').length;
  return (
    <div class="rp-hero-col">
      <h4>{title}</h4>
      <div class="rp-hero-ticks" aria-hidden="true">
        {Array.from({ length: N }, (_, i) => (
          <i key={i} class={`rp-hero-tick is-${ticks[i] ?? 'pending'}`} />
        ))}
      </div>
      <p class="rp-hero-rate" aria-live="polite">
        {done ? (
          <>
            <b data-good={answered >= N * 0.9 || undefined}>{Math.round((answered / N) * 100)}%</b> answered in under 1 s
            {ticks.includes('fallback') && ` (${ticks.filter((t) => t === 'fallback').length} from fallback)`}
          </>
        ) : (
          ticks.length > 0 && 'running…'
        )}
      </p>
    </div>
  );
}
