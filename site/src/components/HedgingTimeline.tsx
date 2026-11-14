import { useState } from 'preact/hooks';
import { PipelineBuilder, hedgingStrategy } from '@ylcnfrht/resilience';
import Range from './Range.tsx';
import { fmtMs } from './sim.ts';
import { type Lane, Timeline, sleepOn, useFakeRun } from './Timeline.tsx';

type Mode = 'latency' | 'parallel' | 'fallback';
interface Attempt { n: number; start: number; end: number; status: 'won' | 'done' | 'failed' | 'aborted' }

/** The real hedging strategy on a fake clock: when each attempt starts, which one wins, which are aborted. */
export default function HedgingTimeline() {
  const [mode, setMode] = useState<Mode>('latency');
  const [delay, setDelay] = useState(150);
  const [primary, setPrimary] = useState(600);
  const [hedge, setHedge] = useState(120);
  const [max, setMax] = useState(1);
  const [primaryFails, setPrimaryFails] = useState(false);

  const { result, version } = useFakeRun(
    async (time) => {
      const attempts: Attempt[] = [];
      const pipeline = new PipelineBuilder<string>({ timeProvider: time })
        .add(hedgingStrategy({ delay: mode === 'latency' ? delay : mode === 'parallel' ? 0 : -1, maxHedgedAttempts: max }))
        .build();
      const outcome = await pipeline.executeOutcome(async ({ signal, attemptNumber: n }) => {
        const a: Attempt = { n, start: time.now(), end: 0, status: 'done' };
        attempts.push(a);
        try {
          // Each later hedge is a little slower, so parallel attempts don't all finish at once.
          await sleepOn(time, n === 0 ? primary : hedge + (n - 1) * 40, signal);
        } catch (error) {
          a.end = time.now();
          a.status = 'aborted';
          throw error;
        }
        a.end = time.now();
        if (n === 0 && primaryFails) {
          a.status = 'failed';
          throw new Error('503');
        }
        return `attempt ${n}`;
      });
      const total = time.now();
      const winner = outcome.ok ? attempts.find((a) => `attempt ${a.n}` === outcome.result) : undefined;
      if (winner) winner.status = 'won';
      return { attempts, total, ok: outcome.ok };
    },
    [mode, delay, primary, hedge, max, primaryFails],
  );

  const attempts = result?.attempts ?? [];
  const total = result?.total ?? 0;
  const tMax = Math.max(primary, total, ...attempts.map((a) => a.end)) * 1.2 || 1; // room for the end labels
  const KIND = { won: 'ok', done: 'muted', failed: 'fail', aborted: 'muted' } as const;
  const TEXT = { won: 'wins', done: 'too late', failed: '503', aborted: 'aborted' };
  const lanes: Lane[] = attempts.map((a): Lane => ({
    label: a.n === 0 ? 'primary' : `hedge ${a.n}`,
    sub: `attempt ${a.n}`,
    segs: [{ from: a.start, to: a.end, kind: KIND[a.status], hatch: a.status === 'aborted', label: TEXT[a.status], title: `attempt ${a.n}: ${TEXT[a.status]} after ${fmtMs(a.end - a.start)}` }],
    marks: [
      a.status === 'won' ? { at: a.end, kind: 'ok', symbol: 'tick', label: fmtMs(a.end) }
      : a.status === 'failed' ? { at: a.end, kind: 'fail', symbol: 'x', label: 'handled failure' }
      : { at: a.end, kind: 'muted', symbol: 'x' },
    ],
  }));
  lanes.push({
    label: 'no hedging',
    sub: 'primary alone',
    segs: [{ from: 0, to: primary, kind: 'lav', thin: true, title: `primary alone: ${primaryFails ? 'fails' : 'answers'} after ${fmtMs(primary)}` }],
    marks: [{ at: primary, kind: primaryFails ? 'fail' : 'lav', symbol: primaryFails ? 'x' : 'dot', label: primaryFails ? 'error' : fmtMs(primary) }],
  });
  const guides =
    mode === 'latency'
      ? Array.from({ length: max }, (_, i) => (i + 1) * delay)
          .filter((t) => t < total)
          .map((at, i) => ({ at, label: i === 0 ? `delay ${fmtMs(delay)}` : undefined }))
      : [];
  const delayArg = mode === 'latency' ? delay : mode === 'parallel' ? 0 : -1;
  const won = attempts.find((a) => a.status === 'won');

  return (
    <div class="rp-demo rp-hedge">
      <div class="rp-controls">
        <label class="rp-field">
          Mode
          <select value={mode} onChange={(e) => setMode(e.currentTarget.value as Mode)}>
            <option value="latency">latency (delay &gt; 0)</option>
            <option value="parallel">parallel (delay 0)</option>
            <option value="fallback">fallback (delay &lt; 0)</option>
          </select>
        </label>
        <Range label="delay" value={delay} min={10} max={1000} step={10} fmt={fmtMs} set={setDelay} disabled={mode !== 'latency'} />
        <Range label="Primary latency" value={primary} min={50} max={2000} step={10} fmt={fmtMs} set={setPrimary} />
        <Range label="Hedge latency" value={hedge} min={20} max={1000} step={10} fmt={fmtMs} set={setHedge} />
        <Range label="maxHedgedAttempts" value={max} min={1} max={3} step={1} fmt={String} set={setMax} />
        <label class="rp-check">
          <input type="checkbox" checked={primaryFails} onChange={(e) => setPrimaryFails(e.currentTarget.checked)} />
          Primary fails (503)
        </label>
      </div>

      <Timeline
        version={version}
        lanes={lanes}
        tMax={tMax}
        fmt={fmtMs}
        guides={guides}
        ariaLabel={`Hedging timeline: ${attempts.length} attempts started, ${won ? `attempt ${won.n} won after ${fmtMs(total)}` : 'all failed'}; without hedging ${fmtMs(primary)}.`}
        legend={[
          { kind: 'ok', label: 'winner' },
          { kind: 'fail', label: 'handled failure' },
          { kind: 'muted', label: 'aborted loser', hatch: true },
          { kind: 'lav', label: 'without hedging' },
        ]}
      />

      <p class="rp-tl-foot" aria-live="polite">
        {won ? (
          <>
            Answer after <b>{fmtMs(total)}</b> from {won.n === 0 ? 'the primary' : `hedge ${won.n}`}
          </>
        ) : (
          <>All attempts failed after <b>{fmtMs(total)}</b></>
        )}
        {' '}(without hedging: <b>{primaryFails ? 'error' : fmtMs(primary)}</b>), <b>{attempts.length}</b> request{attempts.length === 1 ? '' : 's'} sent.
      </p>
      <code class="rp-tl-code">{`hedgingStrategy({ delay: ${delayArg}, maxHedgedAttempts: ${max} })`}</code>
    </div>
  );
}
