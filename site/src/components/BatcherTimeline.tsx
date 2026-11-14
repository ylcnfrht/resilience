import { useState } from 'preact/hooks';
import { createBatcher } from 'resilience-pipeline';
import Range from './Range.tsx';
import { fmtMs, seeded } from './sim.ts';
import { type Kind, type Lane, Timeline, pack, sleepOn, useFakeRun } from './Timeline.tsx';

const LOADS = 12;
const UNKNOWN = 'u6'; // the backend has no such row: only this item rejects
const COLORS: Kind[] = ['accent', 'lav', 'info', 'warn'];
interface Batch { n: number; first: number; keys: Set<string>; flush: number; done: number; size: number; reason: 'maxSize' | 'maxWait' }
interface Load { i: number; key: string; at: number; end: number; ok: boolean; batch: number }

/** The real createBatcher on a fake clock: single loads arriving, the maxWait window, maxSize flushes, per-item results. */
export default function BatcherTimeline() {
  const [maxWait, setMaxWait] = useState(10);
  const [maxSize, setMaxSize] = useState(4);
  const [gap, setGap] = useState(4);
  const [latency, setLatency] = useState(12);
  const [seed, setSeed] = useState(3);

  const { result, version } = useFakeRun(
    async (time) => {
      const random = seeded(seed);
      const batches: Batch[] = [];
      const loads: Load[] = [];
      let open: Batch | undefined;
      let runs = 0;
      const users = createBatcher<string, string>({
        maxSize,
        maxWait,
        key: (id) => id,
        timeProvider: time,
        run: async (ids) => {
          const b = batches[runs++]!; // batches are sent in the order they were opened
          if (open === b) open = undefined;
          Object.assign(b, { flush: time.now(), size: ids.length, reason: ids.length >= maxSize ? 'maxSize' : 'maxWait' });
          await sleepOn(time, latency);
          b.done = time.now();
          return new Map(ids.map((id) => [id, id === UNKNOWN ? new Error(`unknown user ${id}`) : `user ${id}`]));
        },
      });
      let at = 0;
      const done = Array.from({ length: LOADS }, (_, i) => {
        if (i > 0) at += Math.round(-Math.log(1 - random()) * gap);
        const key = i === 5 ? UNKNOWN : `u${1 + Math.floor(random() * 5)}`; // one load per run asks for the missing row
        return new Promise<void>((resolve) =>
          time.setTimeout(() => {
            // Mirror the batcher's grouping, to know which batch each load joins.
            if (!open) batches.push((open = { n: batches.length, first: time.now(), keys: new Set(), flush: 0, done: 0, size: 0, reason: 'maxWait' }));
            const b = open;
            b.keys.add(key);
            if (b.keys.size >= maxSize) open = undefined;
            const load: Load = { i, key, at: time.now(), end: 0, ok: false, batch: b.n };
            loads.push(load);
            const settle = (ok: boolean) => {
              Object.assign(load, { ok, end: time.now() });
              resolve();
            };
            users.load(key).then(() => settle(true), () => settle(false));
          }, at),
        );
      });
      await Promise.all(done);
      return { batches, loads };
    },
    [maxWait, maxSize, gap, latency, seed],
  );

  const batches = result?.batches ?? [];
  const loads = result?.loads ?? [];
  const tMax = Math.max(1, ...batches.map((b) => b.done)) * 1.3; // room for the result labels
  const results = (b: Batch) => {
    const mine = loads.filter((l) => l.batch === b.n);
    const ok = [...new Set(mine.filter((l) => l.ok).map((l) => l.key))].sort();
    const bad = [...new Set(mine.filter((l) => !l.ok).map((l) => l.key))];
    return [...(ok.length ? [{ text: `${ok.join(' ')}`, kind: 'ok' as Kind }] : []), ...(bad.length ? [{ text: `${ok.length ? '  ' : ''}✕ ${bad.join(' ')}`, kind: 'fail' as Kind }] : [])];
  };
  const labelPx = (b: Batch) => results(b).reduce((n, p) => n + p.text.length, 0) * 6.4 + 22;
  // One row per batch where they overlap: the collecting window with each load() as a dot, then run(), then per-item results.
  const lanes = (msPerPx: number): Lane[] =>
    pack(batches, (b) => b.first - 6 * msPerPx, (b) => b.done + labelPx(b) * msPerPx).map(
      (row, r): Lane => ({
        label: r === 0 ? 'batches' : '',
        sub: r === 0 ? 'load() → run()' : undefined,
        segs: row.flatMap((b) => {
          const k = COLORS[b.n % COLORS.length]!;
          return [
            { from: b.first, to: b.flush, kind: k, hatch: true, title: `batch ${b.n + 1} collects from ${fmtMs(b.first)}, sent at ${fmtMs(b.flush)} (${b.reason === 'maxSize' ? `maxSize ${maxSize} reached` : 'maxWait ran out'})` },
            { from: b.flush, to: b.done, kind: k, label: `×${b.size} ${b.reason}`, title: `batch ${b.n + 1}: run([${[...b.keys].join(', ')}]) ${fmtMs(b.flush)}–${fmtMs(b.done)}` },
          ];
        }),
        marks: row.flatMap((b) => {
          const k = COLORS[b.n % COLORS.length]!;
          const res = results(b);
          return [
            ...loads.filter((l) => l.batch === b.n).map((l) => ({ at: l.at, kind: k, symbol: 'dot' as const, title: `load('${l.key}') at ${fmtMs(l.at)}: ${l.ok ? 'resolved' : 'rejected'} after ${fmtMs(l.end - l.at)}` })),
            { at: b.done, kind: 'ok' as Kind, symbol: 'tick' as const, label: res, anchor: 'start' as const, title: `batch ${b.n + 1} results` },
          ];
        }),
      }),
    );

  const sent = batches.reduce((s, b) => s + b.size, 0);
  const avg = loads.length ? loads.reduce((s, l) => s + l.end - l.at, 0) / loads.length : 0;
  return (
    <div class="rp-demo rp-batcher">
      <div class="rp-controls">
        <Range label="maxWait" value={maxWait} min={1} max={40} step={1} fmt={fmtMs} set={setMaxWait} />
        <Range label="maxSize" value={maxSize} min={1} max={8} step={1} fmt={String} set={setMaxSize} />
        <Range label="Gap between loads" value={gap} min={0} max={15} step={1} fmt={fmtMs} set={setGap} />
        <Range label="run() latency" value={latency} min={2} max={40} step={1} fmt={fmtMs} set={setLatency} />
      </div>

      <Timeline
        version={version}
        lanes={lanes}
        tMax={tMax}
        fmt={fmtMs}
        ariaLabel={`Batcher timeline: ${loads.length} loads sent as ${batches.length} batches; ${loads.filter((l) => !l.ok).length} loads rejected.`}
        legend={[
          { kind: 'accent', label: 'collecting (≤ maxWait)', hatch: true },
          { kind: 'accent', label: 'run(items), one call per batch' },
          { kind: 'accent', label: 'load() joins', symbol: 'dot' },
          { kind: 'ok', label: 'resolved ids', symbol: 'tick' },
          { kind: 'fail', label: `rejected (${UNKNOWN} has no row)`, symbol: 'x' },
        ]}
        caption="Colour = batch. Each dot is one load(); equal ids in a batch share one slot (dedupe by key), so ×n counts distinct ids."
      />

      <div class="rp-row rp-tl-foot">
        <p aria-live="polite" style="margin:0;flex:1">
          <b>{loads.length}</b> loads → <b>{batches.length}</b> batch{batches.length === 1 ? '' : 'es'} with <b>{sent}</b> distinct ids; average wait per load <b>{fmtMs(avg)}</b>.
        </p>
        <button type="button" class="rp-btn" onClick={() => setSeed((s) => s + 1)}>
          New arrivals
        </button>
      </div>
      <code class="rp-tl-code">{`createBatcher({ maxSize: ${maxSize}, maxWait: ${maxWait}, key: (id) => id, run: (ids) => usersByIds(ids) })`}</code>
    </div>
  );
}
