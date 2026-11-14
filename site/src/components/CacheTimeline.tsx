import { useState } from 'preact/hooks';
import { type CacheStore, MemoryCacheStore, PipelineBuilder, cacheStrategy } from '@nodelibraries/resilience';
import Range from './Range.tsx';
import { fmtMs } from './sim.ts';
import { type Kind, type Lane, type Seg, Timeline, pack, sleepOn, useFakeRun } from './Timeline.tsx';

// Callers of one key ('fx-rates'), in ms. Three at once on a cold cache, and two at once later, to show coalescing.
const CALLS = [0, 150, 300, 2000, 4000, 6000, 8000, 8150, 10000, 11500, 12600];
const END = 13500;
type Status = 'hit' | 'miss' | 'joined' | 'stale' | 'error';
interface Call { i: number; start: number; end: number; status: Status }
const KIND: Record<Status, Kind> = { hit: 'ok', miss: 'info', joined: 'lav', stale: 'warn', error: 'fail' };

/** The real cache strategy on a fake clock: fresh hits, misses, coalesced callers, stale-if-error and expiry. */
export default function CacheTimeline() {
  const [ttl, setTtl] = useState(3000);
  const [staleTtl, setStaleTtl] = useState(4000);
  const [latency, setLatency] = useState(600);
  const [outage, setOutage] = useState(7000);
  const [coalesce, setCoalesce] = useState(true);

  const { result, version } = useFakeRun(
    async (time) => {
      const calls: Call[] = [];
      const loads: Seg[] = [];
      const stored: number[] = [];
      const hits = new Set<string>(), stale = new Set<string>();
      const mem = new MemoryCacheStore();
      // Records when values are stored, to draw the entry's fresh / stale / gone life.
      const store: CacheStore = { get: (k) => mem.get(k), set: (k, e) => (stored.push(e.storedAt), mem.set(k, e)), delete: (k) => mem.delete(k) };
      const pipeline = new PipelineBuilder<string>({ timeProvider: time })
        .add(
          cacheStrategy({
            key: () => 'fx-rates',
            ttl,
            staleTtl,
            coalesce,
            store,
            onCacheHit: ({ context }) => void hits.add(context.operationKey!),
            onStaleServed: ({ context }) => void stale.add(context.operationKey!),
          }),
        )
        .build();
      const fetchRates = async () => {
        const seg: Seg = { from: time.now(), to: 0, kind: 'info', label: 'load' };
        loads.push(seg);
        await sleepOn(time, latency);
        seg.to = time.now();
        if (seg.from >= outage) {
          Object.assign(seg, { kind: 'fail', label: '503', title: `load failed (503) ${fmtMs(seg.from)}–${fmtMs(seg.to)}` });
          throw new Error('503');
        }
        seg.title = `load ${fmtMs(seg.from)}–${fmtMs(seg.to)}, stored`;
        return 'rates';
      };
      const runs = CALLS.map(
        (at, i) =>
          new Promise<void>((resolve) =>
            time.setTimeout(async () => {
              const id = String(i);
              let loader = false;
              const outcome = await pipeline.executeOutcome(
                (ctx) => {
                  loader = ctx.operationKey === id;
                  return fetchRates();
                },
                { operationKey: id },
              );
              const status: Status = hits.has(id) ? 'hit' : stale.has(id) ? 'stale' : !outcome.ok ? 'error' : loader ? 'miss' : 'joined';
              calls.push({ i, start: at, end: time.now(), status });
              resolve();
            }, at),
          ),
      );
      await Promise.all(runs);
      return { calls: calls.sort((a, b) => a.i - b.i), loads, stored };
    },
    [ttl, staleTtl, latency, outage, coalesce],
  );

  const calls = result?.calls ?? [];
  // The entry's life after each store: fresh for ttl, then stale for staleTtl, then gone until the next store.
  const entry: Seg[] = [];
  (result?.stored ?? []).forEach((s, i, all) => {
    const next = all[i + 1] ?? END;
    const parts: Array<[number, number, Kind, string, boolean?]> = [
      [s, s + ttl, 'ok', 'fresh'],
      [s + ttl, s + ttl + staleTtl, 'warn', 'stale'],
      [s + ttl + staleTtl, END, 'muted', 'gone', true],
    ];
    for (const [from, to, kind, label, hatch] of parts) {
      const end = Math.min(to, next);
      if (end > from) entry.push({ from, to: end, kind, label, hatch, title: `${label} ${fmtMs(from)}–${fmtMs(end)} (age ${fmtMs(from - s)}–${fmtMs(end - s)})` });
    }
  });

  // Pack callers into as few rows as possible without their labels overlapping (needs the on-screen scale).
  const lanes = (msPerPx: number): Lane[] => {
    const label = (c: Call) => (c.status.length * 6.4 + 22) * msPerPx;
    const atEnd = (c: Call) => c.end > END * 0.9; // label flips to the left of the mark
    const rows = pack(calls, (c) => (atEnd(c) ? Math.min(c.start, c.end - label(c)) : c.start), (c) => (atEnd(c) ? c.end : c.end + label(c)));
    return [
      { label: 'cache entry', sub: 'age', segs: entry },
      { label: 'fetchRates()', sub: 'callback runs', segs: result?.loads ?? [] },
      ...rows.map(
        (row, r): Lane => ({
          label: r === 0 ? 'callers' : '',
          sub: r === 0 ? 'execute()' : undefined,
          segs: row.filter((c) => c.end > c.start).map((c) => ({ from: c.start, to: c.end, kind: KIND[c.status], thin: true })),
          marks: row.map((c) => ({
            at: c.end,
            kind: KIND[c.status],
            symbol: c.status === 'error' ? 'x' : c.status === 'stale' ? 'diamond' : 'dot',
            label: c.status,
            anchor: c.end > END * 0.9 ? 'end' : 'start',
            title: `caller ${c.i + 1} at ${fmtMs(c.start)}: ${c.status}, answered after ${fmtMs(c.end - c.start)}`,
          })),
        }),
      ),
    ];
  };
  const count = (s: Status) => calls.filter((c) => c.status === s).length;

  return (
    <div class="rp-demo rp-cache">
      <div class="rp-controls">
        <Range label="ttl" value={ttl} min={500} max={8000} step={250} fmt={fmtMs} set={setTtl} />
        <Range label="staleTtl" value={staleTtl} min={0} max={8000} step={250} fmt={fmtMs} set={setStaleTtl} />
        <Range label="Load latency" value={latency} min={100} max={1500} step={50} fmt={fmtMs} set={setLatency} />
        <Range label="Outage from" value={outage} min={1000} max={END} step={250} fmt={(v) => (v >= END ? 'never' : fmtMs(v))} set={setOutage} />
        <label class="rp-check">
          <input type="checkbox" checked={coalesce} onChange={(e) => setCoalesce(e.currentTarget.checked)} />
          coalesce
        </label>
      </div>

      <Timeline
        version={version}
        lanes={lanes}
        tMax={END}
        fmt={fmtMs}
        bands={outage < END ? [{ from: outage, to: END, kind: 'fail', label: 'dependency down' }] : []}
        ariaLabel={`Cache timeline for ${calls.length} callers of one key: ${count('hit')} fresh hits, ${count('miss')} loads, ${count('joined')} joined a load in flight, ${count('stale')} got a stale value, ${count('error')} failed.`}
        legend={[
          { kind: 'ok', label: 'fresh / hit' },
          { kind: 'warn', label: 'stale (served on error)' },
          { kind: 'muted', label: 'gone', hatch: true },
          { kind: 'info', label: 'miss: runs the load' },
          { kind: 'lav', label: 'joined the load in flight' },
          { kind: 'fail', label: 'error' },
        ]}
      />

      <div class="rp-stats rp-tl-stats" aria-live="polite">
        {(['hit', 'miss', 'joined', 'stale', 'error'] as const).map((s) => (
          <div class="rp-stat" key={s}>
            <span>{s === 'miss' ? 'loads (misses)' : s === 'joined' ? 'coalesced' : s === 'stale' ? 'stale served' : s === 'hit' ? 'fresh hits' : 'errors'}</span>
            <b>{count(s)}</b>
          </div>
        ))}
      </div>
      <code class="rp-tl-code">{`cacheStrategy({ key: () => 'fx-rates', ttl: ${ttl}, staleTtl: ${staleTtl}${coalesce ? '' : ', coalesce: false'} })`}</code>
    </div>
  );
}
