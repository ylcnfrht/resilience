import { useEffect, useId, useRef, useState } from 'preact/hooks';
import { FakeTimeProvider } from 'resilience-pipeline/testing';
import type { TimeProvider } from 'resilience-pipeline';
import './demo.css';
import './Timeline.css';

// Shared by HedgingTimeline, CacheTimeline, LockLeaseTimeline and BatcherTimeline: the real strategy runs on a
// FakeTimeProvider, its events are recorded with fake timestamps, and this draws them as lanes on a time axis.

/** Colour of a bar or mark; maps to the demo state colours. */
export type Kind = 'ok' | 'fail' | 'warn' | 'info' | 'accent' | 'lav' | 'muted';
export interface Seg { from: number; to: number; kind: Kind; label?: string; title?: string; hatch?: boolean; thin?: boolean }
export interface Mark { at: number; kind: Kind; symbol?: 'dot' | 'x' | 'tick' | 'diamond'; /** Parts can have their own colour. */ label?: string | Array<{ text: string; kind: Kind }>; title?: string; anchor?: 'start' | 'end' }
export interface Lane { label: string; sub?: string; segs?: Seg[]; marks?: Mark[] }
export interface Band { from: number; to: number; kind: Kind; label?: string }
export interface Guide { at: number; label?: string }

export interface TimelineProps {
  /** Or a function of the scale (ms per pixel), for callers that pack items into rows by their on-screen size. */
  lanes: Lane[] | ((msPerPx: number) => Lane[]);
  tMax: number;
  fmt: (t: number) => string;
  bands?: Band[];
  guides?: Guide[];
  legend: Array<{ kind: Kind; label: string; hatch?: boolean; symbol?: Mark['symbol'] }>;
  ariaLabel: string;
  caption?: string;
  /** Changes on every new result, to replay the reveal. */
  version: number;
}

export function Timeline({ lanes: lanesProp, tMax, fmt, bands = [], guides = [], legend, ariaLabel, caption, version }: TimelineProps) {
  const id = useId();
  const ref = useRef<HTMLElement>(null);
  const [w, setW] = useState(640);
  useEffect(() => {
    const el = ref.current!;
    const ro = new ResizeObserver(() => setW(Math.max(280, Math.round(el.clientWidth))));
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  // Narrow screens: lane labels go above their bars instead of in a left column, so text keeps its real size.
  const narrow = w < 520;
  const L = narrow ? 6 : 112, R = 14;
  const lanes = typeof lanesProp === 'function' ? lanesProp(tMax / (w - L - R)) : lanesProp;
  const T = bands.some((b) => b.label) || guides.some((g) => g.label) ? 20 : 6;
  const laneH = narrow ? 44 : 34;
  const barY = narrow ? 18 : 8, barH = 18;
  const B = 24;
  const H = T + lanes.length * laneH + B;
  const pw = w - L - R;
  const x = (t: number) => L + (Math.min(Math.max(t, 0), tMax) / tMax) * pw;
  const ticks = niceTicks(tMax, Math.max(3, Math.floor(pw / 90)));
  const bottom = T + lanes.length * laneH;

  return (
    <figure class="rp-tl" ref={ref}>
      <svg width={w} height={H} viewBox={`0 0 ${w} ${H}`} role="img" aria-label={ariaLabel}>
        <defs>
          <pattern id={`${id}-h`} width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
            <line x1="0" y1="0" x2="0" y2="5" class="rp-tl-hatchline" />
          </pattern>
          <mask id={`${id}-m`}>
            <rect key={version} class="rp-tl-reveal" x={L - 10} y={0} width={pw + R + 10} height={H} fill="white" />
          </mask>
        </defs>

        {bands.map((b, i) => (
          <g key={`b${i}`} class={`rp-tl-band k-${b.kind}`}>
            <rect x={x(b.from)} y={T - 2} width={Math.max(0, x(b.to) - x(b.from))} height={bottom - T + 2} />
            {b.label && x(b.to) - x(b.from) > b.label.length * 6 + 6 && (
              <text x={x(b.from) + 4} y={T - 7}>
                {b.label}
              </text>
            )}
          </g>
        ))}
        {ticks.map((t) => (
          <g key={`t${t}`}>
            <line class="rp-grid" x1={x(t)} x2={x(t)} y1={T} y2={bottom} />
            <text class="rp-axis" x={x(t)} y={H - 7} text-anchor={t === 0 ? 'start' : 'middle'}>
              {fmt(t)}
            </text>
          </g>
        ))}
        {lanes.map((lane, i) => {
          const top = T + i * laneH;
          return (
            <g key={`l${i}`}>
              <line class="rp-tl-sep" x1={0} x2={w} y1={top + laneH} y2={top + laneH} />
              <text class="rp-tl-lane" x={narrow ? L : 0} y={narrow ? top + 12 : top + barY + (lane.sub ? 7 : 13)}>
                {lane.label}
                {narrow && lane.sub && <tspan class="rp-tl-sub"> {lane.sub}</tspan>}
              </text>
              {!narrow && lane.sub && (
                <text class="rp-tl-sub" x={0} y={top + barY + 20}>
                  {lane.sub}
                </text>
              )}
            </g>
          );
        })}
        {guides.map((g, i) => (
          <g key={`g${i}`} class="rp-tl-guide">
            <line x1={x(g.at)} x2={x(g.at)} y1={T - 4} y2={bottom} />
            {g.label && (
              <text x={x(g.at) + (x(g.at) > w - 90 ? -4 : 4)} y={T - 7} text-anchor={x(g.at) > w - 90 ? 'end' : 'start'}>
                {g.label}
              </text>
            )}
          </g>
        ))}

        <g mask={`url(#${id}-m)`}>
          {lanes.map((lane, i) => {
            const top = T + i * laneH + barY;
            return (
              <g key={`s${i}`}>
                {lane.segs?.map((s, j) => {
                  const x0 = x(s.from), sw = Math.max(2, x(s.to) - x0);
                  const h = s.thin ? 6 : barH, y0 = top + (barH - h) / 2;
                  return (
                    <g key={j} class={`rp-tl-seg k-${s.kind}`}>
                      <rect x={x0} y={y0} width={sw} height={h} rx={s.thin ? 3 : 4} />
                      {s.hatch && <rect x={x0} y={y0} width={sw} height={h} rx={4} fill={`url(#${id}-h)`} class="rp-tl-hatch" />}
                      {s.title && <title>{s.title}</title>}
                      {s.label && !s.thin && sw > s.label.length * 6.4 + 8 && (
                        <text x={x0 + 5} y={top + 13}>
                          {s.label}
                        </text>
                      )}
                    </g>
                  );
                })}
                {lane.marks?.map((m, j) => (
                  <MarkGlyph key={`m${j}`} m={m} cx={x(m.at)} cy={top + barH / 2} w={w} />
                ))}
              </g>
            );
          })}
        </g>
      </svg>
      <figcaption>
        <span class="rp-tl-legend">
          {legend.map((l) => (
            <span key={l.label} class={`rp-tl-key k-${l.kind}`} data-hatch={l.hatch || undefined} data-symbol={l.symbol}>
              {l.label}
            </span>
          ))}
        </span>
        {caption && <span>{caption}</span>}
      </figcaption>
      <ul class="rp-sr">
        {lanes.map((lane, i) => (
          <li key={i}>
            {lane.label}:{' '}
            {[...(lane.segs ?? []).map((s) => `${s.title ?? s.label ?? s.kind} ${fmt(s.from)} to ${fmt(s.to)}`), ...(lane.marks ?? []).map((m) => `${m.title ?? m.kind} at ${fmt(m.at)}`)].join('; ')}
          </li>
        ))}
      </ul>
    </figure>
  );
}

function MarkGlyph({ m, cx, cy, w }: { m: Mark; cx: number; cy: number; w: number }) {
  const sym = m.symbol ?? 'dot';
  const end = m.anchor === 'end' || (m.anchor !== 'start' && cx > w - 120);
  return (
    <g class={`rp-tl-mark k-${m.kind}`}>
      {m.title && <title>{m.title}</title>}
      {sym === 'dot' && <circle cx={cx} cy={cy} r={4.5} />}
      {sym === 'diamond' && <path d={`M${cx} ${cy - 6}L${cx + 6} ${cy}L${cx} ${cy + 6}L${cx - 6} ${cy}z`} />}
      {sym === 'x' && <path class="rp-tl-stroke" d={`M${cx - 5} ${cy - 5}L${cx + 5} ${cy + 5}M${cx + 5} ${cy - 5}L${cx - 5} ${cy + 5}`} />}
      {sym === 'tick' && <path class="rp-tl-stroke" d={`M${cx - 5} ${cy}L${cx - 1.5} ${cy + 4}L${cx + 5} ${cy - 5}`} />}
      {m.label && (
        // An end-anchored label would sit on its own bar; lift it just above the bar instead.
        <text x={cx + (end ? -6 : 9)} y={end ? cy - 10 : cy + 4} text-anchor={end ? 'end' : 'start'}>
          {typeof m.label === 'string'
            ? m.label
            : m.label.map((p, i) => (
                <tspan key={i} class={`k-${p.kind}`}>
                  {p.text}
                </tspan>
              ))}
        </text>
      )}
    </g>
  );
}

function niceTicks(max: number, count: number): number[] {
  const raw = max / count;
  const p = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * p).find((s) => s >= raw)!;
  const out: number[] = [];
  for (let t = 0; t <= max + 1e-9; t += step) out.push(+t.toFixed(6));
  return out;
}

/** Greedy row packing: puts each item (sorted by `left`) in the first row whose last item ends before it starts. */
export function pack<T>(items: T[], left: (item: T) => number, right: (item: T) => number): T[][] {
  const rows: T[][] = [];
  const ends: number[] = [];
  for (const item of items) {
    let r = ends.findIndex((e) => e <= left(item));
    if (r < 0) r = rows.push([]) - 1;
    rows[r]!.push(item);
    ends[r] = right(item);
  }
  return rows;
}

/** Runs `scenario` on a fresh FakeTimeProvider at t = 0 whenever `deps` change; returns its result (stale runs are dropped). */
export function useFakeRun<T>(scenario: (time: FakeTimeProvider) => Promise<T>, deps: unknown[]): { result?: T; version: number } {
  const [state, setState] = useState<{ result?: T; version: number }>({ version: 0 });
  useEffect(() => {
    let live = true;
    const time = new FakeTimeProvider(0);
    const done = scenario(time);
    void time
      .runAll()
      .then(() => done)
      .then((result) => live && setState((s) => ({ result, version: s.version + 1 })))
      .catch((error: unknown) => console.error('timeline scenario failed', error));
    return () => {
      live = false;
    };
  }, deps);
  return state;
}

/** Sleep on a (fake) clock; with a signal it rejects as soon as the signal aborts. */
export function sleepOn(time: TimeProvider, ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => {
      time.clearTimeout(handle);
      reject(signal!.reason);
    };
    const handle = time.setTimeout(() => {
      signal?.removeEventListener('abort', abort);
      resolve();
    }, ms);
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
  });
}
