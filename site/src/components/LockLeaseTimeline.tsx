import { useState } from 'preact/hooks';
import { type LockStore, type TimeProvider, MemoryLockStore, PipelineBuilder, getLockToken, lockStrategy } from 'resilience-pipeline';
import Range from './Range.tsx';
import { fmtMs } from './sim.ts';
import { type Lane, type Mark, type Seg, Timeline, sleepOn, useFakeRun } from './Timeline.tsx';

type Who = 'A' | 'B';
interface Write { who: Who; token: number; at: number; accepted: boolean; fence: number }

/**
 * Two processes on one lock, run by the real lock strategy on a fake clock. Process A freezes (a GC pause): every timer
 * it set that falls inside the pause fires only when it wakes, exactly like a stopped process.
 */
export default function LockLeaseTimeline() {
  const [ttl, setTtl] = useState(3000);
  const [pause, setPause] = useState(4000);
  const [fencing, setFencing] = useState(true);
  const pauseFrom = ttl / 2;
  const pauseTo = pauseFrom + pause;

  const { result, version } = useFakeRun(
    async (time) => {
      const mem = new MemoryLockStore({ timeProvider: time });
      const owners: string[] = [];
      const who = (owner: string): Who => (owners.includes(owner) ? owners.indexOf(owner) : owners.push(owner) - 1) === 0 ? 'A' : 'B';
      const ev = { acquired: {} as Partial<Record<Who, { at: number; token: number }>>, renews: [] as Array<{ who: Who; at: number; ok: boolean }>, released: {} as Partial<Record<Who, number>> };
      // The shared store, recording what each process asked for.
      const store: LockStore = {
        acquire: async (key, owner, ms) => {
          const w = who(owner);
          const lease = await mem.acquire(key, owner, ms);
          if (lease) ev.acquired[w] = { at: time.now(), token: lease.fencingToken };
          return lease;
        },
        renew: async (key, owner, ms) => {
          const ok = await mem.renew(key, owner, ms);
          ev.renews.push({ who: who(owner), at: time.now(), ok });
          return ok;
        },
        release: async (key, owner) => {
          ev.released[who(owner)] = time.now();
          return mem.release(key, owner);
        },
      };
      // Process A's clock: timers due during the pause fire when it wakes.
      const frozen: TimeProvider = {
        now: () => time.now(),
        setTimeout: (cb, ms) => {
          let at = time.now() + ms;
          if (at >= pauseFrom && at < pauseTo) at = pauseTo;
          return time.setTimeout(cb, at - time.now());
        },
        clearTimeout: (h) => time.clearTimeout(h),
      };

      // The protected resource: rejects a write whose token is not newer than the last one it accepted.
      let fence = 0;
      const writes: Write[] = [];
      const write = (w: Who, token: number) => {
        const accepted = !fencing || fence < token;
        writes.push({ who: w, token, at: time.now(), accepted, fence });
        if (accepted) fence = Math.max(fence, token);
      };

      let lostAt: number | undefined;
      const lock = (clock: TimeProvider, wait: number) =>
        new PipelineBuilder({ timeProvider: clock }).add(lockStrategy({ key: () => 'account:1', ttl, wait, store, onLockLost: () => void (lostAt = time.now()) })).build();

      // A: works for a while (ignoring its signal, as a frozen process would), then writes with its token.
      const a = lock(frozen, 0).executeOutcome(async (ctx) => {
        await sleepOn(frozen, pauseFrom + 500);
        await sleepOn(frozen, 100); // the write's round trip
        write('A', getLockToken(ctx)!);
      });
      // B: asks for the same lock during A's pause and waits for it.
      const b = new Promise((resolve) =>
        time.setTimeout(() => {
          lock(time, 30_000)
            .executeOutcome(async (ctx) => {
              await sleepOn(time, 800);
              write('B', getLockToken(ctx)!);
            })
            .then(resolve);
        }, pauseFrom + 200),
      );
      await Promise.all([a, b]);
      return { ev, writes, lostAt, end: time.now() };
    },
    [ttl, pause, fencing],
  );

  const ev = result?.ev;
  const tMax = Math.max(result?.end ?? 0, pauseTo) * 1.45 || 1; // room on the right for the last labels
  const lanes: Lane[] = [];
  const leaseLane = (w: Who): Lane => {
    const acq = ev?.acquired[w];
    if (!acq) return { label: `${w} lease`, sub: 'none' };
    // Store's view: the lease lives until ttl after the last successful renewal, or until released.
    const renews = ev!.renews.filter((r) => r.who === w);
    const lastOk = Math.max(acq.at, ...renews.filter((r) => r.ok).map((r) => r.at));
    const released = ev!.released[w];
    const expires = lastOk + ttl;
    const end = released !== undefined && released < expires ? released : expires;
    const expired = end === expires;
    const marks: Mark[] = renews.map((r) =>
      r.ok
        ? { at: r.at, kind: 'accent', symbol: 'dot', title: `renewed at ${fmtMs(r.at)}` }
        : { at: r.at, kind: 'fail', symbol: 'x', label: 'lost', anchor: 'start', title: `renewal refused at ${fmtMs(r.at)}: LockLostError, the callback's signal is aborted` },
    );
    // Expiry and the refused renewal land next to each other: one label for both, or they overlap on narrow screens.
    const lostMark = marks.find((m) => m.label === 'lost');
    if (expired && lostMark) lostMark.label = [{ text: 'expired', kind: 'warn' }, { text: ' → lost', kind: 'fail' }];
    if (expired) marks.push({ at: expires, kind: 'warn', symbol: 'diamond', label: lostMark ? undefined : 'expired', title: `lease expired at ${fmtMs(expires)}` });
    return {
      label: `${w} lease`,
      sub: `token ${acq.token}`,
      segs: [{ from: acq.at, to: end, kind: 'ok', title: `${w} holds the lease (token ${acq.token}) ${fmtMs(acq.at)}–${fmtMs(end)}` }],
      marks,
    };
  };
  if (ev) {
    const aDone = Math.max(...(result!.writes.filter((x) => x.who === 'A').map((x) => x.at)), 0);
    const aSegs: Seg[] = [{ from: 0, to: pauseFrom, kind: 'info', label: 'working' }];
    if (pause > 0) aSegs.push({ from: pauseFrom, to: pauseTo, kind: 'warn', hatch: true, label: 'GC pause', title: `process A frozen ${fmtMs(pauseFrom)}–${fmtMs(pauseTo)}` });
    aSegs.push({ from: Math.max(pauseFrom, pauseTo), to: aDone, kind: 'info' });
    lanes.push(leaseLane('A'), {
      label: 'A process',
      sub: 'callback',
      segs: aSegs,
      marks: result!.writes.filter((x) => x.who === 'A').map(writeMark),
    });
    const bAcq = ev.acquired.B;
    const bLane = leaseLane('B');
    bLane.segs = [{ from: pauseFrom + 200, to: bAcq?.at ?? pauseFrom + 200, kind: 'muted', thin: true, title: `B waits for the lock (polling) from ${fmtMs(pauseFrom + 200)}` }, ...(bLane.segs ?? [])];
    bLane.marks = [{ at: pauseFrom + 200, kind: 'muted', symbol: 'dot', title: 'B asks for the lock' }, ...(bLane.marks ?? []), ...result!.writes.filter((x) => x.who === 'B').map(writeMark)];
    lanes.push(bLane);
  }

  const writes = result?.writes ?? [];
  function writeMark(x: Write): Mark {
    const overwrote = x.accepted && x.token < x.fence;
    return {
      at: x.at,
      kind: x.accepted && !overwrote ? 'ok' : 'fail',
      symbol: x.accepted ? 'tick' : 'x',
      label: !x.accepted ? `rejected (${x.token}<${x.fence})` : overwrote ? 'overwrites B' : `write (${x.token})`,
      anchor: 'start',
      title: `${x.who} writes with token ${x.token} at ${fmtMs(x.at)}: ${x.accepted ? 'accepted' : `rejected, the resource already saw token ${x.fence}`}`,
    };
  }
  const stale = writes.find((x) => x.who === 'A');
  const lost = result?.lostAt !== undefined;
  return (
    <div class="rp-demo rp-lock">
      <div class="rp-controls">
        <Range label="ttl" value={ttl} min={1500} max={6000} step={300} fmt={fmtMs} set={setTtl} />
        <Range label="GC pause in A" value={pause} min={0} max={8000} step={250} fmt={fmtMs} set={setPause} />
        <label class="rp-check">
          <input type="checkbox" checked={fencing} onChange={(e) => setFencing(e.currentTarget.checked)} />
          Resource checks fencing token
        </label>
      </div>

      <Timeline
        version={version}
        lanes={lanes}
        tMax={tMax}
        fmt={fmtMs}
        guides={[{ at: ttl / 3, label: `renew every ttl/3 = ${fmtMs(ttl / 3)}` }]}
        ariaLabel={`Lock lease timeline. Process A ${lost ? 'loses its lease during a GC pause' : 'keeps its lease'}; its write is ${stale?.accepted ? 'accepted' : 'rejected'}.`}
        legend={[
          { kind: 'ok', label: 'lease held' },
          { kind: 'accent', label: 'renewal', symbol: 'dot' },
          { kind: 'warn', label: 'GC pause', hatch: true },
          { kind: 'muted', label: 'waiting for the lock' },
          { kind: 'fail', label: 'lost / rejected', symbol: 'x' },
        ]}
      />

      <p class="rp-tl-foot" aria-live="polite">
        {!stale ? (
          ' '
        ) : !lost ? (
          <>
            The pause ends before A's lease does (<b>{fmtMs(ttl)}</b> after its last renewal): A keeps the lock, writes with token 1, and B gets the lock after A releases it.
          </>
        ) : stale.accepted ? (
          <>
            A's lease expired during the pause and B took the lock. A wakes up, still thinks it holds the lock, and its write with token {stale.token} <b>overwrites B's</b>: a lost update.
          </>
        ) : (
          <>
            A's lease expired during the pause and B took the lock with token {stale.fence}. A wakes up and writes with token {stale.token}: the resource <b>rejects it</b>. Its abort signal fired too, but a paused process can't see it in time.
          </>
        )}
      </p>
      <code class="rp-tl-code">{`lockStrategy({ key: () => 'account:1', ttl: ${ttl} })`}</code>
    </div>
  );
}
