// Deterministic simulations behind RateLimiterTimeline and AdaptiveLimitChart. The admission / limit decisions come from the
// library's own objects (passed in), so these files only model time and the dependency.
import type { LimitAlgorithm, RateLimiterAlgorithm } from '@ylcnfrht/resilience';
import { seeded } from './sim.ts';

export interface Mark {
  t: number;
  ok: boolean;
}

/** Feeds request times (ms, ascending) to a rate-limiter algorithm with no queue: each one is admitted or rejected on the spot. */
export function runLimiter(algo: RateLimiterAlgorithm, times: number[]): Mark[] {
  // The in-memory algorithms answer synchronously; a Redis one would not, but none is used here.
  return times.map((t) => ({ t, ok: (algo.acquire(1, t) as { ok: boolean }).ok }));
}

/** Most admitted requests in any `span`-long interval, and where that interval starts. */
export function worstSpan(marks: Mark[], span: number): { count: number; start: number } {
  const ok = marks.filter((m) => m.ok).map((m) => m.t);
  let best = { count: 0, start: 0 };
  for (let i = 0, j = 0; j < ok.length; j++) {
    while (ok[j]! - ok[i]! >= span) i++;
    if (j - i + 1 > best.count) best = { count: j - i + 1, start: ok[i]! };
  }
  return best;
}

export type Pattern = 'edge' | 'steady' | 'random';

/** Request times over `duration` ms for a limit of `limit` per `window` ms. */
export function requestStream(pattern: Pattern, limit: number, window: number, duration: number, seed = 1): number[] {
  const out: number[] = [];
  if (pattern === 'edge') {
    // A quiet first window, then `limit` just before its boundary and `limit` just after it, then steady traffic.
    const gap = (0.24 * window) / limit;
    for (let i = 0; i < limit; i++) out.push(0.75 * window + i * gap);
    for (let i = 0; i < limit; i++) out.push(window + 1 + i * gap);
    for (let t = 2.2 * window; t < duration; t += window / (limit * 0.75)) out.push(t);
  } else if (pattern === 'steady') {
    for (let t = 0; t < duration; t += window / (limit * 1.5)) out.push(t);
  } else {
    const random = seeded(seed);
    for (let t = 0; t < duration; ) {
      // Clumps of 2..limit requests separated by quiet gaps.
      const n = 2 + Math.floor(random() * (limit - 1));
      for (let i = 0; i < n && t < duration; i++, t += 8 + random() * 25) out.push(t);
      t += random() * window * 0.6;
    }
  }
  return out.map(Math.round).filter((t) => t < duration);
}

export interface AdaptivePoint {
  t: number;
  limit: number;
  inflight: number;
  capacity: number;
  /** Latency of the calls admitted in this step, ms (capped at the timeout). */
  latency: number;
}

export interface AdaptiveRun {
  points: AdaptivePoint[];
  drops: number[];
  rejected: number;
  admitted: number;
}

export interface Dependency {
  /** Requests per second offered to the limiter. */
  rate: number;
  /** No-load latency, ms. */
  base: number;
  /** Calls the dependency serves in parallel before they queue inside it. */
  capacity: number;
  /** Capacity while `slow` covers t. */
  slowCapacity: number;
  slow?: [number, number];
  /** The pipeline's timeout (ms): slower calls count as drops. */
  timeout: number;
}

/**
 * Discrete-time model of AdaptiveConcurrencyLimiterStrategy.execute (queueLimit 0) in front of a dependency whose latency
 * grows once more than `capacity` calls run in it. The guards (clamp, app-limited, one cut per round trip) mirror the strategy.
 */
export function runAdaptive(algo: LimitAlgorithm, dep: Dependency, opts: { duration: number; initialLimit: number; minLimit: number; maxLimit: number; seed?: number }): AdaptiveRun {
  const random = seeded(opts.seed ?? 7);
  const DT = 5;
  let limit = opts.initialLimit;
  let inflight = 0;
  let admittedSeq = 0;
  let cutAt = 0;
  let carry = 0;
  let rejected = 0;
  let latency = dep.base;
  const running: { end: number; start: number; inflight: number; dropped: boolean; seq: number }[] = [];
  const points: AdaptivePoint[] = [];
  const drops: number[] = [];

  for (let t = 0; t <= opts.duration; t += DT) {
    const slow = dep.slow && t >= dep.slow[0] && t < dep.slow[1];
    const cap = slow ? dep.slowCapacity : dep.capacity;
    // Finish calls due by now.
    for (let i = running.length - 1; i >= 0; i--) {
      const r = running[i]!;
      if (r.end > t) continue;
      running.splice(i, 1);
      inflight--;
      if (r.dropped && r.seq <= cutAt) continue;
      if (r.dropped) {
        cutAt = admittedSeq;
        drops.push(t);
      }
      let next = algo.onSample({ rtt: r.end - r.start, inflight: r.inflight, dropped: r.dropped, limit });
      if (next > limit && r.inflight * 2 < limit) next = limit;
      limit = Math.min(opts.maxLimit, Math.max(opts.minLimit, Number.isFinite(next) ? next : limit));
    }
    // Admit this step's arrivals.
    carry += (dep.rate * DT) / 1000;
    let sum = 0;
    let n = 0;
    for (; carry >= 1; carry--) {
      if (inflight >= Math.floor(limit)) {
        rejected++;
        continue;
      }
      inflight++;
      const rtt = dep.base * Math.max(1, inflight / cap) * (0.85 + random() * 0.3);
      sum += Math.min(rtt, dep.timeout);
      n++;
      running.push({ start: t, end: t + Math.min(rtt, dep.timeout), inflight, dropped: rtt > dep.timeout, seq: ++admittedSeq });
    }
    if (n) latency = sum / n;
    if (t % 100 === 0) points.push({ t, limit: Math.floor(limit), inflight, capacity: cap, latency });
  }
  return { points, drops, rejected, admitted: admittedSeq };
}
