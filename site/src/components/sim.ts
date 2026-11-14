// Shared by the interactive demos: a seeded random source and a simulated flaky dependency.

/** mulberry32: small, fast, seeded PRNG in [0, 1). */
export function seeded(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Resolves after `ms`, rejects with an AbortError as soon as `signal` aborts (so timeouts and hedging cancel it). */
export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const abort = () => {
      clearTimeout(id);
      reject(signal?.reason ?? new DOMException('Aborted', 'AbortError'));
    };
    const id = setTimeout(() => {
      signal?.removeEventListener('abort', abort);
      resolve();
    }, ms);
    if (signal?.aborted) abort();
    else signal?.addEventListener('abort', abort, { once: true });
  });
}

export interface ServiceConfig {
  /** 0..100 */
  failureRate: number;
  latency: number;
  jitter: number;
  outage: boolean;
  slowdown: boolean;
  /** Probability (0..1) that a call hangs for `hangMs`. */
  hang?: number;
  hangMs?: number;
}

/** A dependency that fails and stalls according to `config()` (read on every call, so sliders apply live). */
export function flakyService(config: () => ServiceConfig, random: () => number) {
  return async (signal?: AbortSignal): Promise<string> => {
    const c = config();
    const failed = c.outage || random() * 100 < c.failureRate;
    let ms = c.latency + random() * c.jitter;
    if (c.slowdown) ms *= 5;
    if (c.hang && random() < c.hang) ms = c.hangMs ?? 3000;
    // A hard outage fails fast, like a refused connection.
    await sleep(c.outage ? 20 : ms, signal);
    if (failed) throw new Error('503 Service Unavailable');
    return 'user #42';
  };
}

export const fmtMs = (ms: number): string => (ms >= 1000 ? `${+(ms / 1000).toFixed(ms >= 10_000 ? 1 : 2)} s` : `${Math.round(ms)} ms`);
