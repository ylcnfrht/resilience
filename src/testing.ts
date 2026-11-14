import { type Duration, type TimeProvider, toMs } from './core.ts';

interface Timer {
  at: number;
  callback: () => void;
}

// A real macrotask lets every pending promise continuation run before the next timer fires.
const flush = () => new Promise<void>((resolve) => globalThis.setTimeout(resolve, 0));

/**
 * A clock you move by hand, so retries, timeouts and open circuits can be tested without waiting.
 *
 * ```ts
 * const time = new FakeTimeProvider();
 * const pipeline = new ResiliencePipelineBuilder({ timeProvider: time }).addRetry({ delay: '10s' }).addTimeout('1s').build();
 * // Attach the expectation first: the promise settles while runAll() fires timers.
 * const done = assert.rejects(pipeline.execute(() => neverResolves()), TimeoutRejectedError);
 * await time.runAll(); // 4 timeouts and 3 delays, instantly
 * await done;
 * ```
 */
export class FakeTimeProvider implements TimeProvider {
  #now: number;
  #nextId = 1;
  readonly #timers = new Map<number, Timer>();

  constructor(start: number | Date = Date.UTC(2026, 0, 1)) {
    this.#now = typeof start === 'number' ? start : start.getTime();
  }

  now(): number {
    return this.#now;
  }

  setTimeout(callback: () => void, ms: number): number {
    const id = this.#nextId++;
    this.#timers.set(id, { at: this.#now + Math.max(0, ms), callback });
    return id;
  }

  clearTimeout(handle: unknown): void {
    this.#timers.delete(handle as number);
  }

  /** Timers scheduled and not yet fired. */
  get pendingTimers(): number {
    return this.#timers.size;
  }

  /** Moves the clock forward, firing every timer that comes due, in order. */
  async advance(duration: Duration): Promise<void> {
    const target = this.#now + toMs(duration, 'advance');
    await flush();
    for (let next = this.#next(); next && next[1].at <= target; next = this.#next()) {
      await this.#fire(next);
    }
    this.#now = target;
    await flush();
  }

  /** Fires timers until none are left (including ones scheduled while firing). Throws after `limit` timers to catch infinite loops. */
  async runAll(limit = 10_000): Promise<void> {
    await flush();
    for (let fired = 0, next = this.#next(); next; next = this.#next()) {
      if (++fired > limit) throw new Error(`FakeTimeProvider.runAll: more than ${limit} timers fired; is something rescheduling forever?`);
      await this.#fire(next);
    }
  }

  async #fire([id, timer]: [number, Timer]) {
    this.#timers.delete(id);
    this.#now = Math.max(this.#now, timer.at);
    timer.callback();
    await flush();
  }

  #next(): [number, Timer] | undefined {
    let best: [number, Timer] | undefined;
    for (const entry of this.#timers) if (!best || entry[1].at < best[1].at) best = entry;
    return best;
  }
}
