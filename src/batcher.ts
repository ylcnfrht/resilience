import { type Duration, type ResilienceContext, type TimeProvider, ambient, check, systemTime, toMs } from './core.ts';
import { ResiliencePipeline } from './pipeline.ts';

export interface BatcherOptions<I, R> {
  /** Flush as soon as the batch holds this many (distinct) items. */
  maxSize: number;
  /** Flush this long after the first item of a batch arrived. */
  maxWait: Duration;
  /**
   * Loads a batch. Return one result per item, in order, or a Map by `key`. A result that is an `Error` rejects only that item.
   * Gets the pipeline's context: pass `context.signal` on to your I/O.
   */
  run: (items: I[], context: ResilienceContext) => Promise<ReadonlyArray<R | Error> | ReadonlyMap<string, R | Error>>;
  /** Items with the same key share one slot in a batch (and one result). Required when `run` returns a Map. */
  key?: (item: I) => string;
  /** Each batch runs through it as a unit (retried, timed out, rate limited together). */
  pipeline?: ResiliencePipeline;
  timeProvider?: TimeProvider;
}

export interface Batcher<I, R> {
  /** Queues the item for the next batch. Aborting `signal` (default: the ambient pipeline's signal) rejects this call and, until the batch starts, drops the item from it. */
  load(item: I, options?: { signal?: AbortSignal }): Promise<R>;
}

interface Waiter<R> {
  resolve(value: R): void;
  reject(error: unknown): void;
}
interface Slot<I, R> {
  item: I;
  key: string;
  waiters: Set<Waiter<R>>;
}

/**
 * DataLoader / bottleneck-Batcher style batching:
 * ```ts
 * const users = createBatcher({ maxSize: 100, maxWait: '5ms', key: (id) => id, run: (ids, { signal }) => db.usersByIds(ids, { signal }) });
 * const [a, b] = await Promise.all([users.load('1'), users.load('2')]); // one query
 * ```
 */
export function createBatcher<I, R>(options: BatcherOptions<I, R>): Batcher<I, R> {
  const { maxSize, run, key } = options;
  check(Number.isInteger(maxSize) && maxSize > 0, 'maxSize must be a positive integer');
  const maxWait = toMs(options.maxWait, 'maxWait');
  const pipeline = options.pipeline ?? ResiliencePipeline.empty;
  const time = options.timeProvider ?? systemTime;
  let pending = new Map<string, Slot<I, R>>();
  let timer: unknown;
  let seq = 0;

  const flush = () => {
    time.clearTimeout(timer);
    timer = undefined;
    const slots = [...pending.values()];
    pending = new Map();
    if (!slots.length) return;
    const fail = (slot: Slot<I, R>, error: unknown) => slot.waiters.forEach((w) => w.reject(error));
    const settle = (slot: Slot<I, R>, value: R | Error) => (value instanceof Error ? fail(slot, value) : slot.waiters.forEach((w) => w.resolve(value)));
    // A batch is shared by many callers: run it outside whichever caller's execution triggered the flush, so one
    // caller's deadline doesn't cancel the others' items.
    ambient
      .run(undefined as unknown as ResilienceContext, () => pipeline.execute((context) => run(slots.map((s) => s.item), context)))
      .then((results) => {
        if (results instanceof Map) {
          check(key !== undefined, 'Batcher: run returned a Map but no `key` option is set');
          for (const slot of slots) results.has(slot.key) ? settle(slot, results.get(slot.key)) : fail(slot, new Error(`Batcher: no result for key ${slot.key}`));
        } else {
          const list = results as ReadonlyArray<R | Error>;
          check(list.length === slots.length, `Batcher: run returned ${list.length} results for ${slots.length} items`);
          slots.forEach((slot, i) => settle(slot, list[i]!));
        }
      })
      .catch((error: unknown) => slots.forEach((slot) => fail(slot, error)));
  };

  return {
    load(item, loadOptions = {}) {
      const signal = loadOptions.signal ?? ambient.getStore()?.signal;
      if (signal?.aborted) return Promise.reject(signal.reason);
      const k = key ? key(item) : String(seq++);
      let slot = pending.get(k);
      if (!slot) {
        slot = { item, key: k, waiters: new Set() };
        pending.set(k, slot);
        if (pending.size === 1) timer = time.setTimeout(flush, maxWait);
      }
      const current = slot;
      const promise = new Promise<R>((resolve, reject) => {
        const onAbort = () => {
          current.waiters.delete(waiter);
          if (!current.waiters.size && pending.get(k) === current) pending.delete(k);
          if (!pending.size) time.clearTimeout(timer);
          reject(signal!.reason);
        };
        const waiter: Waiter<R> = {
          resolve: (v) => (signal?.removeEventListener('abort', onAbort), resolve(v)),
          reject: (e) => (signal?.removeEventListener('abort', onAbort), reject(e)),
        };
        current.waiters.add(waiter);
        signal?.addEventListener('abort', onAbort, { once: true });
      });
      if (pending.size >= maxSize) flush();
      return promise;
    },
  };
}
