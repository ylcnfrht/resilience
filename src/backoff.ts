import { type Duration, check, toMs } from './core.ts';
import { type Jitter, computeDelay } from './retry.ts';

type Attempt = number | { readonly attemptNumber: number };

export interface BackoffOptions {
  /** Base delay. */
  delay: Duration;
  /** Cap for a single delay. */
  maxDelay?: Duration;
  /** Same as retry's `useJitter`, minus `true` (Polly's decorrelated jitter needs state across attempts). Default false. */
  useJitter?: Exclude<Jitter, true>;
  randomizer?: () => number;
}

const make = (type: 'constant' | 'linear' | 'exponential', o: BackoffOptions, factor = 2) => {
  const base = toMs(o.delay, 'delay');
  const max = o.maxDelay === undefined ? undefined : toMs(o.maxDelay, 'maxDelay');
  check(base >= 0 && factor >= 1, 'delay must be >= 0 and factor >= 1');
  return (attempt: Attempt): number => computeDelay(type, o.useJitter ?? false, typeof attempt === 'number' ? attempt : attempt.attemptNumber, base, max, undefined, o.randomizer, factor);
};

/**
 * Delay schedules as plain functions of the attempt number (0-based), for `delayGenerator`, `waitFor({ interval })`,
 * or your own loops: `addRetry({ delayGenerator: backoff.exponential({ delay: '100ms', maxDelay: '10s', useJitter: 'full' }) })`.
 */
export const backoff = {
  constant: (options: BackoffOptions) => make('constant', options),
  linear: (options: BackoffOptions) => make('linear', options),
  exponential: (options: BackoffOptions & { factor?: number }) => make('exponential', options, options.factor ?? 2),
  /** A fixed list of delays (cockatiel's IterableBackoff); the last one repeats: `backoff.sequence(['100ms', '1s', '5s'])`. */
  sequence: (delays: readonly Duration[]) => {
    check(delays.length > 0, 'sequence needs at least one delay');
    const ms = delays.map((d) => toMs(d, 'delay'));
    return (attempt: Attempt): number => ms[Math.min(typeof attempt === 'number' ? attempt : attempt.attemptNumber, ms.length - 1)]!;
  },
};
