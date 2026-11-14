import { type MaybePromise, type ResilienceContext, isTimeoutError } from './core.ts';
import { type HttpAdapter, type HttpResilienceOptions, type RequestResilience, httpExecutor, toUrl } from './fetch.ts';

// Structural types only: this module must not import axios.
import type {} from 'axios';

/** The parts of an axios response we read. */
export interface AxiosLikeResponse {
  status: number;
  headers: unknown;
  data?: unknown;
}

/** The parts of an axios request config we read. */
export interface AxiosLikeConfig {
  url?: string;
  baseURL?: string;
  method?: string;
  headers?: unknown;
  signal?: unknown;
  resilience?: RequestResilience<AxiosLikeResponse>;
  [key: string]: any;
}

type AxiosLikeAdapter = (config: any) => Promise<any>;

/** An axios instance (`axios.create()`) or the default `axios` export. */
export interface AxiosLike {
  defaults: { adapter?: unknown };
  /** Present on the default export. */
  getAdapter?(adapters: any, config?: any): AxiosLikeAdapter;
}

export interface AxiosResilienceOptions extends HttpResilienceOptions<AxiosLikeResponse> {
  /**
   * `axios.getAdapter`, to resolve adapter names like `['xhr', 'http', 'fetch']` for an `axios.create()` instance.
   * Default: taken from the default export, or loaded with `import('axios')` on first use (pass it where that fails, e.g. bundled browser code).
   */
  getAdapter?: AxiosLike['getAdapter'];
  /** Called before every attempt; return a replacement config, e.g. with a refreshed auth header. */
  onBeforeAttempt?: (config: AxiosLikeConfig, context: ResilienceContext) => MaybePromise<AxiosLikeConfig | void>;
}

export interface AxiosResilience {
  /** Restores the instance's previous adapter and releases the pipelines. */
  eject(): void;
}

declare module 'axios' {
  interface AxiosRequestConfig {
    /** Per-request overrides for `attachResilience`: `axios.get(url, { resilience: { retries: 0 } })`. */
    resilience?: RequestResilience<AxiosLikeResponse>;
  }
}

// Errors without a response worth retrying: connection failures and axios' own per-request timeout.
const NETWORK_CODES = new Set(['ECONNRESET', 'ECONNREFUSED', 'ECONNABORTED', 'ETIMEDOUT', 'EPIPE', 'EAI_AGAIN', 'ENETUNREACH', 'EHOSTUNREACH', 'ERR_NETWORK', 'UND_ERR_SOCKET']);

const axiosAdapter: HttpAdapter<AxiosLikeResponse> = {
  // A non-2xx status arrives as an AxiosError carrying the response.
  response: (o) => (o.ok ? o.result : (o.error as { response?: AxiosLikeResponse } | null)?.response),
  header: (r, name) => {
    const h = r.headers as { get?: (name: string) => unknown; [key: string]: unknown } | undefined;
    const value = typeof h?.get === 'function' ? h.get(name) : h?.[name];
    return typeof value === 'string' ? value : value == null ? undefined : String(value);
  },
  isTransientError: (e) => isTimeoutError(e) || NETWORK_CODES.has((e as { code?: string } | null)?.code ?? ''),
  // A `responseType: 'stream'` body would keep its socket open.
  discard: (r) => (r.data as { destroy?: () => void } | null)?.destroy?.(),
};

/**
 * Runs every request of an axios instance through a resilience pipeline (total timeout → retry → per host: circuit
 * breaker → attempt timeout), by wrapping its adapter. Only idempotent methods (or requests with an `Idempotency-Key`
 * header) are retried. Override per request with `config.resilience`.
 *
 * ```ts
 * const api = axios.create({ baseURL: 'https://api.example.com' });
 * const resilience = attachResilience(api, { name: 'api' });
 * await api.post('/orders', body, { resilience: { retries: 0 } });
 * resilience.eject();
 * ```
 */
export function attachResilience(instance: AxiosLike, options: AxiosResilienceOptions = {}): AxiosResilience {
  const previous = instance.defaults.adapter;
  const executor = httpExecutor(options, axiosAdapter);
  let getAdapter: Promise<NonNullable<AxiosLike['getAdapter']>> | undefined;
  const given = options.getAdapter ?? instance.getAdapter?.bind(instance);
  const axiosPackage = 'axios'; // not a literal in import(): bundlers must not pull axios into this module
  const resolve = async (config: AxiosLikeConfig): Promise<AxiosLikeAdapter> => {
    if (typeof previous === 'function') return previous as AxiosLikeAdapter;
    // The default (['xhr', 'http', 'fetch']) is a list of names only axios can resolve.
    getAdapter ??= given ? Promise.resolve(given) : import(axiosPackage).then((m: { default: Required<AxiosLike> }) => m.default.getAdapter);
    return (await getAdapter)(previous, config);
  };

  const adapter = (config: AxiosLikeConfig) => {
    const { resilience, signal } = config;
    const headers = config.headers as { has?: (name: string) => boolean } | undefined;
    return executor.execute(
      {
        url: toUrl(config.url, config.baseURL),
        method: config.method ?? 'GET',
        idempotencyKey: Boolean(headers?.has?.('idempotency-key')),
        body: config.data,
        resilience,
        signal: signal as AbortSignal | undefined,
      },
      async (context) => {
        let attempt: AxiosLikeConfig = { ...config, signal: context.signal };
        if (options.onBeforeAttempt) attempt = (await options.onBeforeAttempt(attempt, context)) ?? attempt;
        return (await resolve(attempt))(attempt);
      },
    );
  };
  instance.defaults.adapter = adapter;

  return {
    eject() {
      if (instance.defaults.adapter === adapter) instance.defaults.adapter = previous;
      executor.dispose();
    },
  };
}
