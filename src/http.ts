import { type InboundOptions, HTTP_REQUEST, describeResilienceError, inboundPipeline, rateLimitHeaders, rejection } from './inbound.ts';

export { type InboundOptions, type ResilienceErrorDescription, HTTP_REQUEST, describeResilienceError } from './inbound.ts';

/** HTTP status for a resilience error: 504 timeout, 429 rate limited, 503 open circuit / full bulkhead, 409 lock held; undefined for other errors. */
export function httpStatusFor(error: unknown): number | undefined {
  return describeResilienceError(error)?.httpStatus;
}

/** `Retry-After` seconds for a rejection, when known. */
export function retryAfterSeconds(error: unknown): number | undefined {
  return describeResilienceError(error)?.retryAfter;
}

interface NodeRequest {
  once(event: 'close', listener: () => void): unknown;
}
interface NodeResponse {
  readonly headersSent: boolean;
  readonly writableFinished?: boolean;
  statusCode: number;
  setHeader(name: string, value: string | number): unknown;
  end(body?: string): unknown;
  once(event: 'close' | 'finish', listener: () => void): unknown;
}

/**
 * Express / Connect middleware: request timeout, load shedding, client-disconnect cancellation and deadline propagation.
 * `app.use(resilienceMiddleware({ timeout: '5s', concurrency: { permitLimit: 200, queueLimit: 100 } }))`
 */
export function resilienceMiddleware(options: InboundOptions = {}) {
  const pipeline = inboundPipeline(options);
  return (req: NodeRequest, res: NodeResponse, next: (error?: unknown) => void): void => {
    const disconnected = new AbortController();
    let handed = false;
    const properties = new Map<string, unknown>([[HTTP_REQUEST, req]]);
    const setHeaders = () => {
      for (const [name, value] of Object.entries(rateLimitHeaders(properties, options.rateLimitHeaders))) res.setHeader(name, value);
    };
    res.once('close', () => {
      if (!res.writableFinished) disconnected.abort(new Error('Client disconnected'));
    });
    pipeline
      .execute(
        () =>
          new Promise<void>((done) => {
            setHeaders();
            res.once('finish', done);
            res.once('close', done);
            handed = true;
            next();
          }),
        { signal: disconnected.signal, properties },
      )
      .catch((error: unknown) => {
        const r = rejection(error, properties, options.rateLimitHeaders);
        if (res.headersSent || disconnected.signal.aborted) return;
        // Not a resilience error (e.g. an injected chaos fault) before the handler ran: let Express's error handler respond.
        if (!r) return void (handed ? undefined : next(error));
        for (const [name, value] of Object.entries(r.headers)) res.setHeader(name, value);
        res.statusCode = r.status;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(r.body));
      });
  };
}

/** Hono middleware: `app.use(honoResilience({ timeout: '5s' }))`. Responds 504/503/429 on resilience errors. */
export function honoResilience(options: InboundOptions = {}) {
  const pipeline = inboundPipeline(options);
  return async (
    c: {
      req: { raw: Request };
      json(body: unknown, status: number, headers?: Record<string, string>): Response;
      header?(name: string, value: string): void;
      error?: unknown;
      res?: Response;
    },
    next: () => Promise<void>,
  ) => {
    const properties = new Map<string, unknown>([[HTTP_REQUEST, c]]);
    // A downstream error (even another pipeline's rejection) is Hono's to handle, like in Express and Fastify.
    let thrown: { error: unknown } | undefined;
    const reply = (error: unknown) => {
      const r = rejection(error, properties, options.rateLimitHeaders);
      if (!r) throw error;
      const res = c.json(r.body, r.status, Object.keys(r.headers).length ? r.headers : undefined);
      // Hono ignores a returned Response once app.onError has finalized one; assign it explicitly.
      if ('res' in c) c.res = res;
      return res;
    };
    let handler: Promise<unknown> | undefined;
    try {
      await pipeline.execute(() => ((handler = next().catch((error) => void (thrown = { error })))), { signal: c.req.raw.signal, properties });
      for (const [name, value] of Object.entries(rateLimitHeaders(properties, options.rateLimitHeaders))) c.header?.(name, value);
    } catch (error) {
      // An aborted handler usually fails within the same tick and app.onError would overwrite our answer; give it one
      // macrotask to settle first, without letting a handler that ignores its signal delay the response.
      if (handler) await Promise.race([handler, new Promise((r) => setTimeout(r, 0))]);
      return reply(error);
    }
    if (thrown) throw thrown.error;
  };
}
