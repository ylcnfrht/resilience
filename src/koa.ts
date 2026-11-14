import { HTTP_REQUEST, type InboundOptions, inboundPipeline, rateLimitHeaders, rejection } from './inbound.ts';

interface KoaContextLike {
  readonly res: { readonly writableFinished?: boolean; once(event: 'close', listener: () => void): unknown };
  status: number;
  body: unknown;
  set(headers: Record<string, string>): void;
}

/**
 * Koa middleware: request timeout with deadline propagation, load shedding, per-key rate limit (`key` receives Koa's `ctx`)
 * with RateLimit-* headers, client-disconnect cancellation; resilience errors become 504/503/429/409/403 JSON.
 * `app.use(koaResilience({ timeout: '5s', concurrency: { permitLimit: 200, queueLimit: 100 } }))`
 */
export function koaResilience(options: InboundOptions = {}) {
  const pipeline = inboundPipeline(options);
  return async (ctx: KoaContextLike, next: () => Promise<unknown>): Promise<void> => {
    const disconnected = new AbortController();
    const properties = new Map<string, unknown>([[HTTP_REQUEST, ctx]]);
    ctx.res.once('close', () => {
      if (!ctx.res.writableFinished) disconnected.abort(new Error('Client disconnected'));
    });
    // A downstream error (even another pipeline's rejection) is Koa's to handle, like in Express and Fastify.
    let thrown: { error: unknown } | undefined;
    const reply = (error: unknown) => {
      const r = rejection(error, properties, options.rateLimitHeaders);
      if (!r) throw error;
      ctx.status = r.status;
      ctx.set(r.headers);
      ctx.body = r.body;
    };
    try {
      await pipeline.execute(
        () => {
          ctx.set(rateLimitHeaders(properties, options.rateLimitHeaders));
          return next().catch((error) => void (thrown = { error }));
        },
        { signal: disconnected.signal, properties },
      );
    } catch (error) {
      return reply(error);
    }
    if (thrown) throw thrown.error;
  };
}
