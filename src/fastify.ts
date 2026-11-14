import { type ResilienceContext, ambient } from './core.ts';
import { HTTP_REQUEST, type InboundOptions, inboundPipeline, isOwnRejection, rateLimitHeaders, rejection } from './inbound.ts';

interface RawResponse {
  readonly headersSent: boolean;
  readonly writableFinished?: boolean;
  once(event: 'close' | 'finish', listener: () => void): unknown;
}
interface FastifyReplyLike {
  readonly raw: RawResponse;
  code(status: number): FastifyReplyLike;
  headers(values: Record<string, string>): FastifyReplyLike;
  send(body?: unknown): FastifyReplyLike;
}
type RouteHandler = (this: unknown, request: any, reply: any) => unknown;
// Loose on purpose: Fastify's overloaded addHook isn't assignable to a narrower structural overload set.
interface FastifyInstanceLike {
  addHook(...args: any[]): unknown;
}

const CONTEXT = Symbol('resilience-pipeline.context');
const PROPERTIES = Symbol('resilience-pipeline.properties');

/**
 * Fastify plugin: request timeout with deadline propagation, load shedding, per-key rate limit with RateLimit-* headers,
 * client-disconnect cancellation; resilience errors become 504/503/429/409/403 JSON (+ `Retry-After`).
 * Applies to the instance it is registered on (it skips encapsulation, like fastify-plugin). Register it before your
 * routes and await it, so its `onRoute` hook sees them:
 * `await app.register(fastifyResilience, { timeout: '5s', rateLimit: { key: (req) => req.ip, permitLimit: 100, window: '1m' } })`
 */
export function fastifyResilience(instance: FastifyInstanceLike, options: InboundOptions, done: (error?: Error) => void): void {
  const pipeline = inboundPipeline(options);
  // The pipeline callback runs in onRequest, but Fastify reaches the handler from other async resources (body parsing,
  // async hooks), which loses AsyncLocalStorage. Re-entering the request's context around the handler makes nested
  // pipelines inherit its deadline whatever ran in between.
  instance.addHook('onRoute', (route: { handler: RouteHandler }) => {
    const handler = route.handler;
    route.handler = function (request: any, reply: any) {
      const context: ResilienceContext | undefined = request[CONTEXT];
      if (!context) return handler.call(this, request, reply);
      const out = ambient.run(context, () => handler.call(this, request, reply));
      // The request's own deadline surfacing through a nested pipeline is still this plugin's 504 (only async handlers can hit it).
      return typeof (out as PromiseLike<unknown> | undefined)?.then !== 'function'
        ? out
        : Promise.resolve(out).catch((error: unknown) => {
            const r = isOwnRejection(error, context.signal) ? rejection(error, request[PROPERTIES], options.rateLimitHeaders) : undefined;
            if (!r) throw error;
            return reply.code(r.status).headers(r.headers).send(r.body);
          });
    };
  });
  instance.addHook('onRequest', (request: any, reply: FastifyReplyLike, next: (error?: Error) => void) => {
    const res = reply.raw;
    const disconnected = new AbortController();
    const properties = new Map<string, unknown>([[HTTP_REQUEST, request]]);
    let started = false;
    res.once('close', () => {
      if (!res.writableFinished) disconnected.abort(new Error('Client disconnected'));
    });
    pipeline
      .execute(
        (context) =>
          new Promise<void>((finished) => {
            started = true;
            request[CONTEXT] = context;
            request[PROPERTIES] = properties;
            reply.headers(rateLimitHeaders(properties, options.rateLimitHeaders));
            res.once('finish', finished);
            res.once('close', finished);
            next();
          }),
        { signal: disconnected.signal, properties },
      )
      .catch((error: unknown) => {
        const r = rejection(error, properties, options.rateLimitHeaders);
        if (!r) return void (started || next(error as Error));
        if (res.headersSent || disconnected.signal.aborted) return;
        reply.code(r.status).headers(r.headers).send(r.body);
      });
  });
  done();
}

// Same effect as wrapping with fastify-plugin: hooks land on the parent instance, not an encapsulated child.
Object.assign(fastifyResilience, { [Symbol.for('skip-override')]: true, [Symbol.for('fastify.display-name')]: 'resilience-pipeline' });
