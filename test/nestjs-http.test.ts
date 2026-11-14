// Real Nest HTTP routing for decorated controllers. @nestjs/platform-express isn't installed, so a minimal node:http
// adapter (AbstractHttpAdapter) stands in: Nest's own router, exception filter and response metadata still run.
import 'reflect-metadata';
import assert from 'node:assert/strict';
import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { test } from 'node:test';
import { Controller, Get, HttpCode, Module } from '@nestjs/common';
import { AbstractHttpAdapter, NestFactory } from '@nestjs/core';
import { ResilienceModule, Retry, Timeout, getResilienceSignal } from '../src/nestjs.ts';

class NodeHttpAdapter extends AbstractHttpAdapter<http.Server, http.IncomingMessage, http.ServerResponse> {
  readonly routes = new Map<string, (req: unknown, res: unknown, next: () => void) => unknown>();
  #notFound?: (req: unknown, res: unknown) => unknown;
  constructor() {
    super({ use() {} });
    this.instance.get = (path: string, handler: (req: unknown, res: unknown, next: () => void) => unknown) => void this.routes.set(`GET ${path}`, handler);
  }
  initHttpServer() {
    this.httpServer = http.createServer((req, res) => {
      const handler = this.routes.get(`${req.method} ${req.url}`);
      void (handler ? handler(req, res, () => {}) : this.#notFound?.(req, res));
    });
  }
  close() {
    return new Promise<void>((resolve) => (this.httpServer.closeAllConnections(), this.httpServer.close(() => resolve())));
  }
  reply(res: http.ServerResponse, body: unknown, status?: number) {
    if (status) res.statusCode = status;
    res.end(typeof body === 'string' ? body : JSON.stringify(body));
  }
  status(res: http.ServerResponse, code: number) {
    res.statusCode = code;
  }
  end(res: http.ServerResponse, message?: string) {
    res.end(message);
  }
  isHeadersSent(res: http.ServerResponse) {
    return res.headersSent;
  }
  getHeader(res: http.ServerResponse, name: string) {
    return res.getHeader(name);
  }
  setHeader(res: http.ServerResponse, name: string, value: string) {
    res.setHeader(name, value);
  }
  appendHeader(res: http.ServerResponse, name: string, value: string) {
    res.appendHeader(name, value);
  }
  getRequestMethod(req: http.IncomingMessage) {
    return req.method;
  }
  getRequestUrl(req: http.IncomingMessage) {
    return req.url;
  }
  getRequestHostname(req: http.IncomingMessage) {
    return req.headers.host;
  }
  setNotFoundHandler(handler: (req: unknown, res: unknown) => unknown) {
    this.#notFound = handler;
  }
  setErrorHandler() {}
  registerParserMiddleware() {}
  enableCors() {}
  useStaticAssets() {}
  setViewEngine() {}
  render() {}
  redirect() {}
  createMiddlewareFactory() {
    return () => {};
  }
  getType() {
    return 'node-http';
  }
  applyVersionFilter(handler: Function) {
    return handler as never;
  }
}

function decorate(cls: { prototype: object }, key: string, ...decorators: MethodDecorator[]) {
  const descriptor = Object.getOwnPropertyDescriptor(cls.prototype, key)!;
  for (const d of [...decorators].reverse()) d(cls.prototype, key, descriptor);
}

test('nestjs over HTTP: decorated controller routes run through their pipelines, with route metadata kept', async () => {
  class OrdersController {
    flakyCalls = 0;
    async flaky() {
      if (++this.flakyCalls < 3) throw new Error('flaky');
      return getResilienceSignal() instanceof AbortSignal ? 'created' : 'no signal';
    }
    async hang() {
      return new Promise<never>(() => {}); // only the timeout ends it
    }
  }
  decorate(OrdersController, 'flaky', Get('flaky'), HttpCode(201), Retry({ delay: 0 }));
  decorate(OrdersController, 'hang', Get('hang'), Timeout(20));
  Controller('orders')(OrdersController);
  class AppModule {}
  Module({ imports: [ResilienceModule.forRoot()], controllers: [OrdersController] })(AppModule);

  const app = await NestFactory.create(AppModule, new NodeHttpAdapter(), { logger: false });
  await app.init();
  const server = app.getHttpServer() as http.Server;
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  try {
    const created = await fetch(`${url}/orders/flaky`);
    assert.equal(created.status, 201, '@HttpCode metadata copied to the wrapped handler');
    assert.equal(await created.text(), 'created');
    assert.equal(app.get(OrdersController).flakyCalls, 3, 'retried inside the route');

    const timedOut = await fetch(`${url}/orders/hang`);
    assert.equal(timedOut.status, 500, "Nest's exception filter answers the TimeoutRejectedError");
  } finally {
    await app.close();
  }
});
