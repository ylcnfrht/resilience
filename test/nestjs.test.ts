import 'reflect-metadata';
import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { BrokenCircuitError, ResiliencePipelineRegistry, TimeoutRejectedError } from '../src/index.ts';
import { CircuitBreaker, Fallback, ResilienceModule, Retry, Timeout, UsePipeline, getResilienceSignal } from '../src/nestjs.ts';

// Never settles: only the timeout can end the attempt (no race against a sleep).
const hang = () => new Promise<never>(() => {});

// Type stripping can't run `@decorator` syntax, so apply decorators the way TypeScript does: listed top to bottom, applied bottom-up.
function decorate(cls: { prototype: object }, key: string, ...decorators: MethodDecorator[]) {
  const descriptor = Object.getOwnPropertyDescriptor(cls.prototype, key)!;
  for (const d of [...decorators].reverse()) d(cls.prototype, key, descriptor);
}

class PaymentService {
  flakyCalls = 0;
  slowCalls = 0;
  sharedCalls = 0;

  async flaky() {
    if (++this.flakyCalls < 3) throw new Error('flaky');
    return getResilienceSignal() instanceof AbortSignal ? 'ok, signal available' : 'no signal';
  }
  async slowThenFast() {
    if (++this.slowCalls < 3) await hang();
    return 'fast';
  }
  async down() {
    this.sharedCalls++;
    throw new Error('down');
  }
  async otherDown() {
    this.sharedCalls++;
    throw new Error('down');
  }
  async withFallback(): Promise<string> {
    throw new Error('nope');
  }
  plain() {
    return this;
  }
}
decorate(PaymentService, 'flaky', Retry({ delay: 0 }));
decorate(PaymentService, 'slowThenFast', Retry({ delay: 0, maxRetryAttempts: 3 }), Timeout(20)); // retry outermost → each attempt gets 20ms
decorate(PaymentService, 'down', UsePipeline('upstream'));
decorate(PaymentService, 'otherDown', UsePipeline('upstream'));
decorate(PaymentService, 'withFallback', Fallback({ fallbackAction: () => 'cached' }), CircuitBreaker());
Injectable()(PaymentService);

class AppModule {}
Module({
  imports: [
    ResilienceModule.forRoot({
      pipelines: { upstream: (b) => b.addCircuitBreaker({ failureRatio: 0.5, minimumThroughput: 2, breakDuration: 1000 }) },
    }),
  ],
  providers: [PaymentService],
})(AppModule);

test('nestjs: decorators wrap provider methods with their pipelines', async () => {
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  try {
    const svc = app.get(PaymentService);

    assert.equal(await svc.flaky(), 'ok, signal available');
    assert.equal(svc.flakyCalls, 3);

    assert.equal(await svc.slowThenFast(), 'fast');
    assert.equal(svc.slowCalls, 3);

    // Two methods share the named pipeline's circuit breaker.
    await assert.rejects(svc.down(), /down/);
    await assert.rejects(svc.otherDown(), /down/);
    await assert.rejects(svc.down(), BrokenCircuitError);
    assert.equal(svc.sharedCalls, 2);

    assert.equal(await svc.withFallback(), 'cached');
    assert.equal(svc.plain(), svc, 'undecorated methods are untouched');
    assert.ok(app.get(ResiliencePipelineRegistry).getPipeline('upstream'));
  } finally {
    await app.close();
  }
});

test('nestjs: timeout decorator alone', async () => {
  class Slow {
    async run() {
      await hang();
    }
  }
  decorate(Slow, 'run', Timeout(10));
  Injectable()(Slow);
  class M {}
  Module({ imports: [ResilienceModule.forRoot()], providers: [Slow] })(M);
  const app = await NestFactory.createApplicationContext(M, { logger: false });
  try {
    await assert.rejects(app.get(Slow).run(), TimeoutRejectedError);
  } finally {
    await app.close();
  }
});
