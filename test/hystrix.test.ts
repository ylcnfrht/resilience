import assert from 'node:assert/strict';
import { test } from 'node:test';
import { createHystrixStream } from '../src/hystrix.ts';
import { ResiliencePipelineBuilder } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

test('hystrix: HystrixCommand metrics per pipeline, and an event-stream handler', async () => {
  const time = new FakeTimeProvider();
  const stream = createHystrixStream({ pipelines: ['hystrix-payments', 'hystrix-idle'], interval: '1s', timeProvider: time });
  const pipeline = new ResiliencePipelineBuilder({ name: 'hystrix-payments', timeProvider: time }).addCircuitBreaker({ consecutiveFailures: 2, breakDuration: '1m' }).build();
  await pipeline.execute(() => 'ok');
  for (let i = 0; i < 3; i++) await pipeline.execute(() => Promise.reject(new Error('boom'))).catch(() => {}); // 2 failures open it, 1 short-circuit

  const [payments, idle] = stream.commands() as Array<Record<string, any>>;
  assert.equal(payments!.type, 'HystrixCommand');
  assert.equal(payments!.name, 'hystrix-payments');
  assert.equal(payments!.isCircuitBreakerOpen, true);
  assert.equal(payments!.requestCount, 4);
  assert.equal(payments!.errorCount, 3);
  assert.equal(payments!.errorPercentage, 75);
  assert.equal(payments!.rollingCountSuccess, 1);
  assert.equal(payments!.rollingCountFailure, 2);
  assert.equal(payments!.rollingCountShortCircuited, 1);
  assert.deepEqual(Object.keys(payments!.latencyExecute).map(Number).sort((a, b) => a - b), [0, 25, 50, 75, 90, 95, 99, 99.5, 100]);
  assert.equal(idle!.requestCount, 0);

  const written: string[] = [];
  let close!: () => void;
  let head: unknown;
  stream.handler({ once: (_e, listener) => (close = listener) }, { writeHead: (_s, h) => (head = h), write: (c) => written.push(c) });
  assert.equal((head as Record<string, string>)['content-type'], 'text/event-stream');
  assert.equal(written.length, 2);
  assert.match(written[0]!, /^data: \{"type":"HystrixCommand".*\}\n\n$/);
  await time.advance('1s');
  assert.equal(written.length, 4);
  close();
  await time.advance('5s');
  assert.equal(written.length, 4, 'stops writing once the client is gone');

  await time.advance('10s');
  assert.equal((stream.commands()[0] as Record<string, unknown>).rollingCountShortCircuited, 0, 'rolls out of the window');
  stream.dispose();
  assert.equal(time.pendingTimers, 0);
});
