import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
  BrokenCircuitError,
  PipelineBuilder,
  ResiliencePipelineBuilder,
  type TelemetryEvent,
  TimeoutRejectedError,
  circuitBreakerStrategy,
  onTelemetry,
  retryStrategy,
  timeoutStrategy,
} from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

test('PipelineBuilder + strategy definitions behave like the full builder', async () => {
  let calls = 0;
  const time = new FakeTimeProvider();
  const lean = new PipelineBuilder({ name: 'lean', timeProvider: time }).add(retryStrategy({ delay: 0, maxRetryAttempts: 2 })).add(timeoutStrategy('20ms')).build();
  const done = lean.execute(async () => {
    if (++calls < 3) await new Promise(() => {}); // too slow: the timeout fires and retry tries again
    return 'ok';
  });
  await time.runAll();
  assert.equal(await done, 'ok');
  assert.equal(calls, 3);

  const breaker = new PipelineBuilder().add(circuitBreakerStrategy({ failureRatio: 1, minimumThroughput: 1 })).build();
  await assert.rejects(breaker.execute(() => Promise.reject(new Error('down'))));
  await assert.rejects(breaker.execute(() => 1), BrokenCircuitError);
});

test('ResiliencePipelineBuilder is a PipelineBuilder: add() and shortcuts mix, names reach telemetry', async () => {
  const seen: string[] = [];
  const off = onTelemetry((e: TelemetryEvent) => void (!['OnPipelineExecuting', 'OnPipelineExecuted', 'OnExecutionAttempt', 'OnStrategyExecuted'].includes(e.event) && seen.push(`${e.source.strategyName}:${e.event}`)));
  try {
    const builder = new ResiliencePipelineBuilder().add(retryStrategy({ name: 'outer-retry', delay: 0, maxRetryAttempts: 1 })).addTimeout({ timeout: '10ms', name: 'per-try' });
    assert.ok(builder instanceof PipelineBuilder);
    await assert.rejects(builder.build().execute(() => new Promise(() => {})), TimeoutRejectedError);
  } finally {
    off();
  }
  assert.deepEqual(seen, ['per-try:OnTimeout', 'outer-retry:OnRetry', 'per-try:OnTimeout', 'outer-retry:OnRetryExhausted']);
});
