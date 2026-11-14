import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Outcome, ResiliencePipelineBuilder, onTelemetry, setGlobalChaos, weighted } from '../src/index.ts';
import { FakeTimeProvider } from '../src/testing.ts';

function events() {
  const names: string[] = [];
  const stop = onTelemetry((e) => void (e.event.startsWith('Chaos.') && names.push(e.event)));
  return { names, stop };
}

test('chaosBehavior: runs the behavior (and its hook) before the callback, reports Chaos.OnBehavior', async () => {
  const order: string[] = [];
  const t = events();
  try {
    const p = new ResiliencePipelineBuilder()
      .addChaosBehavior({ injectionRate: 1, behavior: () => void order.push('behavior'), onBehaviorInjected: () => void order.push('hook') })
      .build();
    assert.equal(await p.execute(() => (order.push('callback'), 'ok')), 'ok');
    assert.deepEqual(order, ['hook', 'behavior', 'callback']);
    assert.deepEqual(t.names, ['Chaos.OnBehavior']);
    // Without the hook; a failing behavior fails the execution and the callback never runs.
    let called = false;
    const failing = new ResiliencePipelineBuilder().addChaosBehavior({ injectionRate: 1, behavior: () => Promise.reject(new Error('behavior broke')) }).build();
    await assert.rejects(failing.execute(() => (called = true)), /behavior broke/);
    assert.equal(called, false);
  } finally {
    t.stop();
  }
  assert.throws(() => new ResiliencePipelineBuilder().addChaosBehavior({} as never).build(), /behavior is required/);
});

test('chaos: generators decide enabled / rate; default rate 0.001 with Math.random', async () => {
  let behaviors = 0;
  const behavior = () => void behaviors++;
  const contexts: unknown[] = [];
  const gen = new ResiliencePipelineBuilder()
    .addChaosBehavior({ behavior, enabledGenerator: ({ context }) => (contexts.push(context.operationKey), true), injectionRateGenerator: async () => 1, randomizer: () => 0.99 })
    .build();
  await gen.execute(() => 1, { operationKey: 'op' });
  assert.equal(behaviors, 1);
  assert.deepEqual(contexts, ['op']);

  const disabled = new ResiliencePipelineBuilder().addChaosBehavior({ behavior, enabledGenerator: async () => false, injectionRate: 1 }).build();
  await disabled.execute(() => 1);
  assert.equal(behaviors, 1);

  // Default rate is 0.001: a randomizer just above it never injects, just below it does.
  await new ResiliencePipelineBuilder().addChaosBehavior({ behavior, randomizer: () => 0.0011 }).build().execute(() => 1);
  assert.equal(behaviors, 1);
  await new ResiliencePipelineBuilder().addChaosBehavior({ behavior, randomizer: () => 0.0009 }).build().execute(() => 1);
  assert.equal(behaviors, 2);
  // And the default randomizer (Math.random) is used when none is given.
  const random = Math.random;
  Math.random = () => 0;
  try {
    await new ResiliencePipelineBuilder().addChaosBehavior({ behavior }).build().execute(() => 1);
  } finally {
    Math.random = random;
  }
  assert.equal(behaviors, 3);
  assert.throws(() => new ResiliencePipelineBuilder().addChaosBehavior({ behavior, injectionRate: 1.5 }).build(), /injectionRate/);
});

test('chaosFault / chaosOutcome: a generator returning undefined skips injection; hooks receive the injected value', async () => {
  const seen: unknown[] = [];
  const t = events();
  try {
    const skip = new ResiliencePipelineBuilder().addChaosFault({ injectionRate: 1, faultGenerator: () => undefined }).build();
    assert.equal(await skip.execute(() => 'through'), 'through');
    const fault = new ResiliencePipelineBuilder()
      .addChaosFault({ injectionRate: 1, faultGenerator: async () => new Error('generated'), onFaultInjected: ({ fault }) => void seen.push(fault) })
      .build();
    await assert.rejects(fault.execute(() => 1), /generated/);
    assert.equal((seen[0] as Error).message, 'generated');

    const noOutcome = new ResiliencePipelineBuilder<number>().addChaosOutcome({ injectionRate: 1, outcomeGenerator: () => undefined }).build();
    assert.equal(await noOutcome.execute(() => 1), 1);
    const outcome = new ResiliencePipelineBuilder<number>()
      .addChaosOutcome({ injectionRate: 1, outcomeGenerator: async () => Outcome.fromResult(7), onOutcomeInjected: ({ outcome }) => void seen.push(outcome) })
      .build();
    assert.equal(await outcome.execute(() => 1), 7);
    assert.deepEqual(seen[1], Outcome.fromResult(7));
    assert.deepEqual(t.names, ['Chaos.OnFault', 'Chaos.OnOutcome']);
  } finally {
    t.stop();
  }
  assert.throws(() => new ResiliencePipelineBuilder().addChaosFault({ injectionRate: 1 }).build(), /fault or faultGenerator/);
  assert.throws(() => new ResiliencePipelineBuilder().addChaosOutcome({ injectionRate: 1 }).build(), /outcome or outcomeGenerator/);
});

test('chaosLatency: fixed default 30 s, generator, hook; an abort during the delay fails with the abort reason', async () => {
  const time = new FakeTimeProvider();
  const injected: number[] = [];
  const p = new ResiliencePipelineBuilder({ timeProvider: time }).addChaosLatency({ injectionRate: 1, onLatencyInjected: ({ latency }) => void injected.push(latency) }).build();
  let done = false;
  const run = p.execute(() => 'late').then((v) => ((done = true), v));
  await time.advance(29_999);
  assert.equal(done, false);
  await time.advance(1);
  assert.equal(await run, 'late');
  assert.deepEqual(injected, [30_000]);

  const gen = new ResiliencePipelineBuilder({ timeProvider: time }).addChaosLatency({ injectionRate: 1, latencyGenerator: async () => '2s' as const }).build();
  const genRun = gen.execute(() => 'ok');
  await time.advance(2000);
  assert.equal(await genRun, 'ok');

  const controller = new AbortController();
  let called = false;
  const aborted = p.execute(() => (called = true), { signal: controller.signal });
  await time.advance(10);
  controller.abort(new Error('caller gave up'));
  await assert.rejects(aborted, /caller gave up/);
  assert.equal(called, false);
  assert.equal(time.pendingTimers, 0, 'the latency timer was cleared');
});

test('setGlobalChaos: latency only, fault factory, filter, abort during latency, validation', async () => {
  const time = new FakeTimeProvider();
  const p = new ResiliencePipelineBuilder({ name: 'g', timeProvider: time }).build();

  let off = setGlobalChaos({ injectionRate: 1, latency: '1s', randomizer: () => 0 });
  try {
    let done = false;
    const run = p.execute(() => 'slow').then((v) => ((done = true), v));
    await time.advance(999);
    assert.equal(done, false, 'latency only: delayed, not failed');
    await time.advance(1);
    assert.equal(await run, 'slow');

    const controller = new AbortController();
    const aborted = p.execute(() => 'never', { signal: controller.signal });
    await time.advance(1);
    controller.abort(new Error('stop'));
    await assert.rejects(aborted, /stop/);
  } finally {
    off();
  }

  off = setGlobalChaos({ fault: (ctx: { operationKey?: string }) => new Error(`fault for ${ctx.operationKey}`), filter: (ctx) => ctx.operationKey !== 'spared', randomizer: () => 0 });
  try {
    await assert.rejects(p.execute(() => 1, { operationKey: 'k' }), /fault for k/);
    assert.equal(await p.execute(() => 1, { operationKey: 'spared' }), 1);
  } finally {
    off();
  }

  // Default rate 0.05 with Math.random.
  const random = Math.random;
  Math.random = () => 0.06;
  off = setGlobalChaos({ fault: new Error('x') });
  try {
    assert.equal(await p.execute(() => 'missed'), 'missed');
    Math.random = () => 0.04;
    await assert.rejects(p.execute(() => 1), /x/);
  } finally {
    off();
    Math.random = random;
  }
  off(); // a second call is harmless
  assert.throws(() => setGlobalChaos({ injectionRate: 2, fault: 1 }), /injectionRate/);
  assert.throws(() => setGlobalChaos({}), /fault or latency/);
});

test('weighted: picks by weight; the floating-point edge falls back to the last entry', () => {
  const edge = weighted([{ weight: 1, value: 'a' }, { weight: 1, value: () => 'b' }], () => 1); // pick == total: loop never goes below 0
  assert.equal(edge(), 'b');
  assert.equal(weighted([{ weight: 1, value: () => 'a' }, { weight: 2, value: 'c' }], () => 1)(), 'c');
  assert.equal(weighted([{ weight: 1, value: 'a' }, { weight: 2, value: 'c' }], () => 0.5)(), 'c');
  assert.equal(weighted([{ weight: 1, value: 'a' }, { weight: 2, value: 'c' }], () => 0.3)(), 'a');
  assert.throws(() => weighted([{ weight: 0, value: 1 }]), /weights/);
  assert.throws(() => weighted([{ weight: 2, value: 1 }, { weight: -1, value: 2 }]), /weights/);
  assert.ok(['a', 'b'].includes(weighted([{ weight: 1, value: 'a' }, { weight: 1, value: 'b' }])()));
});
