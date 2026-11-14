import { type ResiliencePipeline } from './pipeline.ts';

type PipelineSource = ResiliencePipeline<any> | ((instance: any) => ResiliencePipeline<any>);

/**
 * Runs a method through a pipeline. Works with both TypeScript's `experimentalDecorators` and standard (TC39) decorators:
 *
 * ```ts
 * class PaymentClient {
 *   @resilient(paymentPipeline)
 *   async charge(order: Order) { ... }   // use getResilienceSignal() inside to get the signal
 *
 *   @resilient((self: PaymentClient) => self.pipeline)   // per-instance pipeline
 *   async refund(id: string) { ... }
 * }
 * ```
 */
export function resilient(pipeline: PipelineSource): any {
  const wrap = (method: (...args: any[]) => unknown) =>
    function (this: unknown, ...args: any[]) {
      const p = typeof pipeline === 'function' ? pipeline(this) : pipeline;
      return p.execute(() => method.apply(this, args));
    };
  return (target: any, context: any, descriptor?: PropertyDescriptor) => {
    // Standard decorators: (method, { kind: 'method', ... }) → return the replacement.
    if (context && typeof context === 'object' && context.kind === 'method') return wrap(target);
    // Legacy (experimentalDecorators): (prototype, key, descriptor) → mutate the descriptor.
    if (!descriptor || typeof descriptor.value !== 'function') throw new TypeError('@resilient can only decorate methods.');
    descriptor.value = wrap(descriptor.value);
    return descriptor;
  };
}
