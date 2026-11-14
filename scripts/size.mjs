// Bundle-size budgets (min+gzip, browser). Fails when a scenario grows past its budget.
// Run after `npm run build`: `npm run check:size`.
import { build } from 'esbuild';
import { gzipSync } from 'node:zlib';
import { fileURLToPath } from 'node:url';

const dist = fileURLToPath(new URL('../dist/esm/', import.meta.url));
const scenarios = {
  'retry() one-liner': [`import { retry } from '${dist}index.js'; retry(() => 1);`, 4.3],
  'PipelineBuilder: retry + timeout': [`import { PipelineBuilder, retryStrategy, timeoutStrategy } from '${dist}index.js'; new PipelineBuilder().add(retryStrategy()).add(timeoutStrategy('2s')).build();`, 5.0],
  'PipelineBuilder: retry + breaker + timeout': [`import { PipelineBuilder, retryStrategy, circuitBreakerStrategy, timeoutStrategy } from '${dist}index.js'; new PipelineBuilder().add(retryStrategy()).add(circuitBreakerStrategy()).add(timeoutStrategy('2s')).build();`, 7.3],
  '/fetch createResilientFetch': [`import { createResilientFetch } from '${dist}fetch.js'; createResilientFetch();`, 9.3],
  'ResiliencePipelineBuilder (everything)': [`import { ResiliencePipelineBuilder } from '${dist}index.js'; new ResiliencePipelineBuilder().addRetry().build();`, 15.3],
};

let failed = false;
for (const [name, [code, budgetKb]] of Object.entries(scenarios)) {
  const out = await build({ stdin: { contents: code, resolveDir: dist }, bundle: true, minify: true, format: 'esm', platform: 'browser', write: false });
  const kb = gzipSync(out.outputFiles[0].contents).length / 1024;
  const ok = kb <= budgetKb;
  failed ||= !ok;
  console.log(`${ok ? '✓' : '✗'} ${name.padEnd(44)} ${kb.toFixed(2).padStart(6)} KB  (budget ${budgetKb} KB)`);
}
if (failed) process.exit(1);
