// Script-tag build for CDNs (unpkg / jsDelivr): <script src=".../dist/browser/resilience-pipeline.min.js"></script>
// exposes `window.resiliencePipeline`. Bundlers and Node use the ESM/CJS builds instead.
import { build } from 'esbuild';

await build({
  entryPoints: [new URL('../dist/esm/index.js', import.meta.url).pathname],
  outfile: new URL('../dist/browser/resilience-pipeline.min.js', import.meta.url).pathname,
  bundle: true,
  minify: true,
  format: 'iife',
  globalName: 'resiliencePipeline',
  platform: 'browser',
  target: 'es2022',
  sourcemap: true,
});
