// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import preact from '@astrojs/preact';
import starlightTypeDoc, { typeDocSidebarGroup } from 'starlight-typedoc';
import starlightLinksValidator from 'starlight-links-validator';
import { fileURLToPath } from 'node:url';

const lib = fileURLToPath(new URL('../src/', import.meta.url));

// Every public entry point of the package, for the generated API reference.
const entryPoints = [
  'index',
  'testing',
  'outcome',
  'fetch',
  'axios',
  'http',
  'fastify',
  'koa',
  'nestjs',
  'redis',
  'adapters',
  'otel',
  'prometheus',
  'hystrix',
].map((name) => `../src/${name}.ts`);

// Parallel builds (several contributors at once) need separate output/cache dirs; SKIP_TYPEDOC speeds up content checks.
const skipTypeDoc = process.env.SKIP_TYPEDOC === '1';

export default defineConfig({
  devToolbar: { enabled: false },
  site: 'https://resilience.ferhatyalcin.dev',
  outDir: process.env.ASTRO_OUT_DIR ?? './dist',
  cacheDir: process.env.ASTRO_CACHE_DIR ?? './node_modules/.astro',
  vite: {
    // Interactive components import the library straight from source, so the playground always runs the current code.
    resolve: { alias: [{ find: /^@ylcnfrht\/resilience$/, replacement: `${lib}index.ts` }, { find: /^@ylcnfrht\/resilience\/(.*)$/, replacement: `${lib}$1.ts` }] },
    server: { fs: { allow: ['..'] } },
    // ec.config.mjs pulls twoslash + the TypeScript compiler into SSR; bundling them exhausts the heap.
    ssr: { external: ['typescript', 'twoslash', 'expressive-code-twoslash', '@typescript/vfs'] },
  },
  integrations: [
    preact(),
    starlight({
      title: '@ylcnfrht/resilience',
      description: 'Resilience pipelines for TypeScript: retry, circuit breaker, timeout, hedging, rate limiting, typed errors — for Node, Bun, Deno, Workers and browsers.',
      logo: { src: './src/assets/logo.svg' },
      favicon: '/favicon.svg',
      head: [
        { tag: 'meta', attrs: { property: 'og:image', content: 'https://resilience.ferhatyalcin.dev/og.png' } },
        { tag: 'meta', attrs: { property: 'og:image:width', content: '1200' } },
        { tag: 'meta', attrs: { property: 'og:image:height', content: '630' } },
        { tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
        { tag: 'meta', attrs: { name: 'twitter:image', content: 'https://resilience.ferhatyalcin.dev/og.png' } },
      ],
      customCss: ['@fontsource-variable/geist', '@fontsource-variable/geist-mono', './src/styles/theme.css'],
      components: { Header: './src/components/overrides/Header.astro' },
      social: [{ icon: 'seti:npm', label: 'npm', href: 'https://www.npmjs.com/package/@ylcnfrht/resilience' }],
      lastUpdated: false,
      pagination: true,
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      plugins: [
        ...(skipTypeDoc ? [] : [starlightTypeDoc({
          entryPoints,
          tsconfig: './typedoc.tsconfig.json',
          output: 'api',
          sidebar: { label: 'API', collapsed: true },
          typeDoc: { excludeInternal: true, excludePrivate: true, readme: 'none', entryFileName: 'overview' },
        })]),
        ...(process.env.SKIP_LINKS === '1' ? [] : [starlightLinksValidator({ errorOnLocalLinks: false, exclude: ['/playground', '/playground/', ...(skipTypeDoc ? ['/api/**'] : [])] })]),
      ],
      sidebar: [
        { label: 'Start here', items: [{ autogenerate: { directory: 'start' } }] },
        { label: 'Concepts', items: [{ autogenerate: { directory: 'concepts' } }] },
        { label: 'Strategies', items: [{ autogenerate: { directory: 'strategies' } }] },
        { label: 'Integrations', items: [{ autogenerate: { directory: 'integrations' } }] },
        { label: 'Guides & recipes', items: [{ autogenerate: { directory: 'guides' } }] },
        { label: 'Migrating', collapsed: true, items: [{ autogenerate: { directory: 'migration' } }] },
        { label: 'Reference', items: [{ autogenerate: { directory: 'reference' } }] },
        ...(skipTypeDoc ? [] : [typeDocSidebarGroup]),
      ],
    }),
  ],
});
