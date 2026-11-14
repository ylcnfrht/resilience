// llms.txt (https://llmstxt.org): an index of every docs page so coding agents can find the right one.
import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';

const sections = ['start', 'concepts', 'strategies', 'integrations', 'guides', 'migration', 'reference'];

export const GET: APIRoute = async ({ site }) => {
  const docs = await getCollection('docs');
  const body = sections
    .map((s) => {
      const items = docs
        .filter((d) => d.id.startsWith(`${s}/`))
        .sort((a, b) => a.id.localeCompare(b.id))
        .map((d) => `- [${d.data.title}](${new URL(`/${d.id}/`, site)})${d.data.description ? `: ${d.data.description}` : ''}`);
      return `## ${s[0].toUpperCase()}${s.slice(1)}\n\n${items.join('\n')}`;
    })
    .join('\n\n');
  const head = `# @nodelibraries/resilience\n\n> Resilience pipelines for TypeScript: retry, circuit breaker, timeout, hedging, rate limiting and typed errors, for Node, Bun, Deno, Workers and browsers. Zero dependencies.\n\nAPI reference: ${new URL('/api/overview/', site)}\n\n`;
  return new Response(head + body + '\n', { headers: { 'content-type': 'text/plain; charset=utf-8' } });
};
