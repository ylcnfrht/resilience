// Small local HTTP server whose behaviour each example controls, so examples run offline.
import http from 'node:http';
import type { AddressInfo } from 'node:net';

export type Handler = (n: number, req: http.IncomingMessage) => { status?: number; body?: string; delay?: number; headers?: Record<string, string> };

export async function startServer(handler: Handler) {
  let n = 0;
  const server = http.createServer(async (req, res) => {
    const r = handler(++n, req);
    if (r.delay) await new Promise((ok) => setTimeout(ok, r.delay));
    if (res.destroyed) return;
    res.writeHead(r.status ?? 200, r.headers).end(r.body ?? 'ok');
  });
  await new Promise<void>((ok) => server.listen(0, ok));
  const url = `http://localhost:${(server.address() as AddressInfo).port}`;
  return { url, close: () => (server.closeAllConnections(), server.close()), get requests() { return n; } };
}

export const log = (...a: unknown[]) => console.log(`  [${new Date().toISOString().slice(17, 23)}]`, ...a);
