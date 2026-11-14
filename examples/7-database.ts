// 7) Database calls: timeouts that really stop the query, and retrying transient errors (deadlock, serialization).
import pg from 'pg';
import { ResiliencePipelineBuilder, TimeoutRejectedError } from '../src/index.ts';
import { log } from './flaky-server.ts';

const pool = new pg.Pool({
  database: 'postgres',
  max: 5,
  connectionTimeoutMillis: 2_000, // don't wait forever for a free connection when the pool is exhausted
});
// Cancelling needs its own connection: the main pool may be full exactly when we need to cancel.
const cancelPool = new pg.Pool({ database: 'postgres', max: 1 });

// ─── Infrastructure (e.g. src/lib/db.ts) ───────────────────────────────────────────────────

/** Errors where running the same transaction again can succeed. */
const TRANSIENT = new Set([
  '40001', // serialization_failure
  '40P01', // deadlock_detected
  '53300', // too_many_connections
  '57P01', // admin_shutdown (failover / restart)
  '08000', '08003', '08006', // connection lost
  'ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT',
]);
const isTransient = (e: unknown) => TRANSIENT.has((e as { code?: string })?.code ?? '');

const db = new ResiliencePipelineBuilder({ name: 'postgres' })
  .addRetry({
    maxRetryAttempts: 3,
    delay: 20,
    backoffType: 'exponential',
    useJitter: true,
    shouldHandle: ({ outcome }) => !outcome.ok && isTransient(outcome.error),
    onRetry: ({ outcome, attemptNumber }) => log(`   ↻ attempt ${attemptNumber} failed: ${(outcome as { error: Error }).error.message} → retrying whole transaction`),
  })
  .addTimeout(1_000)
  .build();

/**
 * Runs `fn` in a transaction. If the pipeline's signal aborts (timeout / caller cancelled),
 * the query is cancelled on the server too, not just abandoned.
 */
async function transaction<T>(signal: AbortSignal, fn: (c: pg.PoolClient) => Promise<T>, isolation = 'READ COMMITTED'): Promise<T> {
  const client = await pool.connect();
  const pid = (client as unknown as { processID: number }).processID;
  const cancel = () => void cancelPool.query('select pg_cancel_backend($1)', [pid]).catch(() => {});
  signal.addEventListener('abort', cancel, { once: true });
  try {
    await client.query(`begin isolation level ${isolation}`);
    await client.query(`set local statement_timeout = 1000`); // server-side safety net, even if this process dies
    const result = await fn(client);
    await client.query('commit');
    return result;
  } catch (e) {
    await client.query('rollback').catch(() => {});
    throw e;
  } finally {
    signal.removeEventListener('abort', cancel);
    client.release();
  }
}

const activeSleeps = async () =>
  Number((await cancelPool.query(`select count(*) from pg_stat_activity where state = 'active' and query like 'select pg_sleep(3)%'`)).rows[0].count);

// ─── Demo A: why a plain timeout is not enough ──────────────────────────────────────────────

log('A1) naive: timeout around pool.query (driver has no AbortSignal support)');
const naive = new ResiliencePipelineBuilder().addTimeout(300).build();
await naive.execute(() => pool.query('select pg_sleep(3)')).catch((e) => log(`   caller got ${e.name} after 300ms`));
log(`   …but queries still running on the server: ${await activeSleeps()}  ← connection busy for 3 more seconds`);
await cancelPool.query(`select pg_cancel_backend(pid) from pg_stat_activity where query like 'select pg_sleep(3)%' and pid <> pg_backend_pid()`);

log('A2) with transaction(): timeout also cancels the query');
const timeoutOnly = new ResiliencePipelineBuilder().addTimeout(300).build();
await timeoutOnly
  .execute(({ signal }) => transaction(signal, (c) => c.query('select pg_sleep(3)')))
  .catch((e) => log(`   caller got ${e instanceof TimeoutRejectedError ? 'TimeoutRejectedError' : e.name} after 300ms`));
await new Promise((r) => setTimeout(r, 50));
log(`   queries still running on the server: ${await activeSleeps()}  ✓`);

// ─── Demo B: retrying a serialization failure ───────────────────────────────────────────────

await pool.query('create table if not exists rp_demo_accounts (id int primary key, balance int not null)');
await pool.query('insert into rp_demo_accounts values (1, 100) on conflict (id) do update set balance = 100');

// Read-modify-write: two of these at once conflict under SERIALIZABLE.
const withdraw = (amount: number) =>
  db.execute(({ signal }) =>
    transaction(
      signal,
      async (c) => {
        const { rows } = await c.query('select balance from rp_demo_accounts where id = 1');
        await c.query('select pg_sleep(0.05)'); // widen the race window
        await c.query('update rp_demo_accounts set balance = $1 where id = 1', [rows[0].balance - amount]);
      },
      'SERIALIZABLE',
    ),
  );

log('B) two concurrent withdrawals (10 and 20) from balance 100');
await Promise.all([withdraw(10), withdraw(20)]);
log(`   final balance: ${(await pool.query('select balance from rp_demo_accounts where id = 1')).rows[0].balance} (expected 70) ✓`);

await pool.query('drop table rp_demo_accounts');
await pool.end();
await cancelPool.end();
