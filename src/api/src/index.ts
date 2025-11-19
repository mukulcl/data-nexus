import Fastify from 'fastify';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { config } from './config';
import { ch, pg, redis } from './db';
import type { AuthUser } from './types';

const server = Fastify({ logger: true });

server.get('/healthz', async () => ({ status: 'ok' }));

await server.register(swagger, {
  openapi: {
    info: { title: 'Data Nexus API', version: '0.1.0' },
  },
});
await server.register(swaggerUi, { routePrefix: '/docs' });

// On-request timing
server.addHook('onRequest', async (req) => {
  (req as any).requestStart = Date.now();
});

// Simple API key auth + per-user rate limit (Redis)
server.addHook('preHandler', async (req, reply) => {
  if (req.routerPath?.startsWith('/healthz') || req.routerPath?.startsWith('/docs')) return;

  const apiKey = req.headers['x-api-key'];
  if (!apiKey || typeof apiKey !== 'string') {
    return reply.code(401).send({ error: 'Missing API key' });
  }

  const { rows } = await pg.query<AuthUser>('SELECT id, key, user_name, credits FROM api_keys WHERE key = $1', [apiKey]);
  if (!rows.length) {
    return reply.code(401).send({ error: 'Invalid API key' });
  }
  const user = rows[0];
  (req as any).authUser = user;

  // Rate limit per minute
  const minute = Math.floor(Date.now() / 60000);
  const rlKey = `rl:${user.id}:${minute}`;
  const cur = await redis.incr(rlKey);
  if (cur === 1) await redis.expire(rlKey, 60);
  if (cur > config.rateLimit.maxPerMinute) {
    return reply.code(429).send({ error: 'Rate limit exceeded' });
  }
});

// GET /v1/records - basic pagination and optional source filter
server.get('/v1/records', async (req, reply) => {
  const user = (req as any).authUser as AuthUser | undefined;
  if (!user) return reply.code(401).send({ error: 'Unauthorized' });

  const q = req.query as any;
  const page = Math.max(1, Number(q.page || 1));
  const perPageRaw = Number(q.per_page || 50);
  const perPage = Math.min(Math.max(1, perPageRaw), config.paging.maxPerPage);
  const offset = (page - 1) * perPage;

  const filters: string[] = [];
  const params: any = {};
  if (q.source) {
    filters.push(`source = {source:String}`);
    params.source = String(q.source);
  }
  const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';

  const sqlData = `SELECT record_id, source, source_record_id, ts_ingested, normalized_field_1, normalized_field_2, tags, metadata, checksum, is_valid FROM master_records ${where} ORDER BY record_id LIMIT ${perPage} OFFSET ${offset}`;
  const sqlCount = `SELECT count() AS cnt FROM master_records ${where}`;

  const started = Date.now();
  const dataRes = await ch.query({ query: sqlData, query_params: params, format: 'JSONEachRow' });
  const rows = await dataRes.json();
  const countRes = await ch.query({ query: sqlCount, query_params: params, format: 'JSONEachRow' });
  const [{ cnt }] = await countRes.json();

  const durationMs = Date.now() - started;
  const creditsUsed = Math.ceil(config.credits.baseCost + rows.length * config.credits.perRowCost);

  // Deduct credits and log usage in a transaction
  const client = await pg.connect();
  try {
    await client.query('BEGIN');
    const bal = await client.query('SELECT credits FROM api_keys WHERE id = $1 FOR UPDATE', [user.id]);
    const current = Number(bal.rows[0]?.credits ?? 0);
    if (current < creditsUsed) {
      await client.query('ROLLBACK');
      return reply.code(402).send({ error: 'Insufficient Credits' });
    }
    await client.query('UPDATE api_keys SET credits = credits - $1 WHERE id = $2', [creditsUsed, user.id]);
    await client.query(
      'INSERT INTO api_logs (user_id, endpoint, query_text, credits_deducted, duration_ms) VALUES ($1, $2, $3, $4, $5)',
      [user.id, '/v1/records', JSON.stringify(q), creditsUsed, durationMs]
    );
    await client.query('COMMIT');
  } catch (e) {
    server.log.error(e);
    try { await client.query('ROLLBACK'); } catch {}
  } finally {
    client.release();
  }

  return reply.send({
    data: rows,
    meta: {
      total_records: Number(cnt),
      page,
      per_page: perPage,
      credits_used: creditsUsed,
      response_time: `${durationMs}ms`,
    },
  });
});

const port = config.api.port;
const host = '0.0.0.0';

server
  .listen({ port, host })
  .catch((err) => {
    server.log.error(err);
    process.exit(1);
  });
