import { Pool } from 'pg';
import { createClient, ClickHouseClient } from '@clickhouse/client';
import Redis from 'ioredis';
import { config } from './config';

export const pg = new Pool({
  host: config.pg.host,
  port: config.pg.port,
  user: config.pg.user,
  password: config.pg.password,
  database: config.pg.database,
});

export const ch: ClickHouseClient = createClient({
  host: `${config.ch.protocol}://${config.ch.host}:${config.ch.port}`,
  username: config.ch.user,
  password: config.ch.password,
  database: config.ch.database,
});

export const redis = new Redis({
  host: config.redis.host,
  port: config.redis.port,
});
