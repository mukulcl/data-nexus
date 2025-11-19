export const config = {
  api: {
    port: Number(process.env.API_PORT || 8080),
  },
  pg: {
    host: process.env.PG_HOST || 'postgres',
    port: Number(process.env.PG_PORT || 5432),
    user: process.env.PG_USER || 'postgres',
    password: process.env.PG_PASSWORD || 'postgres',
    database: process.env.PG_DATABASE || 'data_nexus',
  },
  ch: {
    protocol: process.env.CH_PROTOCOL || 'http',
    host: process.env.CH_HOST || 'clickhouse',
    port: Number(process.env.CH_PORT || 8123),
    user: process.env.CH_USER || process.env.CLICKHOUSE_USER || 'default',
    password: process.env.CH_PASSWORD || process.env.CLICKHOUSE_PASSWORD || '',
    database: process.env.CH_DATABASE || process.env.CLICKHOUSE_DB || 'data_nexus',
  },
  redis: {
    host: process.env.REDIS_HOST || 'redis',
    port: Number(process.env.REDIS_PORT || 6379),
  },
  rateLimit: {
    maxPerMinute: Number(process.env.RATE_LIMIT_PER_MINUTE || 60),
  },
  credits: {
    baseCost: Number(process.env.CREDITS_BASE_COST || 1),
    perRowCost: Number(process.env.CREDITS_PER_ROW || 0.01),
  },
  paging: {
    maxPerPage: Number(process.env.MAX_PER_PAGE || 200),
  },
};
