export const config = {
  s3: {
    endpoint: process.env.S3_ENDPOINT || 'http://minio:9000',
    accessKey: process.env.S3_ACCESS_KEY || 'minioadmin',
    secretKey: process.env.S3_SECRET_KEY || 'minioadmin',
    rawBucket: process.env.S3_BUCKET_RAW || 'raw',
    cleanBucket: process.env.S3_BUCKET_CLEAN || 'clean',
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
};
