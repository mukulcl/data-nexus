import { S3Client, CreateBucketCommand, HeadBucketCommand, ListObjectsV2Command, HeadObjectCommand, ListObjectsV2CommandOutput, GetObjectCommand } from '@aws-sdk/client-s3';
import { Pool } from 'pg';
import crypto from 'crypto';
import { config } from './config.js';
import { createClient as createCHClient } from '@clickhouse/client';
import { parse as parseCsv } from 'csv-parse/sync';
import { Queue, Worker as MQWorker, QueueEvents, JobsOptions } from 'bullmq';
import ExcelJS from 'exceljs';
import XlsxStreamReaderPkg from 'xlsx-stream-reader';
import { Readable } from 'stream';
import fs from 'fs';
import os from 'os';
import path from 'path';

const s3 = new S3Client({
  region: 'us-east-1',
  endpoint: config.s3.endpoint,
  credentials: {
    accessKeyId: config.s3.accessKey,
    secretAccessKey: config.s3.secretKey,
  },
  forcePathStyle: true,
});

const pgPool = new Pool({
  host: config.pg.host,
  port: config.pg.port,
  user: config.pg.user,
  password: config.pg.password,
  database: config.pg.database,
});

const ch = createCHClient({
  url: `${config.ch.protocol}://${config.ch.host}:${config.ch.port}`,
  username: config.ch.user,
  password: config.ch.password,
  database: config.ch.database,
});

// BullMQ setup
const mqConnection = { host: config.redis.host, port: config.redis.port } as const;
const ingestQueue = new Queue('ingest', { connection: mqConnection });
const ingestEvents = new QueueEvents('ingest', { connection: mqConnection });
ingestEvents.on('failed', (event: any) => {
  const { jobId, failedReason } = event || {};
  console.error('Job failed', jobId, failedReason);
});

async function ensureBucket(name: string) {
  try {
    await s3.send(new HeadBucketCommand({ Bucket: name }));
  } catch {
    await s3.send(new CreateBucketCommand({ Bucket: name }));
  }
}

async function ensureBuckets() {
  await ensureBucket(config.s3.rawBucket);
  await ensureBucket(config.s3.cleanBucket);
}

async function registerNewFiles(): Promise<number> {
  const bucket = config.s3.rawBucket;
  let token: string | undefined = undefined;
  let newCount = 0;

  do {
    const res: ListObjectsV2CommandOutput = await s3.send(
      new ListObjectsV2Command({ Bucket: bucket, ContinuationToken: token })
    );
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
    const items = (res.Contents ?? []) as Array<{ Key?: string; Size?: number; LastModified?: Date }>;

    for (const obj of items) {
      if (!obj.Key) continue;
      const head = await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: obj.Key }));
      const etag = (head.ETag || '').replace(/"/g, '');
      const fallback = crypto
        .createHash('md5')
        .update(`${obj.Key}:${obj.Size ?? ''}:${obj.LastModified?.toISOString() ?? ''}`)
        .digest('hex');
      const checksum = etag || fallback;

      const exists = await pgPool.query(
        'SELECT id FROM ingest_files WHERE file_checksum = $1 OR file_name = $2 LIMIT 1',
        [checksum, obj.Key]
      );
      if (exists.rowCount) continue;

      // Insert as queued and enqueue a job
      const ins = await pgPool.query(
        'INSERT INTO ingest_files (file_name, file_checksum, rows_ingested, status) VALUES ($1, $2, $3, $4) RETURNING id',
        [obj.Key, checksum, 0, 'queued']
      );
      const fileId = ins.rows[0]?.id as number | undefined;
      const jobOpts: JobsOptions = { removeOnComplete: true, removeOnFail: false, attempts: 2, backoff: { type: 'fixed', delay: 2000 } };
      await ingestQueue.add('ingest-file', { id: fileId, key: obj.Key }, jobOpts);
      newCount++;
    }
  } while (token);

  return newCount;
}

async function streamToString(stream: any): Promise<string> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: any) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
  });
}

async function streamToBuffer(stream: any): Promise<Buffer> {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (chunk: any) => chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    stream.on('error', reject);
    stream.on('end', () => resolve(Buffer.concat(chunks)));
  });
}

function inferSource(key: string, fallback?: string): string {
  const k = key.toLowerCase();
  if (k.includes('apollo')) return 'apollo';
  if (k.includes('california')) return 'california';
  if (k.includes('list')) return 'excel_list06';
  return fallback || '';
}

function mapRecord(r: any, key: string) {
  const tags = safeJsonArray(r.tags);
  const verr = safeJsonArray(r.validation_errors);
  const nf2 = toNumber(r.normalized_field_2);
  const isValid = toNumber(r.is_valid);
  const ts = toClickhouseDateTime(r.ts_ingested);
  const checksum = r.checksum || crypto.createHash('md5').update(JSON.stringify(r)).digest('hex');
  const metadata = typeof r.metadata === 'string' ? r.metadata : JSON.stringify(r.metadata ?? {});
  const source = r.source ?? inferSource(key);
  return {
    record_id: String(r.record_id ?? ''),
    source: String(source ?? ''),
    source_record_id: String(r.source_record_id ?? ''),
    ts_ingested: ts,
    normalized_field_1: String(r.normalized_field_1 ?? ''),
    normalized_field_2: nf2,
    tags,
    metadata,
    checksum,
    is_valid: isValid,
    validation_errors: verr,
  };
}

function toClickhouseDateTime(v: any): string {
  const d = v ? new Date(v) : new Date();
  if (isNaN(d.getTime())) {
    const now = new Date();
    return formatDateTime(now);
  }
  return formatDateTime(d);
}

function formatDateTime(d: Date): string {
  const pad = (n: number) => (n < 10 ? '0' + n : '' + n);
  const Y = d.getFullYear();
  const M = pad(d.getMonth() + 1);
  const D = pad(d.getDate());
  const h = pad(d.getHours());
  const m = pad(d.getMinutes());
  const s = pad(d.getSeconds());
  return `${Y}-${M}-${D} ${h}:${m}:${s}`;
}

async function insertRowsBatched(rows: any[], batchSize = 5000) {
  for (let i = 0; i < rows.length; i += batchSize) {
    const chunk = rows.slice(i, i + batchSize);
    await ch.insert({ table: 'master_records', values: chunk as any, format: 'JSONEachRow' });
  }
}

async function processCsvObject(key: string): Promise<number> {
  const bucket = config.s3.rawBucket;
  const obj = (await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))) as any;
  const body = obj.Body as any;
  const text = typeof body?.transformToString === 'function' ? await body.transformToString() : await streamToString(body);
  const records = parseCsv(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    relax_quotes: true,
    escape: '\\'
  }) as any[];

  const rows = records.map((r) => mapRecord(r, key));

  if (rows.length === 0) return 0;
  await insertRowsBatched(rows);
  return rows.length;
}

async function processTsvObject(key: string): Promise<number> {
  const bucket = config.s3.rawBucket;
  const obj = (await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))) as any;
  const body = obj.Body as any;
  const text = typeof body?.transformToString === 'function' ? await body.transformToString() : await streamToString(body);
  const records = parseCsv(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    delimiter: '\t',
    relax_quotes: true
  }) as any[];
  const rows = records.map((r) => mapRecord(r, key));
  if (rows.length === 0) return 0;
  await insertRowsBatched(rows);
  return rows.length;
}

async function processXlsxObject(key: string): Promise<number> {
  const bucket = config.s3.rawBucket;
  const obj = (await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }))) as any;
  const body = obj.Body as any;
  // Write to a temp file to avoid buffering entire ZIP in memory
  const tmpDir = os.tmpdir();
  const safeName = key.replace(/[^a-zA-Z0-9._-]/g, '_');
  const tmpPath = path.join(tmpDir, `dn_${Date.now()}_${safeName}`);
  await new Promise<void>((resolve, reject) => {
    const ws = fs.createWriteStream(tmpPath);
    const webStream = typeof body?.transformToWebStream === 'function' ? body.transformToWebStream() : null;
    const nodeStream: any = webStream ? Readable.fromWeb(webStream as any) : (body as any);
    nodeStream.on('error', reject);
    ws.on('error', reject);
    ws.on('finish', () => resolve());
    nodeStream.pipe(ws);
  });

  const XlsxStreamReader: any = (XlsxStreamReaderPkg as any).default || (XlsxStreamReaderPkg as any);
  const processed = await new Promise<number>((resolve, reject) => {
    let headers: string[] = [];
    let count = 0;
    const batch: any[] = [];
    const BATCH_SIZE = 1000;

    const workBookReader = new XlsxStreamReader();

    workBookReader.on('error', (err: any) => reject(err));
    workBookReader.on('end', async () => {
      try {
        if (batch.length) {
          await ch.insert({ table: 'master_records', values: batch as any, format: 'JSONEachRow' });
          count += batch.length;
        }
        try { fs.unlinkSync(tmpPath); } catch {}
        resolve(count);
      } catch (e) {
        try { fs.unlinkSync(tmpPath); } catch {}
        reject(e);
      }
    });

    workBookReader.on('worksheet', (workSheetReader: any) => {
      // process only first worksheet
      if (workBookReader.workSheets.length > 1 && workSheetReader.id !== 1) {
        workSheetReader.skip();
        return;
      }
      let isFirstRow = true;
      workSheetReader.on('row', async (row: any) => {
        const values: any[] = row.values || [];
        if (isFirstRow) {
          headers = values.map((v) => (v ?? '').toString().trim().toLowerCase());
          isFirstRow = false;
          return;
        }
        const objRow: any = {};
        for (let i = 1; i < values.length; i++) {
          const h = headers[i] || headers[i - 1];
          if (!h) continue;
          objRow[h] = values[i];
        }
        const mapped = mapRecord(objRow, key);
        batch.push(mapped);
        if (batch.length >= BATCH_SIZE) {
          workSheetReader.pause();
          try {
            await ch.insert({ table: 'master_records', values: batch as any, format: 'JSONEachRow' });
            count += batch.length;
            batch.length = 0;
          } catch (e) {
            return reject(e);
          } finally {
            workSheetReader.resume();
          }
        }
      });
      workSheetReader.on('end', async () => {
        // will flush in workBookReader 'end'
      });
      workSheetReader.process();
    });

    fs.createReadStream(tmpPath).pipe(workBookReader);
  });

  return processed;
}

function safeJsonArray(v: any): string[] {
  try {
    if (Array.isArray(v)) return v.map(String);
    if (typeof v === 'string' && v.length) return JSON.parse(v);
  } catch {}
  return [];
}

function toNumber(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

// BullMQ worker to process queued files
const mqWorker = new MQWorker(
  'ingest',
  async (job: any) => {
    const { id, key } = (job?.data || {}) as { id: number; key: string };
    try {
      let processed = 0;
      if (key.endsWith('.csv')) {
        processed = await processCsvObject(key);
      } else if (key.endsWith('.tsv')) {
        processed = await processTsvObject(key);
      } else if (key.endsWith('.xlsx') || key.endsWith('.xls')) {
        processed = await processXlsxObject(key);
      } else {
        // TODO: implement other formats (jsonl/parquet/xlsx/xml)
      }
      await pgPool.query('UPDATE ingest_files SET status = $1, rows_ingested = $2 WHERE id = $3', ['processed', processed, id]);
      console.log(`Processed ${key}: ${processed} rows`);
    } catch (e) {
      console.error(`Error processing ${key}:`, e);
      await pgPool.query('UPDATE ingest_files SET status = $1 WHERE id = $2', ['error', id]);
      throw e;
    }
  },
  { connection: mqConnection, concurrency: 1 }
);

async function bootstrapEnqueueFromDb() {
  const res = await pgPool.query("SELECT id, file_name, status FROM ingest_files WHERE status IN ('new','queued') ORDER BY id ASC LIMIT 100");
  for (const row of res.rows) {
    const id = row.id as number;
    const key = String(row.file_name);
    if (!key) continue;
    try {
      if (row.status === 'new') {
        await pgPool.query('UPDATE ingest_files SET status = $1 WHERE id = $2', ['queued', id]);
      }
      await ingestQueue.add('ingest-file', { id, key }, { removeOnComplete: true, removeOnFail: false, attempts: 2, backoff: { type: 'fixed', delay: 2000 } });
    } catch (e) {
      console.error('bootstrap enqueue error', id, key, e);
    }
  }
}

async function main() {
  console.log('Worker2: ensure buckets and scan for new files');
  await ensureBuckets();
  await bootstrapEnqueueFromDb();

  const tick = async () => {
    try {
      const c = await registerNewFiles();
      if (c > 0) console.log(`Worker2: registered ${c} new file(s) from ${config.s3.rawBucket}`);
    } catch (e) {
      console.error('Worker2 scan error', e);
    }
  };

  await tick();
  setInterval(tick, 60_000);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
