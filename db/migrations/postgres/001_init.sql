-- Postgres initial schema for API keys, logs, and ingest files
CREATE TABLE IF NOT EXISTS api_keys (
  id SERIAL PRIMARY KEY,
  key TEXT UNIQUE,
  user_name TEXT,
  credits BIGINT DEFAULT 0,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_logs (
  id SERIAL PRIMARY KEY,
  user_id INT,
  endpoint TEXT,
  query_text TEXT,
  credits_deducted INT,
  duration_ms INT,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest_files (
  id SERIAL PRIMARY KEY,
  file_name TEXT,
  file_checksum TEXT,
  rows_ingested BIGINT,
  status TEXT,
  created_at TIMESTAMP DEFAULT now()
);
