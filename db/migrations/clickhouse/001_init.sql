-- ClickHouse initial schema for master records
CREATE TABLE IF NOT EXISTS master_records (
  record_id String,
  source String,
  source_record_id String,
  ts_ingested DateTime,
  normalized_field_1 String,
  normalized_field_2 Float64,
  tags Array(String),
  metadata String,
  checksum String,
  is_valid UInt8,
  validation_errors Array(String)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts_ingested)
ORDER BY (record_id);
