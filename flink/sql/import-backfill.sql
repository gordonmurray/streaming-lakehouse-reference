-- Import backfilled candles from staging parquet into Paimon ohlcv_1m
--
-- Prerequisites:
--   1. Run scripts/backfill-candles.py to stage candles.parquet on the volume
--   2. Flink cluster running with the lakehouse-tier table already created
--
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/import-backfill.sql

SET 'execution.checkpointing.interval' = '30s';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.hadoop.;com.codahale.';

-- Source: staged parquet file from backfill script
CREATE TABLE backfill_source (
  pair          STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  `open`        DOUBLE,
  high          DOUBLE,
  low           DOUBLE,
  `close`       DOUBLE,
  tick_count    BIGINT
) WITH (
  'connector' = 'filesystem',
  'path'      = 'file:///opt/flink/warehouse/backfill/',
  'format'    = 'parquet'
);

-- Sink: Paimon ohlcv_1m (must already exist from lakehouse-tier.sql)
CREATE CATALOG paimon_catalog WITH (
  'type'      = 'paimon',
  'warehouse' = '/opt/flink/warehouse/paimon'
);

-- Import all backfilled candles into Paimon (proper write path — handles bucketing and manifests)
INSERT INTO paimon_catalog.crypto.ohlcv_1m
SELECT * FROM backfill_source;
