-- Phase 6: Replay Pipeline — reads replayed ticks, writes candles to Paimon
--
-- The replay service reads from Iceberg (cold tier) and publishes to
-- Iggy topic crypto/replay. This job consumes that topic and computes
-- 1-minute OHLCV candles into the same Paimon table used by the live
-- pipeline. Candles appear at their original historical timestamps.
--
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/replay-pipeline.sql

SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.hadoop.;com.codahale.';

-- ============================================================
-- Source: replayed ticks from Iggy
-- ============================================================

CREATE TABLE replay_ticks (
  pair          STRING,
  price         STRING,
  best_bid      STRING,
  best_ask      STRING,
  volume_24h    STRING,
  `time`        STRING,
  `sequence`    BIGINT,
  event_time AS TO_TIMESTAMP(REPLACE(`time`, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iggy',
  'host'      = 'iggy',
  'stream'    = 'crypto',
  'topic'     = 'replay',
  'format'    = 'json'
);

-- ============================================================
-- Warm Tier: Paimon — same table as live pipeline
-- ============================================================

CREATE CATALOG paimon_catalog WITH (
  'type'      = 'paimon',
  'warehouse' = '/opt/flink/warehouse/paimon'
);

-- Separate table to avoid concurrent write conflicts with the live pipeline
CREATE TABLE IF NOT EXISTS paimon_catalog.crypto.replay_ohlcv_1m (
  pair          STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  `open`        DOUBLE,
  high          DOUBLE,
  low           DOUBLE,
  `close`       DOUBLE,
  tick_count    BIGINT
) WITH (
  'bucket'          = '4',
  'bucket-key'      = 'pair',
  'file.format'     = 'parquet',
  'file.compression' = 'zstd',
  'snapshot.num-retained.min' = '5',
  'snapshot.num-retained.max' = '20'
);

-- ============================================================
-- Pipeline: replay ticks -> 1-min OHLCV -> Paimon
-- ============================================================

INSERT INTO paimon_catalog.crypto.replay_ohlcv_1m
SELECT
  pair,
  window_start,
  window_end,
  FIRST_VALUE(CAST(price AS DOUBLE))  AS `open`,
  MAX(CAST(price AS DOUBLE))          AS high,
  MIN(CAST(price AS DOUBLE))          AS low,
  LAST_VALUE(CAST(price AS DOUBLE))   AS `close`,
  COUNT(*)                            AS tick_count
FROM TABLE(
  TUMBLE(TABLE replay_ticks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY pair, window_start, window_end;
