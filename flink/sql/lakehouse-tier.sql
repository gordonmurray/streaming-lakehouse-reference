-- Phase 5: Tiered Lakehouse — Paimon (warm) + Iceberg (cold)
--
-- Paimon: 1-minute OHLCV candles (warm tier, streaming append)
-- Iceberg: raw tick archive (cold tier, partitioned by day)
--
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/lakehouse-tier.sql

SET 'execution.checkpointing.interval' = '30s';
-- Only Iggy connector in pipeline.jars; Paimon/Iceberg/Hadoop resolved from parent classloader (in /opt/flink/lib/)
SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.iceberg.;org.apache.hadoop.;com.codahale.';

-- ============================================================
-- Source: raw ticks from Iggy (shared by both sinks)
-- ============================================================

CREATE TABLE iggy_ticks (
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
  'topic'     = 'prices',
  'format'    = 'json'
);

-- ============================================================
-- Warm Tier: Paimon — OHLCV candles
-- ============================================================

CREATE CATALOG paimon_catalog WITH (
  'type'      = 'paimon',
  'warehouse' = '/opt/flink/warehouse/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_catalog.crypto;

CREATE TABLE IF NOT EXISTS paimon_catalog.crypto.ohlcv_1m (
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
-- Cold Tier: Iceberg — raw tick archive
-- ============================================================

CREATE CATALOG iceberg_catalog WITH (
  'type'           = 'iceberg',
  'catalog-type'   = 'hadoop',
  'warehouse'      = '/opt/flink/warehouse/iceberg'
);

CREATE DATABASE IF NOT EXISTS iceberg_catalog.crypto;

CREATE TABLE IF NOT EXISTS iceberg_catalog.crypto.ticks (
  pair          STRING,
  price         DOUBLE,
  best_bid      DOUBLE,
  best_ask      DOUBLE,
  volume_24h    DOUBLE,
  event_time    TIMESTAMP(6),
  `sequence`    BIGINT,
  dt            STRING
) PARTITIONED BY (dt)
WITH (
  'write.format.default'              = 'parquet',
  'write.parquet.compression-codec'   = 'zstd',
  'write.target-file-size-bytes'      = '134217728'
);

-- ============================================================
-- Statement Set: both pipelines submitted as a single job
-- ============================================================

EXECUTE STATEMENT SET
BEGIN

-- Pipeline 1: Iggy -> 1-min tumbling window -> Paimon candles
INSERT INTO paimon_catalog.crypto.ohlcv_1m
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
  TUMBLE(TABLE iggy_ticks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
GROUP BY pair, window_start, window_end;

-- Pipeline 2: Iggy -> raw ticks -> Iceberg archive (partitioned by day)
INSERT INTO iceberg_catalog.crypto.ticks
SELECT
  pair,
  CAST(price AS DOUBLE),
  CAST(best_bid AS DOUBLE),
  CAST(best_ask AS DOUBLE),
  CAST(volume_24h AS DOUBLE),
  event_time,
  `sequence`,
  DATE_FORMAT(event_time, 'yyyy-MM-dd') AS dt
FROM iggy_ticks;

END;
