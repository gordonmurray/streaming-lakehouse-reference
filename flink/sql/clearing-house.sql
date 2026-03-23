-- Phase 10: Clearing House — Flink processes OrderRequests into ledger entries
--
-- Consumes OrderRequest from Iggy, applies fees/slippage, writes to Paimon.
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/clearing-house.sql

SET 'execution.checkpointing.interval' = '15s';
SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.iceberg.;org.apache.hadoop.;com.codahale.';

-- ============================================================
-- Source: order requests from Iggy
-- ============================================================

CREATE TABLE order_requests (
  order_id          STRING,
  pair              STRING,
  side              STRING,
  price             DOUBLE,
  quantity          DOUBLE,
  signal_similarity DOUBLE,
  signal_sentiment  STRING,
  `time`            STRING,
  event_time AS TO_TIMESTAMP(REPLACE(`time`, 'Z', ''), 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iggy',
  'host'      = 'iggy',
  'stream'    = 'crypto',
  'topic'     = 'orders',
  'format'    = 'json'
);

-- ============================================================
-- Paimon catalog + tables (already created by seed-balance.sql)
-- ============================================================

CREATE CATALOG paimon_catalog WITH (
  'type'      = 'paimon',
  'warehouse' = '/opt/flink/warehouse/paimon'
);

-- ============================================================
-- Fluss catalog + executed trades table (hot queryable)
-- ============================================================

CREATE CATALOG fluss_catalog WITH (
  'type'      = 'fluss',
  'bootstrap.servers' = 'fluss-coordinator:9123'
);

CREATE DATABASE IF NOT EXISTS fluss_catalog.crypto;

CREATE TABLE IF NOT EXISTS fluss_catalog.crypto.executed_trades (
  trade_id          STRING,
  pair              STRING,
  side              STRING,
  fill_price        DOUBLE,
  quantity          DOUBLE,
  fee               DOUBLE,
  event_time        TIMESTAMP(6)
);

-- ============================================================
-- Clearing House: apply fees/slippage, write to all sinks
-- ============================================================

EXECUTE STATEMENT SET
BEGIN

-- 1. Trade record → Paimon (persistent ledger)
INSERT INTO paimon_catalog.crypto.trades
SELECT
  order_id          AS trade_id,
  pair,
  side,
  price,
  CASE
    WHEN side = 'BUY'  THEN price * 1.0005
    ELSE price * 0.9995
  END               AS fill_price,
  quantity,
  CASE
    WHEN side = 'BUY'  THEN price * 1.0005 * quantity * 0.001
    ELSE price * 0.9995 * quantity * 0.001
  END               AS fee,
  CASE
    WHEN side = 'BUY'  THEN price * 1.0005 * quantity * 1.001
    ELSE price * 0.9995 * quantity * 0.999
  END               AS total_cost,
  signal_similarity,
  signal_sentiment,
  event_time
FROM order_requests;

-- 2. USD balance delta → Paimon (aggregation: sum on read)
INSERT INTO paimon_catalog.crypto.balance
SELECT
  'USD' AS currency,
  CASE
    WHEN side = 'BUY'  THEN -(price * 1.0005 * quantity * 1.001)
    ELSE price * 0.9995 * quantity * 0.999
  END   AS amount
FROM order_requests;

-- 3. Crypto balance delta → Paimon
INSERT INTO paimon_catalog.crypto.balance
SELECT
  REPLACE(pair, '-USD', '') AS currency,
  CASE
    WHEN side = 'BUY'  THEN quantity
    ELSE -quantity
  END   AS amount
FROM order_requests;

-- 4. Executed trade → Fluss (hot tier, sub-second queries)
INSERT INTO fluss_catalog.crypto.executed_trades
SELECT
  order_id          AS trade_id,
  pair,
  side,
  CASE
    WHEN side = 'BUY'  THEN price * 1.0005
    ELSE price * 0.9995
  END               AS fill_price,
  quantity,
  CASE
    WHEN side = 'BUY'  THEN price * 1.0005 * quantity * 0.001
    ELSE price * 0.9995 * quantity * 0.001
  END               AS fee,
  event_time
FROM order_requests;

END;
