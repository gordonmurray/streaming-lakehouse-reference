-- Phase 10: Seed initial balance and create ledger tables
--
-- Run ONCE before submitting the clearing house job.
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/seed-balance.sql

SET 'execution.runtime-mode' = 'batch';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.hadoop.;com.codahale.';

-- ============================================================
-- Paimon catalog
-- ============================================================

CREATE CATALOG paimon_catalog WITH (
  'type'      = 'paimon',
  'warehouse' = '/opt/flink/warehouse/paimon'
);

CREATE DATABASE IF NOT EXISTS paimon_catalog.crypto;

-- ============================================================
-- Balance table (aggregation merge engine — INSERTs are deltas)
-- ============================================================

CREATE TABLE IF NOT EXISTS paimon_catalog.crypto.balance (
  currency      STRING,
  amount        DOUBLE,
  PRIMARY KEY (currency) NOT ENFORCED
) WITH (
  'merge-engine'                      = 'aggregation',
  'fields.amount.aggregate-function'  = 'sum',
  'bucket'                            = '1'
);

-- ============================================================
-- Trades table (deduplicate merge engine — idempotent history)
-- ============================================================

CREATE TABLE IF NOT EXISTS paimon_catalog.crypto.trades (
  trade_id            STRING,
  pair                STRING,
  side                STRING,
  price               DOUBLE,
  fill_price          DOUBLE,
  quantity            DOUBLE,
  fee                 DOUBLE,
  total_cost          DOUBLE,
  signal_similarity   DOUBLE,
  signal_sentiment    STRING,
  event_time          TIMESTAMP(6),
  PRIMARY KEY (trade_id) NOT ENFORCED
) WITH (
  'merge-engine'  = 'deduplicate',
  'bucket'        = '4'
);

-- ============================================================
-- Seed starting balance: $1,000 USD
-- ============================================================

INSERT INTO paimon_catalog.crypto.balance VALUES ('USD', 1000.0);
