-- Phase 3: 1-minute OHLCV candles from Iggy crypto ticks
--
-- Submit via: docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/ohlcv-candles.sql

SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';

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

SET 'sql-client.execution.result-mode' = 'tableau';

SELECT
  pair,
  TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
  FIRST_VALUE(CAST(price AS DOUBLE))  AS `open`,
  MAX(CAST(price AS DOUBLE))          AS high,
  MIN(CAST(price AS DOUBLE))          AS low,
  LAST_VALUE(CAST(price AS DOUBLE))   AS `close`,
  COUNT(*)                            AS tick_count
FROM iggy_ticks
GROUP BY
  pair,
  TUMBLE(event_time, INTERVAL '1' MINUTE)
LIMIT 10;
