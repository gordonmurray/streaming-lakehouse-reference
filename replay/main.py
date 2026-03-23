"""Historical replay — reads Iceberg cold tier and replays ticks to Iggy."""

import asyncio
import json
import logging
import os
import signal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
from apache_iggy import IggyClient, SendMessage
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("replay")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WAREHOUSE_PATH = os.environ.get(
    "REPLAY_WAREHOUSE_PATH",
    "/data/warehouse/iceberg/crypto/ticks/data",
)
REPLAY_SPEED = float(os.environ.get("REPLAY_SPEED", "60"))
REPLAY_DATE = os.environ.get("REPLAY_DATE", "")
REPLAY_PAIRS = os.environ.get("REPLAY_PAIRS", "")
METRICS_PORT = int(os.environ.get("REPLAY_METRICS_PORT", "8002"))

IGGY_USERNAME = os.environ.get("IGGY_USERNAME", "iggy")
IGGY_PASSWORD = os.environ.get("IGGY_PASSWORD", "iggy")
IGGY_HOST = os.environ.get("IGGY_HOST", "iggy")
IGGY_TCP_PORT = os.environ.get("IGGY_TCP_PORT", "8090")
IGGY_STREAM = os.environ.get("IGGY_STREAM", "crypto")
IGGY_REPLAY_TOPIC = os.environ.get("IGGY_REPLAY_TOPIC", "replay")
IGGY_PARTITIONS = int(os.environ.get("IGGY_TOPIC_PARTITIONS", "3"))

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
RECORDS_TOTAL = Gauge("replay_records_total", "Total records to replay")
RECORDS_SENT = Gauge("replay_records_sent", "Records replayed so far")
SPEED_GAUGE = Gauge("replay_speed_multiplier", "Replay speed multiplier")
PROGRESS_PCT = Gauge("replay_progress_pct", "Replay progress percentage")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
shutdown_event = asyncio.Event()


def _handle_signal() -> None:
    log.info("Shutdown signal received")
    shutdown_event.set()


# ---------------------------------------------------------------------------
# Iggy setup
# ---------------------------------------------------------------------------
async def _connect_iggy() -> IggyClient:
    conn_str = f"iggy+tcp://{IGGY_USERNAME}:{IGGY_PASSWORD}@{IGGY_HOST}:{IGGY_TCP_PORT}"
    client = IggyClient.from_connection_string(conn_str)
    await client.connect()
    await client.login_user(IGGY_USERNAME, IGGY_PASSWORD)
    log.info("Connected to Iggy")

    try:
        await client.create_topic(
            IGGY_STREAM, IGGY_REPLAY_TOPIC, partitions_count=IGGY_PARTITIONS
        )
        log.info(
            "Created Iggy topic: %s/%s (%d partitions)",
            IGGY_STREAM, IGGY_REPLAY_TOPIC, IGGY_PARTITIONS,
        )
    except Exception:
        log.debug("Topic %s/%s already exists", IGGY_STREAM, IGGY_REPLAY_TOPIC)

    return client


# ---------------------------------------------------------------------------
# Load records from Iceberg parquet files
# ---------------------------------------------------------------------------
def _load_records() -> list[dict]:
    log.info("Reading parquet files from %s", WAREHOUSE_PATH)

    dataset = ds.dataset(
        WAREHOUSE_PATH,
        format="parquet",
        partitioning=ds.partitioning(
            pa.schema([("dt", pa.string())]),
            flavor="hive",
        ),
    )

    filter_expr = None
    if REPLAY_DATE:
        filter_expr = ds.field("dt") == REPLAY_DATE
        log.info("Filtering to date: %s", REPLAY_DATE)

    table = dataset.to_table(filter=filter_expr)

    if REPLAY_PAIRS:
        pairs = [p.strip() for p in REPLAY_PAIRS.split(",")]
        mask = pc.is_in(table.column("pair"), value_set=pa.array(pairs))
        table = table.filter(mask)
        log.info("Filtering to pairs: %s", pairs)

    table = table.sort_by("event_time")
    rows = table.to_pylist()
    log.info("Loaded %d records", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Replay loop
# ---------------------------------------------------------------------------
async def _replay(iggy: IggyClient, rows: list[dict]) -> None:
    total = len(rows)
    RECORDS_TOTAL.set(total)
    SPEED_GAUGE.set(REPLAY_SPEED)

    log.info("Starting replay: %d records at %.1fx speed", total, REPLAY_SPEED)

    prev_event_time = None
    sent = 0

    for row in rows:
        if shutdown_event.is_set():
            log.info("Replay interrupted by shutdown")
            break

        event_time = row["event_time"]

        # Speed-controlled delay between events
        if prev_event_time is not None and REPLAY_SPEED > 0:
            delta = (event_time - prev_event_time).total_seconds()
            if delta > 0:
                await asyncio.sleep(delta / REPLAY_SPEED)

        prev_event_time = event_time

        # Build JSON message matching poller format (strings for numeric fields)
        tick = {
            "pair": row["pair"],
            "sequence": row.get("sequence", 0),
            "price": str(row["price"]),
            "best_bid": str(row["best_bid"]),
            "best_ask": str(row["best_ask"]),
            "volume_24h": str(row["volume_24h"]),
            "time": event_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        partition_id = hash(row["pair"]) % IGGY_PARTITIONS
        msg = SendMessage(json.dumps(tick))
        await iggy.send_messages(
            IGGY_STREAM, IGGY_REPLAY_TOPIC, partition_id, [msg]
        )

        sent += 1
        RECORDS_SENT.set(sent)
        PROGRESS_PCT.set(sent / total * 100)

        if sent % 10000 == 0:
            log.info("Replayed %d/%d (%.1f%%)", sent, total, sent / total * 100)

    log.info("Replay complete: %d records sent", sent)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    start_http_server(METRICS_PORT)
    log.info("Replay metrics on :%d/metrics", METRICS_PORT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    rows = _load_records()
    if not rows:
        log.error("No records found in %s", WAREHOUSE_PATH)
        return

    iggy = await _connect_iggy()
    await _replay(iggy, rows)


if __name__ == "__main__":
    asyncio.run(main())
