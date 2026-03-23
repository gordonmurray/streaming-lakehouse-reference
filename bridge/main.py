"""Iggy-to-Prometheus bridge — consumes ticks from Iggy and exposes as metrics."""

import asyncio
import json
import logging
import os
import signal

from apache_iggy import IggyClient, PollingStrategy
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("bridge")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
METRICS_PORT = int(os.environ.get("BRIDGE_METRICS_PORT", "8001"))

IGGY_USERNAME = os.environ.get("IGGY_USERNAME", "iggy")
IGGY_PASSWORD = os.environ.get("IGGY_PASSWORD", "iggy")
IGGY_HOST = os.environ.get("IGGY_HOST", "iggy")
IGGY_TCP_PORT = os.environ.get("IGGY_TCP_PORT", "8090")
IGGY_STREAM = os.environ.get("IGGY_STREAM", "crypto")
IGGY_TOPIC = os.environ.get("IGGY_TOPIC", "prices")
IGGY_PARTITIONS = int(os.environ.get("IGGY_TOPIC_PARTITIONS", "3"))
POLL_INTERVAL = float(os.environ.get("BRIDGE_POLL_INTERVAL", "0.05"))

# ---------------------------------------------------------------------------
# Prometheus metrics (same names as Phase 1 poller — drop-in replacement)
# ---------------------------------------------------------------------------
PRICE = Gauge("crypto_price", "Last trade price", ["pair"])
BEST_BID = Gauge("crypto_best_bid", "Best bid price", ["pair"])
BEST_ASK = Gauge("crypto_best_ask", "Best ask price", ["pair"])
VOLUME_24H = Gauge("crypto_volume_24h", "24-hour volume", ["pair"])
SPREAD = Gauge("crypto_spread", "Bid-ask spread", ["pair"])
BRIDGE_CONSUMED = Gauge("bridge_consumed_total", "Total messages consumed from Iggy", ["pair"])
BRIDGE_DEDUPED = Gauge("bridge_deduplicated_total", "Total duplicate messages dropped", ["pair"])
IGGY_CONNECTED = Gauge("bridge_iggy_connected", "Iggy connection status (1=connected)")

_consume_counts: dict[str, int] = {}
_dedup_counts: dict[str, int] = {}

# Sequence-based deduplication: track last seen sequence per pair
_last_sequence: dict[str, int] = {}

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
shutdown_event = asyncio.Event()


def _handle_signal() -> None:
    log.info("Shutdown signal received")
    shutdown_event.set()


# ---------------------------------------------------------------------------
# Tick processing with deduplication
# ---------------------------------------------------------------------------
def _process_tick(tick: dict) -> None:
    pair = tick.get("pair")
    if not pair:
        return

    # Sequence-based dedup: Coinbase sequence numbers are monotonically
    # increasing per product. Drop any tick with a sequence <= last seen.
    seq = tick.get("sequence")
    if seq is not None:
        seq = int(seq)
        last = _last_sequence.get(pair, -1)
        if seq <= last:
            _dedup_counts[pair] = _dedup_counts.get(pair, 0) + 1
            BRIDGE_DEDUPED.labels(pair=pair).set(_dedup_counts[pair])
            return
        _last_sequence[pair] = seq

    price = tick.get("price")
    bid = tick.get("best_bid")
    ask = tick.get("best_ask")
    vol = tick.get("volume_24h")

    if price is not None:
        PRICE.labels(pair=pair).set(float(price))
    if bid is not None:
        BEST_BID.labels(pair=pair).set(float(bid))
    if ask is not None:
        BEST_ASK.labels(pair=pair).set(float(ask))
    if vol is not None:
        VOLUME_24H.labels(pair=pair).set(float(vol))
    if bid is not None and ask is not None:
        SPREAD.labels(pair=pair).set(float(ask) - float(bid))

    _consume_counts[pair] = _consume_counts.get(pair, 0) + 1
    BRIDGE_CONSUMED.labels(pair=pair).set(_consume_counts[pair])


# ---------------------------------------------------------------------------
# Resolve starting offsets — skip to the latest messages on startup
# ---------------------------------------------------------------------------
async def _get_latest_offsets(client: IggyClient) -> dict[int, int]:
    """Find the latest offset per partition so we start consuming live data."""
    offsets: dict[int, int] = {}
    for p in range(IGGY_PARTITIONS):
        msgs = await client.poll_messages(
            IGGY_STREAM, IGGY_TOPIC,
            partition_id=p,
            polling_strategy=PollingStrategy.Last(),
            count=1,
            auto_commit=False,
        )
        if msgs:
            offsets[p] = msgs[0].offset() + 1
        else:
            offsets[p] = 0
    return offsets


# ---------------------------------------------------------------------------
# Iggy consumer loop
# ---------------------------------------------------------------------------
async def _consume() -> None:
    backoff = 1
    max_backoff = 30

    while not shutdown_event.is_set():
        try:
            conn_str = f"iggy+tcp://{IGGY_USERNAME}:{IGGY_PASSWORD}@{IGGY_HOST}:{IGGY_TCP_PORT}"
            client = IggyClient.from_connection_string(conn_str)
            await client.connect()
            await client.login_user(IGGY_USERNAME, IGGY_PASSWORD)
            IGGY_CONNECTED.set(1)
            backoff = 1
            log.info("Connected to Iggy — consuming %s/%s", IGGY_STREAM, IGGY_TOPIC)

            # Start from latest offsets to avoid replaying history
            next_offset = await _get_latest_offsets(client)
            log.info("Starting offsets: %s", next_offset)

            while not shutdown_event.is_set():
                got_messages = False
                for partition_id in range(IGGY_PARTITIONS):
                    messages = await client.poll_messages(
                        IGGY_STREAM,
                        IGGY_TOPIC,
                        partition_id=partition_id,
                        polling_strategy=PollingStrategy.Offset(next_offset[partition_id]),
                        count=100,
                        auto_commit=False,
                    )
                    for msg in messages:
                        try:
                            tick = json.loads(msg.payload())
                            _process_tick(tick)
                        except (json.JSONDecodeError, ValueError) as exc:
                            log.warning("Bad message at offset %d: %s", msg.offset(), exc)
                        next_offset[partition_id] = msg.offset() + 1

                    if messages:
                        got_messages = True

                if not got_messages:
                    await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            break

        except Exception as exc:
            IGGY_CONNECTED.set(0)
            log.warning("Iggy connection lost (%s). Reconnecting in %ds…", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    IGGY_CONNECTED.set(0)
    log.info("Consumer stopped")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    start_http_server(METRICS_PORT)
    log.info("Bridge metrics on :%d/metrics", METRICS_PORT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await _consume()


if __name__ == "__main__":
    asyncio.run(main())
