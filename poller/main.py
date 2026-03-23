"""Coinbase WebSocket poller — publishes live crypto ticks to Iggy."""

import asyncio
import json
import logging
import os
import signal

import websockets
from apache_iggy import IggyClient, SendMessage
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("poller")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
WS_URL = os.environ.get("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
PAIRS = [p.strip() for p in os.environ.get("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",")]
METRICS_PORT = int(os.environ.get("POLLER_METRICS_PORT", "8000"))

IGGY_USERNAME = os.environ.get("IGGY_USERNAME", "iggy")
IGGY_PASSWORD = os.environ.get("IGGY_PASSWORD", "iggy")
IGGY_HOST = os.environ.get("IGGY_HOST", "iggy")
IGGY_TCP_PORT = os.environ.get("IGGY_TCP_PORT", "8090")
IGGY_STREAM = os.environ.get("IGGY_STREAM", "crypto")
IGGY_TOPIC = os.environ.get("IGGY_TOPIC", "prices")
IGGY_PARTITIONS = int(os.environ.get("IGGY_TOPIC_PARTITIONS", "3"))

# ---------------------------------------------------------------------------
# Prometheus metrics (poller health only — price metrics move to bridge)
# ---------------------------------------------------------------------------
WS_MESSAGES = Gauge("poller_ws_messages_total", "Total WebSocket messages received", ["pair"])
WS_CONNECTED = Gauge("poller_ws_connected", "WebSocket connection status (1=connected)")
IGGY_CONNECTED = Gauge("poller_iggy_connected", "Iggy connection status (1=connected)")
IGGY_PUBLISHED = Gauge("poller_iggy_published_total", "Total messages published to Iggy", ["pair"])

_msg_counts: dict[str, int] = {}
_pub_counts: dict[str, int] = {}

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

    # Create stream and topic if they don't exist (idempotent)
    try:
        await client.create_stream(IGGY_STREAM)
        log.info("Created Iggy stream: %s", IGGY_STREAM)
    except Exception:
        log.debug("Stream %s already exists", IGGY_STREAM)

    try:
        await client.create_topic(IGGY_STREAM, IGGY_TOPIC, partitions_count=IGGY_PARTITIONS)
        log.info("Created Iggy topic: %s/%s (%d partitions)", IGGY_STREAM, IGGY_TOPIC, IGGY_PARTITIONS)
    except Exception:
        log.debug("Topic %s/%s already exists", IGGY_STREAM, IGGY_TOPIC)

    IGGY_CONNECTED.set(1)
    return client


# ---------------------------------------------------------------------------
# WebSocket loop
# ---------------------------------------------------------------------------
async def _subscribe(ws) -> None:
    msg = {
        "type": "subscribe",
        "product_ids": PAIRS,
        "channels": ["ticker"],
    }
    await ws.send(json.dumps(msg))
    log.info("Subscribed to ticker channel for %s", PAIRS)


async def _publish(iggy: IggyClient, data: dict) -> None:
    pair = data.get("product_id")
    if not pair:
        return

    tick = {
        "pair": pair,
        "sequence": data.get("sequence"),
        "price": data.get("price"),
        "best_bid": data.get("best_bid"),
        "best_ask": data.get("best_ask"),
        "volume_24h": data.get("volume_24h"),
        "time": data.get("time"),
    }

    partition_id = hash(pair) % IGGY_PARTITIONS
    msg = SendMessage(json.dumps(tick))
    await iggy.send_messages(IGGY_STREAM, IGGY_TOPIC, partition_id, [msg])

    _msg_counts[pair] = _msg_counts.get(pair, 0) + 1
    WS_MESSAGES.labels(pair=pair).set(_msg_counts[pair])
    _pub_counts[pair] = _pub_counts.get(pair, 0) + 1
    IGGY_PUBLISHED.labels(pair=pair).set(_pub_counts[pair])


async def _listen(iggy: IggyClient) -> None:
    backoff = 1
    max_backoff = 30

    while not shutdown_event.is_set():
        try:
            log.info("Connecting to %s", WS_URL)
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=10) as ws:
                WS_CONNECTED.set(1)
                backoff = 1
                await _subscribe(ws)

                async for raw in ws:
                    if shutdown_event.is_set():
                        break
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    if data.get("type") == "ticker":
                        await _publish(iggy, data)

        except (websockets.ConnectionClosed, OSError) as exc:
            WS_CONNECTED.set(0)
            log.warning("Connection lost (%s). Reconnecting in %ds…", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

        except asyncio.CancelledError:
            break

    WS_CONNECTED.set(0)
    log.info("Listener stopped")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    start_http_server(METRICS_PORT)
    log.info("Poller health metrics on :%d/metrics", METRICS_PORT)
    log.info("Tracking pairs: %s", PAIRS)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    iggy = await _connect_iggy()
    await _listen(iggy)


if __name__ == "__main__":
    asyncio.run(main())
