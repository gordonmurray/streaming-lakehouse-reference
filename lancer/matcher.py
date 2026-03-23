"""Live signal matcher — consumes Iggy ticks, queries LanceDB, pushes annotations."""

import asyncio
import json
import logging
import os
import signal
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone

import httpx
import lancedb
import numpy as np
import uvicorn
from apache_iggy import IggyClient, PollingStrategy
from fastapi import FastAPI
from prometheus_client import Gauge, start_http_server

from vectors import WINDOW_SIZE, DOWNSAMPLE, MIN_VOLATILITY_PCT, zscore_normalize, _downsample

log = logging.getLogger("matcher")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LANCEDB_PATH = os.environ.get("LANCER_DB_PATH", "/data/lancedb")
WINDOW = int(os.environ.get("LANCER_WINDOW_SIZE", str(WINDOW_SIZE)))
TOP_K = int(os.environ.get("LANCER_TOP_K", "5"))
SIMILARITY_THRESHOLD = float(os.environ.get("LANCER_SIMILARITY_THRESHOLD", "0.85"))
MIN_OUTCOME_PCT = float(os.environ.get("LANCER_MIN_OUTCOME_PCT", "0.1"))
API_PORT = int(os.environ.get("LANCER_API_PORT", "8003"))
METRICS_PORT = int(os.environ.get("LANCER_METRICS_PORT", "8004"))
POLL_INTERVAL = float(os.environ.get("LANCER_POLL_INTERVAL", "0.1"))

IGGY_USERNAME = os.environ.get("IGGY_USERNAME", "iggy")
IGGY_PASSWORD = os.environ.get("IGGY_PASSWORD", "iggy")
IGGY_HOST = os.environ.get("IGGY_HOST", "iggy")
IGGY_TCP_PORT = os.environ.get("IGGY_TCP_PORT", "8090")
IGGY_STREAM = os.environ.get("IGGY_STREAM", "crypto")
IGGY_TOPIC = os.environ.get("IGGY_TOPIC", "prices")
IGGY_PARTITIONS = int(os.environ.get("IGGY_TOPIC_PARTITIONS", "3"))

WAREHOUSE_PATH = os.environ.get(
    "LANCER_WAREHOUSE_PATH",
    "/data/warehouse/paimon/crypto.db/ohlcv_1m",
)

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_PASSWORD", "admin")

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
TOP_SIMILARITY = Gauge("lancer_top_similarity", "Best match similarity score", ["pair"])
INDEX_SIZE = Gauge("lancer_index_size", "Total patterns in LanceDB index")
CANDLE_BUFFER_SIZE = Gauge("lancer_candle_buffer", "Candles in sliding buffer", ["pair"])
QUERIES_TOTAL = Gauge("lancer_queries_total", "Total similarity queries run", ["pair"])
ANNOTATIONS_PUSHED = Gauge("lancer_annotations_total", "Total Grafana annotations pushed", ["pair"])

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
# Per-pair: accumulate ticks into 1-min candles, maintain sliding buffer
_tick_accum: dict[str, list[dict]] = defaultdict(list)
_candle_buffers: dict[str, deque] = defaultdict(lambda: deque(maxlen=WINDOW))
_latest_matches: dict[str, list[dict]] = {}
_query_counts: dict[str, int] = defaultdict(int)
_annotation_counts: dict[str, int] = defaultdict(int)

shutdown_event = asyncio.Event()


def _handle_signal() -> None:
    log.info("Shutdown signal received")
    shutdown_event.set()


# ---------------------------------------------------------------------------
# Tick → candle aggregation
# ---------------------------------------------------------------------------
def _minute_key(time_str: str) -> str:
    """Truncate ISO timestamp to minute precision."""
    return time_str[:16]  # 'YYYY-MM-DDTHH:MM'


def _prefill_candle_buffers() -> None:
    """Pre-fill candle buffers from Paimon historical data (most recent per pair)."""
    import glob as globmod
    try:
        import duckdb
    except ImportError:
        log.warning("DuckDB not available — candle buffer will fill from live ticks")
        return

    files = [
        f for f in globmod.glob(f"{WAREHOUSE_PATH}/**/data-*.parquet", recursive=True)
        if os.path.getsize(f) > 0
    ]
    if not files:
        log.info("No Paimon candle data — buffer will fill from live ticks")
        return

    try:
        file_list = ", ".join(f"'{f}'" for f in files)
        con = duckdb.connect()
        rows = con.execute(f"""
            SELECT pair, window_start, open, high, low, close, tick_count
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY pair ORDER BY window_start DESC) as rn
                FROM read_parquet([{file_list}], union_by_name=true)
            )
            WHERE rn <= {WINDOW}
            ORDER BY pair, window_start ASC
        """).fetchall()
        con.close()

        for row in rows:
            pair = row[0]
            candle = {
                "pair": pair,
                "open": row[2],
                "high": row[3],
                "low": row[4],
                "close": row[5],
                "tick_count": row[6],
                "time": str(row[1]),
            }
            _candle_buffers[pair].append(candle)

        for pair, buf in _candle_buffers.items():
            CANDLE_BUFFER_SIZE.labels(pair=pair).set(len(buf))
            log.info("  %s: pre-filled %d/%d candles", pair, len(buf), WINDOW)

        total = sum(len(b) for b in _candle_buffers.values())
        log.info("Pre-filled candle buffers from Paimon: %d candles across %d pairs",
                 total, len(_candle_buffers))
    except Exception as exc:
        log.warning("Failed to pre-fill candle buffers: %s — will fill from live ticks", exc)


def _flush_candle(pair: str, ticks: list[dict]) -> dict | None:
    """Aggregate a minute's ticks into an OHLCV candle."""
    if not ticks:
        return None
    prices = [float(t["price"]) for t in ticks]
    return {
        "pair": pair,
        "open": prices[0],
        "high": max(prices),
        "low": min(prices),
        "close": prices[-1],
        "tick_count": len(prices),
        "time": ticks[0]["time"],
    }


# ---------------------------------------------------------------------------
# LanceDB query
# ---------------------------------------------------------------------------
def _query_similar(table, pair: str, closes: list[float]) -> list[dict]:
    """Z-score normalize current window and query LanceDB for similar patterns.

    Applies downsampling and volatility floor before querying.
    Filters to patterns where the historical outcome exceeded MIN_OUTCOME_PCT.
    """
    arr = np.array(closes, dtype=np.float64)

    # Volatility floor: reject noisy flat windows
    mean_price = float(np.mean(arr))
    if mean_price > 0 and (float(np.std(arr)) / mean_price) * 100 < MIN_VOLATILITY_PCT:
        return []

    # Downsample before vectorizing (must match index dimensions)
    ds = _downsample(arr, DOWNSAMPLE) if DOWNSAMPLE > 1 else arr
    vector, _, _ = zscore_normalize(ds)

    where_clause = f"pair = '{pair}'"
    if MIN_OUTCOME_PCT != 0:
        where_clause += f" AND outcome_pct > {MIN_OUTCOME_PCT}"

    results = (
        table.search(vector.tolist())
        .where(where_clause)
        .limit(TOP_K)
        .to_list()
    )

    matches = []
    for r in results:
        distance = r.get("_distance", float("inf"))
        similarity = 1.0 / (1.0 + distance)
        match = {
            "window_start": r["window_start"],
            "window_end": r["window_end"],
            "similarity": round(similarity, 4),
            "volatility": round(r["volatility"], 6),
            "mean_price": round(r["mean_price"], 2),
            "distance": round(distance, 4),
        }
        # Include outcome data if available
        if "outcome_pct" in r:
            match["outcome_pct"] = round(r["outcome_pct"], 4)
            match["outcome_max_pct"] = round(r.get("outcome_max_pct", 0), 4)
            match["outcome_min_pct"] = round(r.get("outcome_min_pct", 0), 4)
        matches.append(match)

    return matches


# ---------------------------------------------------------------------------
# Grafana annotation
# ---------------------------------------------------------------------------
async def _push_annotation(pair: str, match: dict) -> None:
    """Push a Grafana annotation for a high-similarity match."""
    text = (
        f"Pattern match: {match['similarity']:.0%} similar to "
        f"{match['window_start']} — {match['window_end']}"
    )
    body = {
        "dashboardUID": "crypto-live-ticks",
        "text": text,
        "tags": ["lancer", pair, "pattern-match"],
    }
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{GRAFANA_URL}/api/annotations",
                json=body,
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
                timeout=5.0,
            )
            if resp.status_code == 200:
                _annotation_counts[pair] = _annotation_counts.get(pair, 0) + 1
                ANNOTATIONS_PUSHED.labels(pair=pair).set(_annotation_counts[pair])
            else:
                log.warning("Grafana annotation failed: %d %s", resp.status_code, resp.text)
    except Exception as exc:
        log.warning("Grafana annotation error: %s", exc)


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(title="Lancer — Pattern Matcher")


@app.get("/api/signals/{pair}")
async def get_signals(pair: str):
    return {
        "pair": pair,
        "matches": _latest_matches.get(pair, []),
        "queried_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


@app.get("/api/signals")
async def get_all_signals():
    return _latest_matches


@app.get("/api/health")
async def health():
    return {"status": "ok", "index_loaded": _lance_table is not None}


# ---------------------------------------------------------------------------
# Iggy consumer + matcher loop
# ---------------------------------------------------------------------------
_lance_table = None


async def _consume_and_match() -> None:
    global _lance_table
    backoff = 1

    # Open LanceDB
    db = lancedb.connect(LANCEDB_PATH)
    try:
        _lance_table = db.open_table("patterns")
        count = _lance_table.count_rows()
        INDEX_SIZE.set(count)
        log.info("Loaded LanceDB index: %d patterns", count)
    except Exception as exc:
        log.error("Cannot open LanceDB patterns table: %s", exc)
        log.error("Run with LANCER_MODE=indexer first to build the index")
        return

    # Pre-fill candle buffers from Paimon so queries work immediately
    _prefill_candle_buffers()

    while not shutdown_event.is_set():
        try:
            conn_str = f"iggy+tcp://{IGGY_USERNAME}:{IGGY_PASSWORD}@{IGGY_HOST}:{IGGY_TCP_PORT}"
            client = IggyClient.from_connection_string(conn_str)
            await client.connect()
            await client.login_user(IGGY_USERNAME, IGGY_PASSWORD)
            backoff = 1
            log.info("Connected to Iggy — consuming %s/%s", IGGY_STREAM, IGGY_TOPIC)

            # Start from latest offsets
            next_offset: dict[int, int] = {}
            for p in range(IGGY_PARTITIONS):
                msgs = await client.poll_messages(
                    IGGY_STREAM, IGGY_TOPIC,
                    partition_id=p,
                    polling_strategy=PollingStrategy.Last(),
                    count=1, auto_commit=False,
                )
                next_offset[p] = (msgs[0].offset() + 1) if msgs else 0

            current_minute: dict[str, str] = {}

            while not shutdown_event.is_set():
                got_messages = False
                for pid in range(IGGY_PARTITIONS):
                    messages = await client.poll_messages(
                        IGGY_STREAM, IGGY_TOPIC,
                        partition_id=pid,
                        polling_strategy=PollingStrategy.Offset(next_offset[pid]),
                        count=100, auto_commit=False,
                    )
                    for msg in messages:
                        try:
                            tick = json.loads(msg.payload())
                        except (json.JSONDecodeError, ValueError):
                            continue
                        next_offset[pid] = msg.offset() + 1

                        pair = tick.get("pair")
                        time_str = tick.get("time", "")
                        if not pair or not time_str:
                            continue

                        mk = _minute_key(time_str)

                        # Detect minute boundary → flush candle
                        if pair in current_minute and current_minute[pair] != mk:
                            candle = _flush_candle(pair, _tick_accum[pair])
                            if candle:
                                _candle_buffers[pair].append(candle)
                                CANDLE_BUFFER_SIZE.labels(pair=pair).set(
                                    len(_candle_buffers[pair])
                                )

                                # Query LanceDB when buffer is full
                                if len(_candle_buffers[pair]) >= WINDOW:
                                    closes = [c["close"] for c in _candle_buffers[pair]]
                                    matches = _query_similar(_lance_table, pair, closes)
                                    _latest_matches[pair] = matches
                                    _query_counts[pair] += 1
                                    QUERIES_TOTAL.labels(pair=pair).set(_query_counts[pair])

                                    if matches:
                                        best = matches[0]
                                        TOP_SIMILARITY.labels(pair=pair).set(best["similarity"])
                                        log.info(
                                            "%s: best match %.1f%% to %s",
                                            pair, best["similarity"] * 100, best["window_start"],
                                        )

                                        if best["similarity"] >= SIMILARITY_THRESHOLD:
                                            await _push_annotation(pair, best)

                            _tick_accum[pair] = []

                        current_minute[pair] = mk
                        _tick_accum[pair].append(tick)

                    if messages:
                        got_messages = True

                if not got_messages:
                    await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.warning("Connection lost (%s). Reconnecting in %ds…", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    log.info("Matcher stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_matcher() -> None:
    # Start Prometheus metrics
    start_http_server(METRICS_PORT)
    log.info("Matcher metrics on :%d/metrics", METRICS_PORT)

    # Start FastAPI in a background thread
    config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    api_thread = threading.Thread(target=server.run, daemon=True)
    api_thread.start()
    log.info("API server on :%d", API_PORT)

    # Run the async consumer + matcher
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)
    loop.run_until_complete(_consume_and_match())
