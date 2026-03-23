"""Backfill 6 months of 1-minute OHLCV candles from Coinbase Exchange API.

Writes a single parquet file to the flink-warehouse volume for import
into Paimon via Flink SQL (scripts/import-backfill.sql).

Usage:
    python3 scripts/backfill-candles.py

Requires: pip install pyarrow requests
"""

import datetime
import time
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import requests

# --- Config ---
PAIRS = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "AVAX-USD", "LINK-USD"]
GRANULARITY = 60  # 1-minute candles
MONTHS_BACK = 6
MAX_CANDLES_PER_REQUEST = 300
REQUEST_DELAY = 0.35  # seconds between requests (Coinbase public: 10 req/s)
BASE_URL = "https://api.exchange.coinbase.com/products"
OUTPUT_DIR = "/tmp/backfill"  # written here, then copied to volume

# --- Schema (matches Paimon ohlcv_1m) ---
SCHEMA = pa.schema([
    ("pair", pa.string()),
    ("window_start", pa.timestamp("ms")),
    ("window_end", pa.timestamp("ms")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("tick_count", pa.int64()),
])


def fetch_candles(pair: str, start: datetime.datetime, end: datetime.datetime) -> list[dict]:
    """Fetch candles from Coinbase for a time range. Returns list of dicts."""
    url = f"{BASE_URL}/{pair}/candles"
    params = {
        "granularity": GRANULARITY,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # Coinbase format: [timestamp, low, high, open, close, volume]
    rows = []
    for candle in data:
        ts = candle[0]
        rows.append({
            "pair": pair,
            "window_start": datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc),
            "window_end": datetime.datetime.fromtimestamp(ts + GRANULARITY, tz=datetime.timezone.utc),
            "open": float(candle[3]),
            "high": float(candle[2]),
            "low": float(candle[1]),
            "close": float(candle[4]),
            "tick_count": 1,  # no tick count from REST API; use 1 as placeholder
        })
    return rows


def backfill_pair(pair: str, start: datetime.datetime, end: datetime.datetime) -> list[dict]:
    """Fetch all candles for a pair over the full date range."""
    all_rows = []
    window_seconds = MAX_CANDLES_PER_REQUEST * GRANULARITY  # 300 minutes per request

    current = start
    request_count = 0

    while current < end:
        chunk_end = min(current + datetime.timedelta(seconds=window_seconds), end)
        try:
            rows = fetch_candles(pair, current, chunk_end)
            all_rows.extend(rows)
            request_count += 1

            if request_count % 50 == 0:
                print(f"  {pair}: {len(all_rows):,} candles so far "
                      f"({current.strftime('%Y-%m-%d %H:%M')} → {chunk_end.strftime('%Y-%m-%d %H:%M')})")

        except requests.exceptions.RequestException as exc:
            print(f"  {pair}: request failed at {current}: {exc} — retrying in 5s")
            time.sleep(5)
            continue

        current = chunk_end
        time.sleep(REQUEST_DELAY)

    print(f"  {pair}: {len(all_rows):,} candles total ({request_count} requests)")
    return all_rows


def main():
    import os

    end = datetime.datetime.now(datetime.timezone.utc)
    start = end - datetime.timedelta(days=MONTHS_BACK * 30)

    print(f"Backfilling {MONTHS_BACK} months of 1-minute candles")
    print(f"Range: {start.strftime('%Y-%m-%d')} → {end.strftime('%Y-%m-%d')}")
    print(f"Pairs: {', '.join(PAIRS)}")
    print()

    all_rows = []
    for pair in PAIRS:
        print(f"Fetching {pair}...")
        rows = backfill_pair(pair, start, end)
        all_rows.extend(rows)
        print()

    if not all_rows:
        print("No data fetched — aborting")
        sys.exit(1)

    # Deduplicate by (pair, window_start) — API may return overlapping windows
    seen = set()
    unique_rows = []
    for row in all_rows:
        key = (row["pair"], row["window_start"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(row)

    print(f"Total: {len(unique_rows):,} unique candles (from {len(all_rows):,} raw)")

    # Write parquet
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "candles.parquet")

    table = pa.table({
        "pair": [r["pair"] for r in unique_rows],
        "window_start": [r["window_start"] for r in unique_rows],
        "window_end": [r["window_end"] for r in unique_rows],
        "open": [r["open"] for r in unique_rows],
        "high": [r["high"] for r in unique_rows],
        "low": [r["low"] for r in unique_rows],
        "close": [r["close"] for r in unique_rows],
        "tick_count": [r["tick_count"] for r in unique_rows],
    }, schema=SCHEMA)

    pq.write_table(table, output_path, compression="zstd")
    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nWritten to {output_path} ({size_mb:.1f} MB)")

    # Copy to flink-warehouse volume via jobmanager container
    print("Copying to flink-warehouse volume...")
    import subprocess
    subprocess.run(
        ["docker", "exec", "jobmanager", "mkdir", "-p", "/opt/flink/warehouse/backfill"],
        check=True,
    )
    subprocess.run(
        ["docker", "cp", output_path, "jobmanager:/opt/flink/warehouse/backfill/candles.parquet"],
        check=True,
    )
    print("Done — parquet staged at /opt/flink/warehouse/backfill/candles.parquet")
    print()
    print("Next step: import into Paimon via Flink SQL:")
    print("  docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/import-backfill.sql")


if __name__ == "__main__":
    main()
