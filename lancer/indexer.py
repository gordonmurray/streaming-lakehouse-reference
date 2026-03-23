"""Batch indexer — reads Paimon OHLCV candles and builds LanceDB pattern index."""

import logging
import os
from pathlib import Path

import lancedb
import pyarrow as pa
import pyarrow.parquet as pq

from vectors import WINDOW_SIZE, build_sliding_windows

log = logging.getLogger("indexer")

WAREHOUSE_PATH = os.environ.get(
    "LANCER_WAREHOUSE_PATH",
    "/data/warehouse/paimon/crypto.db/ohlcv_1m",
)
LANCEDB_PATH = os.environ.get("LANCER_DB_PATH", "/data/lancedb")
WINDOW = int(os.environ.get("LANCER_WINDOW_SIZE", str(WINDOW_SIZE)))
MAX_CANDLES_PER_PAIR = int(os.environ.get("LANCER_MAX_CANDLES_PER_PAIR", "100000"))


def run_indexer() -> None:
    log.info("Reading candles from %s", WAREHOUSE_PATH)

    # Read all non-empty parquet files from Paimon bucket directories
    # (Paimon may leave 0-byte files from partial checkpoints)
    parquet_files = [
        str(f) for f in Path(WAREHOUSE_PATH).rglob("data-*.parquet")
        if f.stat().st_size > 0
    ]
    if not parquet_files:
        log.error("No parquet files found in %s", WAREHOUSE_PATH)
        return

    tables = []
    for f in parquet_files:
        try:
            tables.append(pq.read_table(f))
        except Exception as exc:
            log.warning("Skipping %s: %s", f, exc)
    table = pa.concat_tables(tables)
    rows = table.to_pylist()
    log.info("Loaded %d candle records from %d files", len(rows), len(parquet_files))

    if not rows:
        log.error("No candle data found — is the lakehouse pipeline running?")
        return

    # Sort by pair, then window_start
    rows.sort(key=lambda r: (r["pair"], r["window_start"]))

    # Group by pair and build sliding windows
    pairs: dict[str, list[dict]] = {}
    for row in rows:
        pairs.setdefault(row["pair"], []).append(row)

    all_windows: list[dict] = []
    for pair, candles in pairs.items():
        if len(candles) > MAX_CANDLES_PER_PAIR:
            log.info("  %s: %d candles, trimming to most recent %d", pair, len(candles), MAX_CANDLES_PER_PAIR)
            candles = candles[-MAX_CANDLES_PER_PAIR:]
        windows = build_sliding_windows(candles, WINDOW)
        all_windows.extend(windows)
        log.info("  %s: %d candles → %d sliding windows", pair, len(candles), len(windows))

    if not all_windows:
        log.error("No windows generated (need at least %d candles per pair)", WINDOW)
        return

    # Write to LanceDB
    db = lancedb.connect(LANCEDB_PATH)
    tbl = db.create_table("patterns", data=all_windows, mode="overwrite")
    log.info("Indexed %d patterns into LanceDB at %s", tbl.count_rows(), LANCEDB_PATH)
