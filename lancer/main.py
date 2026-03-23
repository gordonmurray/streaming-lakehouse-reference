"""Lancer — Vector pattern matching for crypto price signals.

Dual-mode service:
  indexer: Batch-builds LanceDB index from Paimon OHLCV candles
  signal:  Live consumer — queries LanceDB for historical pattern matches
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MODE = os.environ.get("LANCER_MODE", "indexer")

if MODE == "indexer":
    from indexer import run_indexer
    run_indexer()
elif MODE == "signal":
    from matcher import run_matcher
    run_matcher()
else:
    logging.error("Unknown LANCER_MODE=%s (expected 'indexer' or 'signal')", MODE)
    sys.exit(1)
