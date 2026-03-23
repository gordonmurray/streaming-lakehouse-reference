"""Shared vector operations — Z-score normalization and sliding window construction."""

import numpy as np

WINDOW_SIZE = 60  # 1-hour window (60 × 1-min OHLCV candles)
OUTCOME_MINUTES = 60  # look-ahead horizon for outcome calculation
DOWNSAMPLE = 5  # aggregate N candles into 1 before vectorizing (60/5 = 12 dimensions)
MIN_VOLATILITY_PCT = 0.05  # reject windows where std < 0.05% of mean price (noise floor)


def zscore_normalize(prices: np.ndarray) -> tuple[np.ndarray, float, float]:
    """Z-score normalize a price series.

    Returns (normalized_vector, mean_price, volatility).
    A flat line (std ≈ 0) returns a zero vector.
    """
    mean = float(np.mean(prices))
    std = float(np.std(prices))
    if std < 1e-10:
        return np.zeros(len(prices), dtype=np.float32), mean, std
    return ((prices - mean) / std).astype(np.float32), mean, std


def _downsample(closes: np.ndarray, factor: int) -> np.ndarray:
    """Average consecutive candle closes into larger buckets.

    60 candles with factor=5 → 12 data points (mean of each 5-candle group).
    Reduces dimensionality while preserving the overall shape.
    """
    n = len(closes) - (len(closes) % factor)
    return closes[:n].reshape(-1, factor).mean(axis=1)


def build_sliding_windows(
    candles: list[dict],
    window_size: int = WINDOW_SIZE,
    outcome_minutes: int = OUTCOME_MINUTES,
    downsample: int = DOWNSAMPLE,
    min_volatility_pct: float = MIN_VOLATILITY_PCT,
) -> list[dict]:
    """Build sliding windows (stride=1) from sorted candle data.

    Each candle dict must have: pair, window_start, window_end, close.
    Candle closes are downsampled by averaging groups of `downsample` candles
    before Z-score normalization (e.g., 60 → 12 dimensions with downsample=5).
    Windows with volatility below min_volatility_pct of the mean price are
    rejected as noise.
    When outcome_minutes > 0, each pattern is enriched with what happened
    to the price in the N minutes after the window ended.
    Returns list of dicts ready for LanceDB insertion.
    """
    min_candles = window_size + outcome_minutes if outcome_minutes > 0 else window_size
    if len(candles) < min_candles:
        return []

    closes = np.array([c["close"] for c in candles], dtype=np.float64)
    last_window = len(candles) - window_size + 1 - outcome_minutes if outcome_minutes > 0 else len(candles) - window_size + 1
    windows = []

    for i in range(last_window):
        window_closes = closes[i : i + window_size]
        mean_price = float(np.mean(window_closes))

        # Noise floor: reject windows where volatility < min_volatility_pct of mean price
        raw_std = float(np.std(window_closes))
        if mean_price > 0 and (raw_std / mean_price) * 100 < min_volatility_pct:
            continue

        # Downsample before vectorizing (reduces dimensions, improves distance discrimination)
        ds_closes = _downsample(window_closes, downsample) if downsample > 1 else window_closes
        vector, _, volatility = zscore_normalize(ds_closes)

        pattern = {
            "vector": vector.tolist(),
            "pair": candles[i]["pair"],
            "window_start": str(candles[i]["window_start"]),
            "window_end": str(candles[i + window_size - 1]["window_end"]),
            "volatility": volatility,
            "mean_price": mean_price,
        }

        # Outcome: what happened after this pattern
        if outcome_minutes > 0:
            window_end_price = closes[i + window_size - 1]
            future_closes = closes[i + window_size : i + window_size + outcome_minutes]
            outcome_close = future_closes[-1]
            pattern["outcome_pct"] = round(
                ((outcome_close - window_end_price) / window_end_price) * 100, 4
            )
            pattern["outcome_max_pct"] = round(
                ((float(np.max(future_closes)) - window_end_price) / window_end_price) * 100, 4
            )
            pattern["outcome_min_pct"] = round(
                ((float(np.min(future_closes)) - window_end_price) / window_end_price) * 100, 4
            )

        windows.append(pattern)

    return windows
