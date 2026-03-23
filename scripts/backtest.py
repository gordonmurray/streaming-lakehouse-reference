"""Phase 15 — High-Fidelity Strategy Backtester.

Simulates the consensus engine's trading logic against historical candle data
from Paimon. No running services required — reads parquet files directly.

Uses signal pre-calculation: LanceDB queries run once, then parameter sweeps
iterate over the pre-computed signal stream with pure arithmetic.

Usage:
    python3 scripts/backtest.py                          # single run, current params
    python3 scripts/backtest.py --sweep                  # parameter sweep (all combos)
    python3 scripts/backtest.py --enter 0.25 --exit 0.15 # custom params
    python3 scripts/backtest.py --json results.json      # output to JSON

Requires: pip install duckdb lancedb numpy
"""

import argparse
import glob
import itertools
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field

import duckdb
import lancedb
import numpy as np

# Add project root so we can import lancer/vectors.py
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lancer"))
from vectors import WINDOW_SIZE, OUTCOME_MINUTES, DOWNSAMPLE, MIN_VOLATILITY_PCT, build_sliding_windows, zscore_normalize, _downsample

# ---------------------------------------------------------------------------
# Config defaults (match production .env)
# ---------------------------------------------------------------------------
WAREHOUSE_PATH = os.environ.get(
    "BACKTEST_WAREHOUSE_PATH",
    os.path.join(os.path.dirname(__file__), "..", "data", "warehouse", "paimon", "crypto.db", "ohlcv_1m"),
)
if not os.path.exists(WAREHOUSE_PATH):
    WAREHOUSE_PATH = "/data/warehouse/paimon/crypto.db/ohlcv_1m"

STARTING_BALANCE = 1000.0
TRADE_SIZE_USD = 50.0
TAKER_FEE = 0.001
SLIPPAGE = 0.0005
FEES_PCT = (TAKER_FEE * 2 + SLIPPAGE * 2) * 100  # round-trip as %
MAX_DAILY_DRAWDOWN = 0.02
WINDOW = WINDOW_SIZE
TOP_K = 10  # pre-calc top 10 matches per minute (sweep filters from these)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_candles(warehouse_path: str) -> dict[str, list[dict]]:
    """Load all candles from Paimon parquet files, grouped by pair."""
    files = [
        f for f in glob.glob(f"{warehouse_path}/**/data-*.parquet", recursive=True)
        if os.path.getsize(f) > 0
    ]
    if not files:
        print(f"No parquet files found in {warehouse_path}")
        sys.exit(1)

    file_list = ", ".join(f"'{f}'" for f in files)
    con = duckdb.connect()
    con.execute("SET max_memory='1GB'")
    rows = con.execute(f"""
        SELECT pair, window_start, window_end, open, high, low, close, tick_count
        FROM read_parquet([{file_list}], union_by_name=true)
        ORDER BY pair, window_start
    """).fetchall()
    con.close()

    candles_by_pair: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        candles_by_pair[r[0]].append({
            "pair": r[0],
            "window_start": str(r[1]),
            "window_end": str(r[2]),
            "open": r[3], "high": r[4], "low": r[5], "close": r[6],
            "tick_count": r[7],
        })
    return dict(candles_by_pair)


# ---------------------------------------------------------------------------
# Signal pre-calculation (THE key optimisation)
# ---------------------------------------------------------------------------
@dataclass
class Signal:
    pair: str
    timestamp: str
    price: float
    similarity: float
    avg_outcome: float
    avg_max_outcome: float


def precalculate_signals(
    candles_by_pair: dict[str, list[dict]],
    index_table: lancedb.table.Table,
    train_end_time: str,
) -> list[Signal]:
    """Generate the signal stream once — query LanceDB for every test minute.

    Returns a chronologically sorted list of Signals. The sweep then filters
    this list with simple arithmetic instead of re-querying LanceDB.
    """
    signals = []
    buffers: dict[str, list[float]] = defaultdict(list)

    # Collect test candles sorted by time
    test_candles = []
    for pair, candles in candles_by_pair.items():
        for c in candles:
            if c["window_start"] > train_end_time:
                test_candles.append(c)
    test_candles.sort(key=lambda c: c["window_start"])

    total = len(test_candles)
    queried = 0

    for candle in test_candles:
        pair = candle["pair"]
        price = candle["close"]
        ts = candle["window_start"]

        buffers[pair].append(price)
        if len(buffers[pair]) > WINDOW:
            buffers[pair] = buffers[pair][-WINDOW:]

        if len(buffers[pair]) < WINDOW:
            continue

        # Query LanceDB — no outcome filter here (we want all matches for sweep)
        arr = np.array(buffers[pair], dtype=np.float64)

        # Volatility floor: skip noisy flat windows
        mean_price = float(np.mean(arr))
        if mean_price > 0 and (float(np.std(arr)) / mean_price) * 100 < MIN_VOLATILITY_PCT:
            signals.append(Signal(pair=pair, timestamp=ts, price=price,
                                  similarity=0.0, avg_outcome=0.0, avg_max_outcome=0.0))
            continue

        # Downsample to match index dimensions
        ds = _downsample(arr, DOWNSAMPLE) if DOWNSAMPLE > 1 else arr
        vector, _, _ = zscore_normalize(ds)

        try:
            results = (
                index_table.search(vector.tolist())
                .where(f"pair = '{pair}' AND window_end < '{ts}'")
                .limit(TOP_K)
                .to_list()
            )
        except Exception:
            continue

        if not results:
            signals.append(Signal(pair=pair, timestamp=ts, price=price,
                                  similarity=0.0, avg_outcome=0.0, avg_max_outcome=0.0))
            continue

        distance = results[0].get("_distance", float("inf"))
        similarity = 1.0 / (1.0 + distance)
        outcomes = [r.get("outcome_pct", 0.0) for r in results if "outcome_pct" in r]
        max_outcomes = [r.get("outcome_max_pct", 0.0) for r in results if "outcome_max_pct" in r]

        signals.append(Signal(
            pair=pair, timestamp=ts, price=price,
            similarity=similarity,
            avg_outcome=sum(outcomes) / len(outcomes) if outcomes else 0.0,
            avg_max_outcome=sum(max_outcomes) / len(max_outcomes) if max_outcomes else 0.0,
        ))

        queried += 1
        if queried % 2000 == 0:
            print(f"  Pre-calc: {queried:,} signals generated ({queried * 100 // total}%)")

    print(f"  Pre-calc complete: {len(signals):,} signals from {total:,} test candles")
    return signals


# ---------------------------------------------------------------------------
# Simulation (runs on pre-calculated signals — pure arithmetic, very fast)
# ---------------------------------------------------------------------------
@dataclass
class SimResult:
    starting_balance: float
    ending_balance: float
    net_pnl: float
    return_pct: float
    total_trades: int
    buys: int
    sells: int
    blocked: int
    wins: int
    losses: int
    win_rate: float
    avg_win: float
    avg_loss: float
    max_drawdown_pct: float
    total_fees: float
    avg_mae: float
    sharpe_ratio: float = 0.0
    per_pair: dict = field(default_factory=dict)
    params: dict = field(default_factory=dict)


def simulate(
    signals: list[Signal],
    enter_sim: float = 0.20,
    exit_sim: float = 0.10,
    min_outcome_pct: float = 0.1,
    min_viability_ratio: float = 1.5,
) -> SimResult:
    """Run simulation over pre-calculated signals. Pure arithmetic — microseconds per signal."""

    cash = STARTING_BALANCE
    positions: dict[str, dict] = {}
    round_trips: list[dict] = []
    buy_count = 0
    sell_count = 0
    blocked_count = 0

    peak_equity = STARTING_BALANCE
    max_drawdown_pct = 0.0
    daily_snapshot = STARTING_BALANCE
    daily_date = ""
    circuit_broken = False
    total_fees = 0.0

    pending_buys: dict[str, dict] = {}  # pair -> buy info for round-trip matching

    for sig in signals:
        pair = sig.pair
        price = sig.price
        holding = pair in positions

        # Daily reset
        current_date = sig.timestamp[:10]
        if current_date != daily_date:
            daily_snapshot = cash + sum(p["entry_price"] * p["quantity"] for p in positions.values())
            daily_date = current_date
            circuit_broken = False

        # Viability check
        viable = sig.avg_max_outcome >= FEES_PCT * min_viability_ratio if sig.avg_max_outcome > 0 else False

        # Filter outcome by threshold
        outcome_positive = sig.avg_outcome > min_outcome_pct if min_outcome_pct > 0 else sig.avg_outcome > 0

        # --- Entry ---
        if (not holding and not circuit_broken
                and sig.similarity >= enter_sim
                and outcome_positive and viable):
            quantity = TRADE_SIZE_USD / price
            if cash >= TRADE_SIZE_USD:
                buy_price = price * (1 + SLIPPAGE)
                fee = buy_price * quantity * TAKER_FEE
                cost = buy_price * quantity + fee
                cash -= cost
                total_fees += fee
                positions[pair] = {"quantity": quantity, "entry_price": price}
                pending_buys[pair] = {"price": price, "fee": fee, "quantity": quantity}
                buy_count += 1

        elif (not holding and sig.similarity >= enter_sim and outcome_positive
                and (circuit_broken or not viable)):
            blocked_count += 1

        # --- Exit ---
        elif holding and sig.similarity < exit_sim:
            pos = positions[pair]
            sell_price = price * (1 - SLIPPAGE)
            fee = sell_price * pos["quantity"] * TAKER_FEE
            proceeds = sell_price * pos["quantity"] - fee
            cash += proceeds
            total_fees += fee
            sell_count += 1

            if pair in pending_buys:
                buy = pending_buys.pop(pair)
                pnl = (price - buy["price"]) * buy["quantity"] - buy["fee"] - fee
                round_trips.append({"pair": pair, "pnl": pnl})

            del positions[pair]

        # Drawdown
        equity = cash + sum(p["quantity"] * p["entry_price"] for p in positions.values())
        if equity > peak_equity:
            peak_equity = equity
        dd = (peak_equity - equity) / peak_equity if peak_equity > 0 else 0
        if dd > max_drawdown_pct:
            max_drawdown_pct = dd
        if daily_snapshot > 0 and (equity - daily_snapshot) / daily_snapshot < -MAX_DAILY_DRAWDOWN:
            circuit_broken = True

    # Close remaining positions
    for pair, pos in list(positions.items()):
        cash += pos["quantity"] * pos["entry_price"]  # approximate close at entry
        if pair in pending_buys:
            round_trips.append({"pair": pair, "pnl": 0.0})
        sell_count += 1

    # Stats
    wins = [rt for rt in round_trips if rt["pnl"] > 0]
    losses = [rt for rt in round_trips if rt["pnl"] <= 0]
    avg_win = sum(rt["pnl"] for rt in wins) / len(wins) if wins else 0.0
    avg_loss = sum(rt["pnl"] for rt in losses) / len(losses) if losses else 0.0

    # Sharpe
    sharpe = 0.0
    if len(round_trips) >= 2:
        rt_returns = [rt["pnl"] / TRADE_SIZE_USD for rt in round_trips]
        mean_ret = np.mean(rt_returns)
        std_ret = np.std(rt_returns)
        if std_ret > 1e-10:
            sharpe = float(mean_ret / std_ret) * np.sqrt(252)

    # Per-pair
    per_pair = {}
    all_pairs = set(s.pair for s in signals)
    for pair in all_pairs:
        pair_rts = [rt for rt in round_trips if rt["pair"] == pair]
        pair_pnls = [rt["pnl"] for rt in pair_rts]
        per_pair[pair] = {
            "trades": len(pair_rts),
            "pnl": round(sum(pair_pnls), 2) if pair_pnls else 0.0,
            "wins": len([p for p in pair_pnls if p > 0]),
            "losses": len([p for p in pair_pnls if p <= 0]),
            "best_trade": round(max(pair_pnls), 2) if pair_pnls else 0.0,
            "worst_trade": round(min(pair_pnls), 2) if pair_pnls else 0.0,
        }

    net_pnl = cash - STARTING_BALANCE
    return SimResult(
        starting_balance=STARTING_BALANCE,
        ending_balance=round(cash, 2),
        net_pnl=round(net_pnl, 2),
        return_pct=round((net_pnl / STARTING_BALANCE) * 100, 2),
        total_trades=buy_count + sell_count,
        buys=buy_count, sells=sell_count, blocked=blocked_count,
        wins=len(wins), losses=len(losses),
        win_rate=round(len(wins) / len(round_trips) * 100, 1) if round_trips else 0,
        avg_win=round(avg_win, 2), avg_loss=round(avg_loss, 2),
        max_drawdown_pct=round(max_drawdown_pct * 100, 2),
        total_fees=round(total_fees, 2),
        avg_mae=0.0,
        sharpe_ratio=round(sharpe, 2),
        per_pair=per_pair,
        params={
            "enter_similarity": enter_sim, "exit_similarity": exit_sim,
            "min_outcome_pct": min_outcome_pct, "min_viability_ratio": min_viability_ratio,
        },
    )


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
def print_report(result: SimResult) -> None:
    p = result.params
    print(f"\n{'='*70}")
    print(f"BACKTEST REPORT")
    print(f"{'='*70}")
    print(f"Parameters: enter={p['enter_similarity']} exit={p['exit_similarity']} "
          f"min_outcome={p['min_outcome_pct']} viability={p['min_viability_ratio']}")
    print(f"{'─'*70}")
    print(f"Starting balance:  ${result.starting_balance:,.2f}")
    print(f"Ending balance:    ${result.ending_balance:,.2f}")
    print(f"Net P&L:           ${result.net_pnl:+,.2f} ({result.return_pct:+.1f}%)")
    print(f"{'─'*70}")
    print(f"Total trades:      {result.total_trades} ({result.buys} buys, {result.sells} sells)")
    print(f"Round trips:       {result.wins + result.losses}")
    print(f"Win rate:          {result.win_rate}%")
    print(f"Avg win:           ${result.avg_win:+.2f}")
    print(f"Avg loss:          ${result.avg_loss:+.2f}")
    print(f"Max drawdown:      {result.max_drawdown_pct:.1f}%")
    print(f"Total fees:        ${result.total_fees:.2f}")
    print(f"Sharpe ratio:      {result.sharpe_ratio:.2f}")
    print(f"Blocked trades:    {result.blocked}")
    print(f"{'─'*70}")
    print(f"Per-pair breakdown:")
    for pair, stats in sorted(result.per_pair.items()):
        if stats["trades"] > 0:
            wr = round(stats["wins"] / stats["trades"] * 100) if stats["trades"] > 0 else 0
            print(f"  {pair:10s}  {stats['trades']:3d} trades  P&L ${stats['pnl']:+8.2f}  "
                  f"W/L {stats['wins']}/{stats['losses']} ({wr}%)  "
                  f"best=${stats['best_trade']:+.2f} worst=${stats['worst_trade']:+.2f}")
    print(f"{'='*70}\n")


# ---------------------------------------------------------------------------
# Parameter sweep (runs on pre-calculated signals — seconds, not hours)
# ---------------------------------------------------------------------------
def run_sweep(signals: list[Signal]) -> list[SimResult]:
    enter_range = [round(x, 2) for x in np.arange(0.10, 0.45, 0.05)]
    exit_range = [round(x, 2) for x in np.arange(0.05, 0.25, 0.05)]
    outcome_range = [round(x, 1) for x in np.arange(0.0, 0.6, 0.1)]
    viability_range = [round(x, 1) for x in np.arange(1.0, 3.0, 0.5)]

    valid_combos = [
        (enter, exit_, outcome, viability)
        for enter, exit_, outcome, viability
        in itertools.product(enter_range, exit_range, outcome_range, viability_range)
        if exit_ < enter
    ]
    print(f"Running {len(valid_combos)} parameter combinations...")

    start = time.time()
    results = []
    for i, (enter, exit_, outcome, viability) in enumerate(valid_combos):
        result = simulate(signals, enter_sim=enter, exit_sim=exit_,
                          min_outcome_pct=outcome, min_viability_ratio=viability)
        results.append(result)

    elapsed = time.time() - start
    results.sort(key=lambda r: r.net_pnl, reverse=True)
    print(f"Sweep complete: {len(results)} combinations in {elapsed:.1f}s")
    return results


def print_sweep_results(results: list[SimResult], top_n: int = 15) -> None:
    print(f"\n{'='*100}")
    print(f"TOP {top_n} PARAMETER COMBINATIONS (by Net P&L)")
    print(f"{'='*100}")
    print(f"{'Enter':>6s} {'Exit':>6s} {'MinOut':>7s} {'Viab':>5s} │ "
          f"{'P&L':>8s} {'Return':>7s} {'Trades':>6s} {'WinRate':>7s} {'Sharpe':>6s} {'MaxDD':>6s} {'Fees':>7s}")
    print(f"{'─'*100}")
    for r in results[:top_n]:
        p = r.params
        print(f"{p['enter_similarity']:6.2f} {p['exit_similarity']:6.2f} "
              f"{p['min_outcome_pct']:7.1f} {p['min_viability_ratio']:5.1f} │ "
              f"${r.net_pnl:+7.2f} {r.return_pct:+6.1f}% {r.total_trades:6d} "
              f"{r.win_rate:6.1f}% {r.sharpe_ratio:+5.2f} {r.max_drawdown_pct:5.1f}% ${r.total_fees:6.2f}")
    print(f"{'='*100}")

    print(f"\nBOTTOM 5:")
    for r in results[-5:]:
        p = r.params
        print(f"{p['enter_similarity']:6.2f} {p['exit_similarity']:6.2f} "
              f"{p['min_outcome_pct']:7.1f} {p['min_viability_ratio']:5.1f} │ "
              f"${r.net_pnl:+7.2f} {r.return_pct:+6.1f}% {r.total_trades:6d}")


# ---------------------------------------------------------------------------
# Index building
# ---------------------------------------------------------------------------
def build_index(candles_by_pair: dict[str, list[dict]], max_candles: int) -> lancedb.table.Table:
    import tempfile
    db_path = tempfile.mkdtemp(prefix="backtest_lancedb_")
    db = lancedb.connect(db_path)

    all_windows = []
    for pair, candles in candles_by_pair.items():
        train_candles = candles[:max_candles]
        windows = build_sliding_windows(train_candles, WINDOW, OUTCOME_MINUTES)
        all_windows.extend(windows)

    if not all_windows:
        print("No windows generated from training data")
        sys.exit(1)

    return db.create_table("patterns", data=all_windows, mode="overwrite")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Strategy backtester")
    parser.add_argument("--sweep", action="store_true", help="Run parameter sweep")
    parser.add_argument("--enter", type=float, default=0.20)
    parser.add_argument("--exit", type=float, default=0.10)
    parser.add_argument("--min-outcome", type=float, default=0.1)
    parser.add_argument("--viability", type=float, default=1.5)
    parser.add_argument("--json", type=str, help="Output results to JSON file")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--warehouse", type=str, default=WAREHOUSE_PATH)
    parser.add_argument("--max-candles", type=int, default=20000)
    args = parser.parse_args()

    print("Loading candle data...")
    candles_by_pair = load_candles(args.warehouse)
    for pair in candles_by_pair:
        if len(candles_by_pair[pair]) > args.max_candles:
            candles_by_pair[pair] = candles_by_pair[pair][-args.max_candles:]

    total = sum(len(c) for c in candles_by_pair.values())
    print(f"Loaded {total:,} candles across {len(candles_by_pair)} pairs")

    # Train/test split using global time bounds
    all_starts = sorted(set(
        c["window_start"] for candles in candles_by_pair.values() for c in candles
    ))
    split_idx = int(len(all_starts) * 0.7)
    train_end_time = all_starts[split_idx]
    print(f"Train/test split at: {train_end_time}")

    # Build index from training data
    print("Building LanceDB index...")
    train_data = {
        pair: [c for c in candles if c["window_start"] <= train_end_time]
        for pair, candles in candles_by_pair.items()
    }
    index_table = build_index(train_data, max_candles=args.max_candles)
    print(f"Index: {index_table.count_rows():,} patterns")

    # Pre-calculate signals (one-time LanceDB pass)
    print("Pre-calculating signals (one-time LanceDB pass)...")
    signals = precalculate_signals(candles_by_pair, index_table, train_end_time)

    if args.sweep:
        results = run_sweep(signals)
        print_sweep_results(results)
        if args.json:
            with open(args.json, "w") as f:
                json.dump([{
                    "params": r.params, "net_pnl": r.net_pnl,
                    "return_pct": r.return_pct, "total_trades": r.total_trades,
                    "win_rate": r.win_rate, "sharpe_ratio": r.sharpe_ratio,
                    "max_drawdown_pct": r.max_drawdown_pct, "total_fees": r.total_fees,
                } for r in results], f, indent=2)
            print(f"\nFull results written to {args.json}")
    else:
        result = simulate(signals, enter_sim=args.enter, exit_sim=args.exit,
                          min_outcome_pct=args.min_outcome, min_viability_ratio=args.viability)
        print_report(result)
        if args.json:
            with open(args.json, "w") as f:
                json.dump({
                    "params": result.params,
                    "summary": {
                        "net_pnl": result.net_pnl, "return_pct": result.return_pct,
                        "total_trades": result.total_trades, "win_rate": result.win_rate,
                        "sharpe_ratio": result.sharpe_ratio,
                        "max_drawdown_pct": result.max_drawdown_pct, "total_fees": result.total_fees,
                    },
                    "per_pair": result.per_pair,
                }, f, indent=2)
            print(f"Results written to {args.json}")


if __name__ == "__main__":
    main()
