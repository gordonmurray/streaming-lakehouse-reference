"""Consensus Engine — signal-driven paper trading with DuckDB audit."""

import asyncio
import json
import logging
import os
import re
import signal
import threading
import time
import uuid
from dataclasses import dataclass, field

import duckdb
import httpx
import uvicorn
from apache_iggy import IggyClient, SendMessage
from fastapi import FastAPI
from prometheus_client import Gauge, Counter, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("consensus")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
LANCER_URL = os.environ.get("LANCER_URL", "http://localhost:8003")
ANALYST_URL = os.environ.get("ANALYST_URL", "http://localhost:8005")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3001")
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_PASSWORD", "admin")

IGGY_HOST = os.environ.get("IGGY_HOST", "localhost")
IGGY_TCP_PORT = os.environ.get("IGGY_TCP_PORT", "8090")
IGGY_USERNAME = os.environ.get("IGGY_USERNAME", "iggy")
IGGY_PASSWORD = os.environ.get("IGGY_PASSWORD", "iggy")
IGGY_STREAM = os.environ.get("IGGY_STREAM", "crypto")
IGGY_ORDERS_TOPIC = os.environ.get("IGGY_ORDERS_TOPIC", "orders")
IGGY_PARTITIONS = int(os.environ.get("IGGY_TOPIC_PARTITIONS", "3"))

WAREHOUSE_PATH = os.environ.get(
    "CONSENSUS_WAREHOUSE_PATH", "/data/warehouse/paimon/crypto.db"
)

POLL_INTERVAL = int(os.environ.get("CONSENSUS_POLL_INTERVAL", "60"))
TRADE_SIZE_USD = float(os.environ.get("CONSENSUS_TRADE_SIZE_USD", "1000"))
STARTING_BALANCE = float(os.environ.get("CONSENSUS_STARTING_BALANCE", "100000"))

ENTER_SIMILARITY = float(os.environ.get("CONSENSUS_ENTER_SIMILARITY", "0.85"))
EXIT_SIMILARITY = float(os.environ.get("CONSENSUS_EXIT_SIMILARITY", "0.60"))

API_PORT = int(os.environ.get("CONSENSUS_API_PORT", "8007"))
METRICS_PORT = int(os.environ.get("CONSENSUS_METRICS_PORT", "8008"))
STATE_DIR = os.environ.get("CONSENSUS_STATE_DIR", "/data/consensus")
STATE_FILE = os.path.join(STATE_DIR, "state.json")
STALE_THRESHOLD_SECONDS = int(os.environ.get("CONSENSUS_STALE_THRESHOLD", "300"))
RECONCILE_INTERVAL = int(os.environ.get("CONSENSUS_RECONCILE_INTERVAL", "300"))

# Phase 14: Risk circuit breakers
MAX_SIGNAL_AGE = int(os.environ.get("CONSENSUS_MAX_SIGNAL_AGE", "120"))
MAX_PRICE_AGE = int(os.environ.get("CONSENSUS_MAX_PRICE_AGE", "30"))
MAX_DAILY_DRAWDOWN = float(os.environ.get("CONSENSUS_MAX_DAILY_DRAWDOWN", "0.02"))
EMERGENCY_EXIT = os.environ.get("CONSENSUS_EMERGENCY_EXIT", "hold")
TAKER_FEE = float(os.environ.get("CONSENSUS_TAKER_FEE", "0.001"))
SLIPPAGE = float(os.environ.get("CONSENSUS_SLIPPAGE", "0.0005"))
MIN_VIABILITY_RATIO = float(os.environ.get("CONSENSUS_MIN_VIABILITY_RATIO", "1.5"))

PAIRS = [
    p.strip()
    for p in os.environ.get("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",")
]

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
BALANCE_GAUGE = Gauge("consensus_balance", "Current balance", ["currency"])
POSITION_GAUGE = Gauge("consensus_position", "Current position quantity", ["pair"])
POSITION_VALUE = Gauge("consensus_position_value_usd", "Position value in USD", ["pair"])
EQUITY_GAUGE = Gauge("consensus_equity_usd", "Total equity (cash + positions)")
TRADES_TOTAL = Counter("consensus_trades_total", "Total trades executed", ["pair", "side"])
SIGNALS_CHECKED = Counter("consensus_signals_checked", "Total signal checks", ["pair"])
PNL_GAUGE = Gauge("consensus_pnl_usd", "Unrealized P&L", ["pair"])
STATE_STALE = Gauge("consensus_state_stale", "1 if state was loaded from a stale file")
RECONCILE_DRIFT = Gauge("consensus_reconcile_drift_usd", "Absolute cash drift between local state and Paimon ledger")
CIRCUIT_BREAKER = Gauge("consensus_circuit_breaker", "0=OK, 1=drawdown tripped, 2=kill switch")
TRADES_BLOCKED = Counter("consensus_trades_blocked", "Trades blocked by circuit breakers", ["reason"])

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
@dataclass
class Position:
    quantity: float = 0.0
    entry_price: float = 0.0


_cash_balance: float = STARTING_BALANCE
_positions: dict[str, Position] = {}
_trade_log: list[dict] = []

# Circuit breaker state
_trading_enabled: bool = True
_circuit_broken: bool = False
_daily_equity_snapshot: float = 0.0
_daily_snapshot_date: str = ""

shutdown_event = asyncio.Event()


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
def _save_state() -> None:
    """Atomically write state to JSON file."""
    state = {
        "cash_balance": _cash_balance,
        "positions": {
            pair: {"quantity": pos.quantity, "entry_price": pos.entry_price}
            for pair, pos in _positions.items()
            if pos.quantity > 0
        },
        "trade_log": _trade_log[-200:],
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "trading_enabled": _trading_enabled,
        "circuit_broken": _circuit_broken,
        "daily_equity_snapshot": _daily_equity_snapshot,
        "daily_snapshot_date": _daily_snapshot_date,
    }
    os.makedirs(STATE_DIR, exist_ok=True)
    tmp_path = STATE_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp_path, STATE_FILE)
    log.debug("State saved (%d positions, $%.2f cash)", len(state["positions"]), _cash_balance)


def _load_state() -> None:
    """Load state from JSON file if it exists."""
    global _cash_balance, _positions, _trade_log
    global _trading_enabled, _circuit_broken, _daily_equity_snapshot, _daily_snapshot_date

    if not os.path.exists(STATE_FILE):
        log.info("No state file found — starting fresh ($%.0f)", STARTING_BALANCE)
        STATE_STALE.set(0)
        return

    try:
        with open(STATE_FILE) as f:
            state = json.load(f)

        _cash_balance = state["cash_balance"]
        _positions = {
            pair: Position(quantity=p["quantity"], entry_price=p["entry_price"])
            for pair, p in state.get("positions", {}).items()
        }
        _trade_log = state.get("trade_log", [])
        _trading_enabled = state.get("trading_enabled", True)
        _circuit_broken = state.get("circuit_broken", False)
        _daily_equity_snapshot = state.get("daily_equity_snapshot", 0.0)
        _daily_snapshot_date = state.get("daily_snapshot_date", "")

        # Update breaker metrics on load
        if not _trading_enabled:
            CIRCUIT_BREAKER.set(2)
        elif _circuit_broken:
            CIRCUIT_BREAKER.set(1)
        else:
            CIRCUIT_BREAKER.set(0)

        # Stale state detection
        saved_at = state.get("saved_at", "")
        if saved_at:
            saved_ts = time.mktime(time.strptime(saved_at, "%Y-%m-%dT%H:%M:%SZ"))
            age_seconds = time.time() - saved_ts
            if age_seconds > STALE_THRESHOLD_SECONDS:
                log.warning(
                    "STALE STATE: state file is %.0f minutes old — positions "
                    "may be based on outdated prices",
                    age_seconds / 60,
                )
                STATE_STALE.set(1)
            else:
                STATE_STALE.set(0)
                log.info("State file is %.0f seconds old — OK", age_seconds)
        else:
            STATE_STALE.set(0)

        open_positions = {p: v for p, v in _positions.items() if v.quantity > 0}
        log.info(
            "State restored: $%.2f cash, %d open positions, %d trade log entries",
            _cash_balance, len(open_positions), len(_trade_log),
        )
    except Exception as exc:
        log.error("Failed to load state file: %s — starting fresh", exc)
        _cash_balance = STARTING_BALANCE
        _positions = {}
        _trade_log = []
        STATE_STALE.set(0)


def _read_paimon_balance() -> dict[str, float]:
    """Read Paimon balance via the Flink-written reconciliation snapshot."""
    recon_file = f"{WAREHOUSE_PATH}/reconcile_balance.json"
    if not os.path.exists(recon_file):
        return {}
    try:
        age = time.time() - os.path.getmtime(recon_file)
        if age > RECONCILE_INTERVAL * 2:
            log.debug("Reconciliation file too old (%.0fs) — skipping", age)
            return {}
        with open(recon_file) as f:
            return json.load(f)
    except Exception as exc:
        log.warning("Failed to read reconciliation file: %s", exc)
        return {}


def _read_paimon_latest_buys() -> dict[str, float]:
    """Read latest BUY prices via the Flink-written reconciliation snapshot."""
    recon_file = f"{WAREHOUSE_PATH}/reconcile_trades.json"
    if not os.path.exists(recon_file):
        return {}
    try:
        with open(recon_file) as f:
            return json.load(f)
    except Exception:
        return {}


def _reconcile_from_ledger() -> None:
    """Reconcile in-memory state against the Paimon ledger."""
    global _cash_balance, _positions

    try:
        ledger = _read_paimon_balance()

        if not ledger:
            log.debug("Reconciliation skipped — no balance data available")
            return

        # --- Cash reconciliation ---
        ledger_cash = ledger.pop("USD", None)
        if ledger_cash is not None:
            drift = abs(_cash_balance - ledger_cash)
            RECONCILE_DRIFT.set(round(drift, 2))
            if drift > 0.01:
                log.info(
                    "Reconcile cash: local=$%.2f ledger=$%.2f drift=$%.2f",
                    _cash_balance, ledger_cash, drift,
                )
            _cash_balance = ledger_cash

        # --- Position reconciliation ---
        entry_prices = _read_paimon_latest_buys()

        # Reconcile each non-USD currency
        reconciled_pairs = set()
        for currency, quantity in ledger.items():
            pair = f"{currency}-USD"
            reconciled_pairs.add(pair)

            if quantity <= 0:
                if pair in _positions and _positions[pair].quantity > 0:
                    log.info("Reconcile: closing ghost position %s (ledger quantity=0)", pair)
                    _positions[pair] = Position()
                continue

            existing = _positions.get(pair, Position())
            entry_price = entry_prices.get(pair, existing.entry_price)

            if existing.quantity <= 0:
                log.warning(
                    "GHOST TRADE: discovered %s position in ledger: %.6f @ $%.2f",
                    pair, quantity, entry_price,
                )

            _positions[pair] = Position(quantity=quantity, entry_price=entry_price)

        # Clear local positions that the ledger doesn't know about
        for pair in list(_positions.keys()):
            if pair not in reconciled_pairs and _positions[pair].quantity > 0:
                log.warning(
                    "Reconcile: clearing orphan position %s (not in ledger)",
                    pair,
                )
                _positions[pair] = Position()

        _save_state()
        log.info("Reconciliation complete — cash=$%.2f, positions=%d",
                 _cash_balance, sum(1 for p in _positions.values() if p.quantity > 0))

    except Exception as exc:
        log.warning("Reconciliation failed (will retry next cycle): %s", exc)


def _handle_signal() -> None:
    log.info("Shutdown signal received")
    shutdown_event.set()


# ---------------------------------------------------------------------------
# Signal gathering
# ---------------------------------------------------------------------------
async def _get_similarity(client: httpx.AsyncClient, pair: str) -> tuple[float, float, float, str]:
    """Returns (similarity, avg_outcome_pct, avg_max_outcome_pct, queried_at)."""
    try:
        resp = await client.get(f"{LANCER_URL}/api/signals/{pair}", timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            matches = data.get("matches", [])
            queried_at = data.get("queried_at", "")
            if matches:
                similarity = matches[0].get("similarity", 0.0)
                outcomes = [m.get("outcome_pct", 0.0) for m in matches if "outcome_pct" in m]
                max_outcomes = [m.get("outcome_max_pct", 0.0) for m in matches if "outcome_max_pct" in m]
                avg_outcome = sum(outcomes) / len(outcomes) if outcomes else 0.0
                avg_max_outcome = sum(max_outcomes) / len(max_outcomes) if max_outcomes else 0.0
                return similarity, avg_outcome, avg_max_outcome, queried_at
    except Exception as exc:
        log.warning("Lancer query failed for %s: %s", pair, exc)
    return 0.0, 0.0, 0.0, ""


async def _get_sentiment(client: httpx.AsyncClient, pair: str) -> str:
    try:
        resp = await client.get(f"{ANALYST_URL}/api/latest/{pair}", timeout=5.0)
        if resp.status_code == 200:
            narrative = resp.json().get("narrative", "")
            match = re.search(r"Sentiment:\s*(Bullish|Bearish|Neutral)", narrative)
            if match:
                return match.group(1)
    except Exception as exc:
        log.warning("Analyst query failed for %s: %s", pair, exc)
    return "Unknown"


async def _get_price(client: httpx.AsyncClient, pair: str) -> tuple[float, float]:
    """Returns (price, timestamp_epoch)."""
    try:
        resp = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": f'crypto_price{{pair="{pair}"}}'},
            timeout=5.0,
        )
        results = resp.json()["data"]["result"]
        if results:
            ts = float(results[0]["value"][0])
            price = float(results[0]["value"][1])
            return price, ts
    except Exception as exc:
        log.warning("Price query failed for %s: %s", pair, exc)
    return 0.0, 0.0


# ---------------------------------------------------------------------------
# Iggy order publishing
# ---------------------------------------------------------------------------
async def _publish_order(iggy: IggyClient, order: dict) -> None:
    partition_id = hash(order["pair"]) % IGGY_PARTITIONS
    msg = SendMessage(json.dumps(order))
    await iggy.send_messages(IGGY_STREAM, IGGY_ORDERS_TOPIC, partition_id, [msg])


# ---------------------------------------------------------------------------
# Grafana annotation for trades
# ---------------------------------------------------------------------------
async def _push_trade_annotation(
    client: httpx.AsyncClient, pair: str, side: str, price: float, quantity: float
) -> None:
    color = "green" if side == "BUY" else "red"
    text = f"**{side} {pair}**\n\nPrice: ${price:,.2f}\nQuantity: {quantity:.6f}\nValue: ${price * quantity:,.2f}"
    body = {
        "dashboardUID": "crypto-live-ticks",
        "text": text,
        "tags": ["trade", pair, side.lower()],
    }
    try:
        await client.post(
            f"{GRAFANA_URL}/api/annotations",
            json=body,
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            timeout=5.0,
        )
    except Exception as exc:
        log.warning("Trade annotation failed: %s", exc)


# ---------------------------------------------------------------------------
# Trading logic
# ---------------------------------------------------------------------------
async def _evaluate_and_trade(
    http: httpx.AsyncClient, iggy: IggyClient, pair: str
) -> None:
    global _cash_balance, _circuit_broken

    SIGNALS_CHECKED.labels(pair=pair).inc()

    similarity, avg_outcome, avg_max_outcome, queried_at = await _get_similarity(http, pair)
    sentiment = await _get_sentiment(http, pair)
    price, price_ts = await _get_price(http, pair)

    if price <= 0:
        return

    pos = _positions.get(pair, Position())
    holding = pos.quantity > 0

    log.info(
        "%s: similarity=%.2f outcome=%.2f%% sentiment=%s price=$%.2f holding=%s",
        pair, similarity, avg_outcome, sentiment, price, holding,
    )

    side = None

    # --- Circuit breaker checks (block BUY only, SELLs always allowed) ---
    entry_blocked = False
    block_reason = None

    # Kill switch
    if not _trading_enabled:
        entry_blocked = True
        block_reason = "kill_switch"

    # Drawdown breaker
    elif _circuit_broken:
        entry_blocked = True
        block_reason = "drawdown"

    # 14a: Temporal integrity — signal freshness
    elif queried_at:
        try:
            signal_ts = time.mktime(time.strptime(queried_at, "%Y-%m-%dT%H:%M:%SZ"))
            signal_age = time.time() - signal_ts
            if signal_age > MAX_SIGNAL_AGE:
                entry_blocked = True
                block_reason = "stale_signal"
        except (ValueError, OverflowError):
            pass

    # 14a: Temporal integrity — price freshness
    if not entry_blocked and price_ts > 0:
        price_age = time.time() - price_ts
        if price_age > MAX_PRICE_AGE:
            entry_blocked = True
            block_reason = "stale_price"

    # 14a: Pipeline heartbeat — check both price and signal liveness
    if not entry_blocked:
        try:
            hb_resp = await http.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": 'time() - max(timestamp(crypto_price))'},
                timeout=3.0,
            )
            price_lag = float(hb_resp.json()["data"]["result"][0]["value"][1])
            if price_lag > 60:
                entry_blocked = True
                block_reason = "no_heartbeat"
        except Exception:
            pass
        try:
            hb_resp = await http.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": 'time() - max(timestamp(lancer_queries_total))'},
                timeout=3.0,
            )
            signal_lag = float(hb_resp.json()["data"]["result"][0]["value"][1])
            if signal_lag > 180:
                entry_blocked = True
                block_reason = "no_signal_heartbeat"
        except Exception:
            pass

    # 14c: Economic viability
    if not entry_blocked and avg_max_outcome > 0:
        estimated_fees_pct = (TAKER_FEE * 2 + SLIPPAGE * 2) * 100  # as percentage
        if avg_max_outcome < estimated_fees_pct * MIN_VIABILITY_RATIO:
            entry_blocked = True
            block_reason = "uneconomic"

    # --- Entry decision ---
    if (not holding
            and not entry_blocked
            and similarity >= ENTER_SIMILARITY
            and sentiment == "Bullish"
            and avg_outcome > 0):
        quantity = TRADE_SIZE_USD / price
        if _cash_balance >= TRADE_SIZE_USD:
            side = "BUY"
            pos = Position(quantity=quantity, entry_price=price)
            _positions[pair] = pos
            _cash_balance -= TRADE_SIZE_USD
            log.info(">>> BUY %s: %.6f @ $%.2f ($%.2f) [outcome=%.2f%% max=%.2f%%]",
                     pair, quantity, price, TRADE_SIZE_USD, avg_outcome, avg_max_outcome)

    elif (not holding
            and entry_blocked
            and similarity >= ENTER_SIMILARITY
            and sentiment == "Bullish"):
        TRADES_BLOCKED.labels(reason=block_reason).inc()
        log.info(">>> BLOCKED %s BUY: %s", pair, block_reason)
        _trade_log.append({
            "pair": pair, "side": "BUY", "price": price,
            "quantity": TRADE_SIZE_USD / price,
            "signal_similarity": round(similarity, 4),
            "signal_sentiment": sentiment,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime()),
            "timestamp": time.time(),
            "status": "BLOCKED", "reason": block_reason,
        })

    # EXIT: holding AND (similarity < threshold OR Bearish)
    # Exits are never blocked — protecting capital is always allowed
    elif holding and (similarity < EXIT_SIMILARITY or sentiment == "Bearish"):
        quantity = pos.quantity
        proceeds = quantity * price
        side = "SELL"
        pnl = (price - pos.entry_price) * quantity
        _cash_balance += proceeds
        _positions[pair] = Position()
        log.info(
            ">>> SELL %s: %.6f @ $%.2f (P&L: $%.2f)",
            pair, quantity, price, pnl,
        )

    if side:
        order = {
            "order_id": str(uuid.uuid4()),
            "pair": pair,
            "side": side,
            "price": price,
            "quantity": quantity,
            "signal_similarity": round(similarity, 4),
            "signal_sentiment": sentiment,
            "time": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime()),
        }
        await _publish_order(iggy, order)
        await _push_trade_annotation(http, pair, side, price, quantity)
        TRADES_TOTAL.labels(pair=pair, side=side).inc()

        _trade_log.append({**order, "timestamp": time.time()})
        _save_state()

    # Update metrics
    POSITION_GAUGE.labels(pair=pair).set(pos.quantity if pair in _positions else 0)
    if pos.quantity > 0:
        POSITION_VALUE.labels(pair=pair).set(pos.quantity * price)
        PNL_GAUGE.labels(pair=pair).set((price - pos.entry_price) * pos.quantity)
    else:
        POSITION_VALUE.labels(pair=pair).set(0)
        PNL_GAUGE.labels(pair=pair).set(0)


def _update_equity_metrics() -> None:
    global _circuit_broken, _daily_equity_snapshot, _daily_snapshot_date

    BALANCE_GAUGE.labels(currency="USD").set(_cash_balance)
    total_equity = _cash_balance
    for pair, pos in _positions.items():
        if pos.quantity > 0:
            total_equity += pos.quantity * pos.entry_price  # approximate
    EQUITY_GAUGE.set(total_equity)

    # Daily drawdown check
    today = time.strftime("%Y-%m-%d", time.gmtime())
    if today != _daily_snapshot_date:
        _daily_equity_snapshot = total_equity
        _daily_snapshot_date = today
        _circuit_broken = False
        CIRCUIT_BREAKER.set(0 if _trading_enabled else 2)
        log.info("Daily equity snapshot: $%.2f (date=%s)", total_equity, today)

    if _daily_equity_snapshot > 0 and not _circuit_broken:
        drawdown = (total_equity - _daily_equity_snapshot) / _daily_equity_snapshot
        if drawdown < -MAX_DAILY_DRAWDOWN:
            _circuit_broken = True
            CIRCUIT_BREAKER.set(1)
            _save_state()
            log.warning(
                "CIRCUIT BREAKER TRIPPED: equity $%.2f is %.1f%% below daily snapshot $%.2f (limit %.1f%%)",
                total_equity, drawdown * 100, _daily_equity_snapshot, MAX_DAILY_DRAWDOWN * 100,
            )


# ---------------------------------------------------------------------------
# FastAPI — audit endpoint via DuckDB
# ---------------------------------------------------------------------------
app = FastAPI(title="Consensus — Paper Trading Engine")


@app.get("/api/audit")
async def audit():
    """Query Paimon trades table via DuckDB for strategy audit."""
    try:
        con = duckdb.connect()
        trades_path = f"{WAREHOUSE_PATH}/trades"

        # Find all parquet files in the Paimon trades table
        result = con.execute(f"""
            SELECT
                COUNT(*) as total_trades,
                SUM(CASE WHEN side = 'BUY' THEN 1 ELSE 0 END) as buys,
                SUM(CASE WHEN side = 'SELL' THEN 1 ELSE 0 END) as sells,
                SUM(fee) as total_fees,
                SUM(CASE WHEN side = 'SELL' THEN total_cost ELSE -total_cost END) as net_pnl
            FROM read_parquet('{trades_path}/**/data-*.parquet', union_by_name=true)
        """).fetchone()

        return {
            "total_trades": result[0],
            "buys": result[1],
            "sells": result[2],
            "total_fees": round(result[3], 2) if result[3] else 0,
            "net_pnl": round(result[4], 2) if result[4] else 0,
            "starting_balance": STARTING_BALANCE,
        }
    except Exception as exc:
        return {"error": str(exc), "note": "No trades yet — Flink clearing house may not have processed orders"}


@app.get("/api/positions")
async def positions():
    return {
        "cash_balance": round(_cash_balance, 2),
        "positions": {
            pair: {"quantity": pos.quantity, "entry_price": pos.entry_price}
            for pair, pos in _positions.items()
            if pos.quantity > 0
        },
    }


@app.get("/api/trades")
async def trades():
    return _trade_log[-50:]  # last 50 trades


@app.get("/api/candles/{pair}")
async def candles(pair: str, minutes: int = 60):
    """Return OHLCV candles from Paimon for Grafana candlestick panel."""
    import glob as globmod
    ohlcv_path = f"{WAREHOUSE_PATH}/ohlcv_1m"
    try:
        # Filter out 0-byte parquet files (Paimon partial checkpoint artefacts)
        files = [
            f for f in globmod.glob(f"{ohlcv_path}/**/data-*.parquet", recursive=True)
            if os.path.getsize(f) > 0
        ]
        if not files:
            return []
        file_list = ", ".join(f"'{f}'" for f in files)
        con = duckdb.connect()
        rows = con.execute(f"""
            SELECT
                strftime(window_start, '%Y-%m-%dT%H:%M:%SZ') as time,
                open, high, low, close, tick_count as volume
            FROM read_parquet([{file_list}], union_by_name=true)
            WHERE pair = $1
              AND window_start >= now() - INTERVAL '{int(minutes)} minutes'
            ORDER BY window_start ASC
        """, [pair]).fetchall()
        con.close()
        return [
            {"time": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]}
            for r in rows
        ]
    except Exception as exc:
        log.warning("Candle query failed: %s", exc)
        return []


@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "pairs": PAIRS,
        "cash_balance": round(_cash_balance, 2),
        "trading_enabled": _trading_enabled,
        "circuit_broken": _circuit_broken,
    }


@app.get("/api/killswitch")
async def killswitch_status():
    return {"trading_enabled": _trading_enabled, "circuit_broken": _circuit_broken}


@app.post("/api/killswitch")
async def killswitch_activate():
    global _trading_enabled
    _trading_enabled = False
    CIRCUIT_BREAKER.set(2)
    _save_state()
    log.warning("KILL SWITCH ACTIVATED — all trading halted")
    return {"trading_enabled": False}


@app.delete("/api/killswitch")
async def killswitch_deactivate():
    global _trading_enabled
    _trading_enabled = True
    CIRCUIT_BREAKER.set(1 if _circuit_broken else 0)
    _save_state()
    log.info("Kill switch deactivated — trading re-enabled")
    return {"trading_enabled": True}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
async def _consensus_loop() -> None:
    global _cash_balance

    # Connect to Iggy
    conn_str = f"iggy+tcp://{IGGY_USERNAME}:{IGGY_PASSWORD}@{IGGY_HOST}:{IGGY_TCP_PORT}"
    iggy = IggyClient.from_connection_string(conn_str)
    await iggy.connect()
    await iggy.login_user(IGGY_USERNAME, IGGY_PASSWORD)
    log.info("Connected to Iggy")

    # Create orders topic
    try:
        await iggy.create_topic(
            IGGY_STREAM, IGGY_ORDERS_TOPIC, partitions_count=IGGY_PARTITIONS
        )
        log.info("Created Iggy topic: %s/%s", IGGY_STREAM, IGGY_ORDERS_TOPIC)
    except Exception:
        log.debug("Topic %s/%s already exists", IGGY_STREAM, IGGY_ORDERS_TOPIC)

    log.info(
        "Consensus engine ready — checking %s every %ds (trade size $%.0f)",
        PAIRS, POLL_INTERVAL, TRADE_SIZE_USD,
    )

    last_reconcile = 0.0

    async with httpx.AsyncClient() as http:
        while not shutdown_event.is_set():
            for pair in PAIRS:
                if shutdown_event.is_set():
                    break
                await _evaluate_and_trade(http, iggy, pair)

            _update_equity_metrics()

            # Periodic reconciliation against Paimon ledger (background thread)
            now = time.time()
            if now - last_reconcile >= RECONCILE_INTERVAL:
                last_reconcile = now
                threading.Thread(
                    target=_reconcile_from_ledger, daemon=True
                ).start()

            try:
                await asyncio.wait_for(
                    shutdown_event.wait(), timeout=POLL_INTERVAL
                )
            except asyncio.TimeoutError:
                pass


async def main() -> None:
    start_http_server(METRICS_PORT)
    log.info("Consensus metrics on :%d/metrics", METRICS_PORT)

    _load_state()

    # Start FastAPI in background
    config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    api_thread = threading.Thread(target=server.run, daemon=True)
    api_thread.start()
    log.info("API server on :%d", API_PORT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await _consensus_loop()


if __name__ == "__main__":
    asyncio.run(main())
