"""LLM Analyst — RAG-style market commentary via Ollama, pushed to Grafana."""

import asyncio
import json
import logging
import os
import signal
import threading
import time

import httpx
import uvicorn
from fastapi import FastAPI
from prometheus_client import Gauge, Counter, Histogram, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("analyst")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://ollama:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.2:3b")
PROMETHEUS_URL = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
LANCER_URL = os.environ.get("LANCER_URL", "http://lancer:8003")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_PASSWORD", "admin")

ANALYSIS_INTERVAL = int(os.environ.get("ANALYST_INTERVAL", "300"))  # 5 minutes
API_PORT = int(os.environ.get("ANALYST_API_PORT", "8005"))
METRICS_PORT = int(os.environ.get("ANALYST_METRICS_PORT", "8006"))

PAIRS = [p.strip() for p in os.environ.get("TRADING_PAIRS", "BTC-USD,ETH-USD,SOL-USD").split(",")]

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
CYCLES_TOTAL = Counter("analyst_cycles_total", "Total analysis cycles completed")
CYCLE_DURATION = Histogram("analyst_cycle_duration_seconds", "Time per analysis cycle")
ANNOTATIONS_PUSHED = Counter("analyst_annotations_total", "Grafana annotations pushed", ["pair"])
LLM_ERRORS = Counter("analyst_llm_errors_total", "LLM call failures")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_latest_narratives: dict[str, dict] = {}

shutdown_event = asyncio.Event()


def _handle_signal() -> None:
    log.info("Shutdown signal received")
    shutdown_event.set()


# ---------------------------------------------------------------------------
# FastAPI — serves latest narrative for Grafana JSON API datasource
# ---------------------------------------------------------------------------
app = FastAPI(title="Analyst — LLM Market Commentary")


@app.get("/api/latest")
async def get_latest():
    return _latest_narratives


@app.get("/api/latest/{pair}")
async def get_latest_pair(pair: str):
    return _latest_narratives.get(pair, {"narrative": "Awaiting first analysis cycle…"})


@app.get("/api/health")
async def health():
    return {"status": "ok", "model": OLLAMA_MODEL}


# ---------------------------------------------------------------------------
# Ollama model management
# ---------------------------------------------------------------------------
async def _ensure_model(client: httpx.AsyncClient) -> bool:
    """Check if model exists, pull if not. Returns True when ready."""
    try:
        resp = await client.get(f"{OLLAMA_URL}/api/tags", timeout=10.0)
        if resp.status_code == 200:
            models = [m["name"] for m in resp.json().get("models", [])]
            if OLLAMA_MODEL in models:
                log.info("Model %s already available", OLLAMA_MODEL)
                return True

        log.info("Pulling model %s (this may take a few minutes)…", OLLAMA_MODEL)
        resp = await client.post(
            f"{OLLAMA_URL}/api/pull",
            json={"name": OLLAMA_MODEL},
            timeout=600.0,
        )
        if resp.status_code == 200:
            log.info("Model %s pulled successfully", OLLAMA_MODEL)
            return True
        else:
            log.error("Failed to pull model: %d %s", resp.status_code, resp.text)
            return False
    except Exception as exc:
        log.error("Ollama not reachable: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Context gathering
# ---------------------------------------------------------------------------
async def _get_prices(client: httpx.AsyncClient) -> dict[str, float]:
    """Get current prices from Prometheus."""
    prices = {}
    try:
        resp = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={"query": "crypto_price"},
            timeout=5.0,
        )
        for result in resp.json()["data"]["result"]:
            pair = result["metric"].get("pair", "")
            prices[pair] = float(result["value"][1])
    except Exception as exc:
        log.warning("Failed to get prices: %s", exc)
    return prices


async def _get_price_trend(client: httpx.AsyncClient, pair: str) -> dict:
    """Get 15-minute price trend from Prometheus range query."""
    try:
        now = time.time()
        resp = await client.get(
            f"{PROMETHEUS_URL}/api/v1/query_range",
            params={
                "query": f'crypto_price{{pair="{pair}"}}',
                "start": now - 900,  # 15 minutes ago
                "end": now,
                "step": "60",  # 1-minute resolution
            },
            timeout=5.0,
        )
        results = resp.json()["data"]["result"]
        if not results or not results[0]["values"]:
            return {}

        values = [float(v[1]) for v in results[0]["values"]]
        if len(values) < 2:
            return {}

        current = values[-1]
        start = values[0]
        delta_pct = ((current - start) / start) * 100
        high = max(values)
        low = min(values)
        volatility_pct = ((high - low) / start) * 100

        return {
            "current": round(current, 2),
            "price_15m_ago": round(start, 2),
            "change_pct": round(delta_pct, 3),
            "high_15m": round(high, 2),
            "low_15m": round(low, 2),
            "volatility_pct": round(volatility_pct, 3),
            "direction": "rising" if delta_pct > 0.05 else "falling" if delta_pct < -0.05 else "flat",
        }
    except Exception as exc:
        log.warning("Failed to get trend for %s: %s", pair, exc)
        return {}


async def _get_pattern_matches(client: httpx.AsyncClient, pair: str) -> list[dict]:
    """Get latest pattern matches from Lancer API."""
    try:
        resp = await client.get(
            f"{LANCER_URL}/api/signals/{pair}",
            timeout=5.0,
        )
        if resp.status_code == 200:
            return resp.json().get("matches", [])
    except Exception as exc:
        log.warning("Failed to get Lancer matches for %s: %s", pair, exc)
    return []


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------
def _build_prompt(pair: str, trend: dict, matches: list[dict]) -> str:
    """Build a RAG-style prompt with market context."""

    context_lines = [f"You are a concise crypto market analyst. Analyse {pair}."]

    if trend:
        context_lines.append(
            f"\nCurrent price: ${trend['current']:,.2f}. "
            f"Over the last 15 minutes: {trend['direction']} "
            f"({trend['change_pct']:+.3f}%). "
            f"Range: ${trend['low_15m']:,.2f} – ${trend['high_15m']:,.2f} "
            f"(volatility: {trend['volatility_pct']:.3f}%)."
        )

    if matches:
        best = matches[0]
        context_lines.append(
            f"\nPattern detection: Lancer found a {best['similarity']:.0%} match "
            f"to historical window {best['window_start']} — {best['window_end']}. "
            f"That pattern had mean price ${best['mean_price']:,.2f} "
            f"and volatility {best['volatility']:.6f}."
        )
        if len(matches) > 1:
            other_scores = ", ".join(f"{m['similarity']:.0%}" for m in matches[1:4])
            context_lines.append(f"Other matches: {other_scores}.")

    context_lines.append(
        "\nProvide a brief market observation in this exact format:\n"
        "Sentiment: [Bullish/Bearish/Neutral]\n"
        "Observation: [1-2 sentences on price action]\n"
        "Pattern: [1 sentence on what the pattern match suggests, or 'No strong pattern match' if similarity < 50%]\n"
        "\nBe specific with numbers. No disclaimers. No financial advice caveats."
    )

    return "\n".join(context_lines)


# ---------------------------------------------------------------------------
# Ollama call
# ---------------------------------------------------------------------------
async def _call_ollama(client: httpx.AsyncClient, prompt: str) -> str | None:
    """Call Ollama generate endpoint."""
    try:
        resp = await client.post(
            f"{OLLAMA_URL}/api/generate",
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": 0.3,
                    "num_predict": 200,
                },
            },
            timeout=120.0,
        )
        if resp.status_code == 200:
            return resp.json().get("response", "").strip()
        else:
            log.error("Ollama error: %d %s", resp.status_code, resp.text[:200])
            LLM_ERRORS.inc()
            return None
    except Exception as exc:
        log.error("Ollama call failed: %s", exc)
        LLM_ERRORS.inc()
        return None


# ---------------------------------------------------------------------------
# Grafana annotation
# ---------------------------------------------------------------------------
async def _push_annotation(client: httpx.AsyncClient, pair: str, narrative: str) -> None:
    """Push narrative as a Grafana annotation on the live ticks dashboard."""
    body = {
        "dashboardUID": "crypto-live-ticks",
        "text": f"**{pair} — AI Analyst**\n\n{narrative}",
        "tags": ["analyst", pair, "llm"],
    }
    try:
        resp = await client.post(
            f"{GRAFANA_URL}/api/annotations",
            json=body,
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            timeout=5.0,
        )
        if resp.status_code == 200:
            ANNOTATIONS_PUSHED.labels(pair=pair).inc()
            log.info("Annotation pushed for %s", pair)
        else:
            log.warning("Grafana annotation failed: %d %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("Grafana annotation error: %s", exc)


# ---------------------------------------------------------------------------
# Main analysis loop
# ---------------------------------------------------------------------------
async def _analysis_loop() -> None:
    async with httpx.AsyncClient() as client:
        # Wait for Ollama and pull model
        while not shutdown_event.is_set():
            if await _ensure_model(client):
                break
            log.info("Waiting for Ollama… retrying in 15s")
            await asyncio.sleep(15)

        log.info("Analyst ready — running every %ds for %s", ANALYSIS_INTERVAL, PAIRS)

        while not shutdown_event.is_set():
            start = time.time()

            for pair in PAIRS:
                if shutdown_event.is_set():
                    break

                # Gather context
                trend = await _get_price_trend(client, pair)
                matches = await _get_pattern_matches(client, pair)

                if not trend:
                    log.warning("No trend data for %s, skipping", pair)
                    continue

                # Build prompt and call LLM
                prompt = _build_prompt(pair, trend, matches)
                narrative = await _call_ollama(client, prompt)

                if narrative:
                    _latest_narratives[pair] = {
                        "pair": pair,
                        "narrative": narrative,
                        "timestamp": time.time(),
                        "trend": trend,
                        "top_match": matches[0] if matches else None,
                    }
                    await _push_annotation(client, pair, narrative)
                    log.info("%s narrative:\n%s", pair, narrative)

            elapsed = time.time() - start
            CYCLES_TOTAL.inc()
            CYCLE_DURATION.observe(elapsed)
            log.info("Cycle complete in %.1fs", elapsed)

            # Sleep until next cycle
            remaining = max(0, ANALYSIS_INTERVAL - elapsed)
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                pass  # Normal — timeout means it's time for the next cycle


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    start_http_server(METRICS_PORT)
    log.info("Analyst metrics on :%d/metrics", METRICS_PORT)

    # Start FastAPI in background thread
    config = uvicorn.Config(app, host="0.0.0.0", port=API_PORT, log_level="warning")
    server = uvicorn.Server(config)
    api_thread = threading.Thread(target=server.run, daemon=True)
    api_thread.start()
    log.info("API server on :%d", API_PORT)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal)

    await _analysis_loop()


if __name__ == "__main__":
    asyncio.run(main())
