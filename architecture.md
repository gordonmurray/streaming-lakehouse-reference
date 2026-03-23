# architecture.md — System Design

## Project: Streaming Lakehouse Reference

---

## Purpose

A personal crypto price monitoring, paper trading simulation, and data
engineering learning platform. Real market data. No real money.
**Inspired Engineering Approach**: Using high-frequency WebSockets, tiered
streaming storage, vector similarity search, and LLM-generated commentary
to build a "Market Time Machine."

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│  Coinbase Exchange Public WebSocket (wss://ws-feed.exchange...)      │
│  6 pairs: BTC, ETH, SOL, DOGE, AVAX, LINK — sub-second ticks       │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   POLLER (Python Ingestion)                          │
│  Persistent WebSocket. Normalises ticks → Iggy topic crypto/prices  │
└──────────────────────┬──────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         IGGY BROKER                                 │
│  Event spine. Topics: crypto/prices, crypto/orders, crypto/replay   │
│  Persistent storage at /app/local_data/ (bind-mounted to host)      │
└───┬──────────┬───────────┬───────────┬──────────────────────────────┘
    │          │           │           │
    ▼          ▼           ▼           ▼
┌────────┐ ┌────────┐ ┌────────┐ ┌────────────────────────────────────┐
│BRIDGE  │ │LANCER  │ │REPLAY  │ │          APACHE FLINK              │
│Iggy →  │ │Signal  │ │Iceberg │ │  3 streaming jobs (HA via ZK):     │
│Prom    │ │Mode    │ │→ Iggy  │ │                                    │
│metrics │ │(live)  │ │(on     │ │  1. Lakehouse: Iggy → Paimon OHLCV │
└───┬────┘ └───┬────┘ │demand) │ │     + Iceberg tick archive         │
    │          │      └────────┘ │  2. Hot tier: Iggy → Fluss ticks   │
    ▼          │                 │  3. Clearing house: Iggy orders →   │
┌────────┐     │                 │     Paimon balance/trades + Fluss   │
│GRAFANA │     │                 └──────┬─────────┬───────────────────┘
│5 dash- │     │                        │         │
│boards  │     │                        ▼         ▼
│candle- │     │                 ┌────────────┐ ┌───────────────┐
│sticks  │     │                 │  PAIMON    │ │ ICEBERG       │
└───▲────┘     │                 │  (Warm)    │ │ (Cold)        │
    │          │                 │  ohlcv_1m  │ │ Raw ticks     │
    │          │                 │  balance   │ │ Time-travel   │
    │          │                 │  trades    │ │ Replay source │
    │          │                 └──────┬─────┘ └───────────────┘
    │          │                        │
    │          │                        ▼
    │          │              ┌──────────────────┐
    │          │              │ RECONCILIATION   │
    │          │              │ Host cron script  │
    │          │              │ Flink SQL → JSON  │
    │          │              │ (every 5 min)     │
    │          │              └────────┬─────────┘
    │          │                       │
    │          ▼                       ▼
    │   ┌────────────┐     ┌──────────────────────┐
    │   │  LANCEDB   │     │   CONSENSUS ENGINE   │
    │   │  Vector    │────▶│   (Paper Trading)    │
    │   │  patterns  │     │   Signals + Ledger   │
    │   │  38K+      │     │   → Iggy orders      │
    │   └────────────┘     └──────────┬───────────┘
    │                                 │
    │   ┌────────────┐                │
    │   │  ANALYST   │────────────────┘
    │   │  Ollama    │  sentiment signals
    │   │  LLM      │
    │   └────────────┘
    │          │
    └──────────┘ annotations
```

### Key Data Paths

1. **Live ticks**: Coinbase → Poller → Iggy `prices` → Bridge → Prometheus → Grafana
2. **Candles**: Iggy `prices` → Flink tumbling window → Paimon `ohlcv_1m` → DuckDB → Grafana candlestick
3. **Archive**: Iggy `prices` → Flink → Iceberg (partitioned by day, parquet+zstd)
4. **Hot SQL**: Iggy `prices` → Flink → Fluss (sub-second streaming SQL)
5. **Pattern matching**: Paimon `ohlcv_1m` → Lancer indexer → LanceDB; live Iggy → Lancer signal mode → similarity scores
6. **Trading loop**: Lancer similarity + Analyst sentiment → Consensus engine → Iggy `orders` → Flink clearing house → Paimon balance/trades + Fluss executed_trades
7. **Reconciliation**: Host cron → Flink SQL (reads Paimon via snapshot layer) → JSON files → Consensus engine adopts ledger truth
8. **Replay**: Iceberg archive → Replay service → Iggy `replay` → Flink → Paimon `replay_ohlcv_1m`

---

## Data Integrity & Deduplication

The system uses **sequence-based deduplication** at the application layer to
ensure accurate data for downstream consumers (Flink OHLCV aggregations,
Iceberg archive).

1. **Source Uniqueness**: Every Coinbase ticker message includes a `sequence`
   number — a monotonically increasing 64-bit integer per product.
2. **Payload Inclusion**: The ingestion service includes the `sequence` number
   in every tick published to Iggy.
3. **Consumer-Side Dedup**: The bridge (and any future consumer) tracks the
   last seen `sequence` per pair and drops any tick with a sequence ≤ the last
   seen value. This handles duplicates from WebSocket reconnects or Iggy
   re-reads.

---

## Component Responsibilities

### Poller (Python Ingestion)
- Persistent WebSocket connection to Coinbase Exchange.
- Publishes normalised JSON ticks to Iggy `crypto/prices`.
- Handles reconnection, rate limiting, sequence tracking.
- Creates Iggy stream/topic on startup.

### Iggy
- Central event spine. Three topics: `prices` (live ticks), `orders` (trade intents), `replay` (historical).
- Persistent storage at `/app/local_data/` (host bind mount at `./data/iggy/`).
- Web UI on port 8888.

### Bridge
- Consumes Iggy `crypto/prices`, exposes as Prometheus metrics.
- Sequence-based deduplication per pair.

### Apache Flink (3 streaming jobs, ZooKeeper HA)
- **Lakehouse pipeline**: Iggy → 1-min OHLCV candles → Paimon + raw ticks → Iceberg.
- **Hot tier pipeline**: Iggy → Fluss streaming table.
- **Clearing house**: Iggy `orders` → applies 0.1% fees + 0.05% slippage → Paimon `balance` (aggregation deltas) + `trades` (deduplicated history) + Fluss `executed_trades`.
- Checkpoints on `flink-warehouse` volume, HA via ZooKeeper.

### Apache Fluss (Hot Tier)
- Streaming storage for sub-second SQL queries.
- Tables: `ticks` (live), `executed_trades` (clearing house output).
- Uses existing ZooKeeper for coordination.

### Apache Paimon (Warm Tier)
- `ohlcv_1m`: append-only 1-minute candle table (safe for DuckDB reads).
- `balance`: PK table, `merge-engine=aggregation`, `SUM` on amount. Every INSERT is a delta. **Not safe for DuckDB** — must read via Flink SQL.
- `trades`: PK table, `merge-engine=deduplicate`. **Not safe for DuckDB** — must read via Flink SQL.
- Filesystem catalog on `flink-warehouse` Docker volume.

### Apache Iceberg (Cold Tier)
- Raw tick archive, identity-partitioned by date.
- Source for the Replay Engine (Time Machine).
- Hadoop catalog on `flink-warehouse` volume.

### Lancer (LanceDB Vector Search)
- **Indexer mode**: reads Paimon OHLCV candles, builds Z-score normalised 60-candle sliding windows, writes to LanceDB.
- **Signal mode**: consumes live Iggy ticks, builds candle buffer, queries LanceDB every minute for similar historical patterns.
- Pushes Grafana annotations when similarity exceeds threshold.

### Analyst (LLM)
- Every 5 minutes: gathers price trends from Prometheus + pattern matches from Lancer API.
- Builds RAG-style prompt, calls host Ollama (Llama 3.2 8B).
- Pushes structured narrative as Grafana annotation.
- FastAPI `/api/latest/{pair}` for current sentiment.

### Consensus Engine (Paper Trading)
- Polls Lancer (similarity) + Analyst (sentiment) + Prometheus (price) every 60s.
- Entry rule: similarity ≥ threshold AND sentiment == "Bullish".
- Exit rule: similarity < threshold OR sentiment == "Bearish".
- Publishes `OrderRequest` to Iggy `crypto/orders` for Flink clearing house.
- **State persistence**: JSON file at `./data/consensus/state.json`, atomic writes, stale detection.
- **Ledger reconciliation**: reads Flink SQL snapshot files (written by host cron) every 5 minutes, adopts Paimon balance as truth. Ghost trade discovery, orphan position cleanup, drift metric.
- DuckDB-powered `/api/candles/{pair}` endpoint for Grafana candlestick panel.

### Reconciliation (Host Cron)
- `scripts/reconcile-snapshot.sh` runs every 5 minutes via cron.
- Queries Paimon `balance` and `trades` via Flink SQL client (respects snapshots).
- Writes `reconcile_balance.json` and `reconcile_trades.json` to `flink-warehouse` volume.
- Cleanup step cancels any leftover batch SELECT jobs via Flink REST API.

### Grafana (5 Dashboards)
- **Crypto Live Ticks**: real-time price, bid/ask, candlestick OHLCV, throughput. Template variable `$pair`.
- **Flink Pipeline**: streaming throughput metrics.
- **Historical Replay**: replay progress, speed, records.
- **Lancer — Pattern Matcher**: similarity scores, index size, buffer fill.
- **Paper Trading — Equity & Positions**: equity curve, cash, positions, P&L, formatted trade log.
- Annotations: AI Analyst (yellow), Pattern Match (cyan), Buy (green), Sell (red).

---

## Technology Decision Rationale

| Technology | Why Here |
|------------|----------|
| WebSockets | Justifies the streaming stack; low latency; real market feel |
| Iggy       | Lightweight, Rust-native, persistent, MCP support for AI agents |
| Fluss      | Hot storage tier; Flink-native; real-time SQL |
| Flink      | Unified processing for live streams and historical replays; HA via ZooKeeper |
| Paimon     | Warm tier with merge engines (aggregation for ledger, deduplicate for trades, append-only for candles) |
| Iceberg    | Cold tier archive; time-travel queries; replay source |
| LanceDB    | Vector similarity search over price patterns; Z-score normalised embeddings |
| DuckDB     | Fast SQL over Paimon append-only tables (candles). **Unsafe for PK/merge-engine tables** |
| Ollama     | Local LLM inference (CPU); no external API dependencies |

---

## Port Map

| Service             | Port  | Network         |
|---------------------|-------|-----------------|
| Poller (metrics)    | 8000  | Docker bridge   |
| Bridge (metrics)    | 8001  | Docker bridge   |
| Replay (metrics)    | 8002  | Docker bridge   |
| Lancer API          | 8003  | Docker bridge   |
| Analyst API         | 8005  | Host network    |
| Consensus API       | 8007  | Host network    |
| Consensus metrics   | 8008  | Host network    |
| Iggy TCP            | 8090  | Docker bridge   |
| Iggy HTTP           | 3000  | Docker bridge   |
| Iggy Web UI         | 8888  | Docker bridge   |
| Flink UI            | 8081  | Docker bridge   |
| Flink metrics       | 9249  | Docker bridge   |
| Fluss coordinator   | 9123  | Docker bridge   |
| Prometheus          | 9090  | Docker bridge   |
| Grafana             | 3001  | Docker bridge   |
| ZooKeeper           | 2181  | Docker bridge   |

---

## Storage Layout

| Location | Type | Contents |
|----------|------|----------|
| `flink-warehouse` (Docker volume) | Named volume | Paimon catalogs, Iceberg catalogs, Flink checkpoints, HA metadata, reconciliation JSON |
| `./data/iggy/` | Bind mount → `/app/local_data/` | Iggy streams, topics, message logs |
| `./data/lancedb/` | Bind mount | LanceDB vector index |
| `./data/consensus/` | Bind mount | Consensus state file (`state.json`) |
| `./data/zookeeper/` | Bind mount | ZooKeeper data |
| `./data/grafana/` | Bind mount | Grafana persistent data |

---

## Critical Constraints

- **DuckDB + Paimon PK tables**: DuckDB reads all parquet files including stale pre-compaction snapshots. Only safe for append-only tables. Use Flink SQL for PK tables with merge engines.
- **Paimon aggregation deltas**: Every INSERT is an arithmetic instruction, not a fact. Seed operations must run exactly once.
- **Flink HA recovers all jobs**: Including completed one-shot seeds. Seed, verify, restart cluster, then submit production jobs.
- **Host networking**: Analyst and Consensus use `network_mode: host` to reach host Ollama and Iggy. Use `172.17.0.1` (Docker bridge gateway) for Prometheus scrape targets.
