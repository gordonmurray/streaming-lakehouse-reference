# Clean Slate — Full Reset with 6-Month Historical Backfill

## Goal

Stop the entire system, wipe all data, backfill 6 months of 1-minute OHLCV
candles from the Coinbase Exchange REST API into Paimon, then restart fresh
with a $1,000 starting balance.

**Expected duration**: ~40 minutes (35 min API fetch + 5 min setup).

---

## Prerequisites

Before running, ensure these one-time setup items are done:

```bash
# Python dependencies for backfill script (host)
pip install --break-system-packages pyarrow requests

# Download connector JARs (if not already present)
./scripts/download-jars.sh
```

---

## Config Changes (do before wiping)

Update `.env`:
```
CONSENSUS_STARTING_BALANCE=1000
CONSENSUS_TRADE_SIZE_USD=50
```

Update `flink/sql/seed-balance.sql` (line 60):
```sql
INSERT INTO paimon_catalog.crypto.balance VALUES ('USD', 1000.0);
```

---

## Step 1 — Stop and Wipe

```bash
# Stop everything
docker compose down

# Wipe root-owned data (containers write as root)
docker run --rm -v $(pwd)/data:/data alpine sh -c \
  "rm -rf /data/iggy/* /data/consensus/* /data/lancedb/* /data/zookeeper/* /data/grafana/* /data/prometheus/*"

# Remove flink-warehouse volume (may need to force-remove stale containers)
docker volume rm streaming-lakehouse-reference_flink-warehouse 2>/dev/null || \
  (docker rm -f $(docker ps -aq --filter volume=streaming-lakehouse-reference_flink-warehouse) 2>/dev/null; \
   docker volume rm streaming-lakehouse-reference_flink-warehouse)

# Clean up
rm -f ./data/reconcile.log
```

---

## Step 2 — Start Infrastructure Only

Start just enough services for the backfill script to stage data on the
flink-warehouse volume:

```bash
docker compose up -d zookeeper iggy jobmanager taskmanager
sleep 15  # wait for Flink cluster to be ready
```

---

## Step 3 — Backfill Historical Candles

```bash
python3 scripts/backfill-candles.py
```

This fetches ~1.5M candles (6 pairs × 6 months) from Coinbase REST API
and stages them as a parquet file on the flink-warehouse volume.

**Duration**: ~35 minutes (rate-limited API calls).
**Output**: `/opt/flink/warehouse/backfill/candles.parquet` (~25 MB).

---

## Step 4 — Import Backfill into Paimon

The lakehouse-tier job creates the `ohlcv_1m` table, then the import job
reads the staged parquet through Paimon's proper write layer.

```bash
# Create tables via lakehouse pipeline
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/lakehouse-tier.sql
sleep 10

# Import backfill into Paimon (job auto-terminates when source is exhausted)
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/import-backfill.sql
sleep 30  # wait for commit
```

**Note**: The import requires `flink-sql-parquet-1.20.3.jar` in the Flink
image. If you see `Could not find any format factory for identifier 'parquet'`,
the JAR is missing from `jars/` — see Prerequisites.

---

## Step 5 — Create Orders Topic

The Flink clearing house needs the Iggy `orders` topic to exist before
submission, or it enters a RESTARTING loop.

```bash
docker exec iggy iggy --username iggy --password iggy topic create crypto orders 3 none
```

---

## Step 6 — Seed Balance and Submit Production Jobs

```bash
# Seed $1,000 balance
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/seed-balance.sql
sleep 15

# Restart Flink to clear seed job from ZooKeeper HA
# (prevents HA from re-running the seed on future restarts)
docker compose restart jobmanager taskmanager
sleep 15

# Submit remaining production jobs (lakehouse auto-recovers via HA)
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/fluss-hot-tier.sql
docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded < flink/sql/clearing-house.sql

# Verify: exactly 3 running jobs
curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys, json
for j in json.load(sys.stdin)['jobs']:
    if j['state'] == 'RUNNING':
        print(f\"  {j['name'][:70]}\")
"
```

---

## Step 7 — Start Remaining Services

```bash
docker compose up -d
```

---

## Step 8 — Index Lancer

The indexer caps at 100K candles per pair (~70 days) to avoid OOM. With
6 months of backfill, it selects the most recent 100K per pair.

```bash
docker compose --profile index run --rm lancer-indexer
```

**Expected**: ~600K patterns indexed. Takes ~30 seconds.

**Note**: The Lancer signal mode needs 60 minutes of live candle data to
fill its buffer before it can produce similarity scores. Until then,
scores will be 0.00 and no trades will fire.

---

## Step 9 — Run Reconciliation

```bash
./scripts/reconcile-snapshot.sh
```

Verify the consensus engine adopted $1,000:

```bash
curl -s http://localhost:8007/api/positions
```

---

## Verification Checklist

```bash
# All services healthy
docker compose ps

# 3 Flink jobs running
curl -s http://localhost:8081/jobs/overview | python3 -c "
import sys, json
running = [j for j in json.load(sys.stdin)['jobs'] if j['state']=='RUNNING']
print(f'{len(running)} jobs running')
for j in running: print(f\"  {j['name'][:70]}\")
"

# Balance is $1,000
curl -s http://localhost:8007/api/positions | python3 -m json.tool

# Candle API has historical data
curl -s 'http://localhost:8007/api/candles/BTC-USD?minutes=1440' | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(f'{len(d)} candles')"

# Lancer index loaded
curl -s http://localhost:8003/api/health | python3 -m json.tool
```

---

## Lessons Learned (Gotchas)

1. **Root-owned files**: Containers write data as root. Use an Alpine
   container to wipe `./data/` directories, not `rm -rf` from the host.

2. **Stale container holds volume**: `docker volume rm` fails if any
   container (even stopped) references the volume. Force-remove the
   container first.

3. **flink-sql-parquet JAR**: Not included in base Flink image. Required
   for the `filesystem` connector with `'format' = 'parquet'`. Paimon
   and Iceberg bring their own parquet readers, so this gap isn't obvious
   until the import step. Baked into `flink/Dockerfile`.

4. **Iggy orders topic**: Must exist before submitting the clearing house
   job. The consensus engine creates it on startup, but if the clearing
   house is submitted first, it enters a RESTARTING loop. Create it
   manually in the clean slate flow.

5. **Lancer OOM on large datasets**: 6 months × 6 pairs = ~4.5M candles.
   Building sliding windows for all of them exhausts container memory
   (exit code 137). The indexer caps at 100K candles per pair
   (`LANCER_MAX_CANDLES_PER_PAIR` env var, default 100K).

6. **Flink task slots**: 8 slots total. The 3 production jobs use 9
   slots (Flink shares some). A batch SELECT query for reconciliation
   cannot get a slot while all 3 jobs + indexer are running. Run the
   reconciliation script after the indexer completes.

7. **Flink SQL client output prefix**: In tableau mode, the first
   `+---+` separator line is prefixed with `Flink SQL> `. The
   reconciliation parser must detect `+--` anywhere in the line, not
   just at position 0.

8. **Seed job in HA**: After seeding, restart Flink to flush the seed
   job from ZooKeeper. Otherwise HA will re-run it on every future
   restart, inflating the balance.

9. **Lancer signal buffer**: ~~The live signal mode needs 60 consecutive
   1-minute candles.~~ **Fixed**: Signal mode now pre-fills its buffer
   from Paimon on startup via DuckDB. Requires `flink-warehouse` volume
   mounted on the `lancer` service. Similarity scores available within
   seconds of startup.

10. **Prometheus data survives clean slate**: `./data/prometheus/` stores
    all historical metrics. If not wiped, Grafana stat panels (trade
    counts, equity history) show stale values from the previous session.
    Must be included in the wipe step.

11. **Flink-Iggy connector stale offsets**: If Flink jobs are recovered
    from checkpoints after Iggy topics were wiped and recreated, the
    connector silently fails with `Poll failed, will retry` on every
    partition. The connector tries to poll from offsets that no longer
    exist in the new topic. Fix: always wipe ZooKeeper HA state and
    Flink checkpoints during clean slate, then submit all jobs fresh
    (no checkpoint recovery). This is a connector-level bug — tracked
    for fix in flink-connector-iggy.

---

## Post-Reset State

| Component | State |
|-----------|-------|
| Iggy | Topics created by producers on startup |
| Paimon `ohlcv_1m` | ~1.5M backfilled + live candles flowing |
| Paimon `balance` | $1,000 USD |
| Paimon `trades` | Empty — populated by clearing house |
| Iceberg | Empty — accumulates live ticks going forward |
| LanceDB | ~600K patterns (100K candles × 6 pairs) |
| Flink HA | 3 production jobs in ZooKeeper |
| Consensus | $1,000, no positions, $50/trade |
| Grafana | Dashboards provisioned from files |

---

## Running the Backtester

The backtester runs offline against Paimon candle data. No running services
required — it reads parquet files directly.

### Prerequisites

```bash
pip install --break-system-packages duckdb lancedb numpy
```

### Copy candle data to host (if needed)

The candle data lives on the `flink-warehouse` Docker volume. To run the
backtester on the host, copy it first:

```bash
mkdir -p /tmp/backtest_candles
docker cp jobmanager:/opt/flink/warehouse/paimon/crypto.db/ohlcv_1m /tmp/backtest_candles/
```

### Single run (current parameters)

```bash
python3 scripts/backtest.py --warehouse /tmp/backtest_candles/ohlcv_1m
```

### Custom parameters

```bash
python3 scripts/backtest.py \
  --warehouse /tmp/backtest_candles/ohlcv_1m \
  --enter 0.25 --exit 0.15 --min-outcome 0.2 --viability 2.0
```

### Parameter sweep

```bash
python3 scripts/backtest.py \
  --warehouse /tmp/backtest_candles/ohlcv_1m \
  --sweep --json sweep_results.json
```

### Memory control

Use `--max-candles` to limit candles per pair (default 20K ~14 days):

```bash
# Light (fast, ~1 min)
python3 scripts/backtest.py --warehouse /tmp/backtest_candles/ohlcv_1m --max-candles 5000

# Full (slow, ~10 min, stop the system first)
python3 scripts/backtest.py --warehouse /tmp/backtest_candles/ohlcv_1m --max-candles 100000
```

**Warning**: Full-scale runs (100K candles/pair) use ~2GB RAM for the
LanceDB index. Only run with the system stopped.
