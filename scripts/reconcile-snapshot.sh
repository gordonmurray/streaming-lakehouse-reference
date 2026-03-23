#!/bin/bash
# Reconciliation snapshot — queries Paimon via Flink SQL and writes JSON
# to the flink-warehouse volume for the consensus engine to read.
#
# Run periodically: */5 * * * * /path/to/reconcile-snapshot.sh
# Or manually: ./scripts/reconcile-snapshot.sh

set -euo pipefail

DEST="/opt/flink/warehouse/paimon/crypto.db"
TMPOUT="/tmp/reconcile_output.txt"

BALANCE_SQL="SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'classloader.parent-first-patterns.additional' = 'org.apache.paimon.;org.apache.hadoop.;com.codahale.';
CREATE CATALOG paimon_catalog WITH ('type' = 'paimon', 'warehouse' = '/opt/flink/warehouse/paimon');
SELECT currency, amount FROM paimon_catalog.crypto.balance LIMIT 100;"

# Run query — allow up to 90s for JVM startup + query
echo "$BALANCE_SQL" | docker exec -i jobmanager /opt/flink/bin/sql-client.sh embedded 2>/dev/null > "$TMPOUT" || true

# Parse the tableau output: extract data rows between 2nd and 3rd +--- separators
balance_json=$(python3 -c "
import json
lines = open('$TMPOUT').read().splitlines()
result = {}
sep_count = 0
for line in lines:
    s = line.strip()
    if '+--' in s and s.rstrip().endswith('+'):
        sep_count += 1
        continue
    # Data rows are between the 2nd and 3rd separator
    if sep_count == 2 and '|' in s:
        cells = [c.strip() for c in s.split('|')[1:-1]]
        if len(cells) == 2:
            try: result[cells[0]] = float(cells[1])
            except: pass
print(json.dumps(result))
" 2>/dev/null) || balance_json="{}"

# Write balance file if we got data
if [ -n "$balance_json" ] && [ "$balance_json" != "{}" ]; then
    echo "$balance_json" | docker exec -i jobmanager bash -c "cat > ${DEST}/reconcile_balance.json" 2>/dev/null
    echo "$(date '+%H:%M:%S') Balance: $balance_json"
else
    echo "$(date '+%H:%M:%S') No balance data — skipped"
fi

# Cancel any leftover SELECT batch jobs
curl -s http://localhost:8081/jobs/overview 2>/dev/null | python3 -c "
import sys, json, subprocess
try:
    jobs = json.load(sys.stdin)['jobs']
    for j in jobs:
        if j['state'] == 'RUNNING' and 'SELECT' in j['name']:
            subprocess.run(['curl', '-s', '-X', 'PATCH',
                'http://localhost:8081/jobs/' + j['jid'] + '?mode=cancel'],
                capture_output=True)
except: pass
" 2>/dev/null

# rm -f "$TMPOUT"  # keep for debugging
