#!/bin/bash
# Download connector JARs required by the Flink image.
#
# All JARs are fetched from Maven Central or GitHub Releases with SHA-256
# verification. Run this once before `docker compose build`.
#
# Usage:
#   ./scripts/download-jars.sh

set -Eeuo pipefail

JARS_DIR="$(cd "$(dirname "$0")/../jars" && pwd)"
mkdir -p "$JARS_DIR"

MAVEN="https://repo1.maven.org/maven2"

# name | URL | SHA-256
JARS=(
  "flink-connector-iggy.jar|https://github.com/gordonmurray/flink-connector-iggy/releases/download/v0.1.1/flink-connector-iggy-0.1.1-SNAPSHOT.jar|17979f13c13b9a17e7b92d304a07622fed1d94682b806e961732fa82a1bd80df"
  "fluss-flink-1.20-0.9.0-incubating.jar|${MAVEN}/org/apache/fluss/fluss-flink-1.20/0.9.0-incubating/fluss-flink-1.20-0.9.0-incubating.jar|06e3a25461e3c4de9f1875050593287a9b61f794c8216c33e8464b2e85776489"
  "paimon-flink-1.20-1.3.1.jar|${MAVEN}/org/apache/paimon/paimon-flink-1.20/1.3.1/paimon-flink-1.20-1.3.1.jar|a333b7a3df6143b782d129895f64575f0007912c4a2c85502d421b8198f42ce1"
  "iceberg-flink-runtime-1.20-1.10.1.jar|${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar|f06b3f2fbd004feeb10adc8957f27d43203a0dc526a9ae2e0a42219fbcdbcfe7"
  "flink-sql-parquet-1.20.3.jar|${MAVEN}/org/apache/flink/flink-sql-parquet/1.20.3/flink-sql-parquet-1.20.3.jar|f02a6a68ba9c17713efffa69eecc80bedf1aa1c71f9ad216c63972793cd21c45"
  "hadoop-client-api-3.3.6.jar|${MAVEN}/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar|f3d2347a6e1c6885d5bcfd4f60c3ac3810ec11068fc161e04329baabf412d963"
  "hadoop-client-runtime-3.3.6.jar|${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar|15f01bc804294df06d2effc87de363a83cf589f50558bdbf48f72541ad8de854"
  "commons-logging-1.2.jar|${MAVEN}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar|daddea1ea0be0f56978ab3006b8ac92834afeefbd9b7e4e6316fca57df0fa636"
)

FAILED=0

for entry in "${JARS[@]}"; do
  IFS='|' read -r name url checksum <<< "$entry"
  dest="${JARS_DIR}/${name}"

  if [ -f "$dest" ]; then
    actual=$(sha256sum "$dest" | awk '{print $1}')
    if [ "$actual" = "$checksum" ]; then
      echo "OK  $name (already present)"
      continue
    else
      echo "MISMATCH  $name — re-downloading"
      rm -f "$dest"
    fi
  fi

  echo "Downloading $name ..."
  if ! curl --fail --show-error --silent --location -o "$dest" "$url"; then
    echo "FAILED to download $name from $url"
    FAILED=1
    continue
  fi

  actual=$(sha256sum "$dest" | awk '{print $1}')
  if [ "$actual" != "$checksum" ]; then
    echo "CHECKSUM FAILED for $name"
    echo "  expected: $checksum"
    echo "  got:      $actual"
    rm -f "$dest"
    FAILED=1
  else
    echo "OK  $name"
  fi
done

if [ "$FAILED" -ne 0 ]; then
  echo ""
  echo "Some JARs failed to download or verify. Check output above."
  exit 1
fi

echo ""
echo "All JARs downloaded and verified in ${JARS_DIR}/"
