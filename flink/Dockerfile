FROM flink:1.20.3-java17

# Phase 3-5: Connector JARs baked into the image
COPY jars/flink-connector-iggy.jar /opt/flink/lib/
COPY jars/fluss-flink-1.20-0.9.0-incubating.jar /opt/flink/lib/
COPY jars/paimon-flink-1.20-1.3.1.jar /opt/flink/lib/
COPY jars/iceberg-flink-runtime-1.20-1.10.1.jar /opt/flink/lib/

# Parquet format factory (needed for filesystem connector parquet reads)
COPY jars/flink-sql-parquet-1.20.3.jar /opt/flink/lib/

# Hadoop 3.3.6 client + transitive deps (needed for Iceberg reads)
COPY jars/hadoop-client-api-3.3.6.jar /opt/flink/lib/
COPY jars/hadoop-client-runtime-3.3.6.jar /opt/flink/lib/
COPY jars/commons-logging-1.2.jar /opt/flink/lib/

# Replace gosu with passthrough so Flink runs as root (local dev only)
RUN mv /usr/local/bin/gosu /usr/local/bin/gosu.real && \
    printf '#!/bin/sh\nshift\nexec "$@"\n' > /usr/local/bin/gosu && \
    chmod +x /usr/local/bin/gosu
