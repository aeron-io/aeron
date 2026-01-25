# Dockerfile for Aeron Cluster Bridge Service
#
# This Dockerfile creates an image for running the Bridge Service components:
# - BridgeSender
# - BridgeReceiver
# - ClusterBridgeMain (unified entry point)
# - BridgeVerifier (test verification)
#
# Architecture Considerations:
# - Single-node optimized: This image is designed for single-machine testing
# - Multi-machine ready: Mount volumes for archive/checkpoint dirs for multi-node
# - Zero-copy friendly: Uses direct memory allocation optimized for Aeron
#
# Build:
#   docker build -t aeron-cluster-bridge -f aeron-cluster-bridge/Dockerfile .
#
# Run Sender:
#   docker run --rm -it --network host aeron-cluster-bridge --mode=sender
#
# Run Receiver:
#   docker run --rm -it --network host aeron-cluster-bridge --mode=receiver

FROM eclipse-temurin:17-jdk-alpine AS builder

WORKDIR /app

# Copy gradle files first for better caching
COPY gradlew gradlew.bat ./
COPY gradle ./gradle
COPY settings.gradle build.gradle version.txt ./
COPY buildSrc ./buildSrc

# Copy source code
COPY aeron-annotations ./aeron-annotations
COPY aeron-client ./aeron-client
COPY aeron-driver ./aeron-driver
COPY aeron-archive ./aeron-archive
COPY aeron-cluster ./aeron-cluster
COPY aeron-cluster-bridge ./aeron-cluster-bridge
COPY config ./config

# Build the project (skip tests for faster builds)
RUN chmod +x gradlew && \
    ./gradlew :aeron-cluster-bridge:build -x test --no-daemon

# Runtime image
FROM eclipse-temurin:17-jre-alpine

LABEL maintainer="Aeron Cluster Bridge" \
      description="Inter-cluster Bridge Service for Aeron" \
      version="1.0.0"

WORKDIR /app

# Create directories for runtime state
RUN mkdir -p /app/archive /app/checkpoints /app/logs

# Copy built artifacts
COPY --from=builder /app/aeron-cluster-bridge/build/libs/*.jar /app/libs/
COPY --from=builder /app/aeron-cluster/build/libs/*.jar /app/libs/
COPY --from=builder /app/aeron-archive/build/libs/*.jar /app/libs/
COPY --from=builder /app/aeron-driver/build/libs/*.jar /app/libs/
COPY --from=builder /app/aeron-client/build/libs/*.jar /app/libs/
COPY --from=builder /app/aeron-annotations/build/libs/*.jar /app/libs/

# Environment variables for Aeron tuning
ENV JAVA_OPTS="-Xms256m -Xmx1g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=5 \
    -Dagrona.disable.bounds.checks=true \
    -Daeron.threading.mode=SHARED \
    -Dbridge.archive.dir=/app/archive \
    -Dbridge.checkpoint.dir=/app/checkpoints"

# Default configuration
ENV BRIDGE_MODE="sender" \
    BRIDGE_DIRECTION="ME_TO_RMS" \
    BRIDGE_MESSAGE_COUNT="100" \
    BRIDGE_STREAM_ID="2001"

# Entry point script
COPY --chmod=755 <<'EOF' /app/entrypoint.sh
#!/bin/sh
set -e

# Build classpath from all jars
CLASSPATH=$(find /app/libs -name "*.jar" | tr '\n' ':')

# Parse mode from arguments or environment
MODE=${1:-$BRIDGE_MODE}
shift 2>/dev/null || true

# Set system properties from environment
JAVA_OPTS="$JAVA_OPTS \
    -Dbridge.direction=$BRIDGE_DIRECTION \
    -Dbridge.message.count=$BRIDGE_MESSAGE_COUNT \
    -Dbridge.stream.id=$BRIDGE_STREAM_ID"

echo "Starting Bridge Service in $MODE mode..."
echo "Direction: $BRIDGE_DIRECTION"
echo "Stream ID: $BRIDGE_STREAM_ID"

exec java $JAVA_OPTS -cp "$CLASSPATH" io.aeron.cluster.bridge.ClusterBridgeMain --mode=$MODE "$@"
EOF

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["sender"]
