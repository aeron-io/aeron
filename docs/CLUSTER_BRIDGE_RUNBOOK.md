# Inter-Cluster Bridge Service — Runbook

## Prerequisites

- **Java 17+** (tested with Eclipse Temurin 17.0.17)
- **Gradle wrapper** (included in repo: `./gradlew` on Linux/macOS, `gradlew.bat` on Windows)
- This repo checked out at commit `e2fba3f1b1` or later on the `master` branch

Verify Java:

```bash
java -version
# Expected: openjdk version "17.x.x"
```

---

## 1. Build the Project

### Linux / macOS

```bash
./gradlew :aeron-cluster-bridge:compileJava
```

### Windows

```cmd
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot
gradlew.bat :aeron-cluster-bridge:compileJava
```

Alternatively, to build everything:

```bash
./gradlew build -x test -x javadoc -x checkstyleMain -x checkstyleTest
```

A successful build produces no errors. The bridge classes are in:

```
aeron-cluster-bridge/build/classes/java/main/io/aeron/cluster/bridge/
```

---

## 2. Start the Required Components

The bridge uses an embedded `ArchivingMediaDriver` — no separate Media Driver
process is needed. The `ClusterBridge` main class starts everything.

### Environment Setup (optional)

```bash
# Override defaults via system properties:
export JAVA_OPTS="-Daeron.bridge.me.to.rms.channel=aeron:udp?endpoint=localhost:20121 \
                  -Daeron.bridge.rms.to.me.channel=aeron:udp?endpoint=localhost:20122"
```

Default channels if not overridden:

| Direction | Channel | Stream ID |
|---|---|---|
| ME to RMS | `aeron:udp?endpoint=localhost:20121` | 1001 |
| RMS to ME | `aeron:udp?endpoint=localhost:20122` | 1002 |

---

## 3. Run a Demo Round Trip (ME to RMS to ME)

### Option A: Run the Integration Test

The fastest way to verify the full round trip:

```bash
# Linux / macOS
./gradlew :aeron-cluster-bridge:test \
    --tests "io.aeron.cluster.bridge.ClusterBridgeIntegrationTest" \
    -Daeron.dir.delete.on.start=true \
    --info

# Windows (Git Bash with JAVA_HOME set)
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.17.10-hotspot"
bash ./gradlew :aeron-cluster-bridge:test \
    --tests "io.aeron.cluster.bridge.ClusterBridgeIntegrationTest" \
    -Daeron.dir.delete.on.start=true \
    --info
```

The test:
1. Starts an embedded `ArchivingMediaDriver`
2. Sends 50 sequenced messages ME to RMS
3. Receives all 50 at the RMS bridge receiver
4. Sends 50 sequenced messages RMS to ME
5. Receives all 50 at the ME bridge receiver
6. Simulates crash/restart and verifies replay catch-up from Archive
7. Asserts: no duplicates, correct ordering, all messages delivered

### Option B: Run the Bridge Main Class

**Using Gradle:**

```bash
./gradlew :aeron-cluster-bridge:run \
    -PmainClass=io.aeron.cluster.bridge.ClusterBridge \
    --args="--mode bridge"
```

**Using Java directly (Linux/macOS):**

```bash
./gradlew :aeron-cluster-bridge:compileJava

java -cp aeron-cluster-bridge/build/classes/java/main:\
aeron-archive/build/classes/java/main:aeron-archive/build/classes/java/generated:\
aeron-driver/build/classes/java/main:aeron-client/build/classes/java/main:\
$(find ~/.gradle/caches -name "agrona-*.jar" -not -name "*javadoc*" -not -name "*sources*" | head -1) \
    -Daeron.dir.delete.on.start=true \
    io.aeron.cluster.bridge.ClusterBridge --mode bridge
```

---

## 4. Demonstrate Replay (Stop Receiver, Publish, Restart, Verify Catch-Up)

The integration test `ClusterBridgeIntegrationTest.shouldCatchUpAfterRestart()`
demonstrates this automatically. Here is the manual sequence:

### Step-by-Step Manual Replay Demo

1. **Start the bridge with embedded driver:**

   ```bash
   java ... io.aeron.cluster.bridge.ClusterBridge --mode bridge
   ```

2. **Observe normal message flow** (the bridge sends periodic heartbeat messages
   internally for demo purposes).

3. **Kill the receiver** (Ctrl+C the bridge process). Note the last checkpoint
   printed to stdout.

4. **Restart the bridge:**

   ```bash
   java ... io.aeron.cluster.bridge.ClusterBridge --mode bridge
   ```

5. **On restart**, the receiver reads its checkpoint file
   (`bridge-checkpoint-me-to-rms.bin` or `bridge-checkpoint-rms-to-me.bin`),
   connects to Archive, and replays from the checkpoint position.

6. **Verify catch-up**: The receiver logs progress as it replays from archive
   and merges with the live stream.

### What the Integration Test Verifies

The `shouldCatchUpAfterRestart()` test method:

1. Starts an `ArchivingMediaDriver` and sender
2. Sends 25 messages, receiver processes them and checkpoints at seq=25
3. Stops the receiver (simulates crash)
4. Sends 25 more messages (seq 26..50) — these accumulate in Archive
5. Restarts the receiver with the existing checkpoint
6. Verifies the receiver catches up from Archive and receives seq 26..50
7. Asserts no duplicates and correct ordering

### Checkpointing & Idempotency Details

The bridge uses a **checkpoint-based** recovery model (not watermarks):

- **Checkpoint**: After each applied message, the receiver writes
  `(lastAppliedSequence, archivePosition)` atomically to disk.
- **Replay**: On restart, the receiver reads the checkpoint and starts
  `ReplayMerge` from the last known archive position.
- **Idempotency**: Messages with `sequence <= lastAppliedSequence` are
  silently skipped during replay. This handles the overlap zone where Archive
  replay may re-deliver messages that were already checkpointed.
- **Aeron Archive as source of truth**: The archive recording is a complete,
  ordered log of all published messages. The checkpoint tells the receiver
  where it left off. Together they guarantee exactly-once application semantics.

```
Checkpoint file (24 bytes):
  magic(4) + direction(4) + lastAppliedSequence(8) + archivePosition(8)

Recovery flow:
  1. Load checkpoint → seq=N, pos=P
  2. ReplayMerge from pos=P
  3. Skip all messages where msg.seq <= N (already applied)
  4. Apply messages where msg.seq > N (new)
  5. Transition to live stream when caught up
```

---

## 5. Docker Setup

### Dockerfile

Create `aeron-cluster-bridge/Dockerfile`:

```dockerfile
FROM eclipse-temurin:17-jre-jammy

# System tuning for low-latency
RUN echo "net.core.rmem_max=8388608" >> /etc/sysctl.conf && \
    echo "net.core.wmem_max=8388608" >> /etc/sysctl.conf

WORKDIR /app

# Copy the fat jar (built via: ./gradlew :aeron-cluster-bridge:shadowJar)
# or copy individual jars and set classpath
COPY build/libs/*.jar /app/
COPY build/classes/ /app/classes/

# Default JVM flags for low-latency
ENV JAVA_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=5 -XX:+AlwaysPreTouch -Xms512m -Xmx512m"

# Expose bridge UDP ports and Archive control
EXPOSE 20121/udp 20122/udp 8010/udp

# Checkpoint directory (mount as volume for persistence)
VOLUME /app/checkpoints

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS \
  -Daeron.dir.delete.on.start=true \
  -Daeron.bridge.checkpoint.dir=/app/checkpoints \
  -cp '/app/*:/app/classes/java/main' \
  io.aeron.cluster.bridge.ClusterBridge $0 $@"]

CMD ["--mode", "bridge"]
```

### docker-compose.yml

For local testing of ME-side and RMS-side bridges:

```yaml
version: "3.8"

services:
  bridge-me:
    build:
      context: ./aeron-cluster-bridge
    container_name: bridge-me
    ports:
      - "20121:20121/udp"
      - "20122:20122/udp"
    volumes:
      - me-checkpoints:/app/checkpoints
      - me-archive:/app/aeron-archive
    environment:
      JAVA_OPTS: >-
        -server -XX:+UseG1GC -XX:MaxGCPauseMillis=5
        -Xms512m -Xmx512m
        -Daeron.bridge.me.to.rms.channel=aeron:udp?endpoint=bridge-rms:20121
        -Daeron.bridge.rms.to.me.channel=aeron:udp?endpoint=bridge-me:20122
    command: ["--mode", "bridge"]
    networks:
      - bridge-net

  bridge-rms:
    build:
      context: ./aeron-cluster-bridge
    container_name: bridge-rms
    ports:
      - "20123:20121/udp"
      - "20124:20122/udp"
    volumes:
      - rms-checkpoints:/app/checkpoints
      - rms-archive:/app/aeron-archive
    environment:
      JAVA_OPTS: >-
        -server -XX:+UseG1GC -XX:MaxGCPauseMillis=5
        -Xms512m -Xmx512m
        -Daeron.bridge.me.to.rms.channel=aeron:udp?endpoint=bridge-rms:20121
        -Daeron.bridge.rms.to.me.channel=aeron:udp?endpoint=bridge-me:20122
    command: ["--mode", "bridge"]
    networks:
      - bridge-net

volumes:
  me-checkpoints:
  me-archive:
  rms-checkpoints:
  rms-archive:

networks:
  bridge-net:
    driver: bridge
```

### Build & Run

```bash
# Build the module first
./gradlew :aeron-cluster-bridge:compileJava

# Build Docker image
docker build -t aeron-bridge ./aeron-cluster-bridge

# Run with docker-compose
docker-compose up -d

# Check logs
docker-compose logs -f bridge-me

# Tear down
docker-compose down -v
```

---

## 6. CI/CD Setup

### GitHub Actions Pipeline

```yaml
# .github/workflows/bridge-ci.yml
name: Bridge CI

on:
  push:
    branches: [master]
    paths: ['aeron-cluster-bridge/**']
  pull_request:
    paths: ['aeron-cluster-bridge/**']

jobs:
  build-and-test:
    runs-on: ubuntu-latest
    timeout-minutes: 15

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'

      - name: Cache Gradle
        uses: actions/cache@v4
        with:
          path: |
            ~/.gradle/caches
            ~/.gradle/wrapper
          key: ${{ runner.os }}-gradle-${{ hashFiles('**/*.gradle*') }}

      - name: Compile
        run: ./gradlew :aeron-cluster-bridge:compileJava

      - name: Checkstyle
        run: ./gradlew :aeron-cluster-bridge:checkstyleMain

      - name: Integration Tests
        run: ./gradlew :aeron-cluster-bridge:test --info

      - name: Upload Test Report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: test-report
          path: aeron-cluster-bridge/build/reports/tests/

  docker:
    needs: build-and-test
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/master'

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'

      - name: Build Jar
        run: ./gradlew :aeron-cluster-bridge:compileJava

      - name: Build Docker Image
        run: docker build -t aeron-bridge:${{ github.sha }} ./aeron-cluster-bridge

      - name: Push to ECR
        if: github.event_name == 'push'
        env:
          AWS_ACCOUNT: ${{ secrets.AWS_ACCOUNT_ID }}
          AWS_REGION: us-east-1
        run: |
          aws ecr get-login-password --region $AWS_REGION | \
            docker login --username AWS --password-stdin $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
          docker tag aeron-bridge:${{ github.sha }} \
            $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/aeron-bridge:${{ github.sha }}
          docker push $AWS_ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com/aeron-bridge:${{ github.sha }}
```

### Production Deployment Checklist

| Step | Command / Action | Verification |
|---|---|---|
| 1. Build | `./gradlew :aeron-cluster-bridge:compileJava` | No compilation errors |
| 2. Checkstyle | `./gradlew :aeron-cluster-bridge:checkstyleMain` | No violations |
| 3. Test | `./gradlew :aeron-cluster-bridge:test` | All 3 tests pass |
| 4. Docker build | `docker build -t aeron-bridge .` | Image builds successfully |
| 5. Push to ECR | `docker push <ecr-url>/aeron-bridge:<tag>` | Image in ECR |
| 6. Deploy to EC2 | Pull image, configure system properties for target endpoints | Bridge starts, archive recording begins |
| 7. Smoke test | Send test messages, verify round-trip | Received count matches sent count |
| 8. Monitor | Check `AeronStat` counters, disk usage, checkpoint files | No errors, checkpoints advancing |

---

## 7. Troubleshooting

### Common Failures

| Symptom | Cause | Fix |
|---|---|---|
| `MediaDriver is already running` | Stale shared memory from previous run | Set `-Daeron.dir.delete.on.start=true` or delete `/dev/shm/aeron-*` (Linux) / `%TEMP%\aeron-*` (Windows) |
| `Publication not connected` | Receiver not started or wrong channel | Check channel/streamId match between sender and receiver |
| `Archive not connected` | ArchivingMediaDriver not running | Ensure `ClusterBridge` starts the embedded driver first |
| `No recordings found` | No data has been published yet | Start the sender before the receiver, or check recording with `AeronStat` |
| `ReplayMerge failed` | Archive replay too slow or recording not found | Check archive directory, verify recording exists with `ArchiveTool describe` |
| `Archive.Context.controlChannel must be set` | Missing Archive configuration | Ensure `controlChannel` and `replicationChannel` are set on Archive.Context |
| `java.io.IOException: No space left` | Archive recordings filled disk | Truncate old recordings or increase disk space |
| `0 messages received` | Sender connected before receiver subscription was active | Ensure receiver is initialized before sender starts publishing. For tests, use `awaitConnected()` pattern. |

### Useful Diagnostic Commands

**Check Media Driver status:**

```bash
# Using the AeronStat tool from aeron-all jar:
java -cp aeron-all/build/libs/aeron-all-*.jar io.aeron.samples.AeronStat
```

**Check Archive recordings:**

```bash
java -cp aeron-all/build/libs/aeron-all-*.jar io.aeron.archive.ArchiveTool \
    <archive-dir> describe
```

**Check Aeron errors:**

```bash
java -cp aeron-all/build/libs/aeron-all-*.jar io.aeron.samples.ErrorStat
```

### Log Locations

| Log | Location |
|---|---|
| Media Driver counters | Shared memory: `/dev/shm/aeron-<user>/` (Linux), `%TEMP%\aeron-<user>\` (Windows) |
| Archive data | `./aeron-archive/` directory (configurable via `aeron.archive.dir`) |
| Bridge checkpoints | Working directory: `bridge-checkpoint-*.bin` (configurable via `aeron.bridge.checkpoint.dir`) |
| Bridge application logs | stdout/stderr |

### System Properties Reference

| Property | Default | Description |
|---|---|---|
| `aeron.bridge.me.to.rms.channel` | `aeron:udp?endpoint=localhost:20121` | ME to RMS live channel |
| `aeron.bridge.rms.to.me.channel` | `aeron:udp?endpoint=localhost:20122` | RMS to ME live channel |
| `aeron.bridge.me.to.rms.stream.id` | `1001` | ME to RMS stream ID |
| `aeron.bridge.rms.to.me.stream.id` | `1002` | RMS to ME stream ID |
| `aeron.bridge.checkpoint.dir` | `.` (working dir) | Directory for checkpoint files |
| `aeron.bridge.message.count` | `100` | Number of messages for demo/test |
| `aeron.dir.delete.on.start` | `false` | Delete stale Media Driver dirs on start |
| `aeron.archive.dir` | `./aeron-archive` | Archive recording directory |

---

## 8. Quick Validation Checklist

After building and running the test, verify:

- [ ] `./gradlew :aeron-cluster-bridge:compileJava` succeeds
- [ ] `./gradlew :aeron-cluster-bridge:checkstyleMain` succeeds
- [ ] `ClusterBridgeIntegrationTest` passes (all 3 tests green)
- [ ] Checkpoint files created in temp directory during test
- [ ] No duplicate sequence numbers in receiver output
- [ ] Receiver catches up after simulated restart (`shouldCatchUpAfterRestart`)
- [ ] Bidirectional flow (ME to RMS and RMS to ME) both work
