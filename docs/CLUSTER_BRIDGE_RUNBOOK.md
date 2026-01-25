# Cluster Bridge Service - Runbook

## Overview

This runbook provides step-by-step instructions for building, running, and demonstrating the Cluster Bridge Service.

## Prerequisites

### Required Software

| Software | Version | Verification Command |
|----------|---------|---------------------|
| Java JDK | 17+ | `java -version` |
| Gradle | 8.x (via wrapper) | `./gradlew --version` |
| Git | 2.x | `git --version` |

### Supported Platforms

| Platform | Build Command | Notes |
|----------|---------------|-------|
| macOS | `./gradlew` | Primary target |
| Linux | `./gradlew` | Fully supported |
| Windows | `.\gradlew.bat` | Supported |

## Build Instructions

### Step 1: Clone and Navigate

```bash
# If not already cloned
git clone <repo-url>
cd aeron
```

### Step 2: Verify Environment

```bash
# Check Java version (must be 17+)
java -version

# Expected output:
# openjdk version "17.x.x"
```

### Step 3: Build the Project

```bash
# macOS/Linux
./gradlew clean build -x test

# Windows
.\gradlew.bat clean build -x test
```

Expected output:
```
BUILD SUCCESSFUL in XXs
XX actionable tasks: XX executed
```

### Step 4: Build with Tests (Optional)

```bash
# Full build with tests (takes longer)
./gradlew build

# Run only bridge-related samples compile
./gradlew :aeron-samples:compileJava
```

## Running the Bridge Demo

### Architecture Overview

```
┌─────────────────┐                    ┌─────────────────┐
│   Terminal 1    │                    │   Terminal 2    │
│                 │                    │                 │
│ BridgeSender    │───── Stream ─────▶│ BridgeReceiver  │
│ (ME → RMS)      │      2001         │ (ME → RMS)      │
│                 │                    │                 │
│ Archive records │                    │ Replay + Live   │
│ all messages    │                    │ join on restart │
└─────────────────┘                    └─────────────────┘
```

### Demo 1: Basic Round Trip (ME → RMS)

#### Terminal 1: Start the Receiver

```bash
# macOS/Linux
./gradlew :aeron-samples:runBridgeReceiver \
    -Dbridge.direction=ME_TO_RMS \
    -Dbridge.stream.id=2001

# Windows
.\gradlew.bat :aeron-samples:runBridgeReceiver ^
    -Dbridge.direction=ME_TO_RMS ^
    -Dbridge.stream.id=2001
```

Expected output:
```
[Bridge Receiver] Starting ME_TO_RMS receiver on stream 2001
[Bridge Receiver] Waiting for messages...
[Bridge Receiver] Checkpoint: seq=0, pos=0
```

#### Terminal 2: Start the Sender

```bash
# macOS/Linux
./gradlew :aeron-samples:runBridgeSender \
    -Dbridge.direction=ME_TO_RMS \
    -Dbridge.stream.id=2001 \
    -Dbridge.message.count=100

# Windows
.\gradlew.bat :aeron-samples:runBridgeSender ^
    -Dbridge.direction=ME_TO_RMS ^
    -Dbridge.stream.id=2001 ^
    -Dbridge.message.count=100
```

Expected output:
```
[Bridge Sender] Starting ME_TO_RMS sender on stream 2001
[Bridge Sender] Recording started: recordingId=0
[Bridge Sender] Sent message seq=1
[Bridge Sender] Sent message seq=2
...
[Bridge Sender] Sent message seq=100
[Bridge Sender] All messages sent. Archive position: 12800
```

#### Terminal 1: Verify Reception

```
[Bridge Receiver] Received: seq=1, type=HEARTBEAT, len=8
[Bridge Receiver] Received: seq=2, type=HEARTBEAT, len=8
...
[Bridge Receiver] Received: seq=100, type=HEARTBEAT, len=8
[Bridge Receiver] Checkpoint updated: seq=100
```

### Demo 2: Bidirectional Flow (ME ↔ RMS)

#### Terminal 1: ME Node (Sender + Receiver)

```bash
./gradlew :aeron-samples:runClusterBridge \
    --args="--mode=ME --message-count=50"
```

#### Terminal 2: RMS Node (Sender + Receiver)

```bash
./gradlew :aeron-samples:runClusterBridge \
    --args="--mode=RMS --message-count=50"
```

Expected: Each node sends 50 messages and receives 50 messages from the other.

### Demo 3: Replay Scenario (Recovery After Crash)

This demo shows deterministic catch-up after receiver restart.

#### Step 1: Start Sender (Keep Running)

```bash
# Terminal 1
./gradlew :aeron-samples:runBridgeSender \
    -Dbridge.direction=ME_TO_RMS \
    -Dbridge.message.count=1000 \
    -Dbridge.message.interval.ms=100
```

#### Step 2: Start Receiver (Let it Process Some Messages)

```bash
# Terminal 2
./gradlew :aeron-samples:runBridgeReceiver \
    -Dbridge.direction=ME_TO_RMS
```

Wait until receiver shows `seq=200` or more.

#### Step 3: Kill the Receiver

Press `Ctrl+C` in Terminal 2 to simulate crash.

```
[Bridge Receiver] Received: seq=200
^C
[Bridge Receiver] Shutting down...
[Bridge Receiver] Final checkpoint: seq=200, pos=25600
```

#### Step 4: Let Sender Continue

Sender continues publishing (Terminal 1):
```
[Bridge Sender] Sent message seq=201
[Bridge Sender] Sent message seq=202
...
[Bridge Sender] Sent message seq=500
```

#### Step 5: Restart Receiver

```bash
# Terminal 2 - Same command
./gradlew :aeron-samples:runBridgeReceiver \
    -Dbridge.direction=ME_TO_RMS
```

Expected output showing replay:
```
[Bridge Receiver] Starting ME_TO_RMS receiver on stream 2001
[Bridge Receiver] Checkpoint found: seq=200, pos=25600
[Bridge Receiver] Initiating replay from position 25600
[ReplayMerge] State: RESOLVE_REPLAY_PORT
[ReplayMerge] State: GET_RECORDING_POSITION
[ReplayMerge] State: REPLAY
[Bridge Receiver] Replayed: seq=201
[Bridge Receiver] Replayed: seq=202
...
[ReplayMerge] State: CATCHUP
[ReplayMerge] State: ATTEMPT_LIVE_JOIN
[ReplayMerge] State: MERGED
[Bridge Receiver] Live stream joined at seq=498
[Bridge Receiver] Received (live): seq=501
[Bridge Receiver] Received (live): seq=502
...
```

#### Step 6: Verify No Duplicates

Check that:
1. No sequence numbers were skipped (201, 202, 203, ...)
2. No sequence numbers were duplicated
3. Checkpoint was updated atomically

### Demo 4: Run the Verifier

The verifier asserts ordering and no-duplicate invariants.

```bash
./gradlew :aeron-samples:runBridgeVerifier \
    -Dbridge.direction=ME_TO_RMS \
    -Dbridge.message.count=1000
```

Expected output:
```
[Verifier] Starting verification: ME_TO_RMS, 1000 messages
[Verifier] Spawning sender...
[Verifier] Spawning receiver...
[Verifier] Sender completed.
[Verifier] Receiver completed.
[Verifier] === VERIFICATION RESULTS ===
[Verifier] Messages sent: 1000
[Verifier] Messages received: 1000
[Verifier] Duplicates: 0
[Verifier] Out-of-order: 0
[Verifier] Missing: 0
[Verifier] === PASS ===
```

## Configuration Reference

### System Properties

| Property | Default | Description |
|----------|---------|-------------|
| `bridge.direction` | `ME_TO_RMS` | Direction: `ME_TO_RMS` or `RMS_TO_ME` |
| `bridge.stream.id` | `2001` | Aeron stream ID |
| `bridge.channel` | `aeron:udp?endpoint=localhost:40456` | Channel URI |
| `bridge.archive.dir` | `./build/aeron-archive` | Archive directory |
| `bridge.checkpoint.dir` | `./build/checkpoints` | Checkpoint directory |
| `bridge.message.count` | `100` | Number of messages to send |
| `bridge.message.interval.ms` | `10` | Interval between messages |
| `bridge.embedded.driver` | `true` | Use embedded media driver |
| `bridge.replay.merge` | `true` | Use ReplayMerge for recovery |

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `AERON_DIR` | Override Aeron directory |
| `JAVA_HOME` | Java installation path |

## Troubleshooting

### Common Issues

#### Issue: "No recordings found"

**Symptom:**
```
Exception: no recordings found
```

**Cause:** Receiver started before sender created any recordings.

**Solution:** Start sender first, wait for "Recording started" message, then start receiver.

#### Issue: "Archive is not connected"

**Symptom:**
```
ArchiveException: archive is not connected
```

**Cause:** Archive process not running or wrong control channel.

**Solution:**
```bash
# Check if archive directory exists
ls -la ./build/aeron-archive/

# Ensure embedded driver is enabled
-Dbridge.embedded.driver=true
```

#### Issue: "Publication not connected"

**Symptom:**
```
Offer failed: NOT_CONNECTED
```

**Cause:** No subscriber on the stream yet.

**Solution:** Start receiver before sender, or use reliable publication settings.

#### Issue: ReplayMerge timeout

**Symptom:**
```
TimeoutException: ReplayMerge no progress: state=CATCHUP
```

**Cause:** Replay couldn't catch up to live stream in time.

**Solution:**
- Increase merge timeout: `-Dbridge.merge.timeout.ms=30000`
- Reduce sender rate temporarily
- Check network/disk I/O

#### Issue: Checkpoint file corrupted

**Symptom:**
```
CheckpointException: checksum mismatch
```

**Solution:**
```bash
# Remove corrupted checkpoint
rm ./build/checkpoints/me_to_rms.checkpoint

# Receiver will replay from beginning
```

### Log Locations

| Component | Log Location | Notes |
|-----------|--------------|-------|
| Media Driver | `./build/aeron-archive/aeron-stat.log` | Use `AeronStat` to read |
| Archive | `./build/aeron-archive/archive.log` | Catalog and recordings |
| Checkpoints | `./build/checkpoints/` | Binary checkpoint files |
| Application | stdout/stderr | Configure with Log4j if needed |

### Diagnostic Commands

#### Check Aeron Counters

```bash
# View live counters
./gradlew :aeron-samples:runAeronStat
```

#### List Archive Recordings

```bash
./gradlew :aeron-samples:runArchiveList \
    -Dbridge.archive.dir=./build/aeron-archive
```

#### Inspect Checkpoint

```bash
# Using hexdump (macOS/Linux)
xxd ./build/checkpoints/me_to_rms.checkpoint
```

### Performance Tuning

#### For Lowest Latency

```bash
-Daeron.sample.idleStrategy=org.agrona.concurrent.BusySpinIdleStrategy
-Daeron.threading.mode=DEDICATED
-Daeron.sender.idle.strategy=noop
-Daeron.receiver.idle.strategy=noop
```

#### For Lower CPU Usage

```bash
-Daeron.sample.idleStrategy=org.agrona.concurrent.BackoffIdleStrategy
-Daeron.threading.mode=SHARED
```

## Cleanup

### Remove All State

```bash
# Remove archive recordings
rm -rf ./build/aeron-archive/

# Remove checkpoints
rm -rf ./build/checkpoints/

# Remove Aeron directories
rm -rf /dev/shm/aeron-*  # Linux
rm -rf /tmp/aeron-*      # macOS
```

### Graceful Shutdown

Always use `Ctrl+C` to allow clean shutdown. The shutdown hook will:
1. Stop recording
2. Flush checkpoint
3. Close publications/subscriptions
4. Shutdown media driver

## Integration Tests

### Run Bridge Integration Tests

```bash
./gradlew :aeron-samples:test --tests "*BridgeTest*"
```

### Run with Verbose Output

```bash
./gradlew :aeron-samples:test --tests "*BridgeTest*" --info
```

## Quick Reference

### Start Commands (Copy-Paste Ready)

**Sender (ME → RMS):**
```bash
./gradlew :aeron-samples:runBridgeSender -Dbridge.direction=ME_TO_RMS
```

**Receiver (ME → RMS):**
```bash
./gradlew :aeron-samples:runBridgeReceiver -Dbridge.direction=ME_TO_RMS
```

**Sender (RMS → ME):**
```bash
./gradlew :aeron-samples:runBridgeSender -Dbridge.direction=RMS_TO_ME -Dbridge.stream.id=2002
```

**Receiver (RMS → ME):**
```bash
./gradlew :aeron-samples:runBridgeReceiver -Dbridge.direction=RMS_TO_ME -Dbridge.stream.id=2002
```

**Verifier:**
```bash
./gradlew :aeron-samples:runBridgeVerifier
```
