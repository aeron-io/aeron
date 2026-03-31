# Aeron Archive — C Server Implementation

This directory contains the native C implementation of the Aeron Archive Server, enabling a fully native C deployment without any Java dependency.

## Architecture

```
aeron-archive/src/main/c/
├── client/          # Archive Client (upstream, already shipped)
└── server/          # Archive Server (new — this implementation)
    ├── Catalog           Recording metadata storage (mmap'd file)
    ├── RecordingWriter   Write publication fragments to segment files
    ├── RecordingReader   Read segment files for replay
    ├── RecordingSession  Per-stream recording state machine
    ├── ReplaySession     Per-request replay state machine
    ├── Conductor         Main agent — drives all sessions
    ├── ControlSession    Per-client connection state machine
    ├── ControlAdapter    SBE request decoder/dispatcher
    ├── ResponseProxy     SBE response encoder
    ├── ReplicationSession  Cross-archive recording replication
    ├── Server            Context, conclude, launch lifecycle
    ├── MarkFile          Liveness detection (mmap'd)
    ├── ArchivingMediaDriver  Bundles aeronmd + archive in one process
    ├── DedicatedConductor  Multi-threaded mode
    ├── SessionWorker     Generic session driver for worker threads
    └── Tool              Catalog inspection and verification
```

## Key Components

| Component | Files | Description |
|-----------|-------|-------------|
| **Catalog** | `aeron_archive_catalog.c/h` | Memory-mapped `archive.catalog` file. Stores recording descriptors in 1024-byte aligned slots. Supports add, find, update, invalidate, iterate. |
| **RecordingWriter** | `aeron_archive_recording_writer.c/h` | Writes Aeron data frames to segment files (`<id>-<pos>.rec`). Handles segment rollover at configurable boundaries. |
| **RecordingReader** | `aeron_archive_recording_reader.c/h` | Reads recorded data back from segment files via mmap. Used by ReplaySession. |
| **Conductor** | `aeron_archive_conductor.c/h` | Main duty-cycle agent. Polls control requests, drives recording/replay/replication sessions. |
| **ControlSession** | `aeron_archive_control_session.c/h` | Per-client state machine (INIT→CONNECTED→AUTHENTICATED→ACTIVE). Processes one request at a time. |
| **ReplaySession** | `aeron_archive_replay_session.c/h` | Reads from segment files and publishes data back to clients via exclusive publication. |
| **ReplicationSession** | `aeron_archive_replication_session.c/h` | Replicates recordings from a source archive. Used by cluster for follower catchup. |
| **Server** | `aeron_archive_server.c/h` | Context configuration, validation (`conclude`), and lifecycle (`launch`/`close`). |
| **ArchivingMediaDriver** | `aeron_archiving_media_driver.c/h` | Launches `aeronmd` + archive server in a single C process. |

## Usage

### Launching a C ArchivingMediaDriver

```c
#include "server/aeron_archiving_media_driver.h"

aeron_archiving_media_driver_t *driver = NULL;

/* Launch driver + archive in one process */
if (aeron_archiving_media_driver_launch(&driver, "/tmp/aeron", "/tmp/archive") < 0)
{
    fprintf(stderr, "launch failed: %s\n", aeron_errmsg());
    return -1;
}

/* Drive the duty cycle */
while (running)
{
    aeron_archiving_media_driver_do_work(driver);
}

aeron_archiving_media_driver_close(driver);
```

### Using with Aeron Cluster (full native stack)

```c
/* 1. Launch C ArchivingMediaDriver */
aeron_archiving_media_driver_t *amd = NULL;
aeron_archiving_media_driver_launch(&amd, aeron_dir, archive_dir);

/* 2. Connect Aeron client */
aeron_context_t *aeron_ctx = NULL;
aeron_t *aeron = NULL;
aeron_context_init(&aeron_ctx);
aeron_context_set_dir(aeron_ctx, aeron_dir);
aeron_init(&aeron, aeron_ctx);
aeron_start(aeron);

/* 3. Connect Archive client */
aeron_archive_context_t *arch_ctx = NULL;
aeron_archive_t *archive = NULL;
aeron_archive_context_init(&arch_ctx);
aeron_archive_context_set_aeron(arch_ctx, aeron);
aeron_archive_connect(&archive, arch_ctx);

/* 4. Start ConsensusModule with archive */
aeron_cm_context_t cm_ctx = {};
cm_ctx.aeron = aeron;
cm_ctx.archive_ctx = arch_ctx;
/* ... configure cluster_members, channels, etc. ... */

/* All components run in pure C — no Java process needed */
```

## Binary Compatibility

- **Catalog format**: Binary-compatible with Java's `archive.catalog` file
- **Segment files**: Same `<recording_id>-<segment_base_position>.rec` naming and frame layout
- **SBE protocol**: Same wire format for control requests/responses
- **Mark file**: Same liveness detection format

A C archive can read catalogs and segments written by a Java archive, and vice versa.

## Testing

| Test Suite | Tests | Description |
|------------|-------|-------------|
| `aeron_archive_catalog_test` | 97 | Catalog CRUD, index, persistence, capacity |
| `aeron_archive_recording_test` | 23 | Writer, reader, session state machine |
| `aeron_archive_replay_test` | 26 | Replay session, list recordings (all/uri/id) |
| `aeron_archive_context_test` | 98 | Server context, mark file, control session, SBE encoding |
| `aeron_archive_conductor_test` | 10 | Conductor lifecycle, delete segments |
| **Total** | **254** | |

```bash
# Build and run from the CMake build directory
cmake --build . --target aeron_archive_catalog_test
ctest -R aeron_archive_catalog_test --output-on-failure

# Run all archive server tests
ctest -R "aeron_archive_(catalog|recording|replay|context|conductor)_test" --output-on-failure
```

## Threading Modes

| Mode | Description |
|------|-------------|
| **Shared** (default) | Single thread drives conductor + all sessions. Lowest latency for light workloads. |
| **Dedicated** | Separate threads for recording and replay. Higher throughput under concurrent load. |
| **Invoker** | No background threads. Caller drives via `do_work()`. Used by ArchivingMediaDriver. |
