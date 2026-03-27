# Aeron Cluster — C Implementation

This directory contains the native C implementation of Aeron Cluster, mirroring the
Java packages as three distinct layers:

| Layer | Directory | Library target |
|-------|-----------|----------------|
| Client | `client/` | `aeron_cluster_c_client` |
| Service | `service/` | `aeron_cluster_c_service` |
| Consensus Module | `consensus/` | `aeron_cluster_c_consensus` |

See `client/README.md` for the client API usage guide.

---

## Architecture

```
External clients
       │  ingress (UDP/IPC)
       ▼
┌─────────────────────────────────────────────┐
│           ConsensusModuleAgent (C)           │  ← main duty-cycle
│                                             │
│  ┌──────────────┐  ┌─────────────────────┐ │
│  │ IngressAdapter│  │  ConsensusAdapter   │ │  ← receive
│  │ (client msgs) │  │  (peer msgs)        │ │
│  └──────┬───────┘  └──────────┬──────────┘ │
│         │                     │             │
│  ┌──────▼──────────────────┐  │             │
│  │      Election (C)        │  │             │
│  │  18-state Raft variant   │◄─┘             │
│  └──────┬───────────────────┘               │
│         │                                   │
│  ┌──────▼───────┐  ┌──────────────────────┐ │
│  │  LogPublisher│  │  EgressPublisher      │ │  ← send
│  │  (log writes)│  │  (client responses)   │ │
│  └──────┬───────┘  └──────────────────────┘ │
│         │                                   │
│  ┌──────▼──────────────────────────────────┐ │
│  │        SessionManager                   │ │
│  │  ClusterSession[] + TimerService        │ │
│  └─────────────────────────────────────────┘ │
│                                             │
│  ┌─────────────────────────────────────────┐ │
│  │   RecordingLog  (recording.log on disk) │ │  ← persistent state
│  └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
         │  IPC (service channel)
         ▼
   ClusteredServiceAgent (C)   ← service/
```

---

## SBE Message Reference

### Client ↔ cluster
| ID | Message | Direction |
|----|---------|-----------|
| 1 | `SessionMessageHeader` | cluster→client (egress app msg) |
| 2 | `SessionEvent` | cluster→client (connect/error) |
| 3 | `SessionConnectRequest` | client→cluster |
| 4 | `SessionCloseRequest` | client→cluster |
| 5 | `SessionKeepAlive` | client→cluster |
| 6 | `NewLeaderEvent` | cluster→client |
| 7 | `Challenge` | cluster→client (auth) |
| 8 | `ChallengeResponse` | client→cluster (auth reply) |
| 26 | `AdminRequest` | client→cluster |
| 27 | `AdminResponse` | cluster→client |

### CM ↔ service (log and IPC)
| ID | Message | Direction |
|----|---------|-----------|
| 1 | `SessionMessageHeader` | log: app message |
| 20 | `TimerEvent` | log: CM→service |
| 21 | `SessionOpenEvent` | log: CM→service |
| 22 | `SessionCloseEvent` | log: CM→service |
| 23 | `ClusterActionRequest` | log: CM→service (snapshot) |
| 24 | `NewLeadershipTermEvent` | log: CM→service |
| 25 | `MembershipChangeEvent` | log: CM→service |
| 30 | `CloseSession` | IPC: service→CM |
| 31 | `ScheduleTimer` | IPC: service→CM |
| 32 | `CancelTimer` | IPC: service→CM |
| 33 | `ServiceAck` | IPC: service→CM |
| 40 | `JoinLog` | IPC: CM→service |
| 42 | `ServiceTerminationPosition` | IPC: CM→service |
| 108 | `RequestServiceAck` | IPC: CM→service |

### Consensus (inter-node)
| ID | Message |
|----|---------|
| 50 | `CanvassPosition` |
| 51 | `RequestVote` |
| 52 | `Vote` |
| 53 | `NewLeadershipTerm` |
| 54 | `AppendPosition` |
| 55 | `CommitPosition` |
| 56 | `CatchupPosition` |
| 57 | `StopCatchup` |
| 74–81 | Join/Termination/Backup/Heartbeat messages |

### Snapshot codecs
| ID | Message |
|----|---------|
| 100 | `SnapshotMarker` (BEGIN/END) |
| 102 | `ClientSession` (service snapshot) |
| 103 | `ClusterSession` (CM snapshot) |
| 104 | `Timer` |
| 105 | `ConsensusModule` state |
| 106 | `ClusterMembers` |
| 107 | `PendingMessageTracker` |

---

## RecordingLog Binary Format

`recording.log` lives in `clusterDir`. Each entry is fixed at 4096 bytes.

```
Offset  Size  Field
     0     8  recordingId         (int64, -1 if invalid)
     8     8  leadershipTermId    (int64)
    16     8  termBaseLogPosition (int64)
    24     8  logPosition         (int64, -1 for open term)
    32     8  timestamp           (int64)
    40     4  serviceId           (int32, -1 for term entries)
    44     4  entryType           (int32)
                                    0 = TERM
                                    1 = SNAPSHOT
                                    2 = STANDBY_SNAPSHOT
                                    bit 31 = INVALID_FLAG
```

---

## Key Implementation Notes

- **RecordingLog is memory-mapped** (`mmap`), not read into heap. This matches the
  Java `MappedByteBuffer` and ensures consistent atomic position updates.

- **Log position vs term base position**: `leadershipTermId` restarts at each new
  election. `termBaseLogPosition` is the absolute log position where the current
  term started. Always track both.

- **Quorum uses ranked positions, not a simple count**: positions are sorted
  descending and the (n/2)-th is taken. A timed-out follower (position = -1)
  naturally loses the sort.

- **Archive is required**: the CM uses `aeron_archive` to record the log and replay
  it for recovering followers. The CM opens a local IPC archive connection.

- **Log channel must use an exclusive publication** with a fixed `sessionId`.
  Followers receive the same session ID in `JoinLog`.

- **`service_count` must match deployed services**: the CM waits for `ServiceAck`
  from every service index. A mismatch will stall indefinitely.

- **BoundedLogAdapter must never read past `commitPosition`**: pass the current
  counter value on every poll; abort the fragment if it would cross the boundary.

- **SnapshotMarker BEGIN/END are mandatory**: the snapshot loader uses them as
  boundaries; missing either marker causes recovery to fail.
