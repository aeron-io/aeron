# Aeron Cluster C — Complete Implementation Guide

This is the top-level guide for implementing a **fully native C cluster node** —
no Java runtime required.  The three sub-guides map to the three Java packages:

| Package | Guide | Status |
|---------|-------|--------|
| `cluster/client/` | `client/IMPLEMENTATION_GUIDE.md` | C done, C++ done |
| `cluster/service/` | `service/SERVICE_IMPLEMENTATION_GUIDE.md` | Guide done, C partial |
| **`cluster/`** (Consensus Module) | **this file** | Guide only |

**The Consensus Module is the cluster itself** — leader election, log replication,
session management, snapshots, and node lifecycle.  Without it you can only write
clients and services that connect to a Java-run cluster.  A pure-C cluster node
requires all three layers.

---

## 1. Architecture

```
External clients
       │  ingress (UDP/IPC)
       ▼
┌─────────────────────────────────────────────┐
│           ConsensusModuleAgent (C)           │  ← main duty-cycle
│                                             │
│  ┌──────────────┐  ┌─────────────────────┐ │
│  │  IngressAdapter│  │  ConsensusAdapter   │ │  ← receive
│  │  (client msgs)│  │  (peer msgs)        │ │
│  └──────┬───────┘  └──────────┬──────────┘ │
│         │                     │             │
│  ┌──────▼───────────────────┐ │             │
│  │      Election (C)        │ │             │
│  │  18-state Raft variant   │◄┘             │
│  └──────┬───────────────────┘               │
│         │                                   │
│  ┌──────▼───────┐  ┌──────────────────────┐│
│  │  LogPublisher│  │  EgressPublisher      ││  ← send
│  │  (log writes)│  │  (client responses)   ││
│  └──────┬───────┘  └──────────────────────┘│
│         │                                   │
│  ┌──────▼──────────────────────────────────┐│
│  │        SessionManager                   ││
│  │  ClusterSession[] + TimerService        ││
│  └─────────────────────────────────────────┘│
│                                             │
│  ┌─────────────────────────────────────────┐│
│  │   RecordingLog  (recording.log on disk) ││  ← persistent state
│  └─────────────────────────────────────────┘│
└─────────────────────────────────────────────┘
         │  IPC (service channel)
         ▼
   ClusteredServiceAgent (C) ← service/SERVICE_IMPLEMENTATION_GUIDE.md
```

---

## 2. Full file structure

```
aeron-cluster/src/main/c/
├── client/                          (done)
├── service/                         (partial)
└── consensus/
    ├── CMakeLists.txt
    │
    │  ── Configuration & context ──
    ├── aeron_consensus_module_configuration.h   ← all CM constants & env vars
    ├── aeron_consensus_module_context.h/.c      ← ConsensusModule.Context
    │
    │  ── Persistent state ──
    ├── aeron_cluster_recording_log.h/.c         ← RecordingLog (binary file)
    ├── aeron_cluster_member.h/.c                ← ClusterMember (peer state)
    │
    │  ── Publisher/adapter pairs ──
    ├── aeron_cluster_consensus_publisher.h/.c   ← ConsensusPublisher (peer msgs out)
    ├── aeron_cluster_consensus_adapter.h/.c     ← ConsensusAdapter (peer msgs in)
    ├── aeron_cluster_ingress_adapter_cm.h/.c    ← IngressAdapter (client msgs in)
    ├── aeron_cluster_egress_publisher.h/.c      ← EgressPublisher (client msgs out)
    ├── aeron_cluster_log_publisher.h/.c         ← LogPublisher (log writes, leader only)
    ├── aeron_cluster_log_adapter_cm.h/.c        ← LogAdapter (log replay, followers)
    ├── aeron_cluster_service_proxy_cm.h/.c      ← ServiceProxy (CM→service channel)
    │
    │  ── Session management ──
    ├── aeron_cluster_cluster_session.h/.c       ← ClusterSession (server-side)
    ├── aeron_cluster_session_manager.h/.c       ← SessionManager
    ├── aeron_cluster_timer_service.h/.c         ← TimerService (priority heap)
    ├── aeron_cluster_pending_message_tracker.h/.c  ← PendingServiceMessageTracker
    │
    │  ── Snapshot ──
    ├── aeron_cluster_cm_snapshot_taker.h/.c     ← ConsensusModuleSnapshotTaker
    ├── aeron_cluster_cm_snapshot_loader.h/.c    ← ConsensusModuleSnapshotAdapter
    │
    │  ── Election ──
    ├── aeron_cluster_election.h/.c              ← Election (18-state machine)
    │
    │  ── Main agent ──
    └── aeron_consensus_module_agent.h/.c        ← ConsensusModuleAgent (3600-line Java equiv)

aeron-cluster/src/main/cpp_wrapper/consensus/
├── CMakeLists.txt
├── ConsensusModule.h                ← C++ wrapper / container
└── ConsensusModuleContext.h         ← C++ Context

aeron-cluster/src/test/c/consensus/
├── CMakeLists.txt
└── ClusterTest.cpp

aeron-cluster/src/test/cpp_wrapper/consensus/
├── CMakeLists.txt
└── ClusterWrapperTest.cpp
```

Library target: **`aeron_cluster_c_consensus`** (links `aeron`, `aeron_cluster_c_client`,
`aeron_cluster_c_service`).

---

## 3. Template IDs — complete reference

### Client ↔ cluster (shared with client layer)
| ID | Message | Direction |
|----|---------|-----------|
| 1 | `SessionMessageHeader` | cluster→client (egress app msg) |
| 2 | `SessionEvent` | cluster→client (connect/error) |
| 3 | `SessionConnectRequest` | client→cluster (connect) |
| 4 | `SessionCloseRequest` | client→cluster (disconnect) |
| 5 | `SessionKeepAlive` | client→cluster |
| 6 | `NewLeaderEvent` | cluster→client (leader change) |
| 7 | `Challenge` | cluster→client (auth) |
| 8 | `ChallengeResponse` | client→cluster (auth reply) |
| 26 | `AdminRequest` | client→cluster (admin) |
| 27 | `AdminResponse` | cluster→client |

### CM ↔ service (shared with service layer)
| ID | Message | Direction |
|----|---------|-----------|
| 20–25 | Timer/Session/Action events | CM→service (via log) |
| 30–33 | CloseSession/Timer/Ack | service→CM |
| 40 | `JoinLog` | CM→service |
| 42 | `ServiceTerminationPosition` | CM→service |
| 108 | `requestServiceAck` | CM→service |

### Consensus (inter-node) — **new to CM layer**
| ID | Message | Direction |
|----|---------|-----------|
| 50 | `CanvassPosition` | peer→peer (election) |
| 51 | `RequestVote` | candidate→peer |
| 52 | `Vote` | peer→candidate |
| 53 | `NewLeadershipTerm` | leader→followers |
| 54 | `AppendPosition` | follower→leader |
| 55 | `CommitPosition` | leader→followers |
| 56 | `CatchupPosition` | follower→leader (request catchup) |
| 57 | `StopCatchup` | leader→follower |
| 74 | `JoinCluster` | passive→leader (join request) |
| 75 | `TerminationPosition` | leader→followers |
| 76 | `TerminationAck` | follower→leader |
| 77 | `BackupQuery` | backup→leader |
| 78 | `BackupResponse` | leader→backup |
| 79 | `HeartbeatRequest` | follower→leader |
| 80 | `HeartbeatResponse` | leader→follower |
| 81 | `StandbySnapshot` | any→leader |

### Cluster administration
| ID | Message |
|----|---------|
| 34 | `ClusterMembersQuery` |
| 41 | `ClusterMembersResponse` |
| 43 | `ClusterMembersExtendedResponse` |
| 70 | `AddPassiveMember` |
| 71 | `ClusterMembersChange` |
| 72 | `SnapshotRecordingQuery` |
| 73 | `SnapshotRecordings` |

### Snapshot codec IDs
| ID | Message |
|----|---------|
| 100 | `SnapshotMarker` |
| 102 | `ClientSession` (service-side) |
| 103 | `ClusterSession` (CM-side) |
| 104 | `Timer` |
| 105 | `ConsensusModule` (CM state) |
| 106 | `ClusterMembers` |
| 107 | `PendingMessageTracker` |

---

## 4. Key constants and defaults

```c
/* aeron_consensus_module_configuration.h */

/* Protocol version */
#define AERON_CM_PROTOCOL_MAJOR_VERSION  1
#define AERON_CM_PROTOCOL_MINOR_VERSION  0
#define AERON_CM_PROTOCOL_PATCH_VERSION  0

/* Snapshot type IDs */
#define AERON_CM_SNAPSHOT_TYPE_ID        INT64_C(1)  /* CM snapshot */
#define AERON_SVC_SNAPSHOT_TYPE_ID       INT64_C(2)  /* service snapshot */

/* Stream IDs (defaults) */
#define AERON_CM_LOG_STREAM_ID_DEFAULT              100
#define AERON_CM_CONSENSUS_MODULE_STREAM_ID_DEFAULT 104  /* CM←service */
#define AERON_CM_SERVICE_STREAM_ID_DEFAULT          105  /* CM→service */
#define AERON_CM_SNAPSHOT_STREAM_ID_DEFAULT         107
#define AERON_CM_CONSENSUS_STREAM_ID_DEFAULT        108  /* inter-node */
#define AERON_CM_INGRESS_STREAM_ID_DEFAULT          101

/* Timeouts */
#define AERON_CM_SESSION_TIMEOUT_NS_DEFAULT         (INT64_C(10)  * 1000000000)
#define AERON_CM_LEADER_HEARTBEAT_TIMEOUT_NS_DEFAULT (INT64_C(10) * 1000000000)
#define AERON_CM_LEADER_HEARTBEAT_INTERVAL_NS_DEFAULT (INT64_C(200) * 1000000)

/* Member defaults */
#define AERON_CM_MEMBER_ID_DEFAULT          0
#define AERON_CM_APPOINTED_LEADER_DEFAULT   -1  /* NULL_VALUE = no appointed leader */

/* Log channel */
#define AERON_CM_LOG_CHANNEL_DEFAULT    "aeron:udp?term-length=64m"

/* Ingress fragment limit */
#define AERON_CM_INGRESS_FRAGMENT_LIMIT_DEFAULT 50

/* Election state codes (matches Java ElectionState enum) */
typedef enum aeron_cluster_election_state_en
{
    AERON_ELECTION_INIT                  = 0,
    AERON_ELECTION_CANVASS               = 1,
    AERON_ELECTION_NOMINATE              = 2,
    AERON_ELECTION_CANDIDATE_BALLOT      = 3,
    AERON_ELECTION_FOLLOWER_BALLOT       = 4,
    AERON_ELECTION_LEADER_LOG_REPLICATION = 5,
    AERON_ELECTION_LEADER_REPLAY         = 6,
    AERON_ELECTION_LEADER_INIT           = 7,
    AERON_ELECTION_LEADER_READY          = 8,
    AERON_ELECTION_FOLLOWER_LOG_REPLICATION = 9,
    AERON_ELECTION_FOLLOWER_REPLAY       = 10,
    AERON_ELECTION_FOLLOWER_CATCHUP_INIT = 11,
    AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT = 12,
    AERON_ELECTION_FOLLOWER_CATCHUP      = 13,
    AERON_ELECTION_FOLLOWER_LOG_INIT     = 14,
    AERON_ELECTION_FOLLOWER_LOG_AWAIT    = 15,
    AERON_ELECTION_FOLLOWER_READY        = 16,
    AERON_ELECTION_CLOSED                = 17,
}
aeron_cluster_election_state_t;

/* ConsensusModule states */
typedef enum aeron_cm_state_en
{
    AERON_CM_STATE_INIT      = 0,
    AERON_CM_STATE_ACTIVE    = 1,
    AERON_CM_STATE_SUSPENDED = 2,
    AERON_CM_STATE_SNAPSHOT  = 3,
    AERON_CM_STATE_QUORUM_SNAPSHOT = 4,
    AERON_CM_STATE_LEAVING   = 5,
    AERON_CM_STATE_CLOSED    = 6,
}
aeron_cm_state_t;
```

---

## 5. RecordingLog binary format

`recording.log` lives in `clusterDir`.  Each entry is fixed at `MAX_ENTRY_LENGTH = 4096` bytes.

```
Offset  Size  Field
     0     8  recordingId       (int64, -1 if invalid)
     8     8  leadershipTermId  (int64)
    16     8  termBaseLogPosition (int64)
    24     8  logPosition       (int64, -1 for open term)
    32     8  timestamp         (int64)
    40     4  serviceId         (int32, -1 for term entries)
    44     4  entryType         (int32)
                                  0 = TERM (leadership term)
                                  1 = SNAPSHOT (service snapshot)
                                  2 = STANDBY_SNAPSHOT
                                  bit 31 set = INVALID_FLAG
    48  variable  endpoint      (varies, not used for snapshots)
```

**C struct:**
```c
typedef struct aeron_cluster_recording_log_entry_stct
{
    int64_t recording_id;
    int64_t leadership_term_id;
    int64_t term_base_log_position;
    int64_t log_position;
    int64_t timestamp;
    int32_t service_id;
    int32_t entry_type;
}
aeron_cluster_recording_log_entry_t;

#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM             0
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT         1
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT 2
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG     (1 << 31)
#define AERON_CLUSTER_RECORDING_LOG_MAX_ENTRY_LENGTH            4096
```

**Key RecordingLog operations (in implementation order):**
```c
int  aeron_cluster_recording_log_open(recording_log_t **log, const char *cluster_dir, bool create_new);
void aeron_cluster_recording_log_close(recording_log_t *log);
int  aeron_cluster_recording_log_append_term(recording_log_t *log, int64_t recording_id,
         int64_t leadership_term_id, int64_t term_base_log_position, int64_t timestamp);
int  aeron_cluster_recording_log_append_snapshot(recording_log_t *log, int64_t recording_id,
         int64_t leadership_term_id, int64_t term_base_log_position,
         int64_t log_position, int64_t timestamp, int32_t service_id);
int  aeron_cluster_recording_log_commit_log_position(recording_log_t *log,
         int64_t leadership_term_id, int64_t log_position);
recording_log_entry_t *aeron_cluster_recording_log_find_last_term(recording_log_t *log);
recording_log_entry_t *aeron_cluster_recording_log_get_latest_snapshot(recording_log_t *log,
         int32_t service_id);
recovery_plan_t *aeron_cluster_recording_log_create_recovery_plan(recording_log_t *log,
         int service_count);
```

---

## 6. ClusterMember — peer state

```c
struct aeron_cluster_member_stct
{
    int32_t  id;
    bool     is_leader;
    bool     is_catchup_committed;
    int64_t  log_position;
    int64_t  leadership_term_id;
    int64_t  candidate_term_id;
    int64_t  change_correlation_id;
    int64_t  time_of_last_append_position_ns;
    int64_t  term_position;

    char    *endpoint;         /* "host:port" */
    char    *consensus_endpoint;
    char    *log_endpoint;
    char    *catchup_endpoint;
    char    *archive_endpoint;

    aeron_exclusive_publication_t *publication;  /* consensus channel to this peer */
};

/* Quorum threshold: floor(n/2) + 1 but stored as n - floor(n/2) for ranked array */
static inline int aeron_cluster_member_quorum_threshold(int member_count)
{
    return member_count / 2 + 1;
}

/* Compute quorum position: sort positions descending, take position at quorum_threshold index */
int64_t aeron_cluster_member_quorum_position(
    aeron_cluster_member_t **members, int member_count,
    int64_t *ranked_positions, int64_t now_ns, int64_t heartbeat_timeout_ns);
```

---

## 7. Election state machine

The election is a separate struct driven by `ConsensusModuleAgent.doWork()` while
`agent->election != NULL`.

```
INIT ──────────────────────► CANVASS ─────────────────► NOMINATE
                              │                             │
                              │ (received votes, leader)    │ (enough votes)
                              ▼                             ▼
                         FOLLOWER_BALLOT          CANDIDATE_BALLOT
                              │                             │
                              │ (leader identified)         │ (won)
                              ▼                             ▼
                         FOLLOWER_LOG_REPLICATION   LEADER_LOG_REPLICATION
                              │                             │
                              ▼                             ▼
                         FOLLOWER_REPLAY            LEADER_REPLAY
                              │                             │
                              ▼                             ▼
                         FOLLOWER_CATCHUP_INIT       LEADER_INIT
                              │                             │
                              ▼                             ▼
                         FOLLOWER_CATCHUP_AWAIT      LEADER_READY ── CLOSED
                              │
                              ▼
                         FOLLOWER_CATCHUP
                              │
                              ▼
                         FOLLOWER_LOG_INIT
                              │
                              ▼
                         FOLLOWER_LOG_AWAIT
                              │
                              ▼
                         FOLLOWER_READY ──────────────────── CLOSED
```

**Key election messages sent/received per state:**

| State | Sends | Receives |
|-------|-------|---------|
| CANVASS | `CanvassPosition` | `CanvassPosition` from peers |
| NOMINATE | `RequestVote` | `Vote` from peers |
| CANDIDATE_BALLOT | — | `Vote` (counting) |
| FOLLOWER_BALLOT | `Vote` | — |
| LEADER_READY | `NewLeadershipTerm` | `AppendPosition` from followers |
| LEADER_REPLAY/INIT | `CommitPosition` | `AppendPosition` |
| FOLLOWER_CATCHUP | `CatchupPosition` | `StopCatchup` from leader |
| FOLLOWER_LOG_AWAIT | — | `NewLeadershipTerm` from leader |
| FOLLOWER_READY | `AppendPosition` | — |

---

## 8. ConsensusModuleAgent — struct and duty cycle

```c
struct aeron_consensus_module_agent_stct
{
    /* Context and identity */
    aeron_cm_context_t              *ctx;
    aeron_t                         *aeron;
    int32_t                          member_id;
    aeron_cm_state_t                 state;
    aeron_cluster_role_t             role;

    /* Timing */
    int64_t  leader_heartbeat_interval_ns;
    int64_t  leader_heartbeat_timeout_ns;
    int64_t  time_of_last_log_update_ns;
    int64_t  time_of_last_append_position_update_ns;
    int64_t  slow_tick_deadline_ns;

    /* Log tracking */
    int64_t  leadership_term_id;
    int64_t  expected_ack_position;
    int64_t  service_ack_id;
    int64_t  last_append_position;
    int64_t  notified_commit_position;
    int64_t  termination_position;
    int64_t  log_subscription_id;
    int64_t  log_recording_id;

    /* Counters (read from aeron counters map) */
    aeron_counter_t  *commit_position_counter;
    aeron_counter_t  *cluster_role_counter;
    aeron_counter_t  *module_state_counter;

    /* Members */
    aeron_cluster_member_t **active_members;
    int                       active_member_count;
    aeron_cluster_member_t   *this_member;
    aeron_cluster_member_t   *leader_member;
    int64_t                  *ranked_positions;  /* size = quorum_threshold */

    /* Components */
    aeron_cluster_ingress_adapter_t      *ingress_adapter;
    aeron_cluster_egress_publisher_t     *egress_publisher;
    aeron_cluster_log_publisher_t        *log_publisher;
    aeron_cluster_log_adapter_cm_t       *log_adapter;        /* NULL when leader */
    aeron_cluster_consensus_adapter_t    *consensus_adapter;
    aeron_cluster_consensus_publisher_t  *consensus_publisher;
    aeron_cluster_service_proxy_cm_t     *service_proxy;
    aeron_cluster_session_manager_t      *session_manager;
    aeron_cluster_timer_service_t        *timer_service;
    aeron_cluster_pending_message_tracker_t *pending_message_trackers; /* array[serviceCount] */
    aeron_cluster_recording_log_t        *recording_log;
    aeron_cluster_election_t             *election;   /* NULL when not in election */

    /* Archive (for recording log and snapshots) */
    aeron_archive_t                      *archive;

    /* Service ack queues */
    service_ack_queue_t                 **service_ack_queues;  /* one per service */
    int                                   service_count;
    int64_t                              *service_client_ids;
};
```

**`do_work()` structure** (mirrors Java `ConsensusModuleAgent.doWork()`):

```c
int aeron_consensus_module_agent_do_work(aeron_consensus_module_agent_t *agent,
                                          int64_t now_ns)
{
    int work_count = 0;

    /* 1. Slow tick (every 1ms): heartbeat check, mark file, counters */
    if (now_ns >= agent->slow_tick_deadline_ns)
    {
        work_count += slow_tick_work(agent, now_ns);
        agent->slow_tick_deadline_ns = now_ns + 1000000LL; /* 1ms */
    }

    /* 2. Election drives everything when active */
    if (NULL != agent->election)
    {
        work_count += aeron_cluster_election_do_work(agent->election, now_ns);
        return work_count;
    }

    /* 3. Poll ingress (client connections and messages) */
    work_count += aeron_cluster_ingress_adapter_poll(agent->ingress_adapter);

    /* 4. Consensus work (role-specific) */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role)
    {
        work_count += leader_consensus_work(agent, now_ns);
    }
    else
    {
        work_count += follower_consensus_work(agent, now_ns);
    }

    /* 5. Poll consensus channel (AppendPosition, CommitPosition, etc.) */
    work_count += aeron_cluster_consensus_adapter_poll(agent->consensus_adapter);

    /* 6. Poll service channel (ServiceAck, ScheduleTimer, etc.) */
    work_count += aeron_cluster_service_proxy_poll(agent->service_proxy);

    /* 7. Timer expiry */
    if (AERON_CLUSTER_ROLE_LEADER == agent->role)
    {
        work_count += check_session_timers(agent, now_ns);
    }

    return work_count;
}
```

**`leader_consensus_work`** (simplified):
```c
static int leader_consensus_work(aeron_consensus_module_agent_t *agent, int64_t now_ns)
{
    /* Publish CommitPosition if it advanced */
    int64_t quorum_pos = compute_quorum_position(agent, now_ns);
    if (quorum_pos > agent->notified_commit_position)
    {
        agent->commit_position_counter->value = quorum_pos;
        agent->notified_commit_position = quorum_pos;
        aeron_cluster_consensus_publisher_commit_position(
            agent->consensus_publisher,
            agent->leadership_term_id, quorum_pos, agent->member_id);
    }
    return 0;
}
```

---

## 9. LogPublisher and LogAdapter

**LogPublisher** (leader only): exclusive publication on the log channel.
- `log_position` tracks the end of the log.
- Every `SessionOpenEvent`, `SessionCloseEvent`, `ClusterActionRequest`,
  `NewLeadershipTermEvent`, and application message is appended here.
- **The log is also recorded by AeronArchive** — the leader passes the log
  channel to archive for recording.

**LogAdapter** (followers and replay): subscription on the log channel, reads
up to `commitPosition` (same bounded-read pattern as service layer).
- During replay: reads to `logReplayPosition`.
- During live: reads the live stream from the leader.

Both use the same SBE codec headers already generated for the service layer
(template IDs 1, 20–25).

---

## 10. Context fields

```c
struct aeron_cm_context_stct
{
    /* Identity */
    int32_t  member_id;            /* env: AERON_CLUSTER_MEMBER_ID */
    int32_t  appointed_leader_id;  /* env: AERON_CLUSTER_APPOINTED_LEADER_ID */
    int      service_count;        /* env: AERON_CLUSTER_SERVICE_COUNT */

    /* Channels and streams */
    char    *log_channel;             /* default: "aeron:udp?term-length=64m" */
    int32_t  log_stream_id;
    char    *ingress_channel;         /* default: "aeron:udp?endpoint=localhost:9010" */
    int32_t  ingress_stream_id;
    char    *consensus_channel;       /* inter-node, e.g. "aeron:udp?term-length=64m" */
    int32_t  consensus_stream_id;
    char    *control_channel;         /* CM↔service IPC */
    int32_t  consensus_module_stream_id;
    int32_t  service_stream_id;
    char    *snapshot_channel;
    int32_t  snapshot_stream_id;

    /* Member topology */
    char    *cluster_members;         /* "0,host:ingress:consensus:log:catchup:archive,...|1,..." */
    char    *cluster_consensus_endpoints; /* for passive/dynamic membership */
    char    *member_endpoints;

    /* Timeouts */
    int64_t  session_timeout_ns;
    int64_t  leader_heartbeat_timeout_ns;
    int64_t  leader_heartbeat_interval_ns;
    int64_t  startup_canvass_timeout_ns;
    int64_t  election_timeout_ns;
    int64_t  election_status_interval_ns;
    int64_t  termination_timeout_ns;

    /* Directories */
    char     cluster_dir[AERON_MAX_PATH];
    char     aeron_directory_name[AERON_MAX_PATH];

    /* Archive context (IPC, same host) */
    aeron_archive_context_t *archive_ctx;

    /* Aeron client */
    aeron_t *aeron;
    bool     owns_aeron_client;

    /* Callbacks */
    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    aeron_error_handler_t      error_handler;
    void                      *error_handler_clientd;

    /* Application version (for compatibility checks) */
    int32_t  app_version;

    /* Ingress */
    bool     is_ingress_ipc_allowed;
    int      ingress_fragment_limit;
};
```

**Parsing `cluster_members` string** (critical — done in `aeron_cluster_member.c`):

Format: `"memberId,ingressEndpoint:consensusEndpoint:logEndpoint:catchupEndpoint:archiveEndpoint|..."`

Example: `"0,localhost:20110:localhost:20111:localhost:20113:localhost:20114:localhost:8010|1,..."`

---

## 11. Implementation order

Work strictly bottom-up to avoid circular dependencies:

```
Phase 1 — Foundations (no I/O, easily unit-tested)
  Step 1   aeron_consensus_module_configuration.h    (constants, enums)
  Step 2   aeron_cluster_recording_log.h/.c          (binary file I/O)
  Step 3   aeron_cluster_member.h/.c                 (parse member string, quorum)
  Step 4   aeron_cluster_timer_service.h/.c          (priority heap, no I/O)

Phase 2 — Publishers and Adapters (each independently testable)
  Step 5   aeron_cluster_consensus_publisher.h/.c    (encode/send only)
  Step 6   aeron_cluster_egress_publisher.h/.c       (encode/send to clients)
  Step 7   aeron_cluster_log_publisher.h/.c          (encode/append to log)
  Step 8   aeron_cluster_consensus_adapter.h/.c      (decode inter-node msgs)
  Step 9   aeron_cluster_ingress_adapter_cm.h/.c     (decode client msgs)
  Step 10  aeron_cluster_log_adapter_cm.h/.c         (decode from log)

Phase 3 — Session management
  Step 11  aeron_cluster_cluster_session.h/.c        (server-side session)
  Step 12  aeron_cluster_session_manager.h/.c        (session lifecycle)
  Step 13  aeron_cluster_pending_message_tracker.h/.c
  Step 14  aeron_cluster_service_proxy_cm.h/.c       (CM→service channel)

Phase 4 — Snapshot
  Step 15  aeron_cluster_cm_snapshot_taker.h/.c
  Step 16  aeron_cluster_cm_snapshot_loader.h/.c

Phase 5 — Core logic
  Step 17  aeron_cluster_election.h/.c               (18-state machine — hardest)
  Step 18  aeron_cm_context.h/.c                     (conclude, validate)
  Step 19  aeron_consensus_module_agent.h/.c          (ties everything together)

Phase 6 — Wrapper + container
  Step 20  C++ ConsensusModule.h / ConsensusModuleContext.h

Phase 7 — Tests
  Step 21  Unit tests (recording_log, member parsing, quorum, election transitions)
  Step 22  System tests (3-node cluster: elect, replicate, snapshot, failover)
```

---

## 12. Key algorithms

### Quorum position
```c
int64_t compute_quorum_position(
    aeron_cluster_member_t **members, int count,
    int64_t *ranked, int64_t now_ns, int64_t heartbeat_timeout_ns)
{
    /* Fill ranked with log_position for members that have sent a
     * heartbeat within heartbeat_timeout_ns, or -1 for timed-out members */
    int threshold = count / 2;  /* index into sorted-descending array */

    /* Sort descending, return ranked[threshold] */
    /* This gives us the (count/2 + 1)-th largest position */
}
```

### Election: canvass timeout
```c
/* Delay before nominating: randomise to avoid split votes */
int64_t nomination_deadline = now_ns + random_in_range(
    ctx->election_timeout_ns,
    ctx->election_timeout_ns * 2);
```

### Heartbeat check
```c
/* In slowTickWork: if follower and no leader heartbeat received within timeout, enter election */
if (AERON_CLUSTER_ROLE_FOLLOWER == agent->role &&
    (now_ns - agent->time_of_last_log_update_ns) > agent->leader_heartbeat_timeout_ns)
{
    enter_election(agent, "leader heartbeat timeout");
}
```

---

## 13. Test requirements

Every Java `ConsensusModuleAgent` callback and `Election` state must have a C test.

**Unit tests** (`ClusterTest.cpp`):
```
RecordingLog:
  shouldAppendAndReloadTermEntry
  shouldAppendAndReloadSnapshotEntry
  shouldCommitLogPosition
  shouldInvalidateEntry
  shouldCreateRecoveryPlan

ClusterMember:
  shouldParseMembersString
  shouldComputeQuorumThreshold
  shouldComputeQuorumPosition (with timeout)

Election:
  shouldTransitionThroughLeaderPath
  shouldTransitionThroughFollowerPath
  shouldHandleSplitVote
  shouldHandleCanvassTimeout
```

**System tests** (3-node cluster, requires Java infrastructure):
```
shouldElectLeaderOnStartup
shouldReplicateLogFromLeaderToFollowers
shouldCommitAtQuorum
shouldTakeAndLoadSnapshot
shouldFailoverWhenLeaderDisconnects
shouldRejectSessionWhenNotLeader
shouldConnectClientAndSendMessage
shouldRouteClientMessageViaLog
shouldHandleTimerExpiry
shouldAddPassiveMember
```

---

## 14. Common pitfalls

1. **RecordingLog must be memory-mapped, not read into heap** — the Java
   implementation uses `MappedByteBuffer`; C should use `mmap()` for the same
   consistency guarantees and atomic position updates.

2. **Election quorum uses ranked positions, not a simple majority count** —
   the Java sorts positions and picks the (n/2)-th, so a timed-out follower
   (position = -1) naturally loses the sort.

3. **Log position vs term base position** — `leadershipTermId` restarts at
   each new election.  `termBaseLogPosition` is the absolute log position
   where the current term started.  Always track both.

4. **Archive is required** — the CM uses AeronArchive to record the log and
   replay it for recovering followers.  The C implementation must embed
   `aeron_cluster_c_client` (archive client) and open a local IPC archive
   connection.

5. **The log channel must be exclusive to the leader** — use an exclusive
   publication with a fixed `sessionId`; followers subscribe and receive the
   same session id in `JoinLog`.

6. **Service count must match** — the CM sends `JoinLog` to each service
   index.  If `service_count` doesn't match the deployed services, the CM
   will wait forever for ACKs.

7. **`CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT`** — standby snapshot is a
   separate snapshot taken when the node is not the leader.  The CM sets bit 0
   of the flags; service layer must check this flag before taking the snapshot.

8. **`extern "C"` guards** — all consensus headers need them (same issue
   as client layer, fixed in commit `81b5cfa`).
