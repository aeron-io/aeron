# Aeron Cluster C Service Layer — Implementation Guide

This document describes how to implement the Aeron Cluster **service-side** C layer
and its C++ wrapper.  The client-side guide lives at
`../client/IMPLEMENTATION_GUIDE.md`.

---

## 0. Architecture: where the service layer sits

```
User application                  Consensus Module (Java)
┌─────────────────────────────┐   ┌──────────────────────────────┐
│  ClusteredService (C impl)  │   │  Drives log & state machine  │
│    ↕ callbacks              │   └────────────┬─────────────────┘
│  ClusteredServiceAgent (C)  │                │ IPC pub/sub (two channels)
│    ├─ ServiceAdapter        │◄───────────────┤  controlChannel (CM→service)
│    │   (receives commands)  │                │  serviceChannel (service→CM)
│    ├─ BoundedLogAdapter     │◄───────────────┘  shared log (Aeron pub)
│    │   (replays log)        │
│    ├─ ConsensusModuleProxy  │──────────────────► serviceChannel (service→CM)
│    │   (scheduleTimer, ack) │
│    └─ ServiceSnapshotTaker/ │
│        Loader               │
└─────────────────────────────┘
```

**Two IPC channels** (both always `aeron:ipc`):

| Direction | Channel variable | Default stream |
|-----------|-----------------|----------------|
| CM → service | `controlChannel` | `consensusModuleStreamId = 104` |
| service → CM | `serviceChannel` | `serviceStreamId = 105` |

The **log** is a regular Aeron `Subscription` on a channel published by the CM.
`BoundedLogAdapter` reads it up to `commitPosition` (a counter), calling service
callbacks for each message type.

---

## 1. File structure

```
aeron-cluster/src/main/c/service/
├── CMakeLists.txt
├── aeron_cluster_service.h                ← ClusteredService callback interface
├── aeron_cluster_service_context.h/.c     ← container context (config + lifecycle)
├── aeron_cluster_client_session.h/.c      ← server-side client session handle
├── aeron_cluster_consensus_module_proxy.h/.c  ← sends to CM (scheduleTimer, ack, …)
├── aeron_cluster_service_adapter.h/.c     ← receives from CM subscription
├── aeron_cluster_bounded_log_adapter.h/.c ← reads replay/live log
├── aeron_cluster_service_snapshot_taker.h/.c  ← writes snapshot to publication
├── aeron_cluster_service_snapshot_loader.h/.c ← reads snapshot from image
└── aeron_clustered_service_agent.h/.c     ← main agent: implements Cluster API

aeron-cluster/src/main/cpp_wrapper/service/
├── CMakeLists.txt
├── ClusteredService.h                     ← pure-virtual base class for user services
└── ClusteredServiceContainer.h            ← C++ container wrapper

aeron-cluster/src/test/c/service/
└── CMakeLists.txt                         ← test registration

aeron-cluster/src/test/cpp_wrapper/service/
├── CMakeLists.txt
└── ClusteredServiceWrapperTest.cpp
```

The library target is `aeron_cluster_c_service` (separate from the client's
`aeron_cluster_c_client`).  They share the same generated SBE codecs.

---

## 2. Key constants (template IDs — verified against aeron-cluster-codecs.xml)

```c
/* Log messages — decoded by BoundedLogAdapter */
#define AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID  1   /* app msg */
#define AERON_CLUSTER_TIMER_EVENT_TEMPLATE_ID             20
#define AERON_CLUSTER_SESSION_OPEN_EVENT_TEMPLATE_ID      21
#define AERON_CLUSTER_SESSION_CLOSE_EVENT_TEMPLATE_ID     22
#define AERON_CLUSTER_ACTION_REQUEST_TEMPLATE_ID          23
#define AERON_CLUSTER_NEW_LEADERSHIP_TERM_EVENT_TEMPLATE_ID 24
#define AERON_CLUSTER_MEMBERSHIP_CHANGE_EVENT_TEMPLATE_ID  25

/* service → CM messages — encoded by ConsensusModuleProxy */
#define AERON_CLUSTER_CLOSE_SESSION_TEMPLATE_ID           30
#define AERON_CLUSTER_SCHEDULE_TIMER_TEMPLATE_ID          31
#define AERON_CLUSTER_CANCEL_TIMER_TEMPLATE_ID            32
#define AERON_CLUSTER_SERVICE_ACK_TEMPLATE_ID             33

/* CM → service messages — decoded by ServiceAdapter */
#define AERON_CLUSTER_JOIN_LOG_TEMPLATE_ID                40
#define AERON_CLUSTER_SERVICE_TERMINATION_POSITION_TEMPLATE_ID 42
#define AERON_CLUSTER_REQUEST_SERVICE_ACK_TEMPLATE_ID     108

/* Snapshot messages */
#define AERON_CLUSTER_SNAPSHOT_CLIENT_SESSION_TEMPLATE_ID 102
#define AERON_CLUSTER_SNAPSHOT_CLUSTER_SESSION_TEMPLATE_ID 103

/* SnapshotMarker (shared with archive, id=65) */
#define AERON_CLUSTER_SNAPSHOT_MARKER_TEMPLATE_ID         100
```

---

## 3. Component implementations

### 3.1 `aeron_cluster_service.h` — ClusteredService callback interface

This is the **only file the user interacts with**.  No .c file is needed.

```c
/* aeron_cluster_service.h — user fills in function pointers */

typedef struct aeron_clustered_service_stct aeron_clustered_service_t;
typedef struct aeron_cluster_t              aeron_cluster_t;         /* forward */
typedef struct aeron_cluster_client_session_stct aeron_cluster_client_session_t;

/* SnapshotImage — thin wrapper so snapshot loader can poll it */
typedef struct aeron_cluster_snapshot_image_stct aeron_cluster_snapshot_image_t;

typedef struct aeron_clustered_service_stct
{
    /**
     * Called once when the service is started (or restarted after a failover).
     * If snapshotImage is non-NULL the service must load state from it.
     */
    void (*on_start)(void *clientd, aeron_cluster_t *cluster,
                     aeron_cluster_snapshot_image_t *snapshot_image);

    void (*on_session_open)(void *clientd, aeron_cluster_client_session_t *session,
                            int64_t timestamp);

    void (*on_session_close)(void *clientd, aeron_cluster_client_session_t *session,
                             int64_t timestamp, int32_t close_reason);

    void (*on_session_message)(void *clientd,
                               aeron_cluster_client_session_t *session,
                               int64_t timestamp,
                               const uint8_t *buffer, size_t length);

    void (*on_timer_event)(void *clientd, int64_t correlation_id, int64_t timestamp);

    /**
     * Take a snapshot.  The service must write its state to snapshotPublication.
     * Use aeron_cluster_service_snapshot_taker_* helpers.
     */
    void (*on_take_snapshot)(void *clientd, aeron_exclusive_publication_t *snapshot_publication);

    void (*on_role_change)(void *clientd, int32_t new_role);  /* aeron_cluster_role_t */

    void (*on_terminate)(void *clientd, aeron_cluster_t *cluster);

    /**
     * Optional callbacks (may be NULL).
     */
    void (*on_new_leadership_term_event)(void *clientd,
                                         int64_t leadership_term_id,
                                         int64_t log_position,
                                         int64_t timestamp,
                                         int64_t term_base_log_position,
                                         int32_t leader_member_id,
                                         int32_t log_session_id,
                                         int32_t app_version);

    int  (*do_background_work)(void *clientd, int64_t now_ns); /* returns work count */

    void *clientd;
}
aeron_clustered_service_t;
```

**No archive analogue exists** — this is a new type specific to service-side.

---

### 3.2 `aeron_cluster_service_context.h/.c` — Container context

Mirrors `ClusteredServiceContainer.Context`.  Key fields:

```c
struct aeron_cluster_service_context_stct
{
    aeron_t *aeron;
    char     aeron_directory_name[AERON_MAX_PATH];
    bool     owns_aeron_client;

    int32_t  service_id;           /* default 0; must be unique per container */

    char    *control_channel;      /* IPC channel CM→service subscription */
    int32_t  consensus_module_stream_id;  /* default 104 */

    char    *service_channel;      /* IPC channel service→CM publication */
    int32_t  service_stream_id;    /* default 105 */

    char    *snapshot_channel;     /* channel for snapshot pub/sub */
    int32_t  snapshot_stream_id;   /* default 107 */

    char     cluster_dir[AERON_MAX_PATH]; /* cluster directory (mark file etc.) */

    int32_t  app_version;          /* application semantic version */

    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    bool                       owns_idle_strategy;

    aeron_error_handler_t      error_handler;
    void                      *error_handler_clientd;

    /* Archive context — must be IPC; used for snapshot recording/replay */
    aeron_archive_context_t   *archive_ctx;
    bool                       owns_archive_ctx;

    aeron_clustered_service_t *service;  /* user-provided callbacks */
};
```

**conclude()** validates fields and starts the aeron client if needed.  It also:
- Creates `ClusterMarkFile` (write service identity into the mark file).
- Verifies `archive_ctx` is IPC.

---

### 3.3 `aeron_cluster_client_session.h/.c` — Server-side client session

```c
struct aeron_cluster_client_session_stct
{
    int64_t    cluster_session_id;
    int32_t    response_stream_id;
    char      *response_channel;
    uint8_t   *encoded_principal;
    size_t     encoded_principal_length;

    aeron_publication_t *response_publication;  /* lazily opened on first offer */
    bool                 is_closing;

    aeron_t *aeron;  /* borrowed from context — needed to open response pub */
};
```

Public API mirrors Java `ClientSession`:
```c
int64_t aeron_cluster_client_session_id(aeron_cluster_client_session_t *session);
int32_t aeron_cluster_client_session_response_stream_id(aeron_cluster_client_session_t *session);
const char *aeron_cluster_client_session_response_channel(aeron_cluster_client_session_t *session);
bool    aeron_cluster_client_session_is_closing(aeron_cluster_client_session_t *session);
int64_t aeron_cluster_client_session_offer(aeron_cluster_client_session_t *session,
    const uint8_t *buffer, size_t length);
/* close() marks is_closing = true; agent sends CloseSession to CM */
void    aeron_cluster_client_session_close(aeron_cluster_client_session_t *session);
```

The response publication uses a `SessionMessageHeader` prepend (same 32 bytes as
client-side ingress, but direction is reversed — service→client egress).

---

### 3.4 `aeron_cluster_consensus_module_proxy.h/.c`

Sends commands **from service to CM** on `serviceChannel`.

```c
struct aeron_cluster_consensus_module_proxy_stct
{
    aeron_exclusive_publication_t *publication;
    int                            retry_attempts;
    int32_t                        service_id;
    int64_t                        cluster_session_id;  /* set by agent for offers */
    int64_t                        leadership_term_id;
    uint8_t                        buffer[AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH];
};

bool aeron_cluster_consensus_module_proxy_schedule_timer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t correlation_id, int64_t deadline);

bool aeron_cluster_consensus_module_proxy_cancel_timer(
    aeron_cluster_consensus_module_proxy_t *proxy, int64_t correlation_id);

bool aeron_cluster_consensus_module_proxy_ack(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t log_position, int64_t timestamp,
    int64_t ack_id, int64_t relevant_id, int32_t service_id);

bool aeron_cluster_consensus_module_proxy_close_session(
    aeron_cluster_consensus_module_proxy_t *proxy, int64_t cluster_session_id);

/* offer to CM — session header + app buffer → forwards to all clients via log */
int64_t aeron_cluster_consensus_module_proxy_offer(
    aeron_cluster_consensus_module_proxy_t *proxy,
    int64_t cluster_session_id, int64_t leadership_term_id,
    const uint8_t *buffer, size_t length);
```

Encoding pattern: identical to `aeron_cluster_ingress_proxy.c` — use
`wrap_and_apply_header`, fill fields, call `ingress_proxy_offer`.

---

### 3.5 `aeron_cluster_service_adapter.h/.c`

**Receives CM commands** on `controlChannel` subscription.  Uses a
`fragment_assembler` (non-controlled, FRAGMENT_LIMIT=1 as in Java).

Handles three message types (ServiceAdapter in Java):

| Template ID | Java method | C callback |
|-------------|-------------|------------|
| 40 `JoinLog` | `onJoinLog()` | `agent_on_join_log()` |
| 42 `ServiceTerminationPosition` | `onServiceTerminationPosition()` | `agent_on_service_termination_position()` |
| 108 `requestServiceAck` | `onRequestServiceAck()` | `agent_on_request_service_ack()` |

```c
struct aeron_cluster_service_adapter_stct
{
    aeron_subscription_t *subscription;
    aeron_fragment_assembler_t *fragment_assembler;
    struct aeron_clustered_service_agent_stct *agent;  /* back-pointer */
};

int aeron_cluster_service_adapter_poll(aeron_cluster_service_adapter_t *adapter);
```

---

### 3.6 `aeron_cluster_bounded_log_adapter.h/.c`

**Reads the cluster log** (replay or live) up to `commitPosition`.  Uses a
controlled fragment assembler since messages can span fragments.

Handles these template IDs:

| ID | Message | Service callback |
|----|---------|-----------------|
| 1  | `SessionMessageHeader` | `on_session_message` |
| 20 | `TimerEvent` | `on_timer_event` |
| 21 | `SessionOpenEvent` | `on_session_open` |
| 22 | `SessionCloseEvent` | `on_session_close` |
| 23 | `ClusterActionRequest` | `on_service_action` (→ take snapshot) |
| 24 | `NewLeadershipTermEvent` | `on_new_leadership_term_event` |
| 25 | `MembershipChangeEvent` | (role update) |

```c
struct aeron_cluster_bounded_log_adapter_stct
{
    aeron_subscription_t *subscription;
    aeron_image_t        *image;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    struct aeron_clustered_service_agent_stct *agent;

    int64_t  max_log_position;   /* from JoinLog.maxLogPosition */
};

/* Returns > 0 if work was done, 0 if at boundary, -1 on error */
int aeron_cluster_bounded_log_adapter_poll(aeron_cluster_bounded_log_adapter_t *adapter,
                                           int64_t commit_position);

bool aeron_cluster_bounded_log_adapter_is_done(aeron_cluster_bounded_log_adapter_t *adapter);
```

The fragment handler calls `AERON_ACTION_ABORT` when the current fragment position
would exceed `commit_position`, preventing the service from reading uncommitted log
entries.

---

### 3.7 `aeron_cluster_service_snapshot_taker.h/.c`

Writes snapshot state to an `aeron_exclusive_publication_t`.  Called from
`on_service_action` when `ClusterActionRequest.action == SNAPSHOT`.

```c
/* Write all live client sessions to the snapshot */
int aeron_cluster_service_snapshot_taker_snapshot_session(
    aeron_exclusive_publication_t *publication,
    aeron_cluster_client_session_t *session);

/* Mark the start and end of a snapshot (SnapshotMarker message) */
int aeron_cluster_service_snapshot_taker_mark_begin(
    aeron_exclusive_publication_t *publication,
    int64_t log_position, int64_t leadership_term_id,
    int32_t index, int32_t type_id, int64_t app_version);

int aeron_cluster_service_snapshot_taker_mark_end(
    aeron_exclusive_publication_t *publication,
    int64_t log_position, int64_t leadership_term_id,
    int32_t index, int32_t type_id, int64_t app_version);
```

**Snapshot format** (must match Java's `ServiceSnapshotTaker`):
1. `SnapshotMarker` (BEGIN)
2. For each live session: `ClientSession` message
3. `SnapshotMarker` (END)

The `type_id` for a service snapshot is `ClusteredServiceContainer.SNAPSHOT_TYPE_ID`
(i.e., `2`).  The service's own user data goes between BEGIN and END, written by the
user in `on_take_snapshot`.

---

### 3.8 `aeron_cluster_service_snapshot_loader.h/.c`

Reads a snapshot image passed to `on_start` when recovering.

```c
struct aeron_cluster_snapshot_image_stct
{
    aeron_image_t *image;
    bool           is_done;
};

/* Polls until is_done; calls on_load_session for each ClientSession message */
typedef void (*aeron_cluster_snapshot_session_loader_func_t)(
    void *clientd,
    int64_t cluster_session_id, int32_t response_stream_id,
    const char *response_channel, const uint8_t *encoded_principal, size_t principal_length);

int aeron_cluster_service_snapshot_loader_load_sessions(
    aeron_cluster_snapshot_image_t *snapshot,
    aeron_cluster_snapshot_session_loader_func_t on_session,
    void *clientd);
```

After calling `load_sessions`, the user polls the image for their own application
state until `SnapshotMarker(END)` is received.

---

### 3.9 `aeron_clustered_service_agent.h/.c` — The main agent

This is the equivalent of Java's `ClusteredServiceAgent`.  It implements the
`Cluster` interface and drives the entire duty cycle.

```c
struct aeron_clustered_service_agent_stct
{
    /* Context + owned resources */
    aeron_cluster_service_context_t *ctx;
    aeron_t                         *aeron;

    /* Components */
    aeron_cluster_consensus_module_proxy_t *consensus_module_proxy;
    aeron_cluster_service_adapter_t        *service_adapter;
    aeron_cluster_bounded_log_adapter_t    *log_adapter;    /* NULL until JoinLog */

    /* Session tracking */
    aeron_cluster_client_session_t **sessions;   /* dynamic array */
    size_t                           sessions_count;
    size_t                           sessions_capacity;

    /* State */
    int32_t  member_id;
    int64_t  cluster_time;
    int64_t  log_position;
    int64_t  max_log_position;
    int64_t  ack_id;
    int64_t  termination_position;
    int32_t  role;                /* aeron_cluster_role_t */
    bool     is_service_active;
    bool     is_abort;

    aeron_counters_reader_t *counters_reader;
    int64_t                  commit_position_counter_id;

    /* Back-pointer for fragment handler callbacks */
    aeron_clustered_service_t *service;
};
```

**Cluster API** (what user code calls via `aeron_cluster_t *cluster`):

```c
/* aeron_cluster_t is typedef'd to aeron_clustered_service_agent_t */
int32_t aeron_cluster_member_id(aeron_cluster_t *cluster);
int64_t aeron_cluster_log_position(aeron_cluster_t *cluster);
int64_t aeron_cluster_time(aeron_cluster_t *cluster);
int32_t aeron_cluster_role(aeron_cluster_t *cluster);

aeron_cluster_client_session_t *aeron_cluster_get_client_session(
    aeron_cluster_t *cluster, int64_t cluster_session_id);

bool    aeron_cluster_close_client_session(aeron_cluster_t *cluster, int64_t id);

bool    aeron_cluster_schedule_timer(aeron_cluster_t *cluster,
            int64_t correlation_id, int64_t deadline);

bool    aeron_cluster_cancel_timer(aeron_cluster_t *cluster, int64_t correlation_id);

int64_t aeron_cluster_offer(aeron_cluster_t *cluster,
            int64_t cluster_session_id,
            const uint8_t *buffer, size_t length);
```

**`do_work()` duty cycle** (called repeatedly by the container):

```c
int aeron_clustered_service_agent_do_work(aeron_clustered_service_agent_t *agent,
                                           int64_t now_ns)
{
    int work_count = 0;

    /* 1. Poll CM→service channel (JoinLog, RequestServiceAck, Termination) */
    work_count += aeron_cluster_service_adapter_poll(agent->service_adapter);

    /* 2. Poll the log (bounded by commitPosition) */
    if (NULL != agent->log_adapter)
    {
        int64_t commit_pos = aeron_counters_reader_counter_value(
            agent->counters_reader, agent->commit_position_counter_id);
        work_count += aeron_cluster_bounded_log_adapter_poll(agent->log_adapter, commit_pos);
    }

    /* 3. Optional: do_background_work callback */
    if (NULL != agent->service->do_background_work)
    {
        work_count += agent->service->do_background_work(agent->service->clientd, now_ns);
    }

    return work_count;
}
```

**`onJoinLog` flow** (called by ServiceAdapter when JoinLog arrives):
1. Create subscription on `logChannel:logStreamId`.
2. Find the `Image` by `logSessionId`.
3. Create `BoundedLogAdapter` with the image.
4. Send `ServiceAck` to CM via `ConsensusModuleProxy.ack()`.

**`onServiceAction` flow** (called by BoundedLogAdapter for `ClusterActionRequest`):
1. Check `flags` — only take snapshot if `standbySnapshotFlags` matches.
2. Open snapshot publication via archive recording.
3. Call `service->on_take_snapshot(publication)`.
4. Close publication; await recording to stop.
5. Send `ServiceAck` to CM.

---

## 4. Key design decisions that differ from the client side

| Aspect | Client | Service |
|--------|--------|---------|
| Entry point | `aeron_cluster_connect()` | `aeron_clustered_service_container_launch()` |
| Ownership | User calls `aeron_cluster_close()` | Container owns everything |
| Threading model | User drives poll loop | Agent runs as background thread OR agent invoker |
| Snapshot | N/A | Must implement load **and** save |
| Sessions | One outbound connection to leader | Many inbound sessions from external clients |
| Publication | Single ingress pub to cluster | One pub per client session (response channel) |
| Codec namespace | `aeron_cluster_client_*` (C headers) | Same codec headers, different message subset |

---

## 5. Startup / lifecycle sequence

```
Container::launch()
  └─► context_conclude()          — validate, start aeron client
  └─► consensus_module_proxy_init()
       └─► add_exclusive_publication(serviceChannel, serviceStreamId)
  └─► service_adapter_init()
       └─► add_subscription(controlChannel, consensusModuleStreamId)
  └─► service->on_start(cluster, NULL)  ← no snapshot on first start
  └─► send ServiceAck(logPosition=0)     ← tell CM we are ready
  └─► duty cycle: do_work() in tight loop

  ... CM sends JoinLog ...
  └─► onJoinLog:
       └─► subscribe to logChannel
       └─► create BoundedLogAdapter
       └─► send ServiceAck again (we've joined)

  ... log replay starts ...
  └─► BoundedLogAdapter dispatches on_session_open/message/close/timer…
  └─► Eventually ClusterActionRequest(SNAPSHOT) → on_take_snapshot

  ... failover: new JoinLog ...
  └─► same flow; on_start is NOT called again (service maintains state in memory)
```

---

## 6. C++ wrapper design

### `ClusteredService.h` — pure virtual base class

```cpp
namespace aeron { namespace cluster { namespace service {

class ClusteredService
{
public:
    virtual ~ClusteredService() = default;

    virtual void onStart(Cluster &cluster, std::shared_ptr<Image> snapshotImage) = 0;
    virtual void onSessionOpen(ClientSession &session, int64_t timestamp) = 0;
    virtual void onSessionClose(ClientSession &session, int64_t timestamp, int32_t closeReason) = 0;
    virtual void onSessionMessage(ClientSession &session, int64_t timestamp,
                                  AtomicBuffer &buffer, util::index_t offset, util::index_t length) = 0;
    virtual void onTimerEvent(int64_t correlationId, int64_t timestamp) = 0;
    virtual void onTakeSnapshot(ExclusivePublication &snapshotPublication) = 0;
    virtual void onRoleChange(Cluster::Role newRole) = 0;
    virtual void onTerminate(Cluster &cluster) = 0;

    // Optional overrides:
    virtual void onNewLeadershipTermEvent(int64_t leadershipTermId, int64_t logPosition,
                                          int64_t timestamp, int64_t termBaseLogPosition,
                                          int32_t leaderMemberId, int32_t logSessionId,
                                          int32_t appVersion) {}
    virtual int doBackgroundWork(int64_t nowNs) { return 0; }
};

}}}
```

The C++ wrapper uses **static trampoline functions** to bridge the C callback
interface, exactly as `ArchiveContext.h` does for archive callbacks:

```cpp
// In ClusteredServiceContainer.h setupContext():
m_c_service.on_session_open = [](void *cd, aeron_cluster_client_session_t *s, int64_t ts) {
    static_cast<Container *>(cd)->m_service.onSessionOpen(*wrapSession(s), ts);
};
```

---

## 7. Test requirements

Every `ClusteredService` Java callback must have a C/C++ test.  Required test file:
`aeron-cluster/src/test/c/service/ClusteredServiceTest.cpp`

**Unit tests** (no running cluster):
```
shouldInitializeServiceContextWithDefaults
shouldSetAndGetServiceContextFields
shouldEncodeAndDecodeSnapshotSession      ← round-trip test
shouldEncodeScheduleTimer
shouldEncodeServiceAck
```

**System tests** (require embedded Java ClusterNode, see
`aeron-cluster/src/test/cpp_wrapper/AeronClusterWrapperTest.cpp` for the
infrastructure pattern):
```
shouldConnectServiceAndReceiveJoinLog
shouldServiceReceiveSessionOpenAndMessage
shouldServiceTakeSnapshot
shouldServiceLoadSnapshot
shouldServiceHandleRoleChange
shouldServiceHandleTimerEvent
```

**C++ wrapper test**: `aeron-cluster/src/test/cpp_wrapper/service/ClusteredServiceWrapperTest.cpp`
```
shouldStartServiceContainer
shouldOfferFromServiceToClient
shouldTakeAndLoadSnapshot
```

---

## 8. Common mistakes to avoid

1. **Sending ack before joining log** — the CM will timeout waiting. The agent
   must send `ServiceAck` exactly at the right lifecycle points (see §5).

2. **Polling log beyond commitPosition** — always pass the current counter value
   to `bounded_log_adapter_poll`; never let the log advance past committed entries.

3. **Opening response publication inside on_session_open** — the publication is
   lazily opened on first offer; do not pre-open in the callback.

4. **Blocking in callbacks** — all callbacks are called from the duty cycle thread;
   any blocking call will stall the entire cluster node.

5. **Not sending BEGIN/END SnapshotMarker** — the snapshot loader won't find the
   boundary and the service will fail to recover.

6. **Missing `extern "C"` guards** — all component headers need them (same fix
   applied to the client headers in commit `81b5cfa`).

7. **Wrong `service_id`** — each `ClusteredServiceContainer` in a node must have a
   unique `service_id` (0-based); the default is 0 and is only correct for a single
   service per node.
