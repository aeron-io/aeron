# Aeron Cluster C Client — Implementation Guide

This document describes how to implement the Aeron Cluster C client layer and its
C++ wrapper, following the exact same pattern as `aeron-archive`.

Reference implementation to study: `aeron-archive/src/main/c/client/`

---

## 1. Why C First

The established Aeron pattern is:

```
Java (authoritative protocol reference)
  └─► C implementation  (aeron-client: 138 files / aeron-archive: 28 files)
        └─► C++ wrapper (thin layer, calls aeron_*() C functions directly)
```

Implementing cluster as C++ directly (as done in old forks) breaks this because:
- No C ABI → no Python/Rust/Go bindings
- C++ shared_ptr ownership clashes with aeron-client's C memory model
- Maintainers will not accept a PR that skips the C layer

---

## 2. File Structure

Mirror aeron-archive 1:1:

```
aeron-cluster/
├── src/main/c/client/
│   ├── aeron_cluster.h                        ← public API (extern "C" guard)
│   ├── aeron_cluster_client.h / .c            ← main struct + lifecycle
│   ├── aeron_cluster_context.h / .c           ← configuration setters/getters
│   ├── aeron_cluster_async_connect.h / .c     ← async connection state machine
│   ├── aeron_cluster_ingress_proxy.h / .c     ← send messages to cluster (ingress)
│   ├── aeron_cluster_egress_poller.h / .c     ← receive messages from cluster (egress)
│   ├── aeron_cluster_credentials_supplier.h/.c← authentication callbacks
│   └── aeron_cluster_client_version.h / .c   ← version string (generated)
│
└── src/main/cpp_wrapper/cluster/client/
    ├── AeronCluster.h      ← wraps aeron_cluster_client_t*  (≈ AeronArchive.h)
    ├── ClusterContext.h    ← wraps aeron_cluster_context_t* (≈ ArchiveContext.h)
    └── EgressPoller.h      ← wraps aeron_cluster_egress_poller_t*
```

---

## 3. SBE C Codec Generation

### 3.1 Add `generateCCodecs` task in build.gradle

In the `project(':aeron-cluster')` block, add alongside the existing
`generateCppCodecs` task:

```groovy
// Inside project(':aeron-cluster') in build.gradle

def generatedCDir = file("${rootDir}/cppbuild/Release/generated/c")
tasks.register('generateCCodecs', JavaExec) {
    inputs.files(codecsFile, sbeFile)
    outputs.dir generatedCDir

    mainClass.set('uk.co.real_logic.sbe.SbeTool')
    jvmArgs('--add-opens', 'java.base/jdk.internal.misc=ALL-UNNAMED')
    classpath = configurations.codecGeneration
    systemProperties(
        'sbe.output.dir'              : generatedCDir,
        'sbe.target.language'         : 'C',
        'sbe.target.namespace'        : 'aeron.cluster.client',  // → directory prefix
        'sbe.validation.xsd'          : sbeFile,
        'sbe.validation.stop.on.error': 'true')
    // Only the client-facing codec file; do NOT include node-state or
    // consensus-module schemas here (they have different schemaIds).
    args = [codecsFile]  // aeron-cluster-codecs.xml only
}
```

Running `./gradlew :aeron-cluster:generateCCodecs` will produce headers under
`cppbuild/Release/generated/c/aeron/cluster/client/` (lowercase, underscore).

### 3.2 How archive uses generated C codecs

In `aeron_archive_proxy.c`:
```c
#include "c/aeron_archive_client/authConnectRequest.h"
#include "c/aeron_archive_client/controlResponse.h"
// ... one include per message type
```

Each generated header exposes inline functions:
```c
// wrap (zero-copy into a stack buffer)
struct aeron_archive_client_authConnectRequest authConnect;
aeron_archive_client_authConnectRequest_wrap_for_encode(
    &authConnect, buffer, offset, bufferLength);
aeron_archive_client_authConnectRequest_responseChannel_set(
    &authConnect, responseChannel, strlen(responseChannel));
```

Do the same for cluster: include `c/aeron/cluster/client/sessionConnectRequest.h`
etc. in `aeron_cluster_ingress_proxy.c`.

---

## 4. Component Implementation

### 4.1 aeron_cluster_context  (`≈ aeron_archive_context`)

**Header** (`aeron_cluster_context.h`):
```c
typedef struct aeron_cluster_context_stct
{
    aeron_t *aeron;
    bool owns_aeron_client;

    char *ingress_channel;
    size_t ingress_channel_length;
    int32_t ingress_stream_id;

    char *egress_channel;
    size_t egress_channel_length;
    int32_t egress_stream_id;

    uint64_t message_timeout_ns;
    int retry_attempts;

    aeron_cluster_credentials_supplier_t credentials_supplier;

    aeron_cluster_egress_listener_func_t  on_message;
    void *on_message_clientd;
    aeron_cluster_new_leader_func_t       on_new_leader;
    void *on_new_leader_clientd;
    aeron_cluster_admin_response_func_t   on_admin_response;
    void *on_admin_response_clientd;

    aeron_error_handler_t error_handler;
    void *error_handler_clientd;
}
aeron_cluster_context_t;

int aeron_cluster_context_init(aeron_cluster_context_t **ctx);
int aeron_cluster_context_close(aeron_cluster_context_t *ctx);
int aeron_cluster_context_conclude(aeron_cluster_context_t *ctx);

// One setter per field — return int (0=ok, -1=error), matching archive pattern
int aeron_cluster_context_set_aeron(aeron_cluster_context_t *ctx, aeron_t *aeron);
int aeron_cluster_context_set_ingress_channel(aeron_cluster_context_t *ctx, const char *channel);
int aeron_cluster_context_set_ingress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int aeron_cluster_context_set_egress_channel(aeron_cluster_context_t *ctx, const char *channel);
int aeron_cluster_context_set_egress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int aeron_cluster_context_set_message_timeout_ns(aeron_cluster_context_t *ctx, uint64_t timeout_ns);
int aeron_cluster_context_set_on_message(aeron_cluster_context_t *ctx, aeron_cluster_egress_listener_func_t handler, void *clientd);
int aeron_cluster_context_set_on_new_leader(aeron_cluster_context_t *ctx, aeron_cluster_new_leader_func_t handler, void *clientd);
int aeron_cluster_context_set_error_handler(aeron_cluster_context_t *ctx, aeron_error_handler_t handler, void *clientd);
```

**Implementation** (`aeron_cluster_context.c`):
```c
int aeron_cluster_context_init(aeron_cluster_context_t **ctx)
{
    aeron_cluster_context_t *_ctx = NULL;
    if (aeron_alloc((void **)&_ctx, sizeof(aeron_cluster_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_cluster_context_t");
        return -1;
    }
    // Fill defaults (match Java defaults)
    _ctx->aeron = NULL;
    _ctx->owns_aeron_client = true;
    _ctx->ingress_stream_id = AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT;  // 101
    _ctx->egress_stream_id  = AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT;   // 102
    _ctx->message_timeout_ns = AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT; // 5s
    _ctx->retry_attempts = AERON_CLUSTER_RETRY_ATTEMPTS_DEFAULT;          // 3
    // ... strdup defaults for channels ...
    *ctx = _ctx;
    return 0;
}

int aeron_cluster_context_set_ingress_channel(aeron_cluster_context_t *ctx, const char *channel)
{
    aeron_free(ctx->ingress_channel);
    if (aeron_alloc_string_copy(&ctx->ingress_channel, channel, &ctx->ingress_channel_length) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }
    return 0;
}
// ... repeat pattern for every setter
```

### 4.2 aeron_cluster_async_connect  (`≈ aeron_archive_async_connect`)

The connection handshake is a state machine. States map directly from Java
`AeronCluster.AsyncConnect`:

```c
typedef enum aeron_cluster_async_connect_state_en
{
    AWAIT_PUBLICATION_CONNECTED   = 0,
    SEND_CONNECT_REQUEST          = 1,
    AWAIT_SUBSCRIPTION_CONNECTED  = 2,
    AWAIT_CHALLENGE_RESPONSE      = 3,
    AWAIT_CONNECT_RESPONSE        = 4,
    DONE                          = 5
}
aeron_cluster_async_connect_state_t;

struct aeron_cluster_async_connect_stct
{
    aeron_cluster_async_connect_state_t state;
    aeron_cluster_context_t *ctx;
    aeron_t *aeron;

    aeron_async_add_subscription_t *async_add_subscription;
    aeron_subscription_t *subscription;                     // egress

    aeron_async_add_exclusive_publication_t *async_add_pub;
    aeron_exclusive_publication_t *publication;             // ingress

    aeron_cluster_ingress_proxy_t *ingress_proxy;
    aeron_cluster_egress_poller_t *egress_poller;

    int64_t deadline_ns;
    int64_t correlation_id;
    int64_t cluster_session_id;
    int32_t leader_member_id;
    int32_t member_count;
    char    detail[256];  // for error messages
};
```

**`aeron_cluster_async_connect_poll`** steps through states just like archive:

```c
int aeron_cluster_async_connect_poll(
    aeron_cluster_client_t **client,
    aeron_cluster_async_connect_t *async)
{
    *client = NULL;

    if (aeron_nano_clock() > async->deadline_ns)
    {
        AERON_SET_ERR(ETIMEDOUT, "%s", "connect timeout");
        return -1;
    }

    switch (async->state)
    {
        case AWAIT_PUBLICATION_CONNECTED:
            return aeron_cluster_async_connect_state_await_pub(async);

        case SEND_CONNECT_REQUEST:
            return aeron_cluster_async_connect_state_send_connect(async);

        case AWAIT_SUBSCRIPTION_CONNECTED:
            return aeron_cluster_async_connect_state_await_sub(async);

        case AWAIT_CONNECT_RESPONSE:
            return aeron_cluster_async_connect_state_await_response(client, async);

        case DONE:
            return 0;
    }
    return 0;
}
```

### 4.3 aeron_cluster_ingress_proxy  (`≈ aeron_archive_proxy`)

Sends SBE-encoded messages to the cluster via an `aeron_exclusive_publication_t`.

```c
typedef struct aeron_cluster_ingress_proxy_stct
{
    aeron_cluster_context_t       *ctx;
    aeron_exclusive_publication_t *publication;
    int64_t                        cluster_session_id;
    int                            retry_attempts;
    uint8_t buffer[AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH]; // 512 bytes
}
aeron_cluster_ingress_proxy_t;
```

Key functions:

```c
// Send SessionConnectRequest during handshake
bool aeron_cluster_ingress_proxy_connect(
    aeron_cluster_ingress_proxy_t *proxy,
    int32_t response_stream_id,
    const char *response_channel,
    int64_t correlation_id,
    aeron_cluster_encoded_credentials_t *credentials);

// Send user message wrapped in SessionMessageHeader
int64_t aeron_cluster_ingress_proxy_offer(
    aeron_cluster_ingress_proxy_t *proxy,
    const uint8_t *buffer,
    size_t length);

// Send keepalive
bool aeron_cluster_ingress_proxy_keepalive(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t correlation_id);

// Send close
bool aeron_cluster_ingress_proxy_close(aeron_cluster_ingress_proxy_t *proxy);
```

Encoding pattern (identical to archive proxy):
```c
bool aeron_cluster_ingress_proxy_connect(...)
{
    struct aeron_cluster_client_sessionConnectRequest codec;
    aeron_cluster_client_messageHeader hdr;

    // Wrap the stack buffer and encode MessageHeader + payload
    aeron_cluster_client_messageHeader_wrap_for_encode(
        &hdr, (char *)proxy->buffer, 0, sizeof(proxy->buffer));
    aeron_cluster_client_messageHeader_blockLength_set(&hdr,
        aeron_cluster_client_sessionConnectRequest_sbeBlockLength());
    aeron_cluster_client_messageHeader_templateId_set(&hdr,
        aeron_cluster_client_sessionConnectRequest_sbeTemplateId());
    // ... schemaId, version ...

    aeron_cluster_client_sessionConnectRequest_wrap_for_encode(
        &codec, (char *)proxy->buffer,
        aeron_cluster_client_messageHeader_encodedLength(),
        sizeof(proxy->buffer));

    aeron_cluster_client_sessionConnectRequest_correlationId_set(&codec, correlation_id);
    aeron_cluster_client_sessionConnectRequest_responseStreamId_set(&codec, response_stream_id);
    aeron_cluster_client_sessionConnectRequest_version_set(&codec,
        AERON_CLUSTER_SEMANTIC_VERSION);
    aeron_cluster_client_sessionConnectRequest_responseChannel_set(
        &codec, response_channel, strlen(response_channel));

    size_t length = aeron_cluster_client_messageHeader_encodedLength() +
                    aeron_cluster_client_sessionConnectRequest_encodedLength(&codec);

    return aeron_cluster_ingress_proxy_offer_once(proxy, length) > 0;
}
```

### 4.4 aeron_cluster_egress_poller  (`≈ aeron_archive_control_response_poller`)

Receives SBE-encoded egress messages via `aeron_subscription_controlled_poll`.

```c
typedef struct aeron_cluster_egress_poller_stct
{
    aeron_subscription_t                 *subscription;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    int                                   fragment_limit;

    // Decoded fields reset each poll
    int64_t  cluster_session_id;
    int64_t  correlation_id;
    int64_t  timestamp;
    int32_t  leader_member_id;
    int32_t  template_id;

    char    *detail;            // heap, grown as needed
    uint32_t detail_malloced_len;

    bool is_poll_complete;
    bool is_session_event;
    bool is_new_leader;
    bool is_admin_response;
    bool was_challenged;
    bool is_code_ok;
    bool is_code_error;
}
aeron_cluster_egress_poller_t;
```

Fragment handler pattern:

```c
aeron_controlled_fragment_handler_action_t
aeron_cluster_egress_poller_on_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_egress_poller_t *poller = (aeron_cluster_egress_poller_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encodedLength())
    {
        return AERON_ACTION_CONTINUE;
    }

    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0, length,
        aeron_cluster_client_messageHeader_actingVersion());

    int32_t schema_id  = aeron_cluster_client_messageHeader_schemaId(&hdr);
    int32_t template_id = aeron_cluster_client_messageHeader_templateId(&hdr);

    if (schema_id != aeron_cluster_client_messageHeader_sbeSchemaId())
    {
        // wrong schema — ignore
        return AERON_ACTION_CONTINUE;
    }

    poller->template_id = template_id;
    size_t offset = aeron_cluster_client_messageHeader_encodedLength();

    switch (template_id)
    {
        case AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID:
            aeron_cluster_egress_poller_on_session_event(poller, buffer, offset, length);
            break;

        case AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID:
            aeron_cluster_egress_poller_on_session_message(poller, buffer, offset, length, header);
            break;

        case AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID:
            aeron_cluster_egress_poller_on_new_leader_event(poller, buffer, offset, length);
            break;

        case AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID:
            aeron_cluster_egress_poller_on_admin_response(poller, buffer, offset, length);
            break;

        default:
            return AERON_ACTION_CONTINUE;
    }

    poller->is_poll_complete = true;
    return AERON_ACTION_BREAK;
}
```

### 4.5 aeron_cluster_client  (`≈ aeron_archive` main struct)

```c
struct aeron_cluster_client_stct
{
    bool                           owns_ctx;
    aeron_cluster_context_t       *ctx;
    aeron_mutex_t                  lock;
    aeron_cluster_ingress_proxy_t *ingress_proxy;
    aeron_subscription_t          *egress_subscription;
    aeron_cluster_egress_poller_t *egress_poller;
    int64_t                        cluster_session_id;
    int32_t                        leader_member_id;
    bool                           is_connected;
};

// Public API
int aeron_cluster_connect(
    aeron_cluster_client_t **client,
    aeron_cluster_context_t *ctx);

int aeron_cluster_async_connect(
    aeron_cluster_async_connect_t **async,
    aeron_cluster_context_t *ctx);

int aeron_cluster_async_connect_poll(
    aeron_cluster_client_t **client,
    aeron_cluster_async_connect_t *async);

int aeron_cluster_close(aeron_cluster_client_t *client);

int64_t aeron_cluster_offer(
    aeron_cluster_client_t *client,
    const uint8_t *buffer,
    size_t offset,
    size_t length);

int aeron_cluster_poll_egress(aeron_cluster_client_t *client);

bool aeron_cluster_send_keepalive(aeron_cluster_client_t *client);

int64_t aeron_cluster_cluster_session_id(aeron_cluster_client_t *client);
int32_t aeron_cluster_leader_member_id(aeron_cluster_client_t *client);
```

---

## 5. C++ Wrapper Pattern

After the C layer is done, the C++ wrapper becomes mechanical. Follow
`aeron-archive/src/main/cpp_wrapper/client/archive/AeronArchive.h` exactly.

```cpp
// ClusterContext.h
class ClusterContext
{
    aeron_cluster_context_t *m_ctx = nullptr;
public:
    ClusterContext() { aeron_cluster_context_init(&m_ctx); }
    ~ClusterContext() { aeron_cluster_context_close(m_ctx); }

    ClusterContext &ingressChannel(const std::string &channel)
    {
        if (aeron_cluster_context_set_ingress_channel(m_ctx, channel.c_str()) < 0)
            throw std::runtime_error(aeron_errmsg());
        return *this;
    }
    ClusterContext &egressChannel(const std::string &channel) { /* same pattern */ }
    // ... one method per setter, return *this for chaining ...

    aeron_cluster_context_t *get() const { return m_ctx; }
};
```

```cpp
// AeronCluster.h
class AeronCluster
{
    aeron_cluster_client_t *m_client = nullptr;

    explicit AeronCluster(aeron_cluster_client_t *client) : m_client(client) {}
public:
    ~AeronCluster() { if (m_client) aeron_cluster_close(m_client); }

    static std::shared_ptr<AeronCluster> connect(ClusterContext &ctx)
    {
        aeron_cluster_client_t *client = nullptr;
        if (aeron_cluster_connect(&client, ctx.get()) < 0)
            throw ClusterException(aeron_errmsg(), SOURCEINFO);
        return std::shared_ptr<AeronCluster>(new AeronCluster(client));
    }

    std::int64_t offer(const AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        return aeron_cluster_offer(m_client,
            buffer.buffer() + offset,
            static_cast<size_t>(length));
    }

    int pollEgress() { return aeron_cluster_poll_egress(m_client); }

    std::int64_t clusterSessionId() const
    {
        return aeron_cluster_cluster_session_id(m_client);
    }
};
```

---

## 6. CMakeLists.txt

```cmake
# aeron-cluster/src/main/c/CMakeLists.txt

set(CLUSTER_CLIENT_C_SOURCES
    client/aeron_cluster_context.c
    client/aeron_cluster_async_connect.c
    client/aeron_cluster_ingress_proxy.c
    client/aeron_cluster_egress_poller.c
    client/aeron_cluster_client.c
    client/aeron_cluster_credentials_supplier.c
    client/aeron_cluster_client_version.c
)

add_library(aeron_cluster_client SHARED ${CLUSTER_CLIENT_C_SOURCES})
target_include_directories(aeron_cluster_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${GENERATED_C_CODECS_DIR}   # cppbuild/Release/generated/c
)
target_link_libraries(aeron_cluster_client PUBLIC aeron_client)

add_library(aeron_cluster_client_static STATIC ${CLUSTER_CLIENT_C_SOURCES})
target_include_directories(aeron_cluster_client_static PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${GENERATED_C_CODECS_DIR}
)
target_link_libraries(aeron_cluster_client_static PUBLIC aeron_client_static)
```

---

## 7. Implementation Order

Work in this sequence — each step is independently testable:

```
Step 1  aeron_cluster_context          (no I/O, pure config)
Step 2  aeron_cluster_egress_poller    (decode only, test with raw bytes)
Step 3  aeron_cluster_ingress_proxy    (encode only, test with buffer comparison)
Step 4  aeron_cluster_async_connect    (state machine, test each state transition)
Step 5  aeron_cluster_client           (glues 1-4 together)
Step 6  C++ wrapper                    (mechanical after 1-5 are solid)
Step 7  Tests against Java cluster     (aeron-cluster-system-tests equivalent)
```

For Step 7, the existing `ClusterClientSmokeTest.cpp` in the old fork
(`aeron-old`) provides the test scenario. Translate its logic into a C test
in `src/test/c/client/ClusterClientSystemTest.c` following
`aeron-archive/src/test/c/` as template.

---

## 8. Key Constants (Java → C mapping)

```c
// aeron_cluster_configuration.h
#define AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT         101
#define AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT          102
#define AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT        (5 * INT64_C(1000000000))
#define AERON_CLUSTER_RETRY_ATTEMPTS_DEFAULT            3
#define AERON_CLUSTER_SESSION_HEADER_LENGTH             (MessageHeader::ENCODED_LENGTH + SessionMessageHeader::ENCODED_LENGTH)

// SBE template IDs (from aeron-cluster-codecs.xml)
#define AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID   1
#define AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID              2
#define AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID     3
#define AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID           4
#define AERON_CLUSTER_ADMIN_REQUEST_TEMPLATE_ID             23
#define AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID            24
#define AERON_CLUSTER_SESSION_KEEPALIVE_TEMPLATE_ID          7
#define AERON_CLUSTER_SESSION_CLOSE_REQUEST_TEMPLATE_ID      6
#define AERON_CLUSTER_CHALLENGE_RESPONSE_TEMPLATE_ID         5

// SBE schema ID (from aeron-cluster-codecs.xml schemaId attribute = 111)
// NOTE: use SessionMessageHeader::sbeSchemaId() — NOT MessageHeader::sbeSchemaId()
// because the generateCCodecs task may process multiple schemas sharing MessageHeader
#define AERON_CLUSTER_SCHEMA_ID                         111
```

---

## 9. Differences from Archive to Watch Out For

| Topic | Archive | Cluster |
|-------|---------|---------|
| Publication type | `aeron_exclusive_publication_t` | `aeron_exclusive_publication_t` (same) |
| Subscription type | single control response | single egress |
| Multi-member routing | no | **yes** — ingress_endpoints maps memberId → publication; on leader change, switch active publication |
| Keepalive | no | yes — `aeron_cluster_send_keepalive()` must be called periodically |
| Challenge/Response | yes (same pattern) | yes |
| Session message wrapping | not applicable | `SessionMessageHeader` prefix on every offer |
| Admin requests | no | yes (template_id 23/24) |

The multi-member ingress routing is the only conceptually new piece.
`aeron_cluster_ingress_proxy_t` needs a small map of
`member_id → aeron_exclusive_publication_t*` and a current leader pointer.
Everything else is a direct translation of the archive pattern.
