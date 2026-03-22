# Aeron Cluster C Client — Implementation Guide

This document describes how to implement the Aeron Cluster C client layer and its
C++ wrapper. Two reference implementations should be studied together:

---

## Readiness Assessment

> **Can a developer follow this guide and produce a C + C++ implementation that
> is usable and likely to be accepted by the Aeron project maintainers?**

### C layer — ready ✓

All structural components are covered with working code examples: context
lifecycle (`init` / `conclude` / `close`), async_connect state machine,
ingress_proxy SBE encoding, egress_poller SBE decoding, credentials_supplier
callbacks, and the main client struct.  The patterns (opaque structs, `AERON_SET_ERR`,
`aeron_alloc`/`aeron_free`, env-var defaults, `owns_*` flags) faithfully mirror
`aeron-archive` and will satisfy the maintainers' review checklist.

**Two implementation bodies still need to be written from the archive reference
without an in-guide example:**

| Missing body | Where to copy from |
|--------------|--------------------|
| `aeron_cluster_context_close` | `aeron_archive_context_close` — free each `char *` field, call `aeron_close` if `owns_aeron_client`, call idle strategy close if `owns_idle_strategy` |
| `aeron_cluster_context_duplicate` | `aeron_archive_context_duplicate` — `aeron_alloc` a new struct, deep-copy all string fields with `aeron_alloc_string_copy`, share non-owned pointers |

Both are mechanical copies; they are omitted here to avoid duplicating the
archive source verbatim.

The public umbrella header `aeron_cluster.h` must also be written: it is a thin
`extern "C"` guard that `#include`s all component headers.  Copy
`aeron-archive/src/main/c/client/aeron_archive.h` and replace the symbol
prefix.

### C++ wrapper — complete ✓

All three original gaps are filled in this guide:

| Previously missing | Now covered in |
|--------------------|---------------|
| `EgressPoller.h` implementation | §5.3 |
| `ClusterException` + exception macros | §5.0 |
| CMake C++ wrapper target | §6 |

### Tests are non-negotiable — same precedent as aeron-client and aeron-archive

Both `aeron-client` and `aeron-archive` migrated their C/C++ tests as part of the
same PR that introduced the C layer.  Maintainers will apply the same standard to
cluster: **a PR without tests will not be merged, regardless of how correct the
implementation looks**.

The required test structure mirrors `aeron-archive/src/test/c/`:

```
aeron-cluster/
└── src/test/c/client/
    ├── ClusterClientTest.c           ← unit tests: context setters/getters,
    │                                    SBE encode/decode round-trips,
    │                                    async_connect state transitions with mocked IO
    └── ClusterClientSystemTest.c     ← integration: connect to a real Java cluster,
                                         send/receive messages, leader failover
```

The system test scenario exists in the deprecated migration fork at:
`/Users/guangwenzhu/aeron-old/aeron-cluster/src/test/cpp_wrapper/ClusterClientSmokeTest.cpp`
Translate its logic into C first (`ClusterClientTest.cpp`), then add a thin
C++ system test that exercises `AeronCluster::connect` and `AeronCluster::offer`.

**Coverage rule: every public method of `AeronCluster.java` must have a
corresponding C or C++ test.**  `aeron_archive_test.cpp` is the structural
template (fixture, naming, helper patterns) — the feature checklist is
`AeronCluster.java`, not archive.

Minimum breakdown:
1. `aeron_cluster_context` — every setter/getter, env-var override, conclude defaults, duplicate
2. `aeron_cluster_egress_poller` — decode each message type; bad schema ignored
3. `aeron_cluster_ingress_proxy` — encode each message type; buffer bytes match Java-generated reference
4. `aeron_cluster_async_connect` — every state transition with pre-recorded byte sequences
5. System tests — one test per Java public method: connect, offer, tryClaim, sendKeepAlive, pollEgress, controlledPollEgress, sendAdminRequest, trackIngressResult, leader failover

### Archive-pattern conformance — high

The guide correctly enforces every convention that Aeron maintainers check:

- Opaque `async_connect` struct lives in `.c`, not `.h` ✓
- `owns_aeron_client = false` in `init`, `true` only in `conclude` ✓
- All string setters heap-copy via `aeron_alloc_string_copy` ✓
- All error paths use `AERON_SET_ERR` / `AERON_APPEND_ERR` ✓
- C++ wrapper bridges lambdas → C function pointers via static trampoline functions ✓
- C++ exceptions use project macros, not `std::runtime_error` ✓
- Tests accompany the implementation in the same PR ✓ (see above)

### Verdict

Implementing from this guide will produce a PR-ready C + C++ submission.
The test suite described above must ship in the same PR.
No architectural rework is needed.

---

- `aeron-client/src/main/c/` — foundational C patterns: context lifecycle,
  `aeron_alloc`/`aeron_free`, `AERON_SET_ERR`/`AERON_APPEND_ERR`, async
  publication/subscription APIs, idle strategy callbacks, and the overall
  naming and encapsulation conventions that all higher-level C clients follow.
- `aeron-archive/src/main/c/client/` — the closest structural analog: SBE codec
  usage, `async_connect` state machine, proxy/poller split, credentials supplier,
  and the C → C++ wrapper pattern that cluster must replicate.

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
│   ├── aeron_cluster_configuration.h / .c     ← constants + env var defaults
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
    ├── EgressPoller.h      ← wraps aeron_cluster_egress_poller_t*
    └── ClusterException.h  ← ClusterException + CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW
```

---

## 3. SBE C Codec Generation

### 3.1 How codec generation works — CMake drives, Gradle executes

**The correct mental model** (same as archive):

```
cmake --build             (developer runs this)
  └─► add_custom_command  (in aeron-cluster/src/main/c/CMakeLists.txt)
        └─► ${GRADLE_WRAPPER} -Dcodec.target.dir=... :aeron-cluster:generateCCodecs
              └─► SBE tool → writes .h files under cppbuild/Release/generated/c/
```

CMake is the driver.  The Gradle task is a tool that CMake invokes via
`${GRADLE_WRAPPER}`.  You never run `./gradlew generateCCodecs` manually;
`cmake --build` does it for you when the schema XML is newer than the outputs.

**Two things to add** before this works:

**A) Add `generateCCodecs` to `build.gradle`** (cluster block, after `generateCodecs`):

```groovy
// build.gradle — project(':aeron-cluster'), codecsFile and sbeFile already defined

def generatedCppDir = file(System.getProperty('codec.target.dir') ?: "${rootDir}/cppbuild/Release/generated")
tasks.register('generateCppCodecs', JavaExec) {
    inputs.files(codecsFile, sbeFile)
    outputs.dir generatedCppDir
    mainClass.set('uk.co.real_logic.sbe.SbeTool')
    jvmArgs('--add-opens', 'java.base/jdk.internal.misc=ALL-UNNAMED')
    classpath = configurations.codecGeneration
    systemProperties(
        'sbe.output.dir'              : generatedCppDir,
        'sbe.target.language'         : 'Cpp',
        'sbe.target.namespace'        : 'aeron.cluster.client',
        'sbe.validation.xsd'          : sbeFile,
        'sbe.validation.stop.on.error': 'true')
    args = [codecsFile]   // aeron-cluster-codecs.xml only; NOT mark or node-state
}

def generatedCDir = file(System.getProperty('codec.target.dir') ?: "${rootDir}/cppbuild/Release/generated/c")
tasks.register('generateCCodecs', JavaExec) {
    inputs.files(codecsFile, sbeFile)
    outputs.dir generatedCDir
    mainClass.set('uk.co.real_logic.sbe.SbeTool')
    jvmArgs('--add-opens', 'java.base/jdk.internal.misc=ALL-UNNAMED')
    classpath = configurations.codecGeneration
    systemProperties(
        'sbe.output.dir'              : generatedCDir,
        'sbe.target.language'         : 'C',
        'sbe.target.namespace'        : 'aeron.cluster.client',
        'sbe.validation.xsd'          : sbeFile,
        'sbe.validation.stop.on.error': 'true')
    args = [codecsFile]
}
```

**B) Add cluster variables and option to top-level `CMakeLists.txt`**,
mirroring the `BUILD_AERON_ARCHIVE_API` block (around line 327):

```cmake
option(BUILD_AERON_CLUSTER_CLIENT_API "Build Aeron Cluster Client API" ON)

if (BUILD_AERON_CLUSTER_CLIENT_API)
    set(CLUSTER_CODEC_TARGET_DIR    "${CMAKE_CURRENT_BINARY_DIR}/generated")
    set(CLUSTER_C_CODEC_TARGET_DIR  "${CMAKE_CURRENT_BINARY_DIR}/generated/c")
    set(CLUSTER_CODEC_SCHEMA_DIR    "${CMAKE_CURRENT_SOURCE_DIR}/aeron-cluster/src/main/resources/cluster")
    set(CLUSTER_CODEC_WORKING_DIR   "${CMAKE_CURRENT_SOURCE_DIR}")

    set(AERON_C_CLUSTER_SOURCE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-cluster/src/main/c")

    add_subdirectory(${AERON_C_CLUSTER_SOURCE_PATH})

    if (AERON_TESTS)
        set(AERON_C_CLUSTER_TEST_PATH "${CMAKE_CURRENT_SOURCE_DIR}/aeron-cluster/src/test/c")
        add_subdirectory(${AERON_C_CLUSTER_TEST_PATH})
    endif ()
endif (BUILD_AERON_CLUSTER_CLIENT_API)
```

The `add_custom_command` that actually invokes Gradle lives in
`aeron-cluster/src/main/c/CMakeLists.txt` — see §6 for the full file.

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
    char aeron_directory_name[AERON_MAX_PATH];  // read from AERON_DIR env var
    bool owns_aeron_client;                      // false by default; conclude() sets true if it creates aeron

    char *ingress_channel;
    size_t ingress_channel_length;
    int32_t ingress_stream_id;

    char *egress_channel;
    size_t egress_channel_length;
    int32_t egress_stream_id;

    uint64_t message_timeout_ns;
    uint64_t new_leader_timeout_ns;   // how long to wait for a new leader publication to connect
    int retry_attempts;

    // Multi-member: "0=host0:port0,1=host1:port1,2=host2:port2"
    // NULL for single-node. conclude() derives per-member channel from this.
    char  *ingress_endpoints;
    size_t ingress_endpoints_length;

    char  *client_name;               // reported to cluster; NULL → default empty
    size_t client_name_length;

    bool is_ingress_exclusive;        // true = exclusive publication (default true; matches Java)

    aeron_idle_strategy_func_t idle_strategy_func;  // required for blocking connect
    void *idle_strategy_state;
    bool owns_idle_strategy;

    aeron_cluster_credentials_supplier_t credentials_supplier;

    aeron_cluster_egress_listener_func_t          on_message;
    void *on_message_clientd;
    aeron_cluster_controlled_egress_listener_func_t on_controlled_message;  // NULL if unused
    void *on_controlled_message_clientd;
    aeron_cluster_new_leader_func_t               on_new_leader;
    void *on_new_leader_clientd;
    aeron_cluster_admin_response_func_t           on_admin_response;
    void *on_admin_response_clientd;

    aeron_error_handler_t error_handler;
    void *error_handler_clientd;

    aeron_cluster_delegating_invoker_func_t delegating_invoker_func;  // for C++ wrapper threading
    void *delegating_invoker_func_clientd;
}
aeron_cluster_context_t;

int aeron_cluster_context_init(aeron_cluster_context_t **ctx);
int aeron_cluster_context_close(aeron_cluster_context_t *ctx);
int aeron_cluster_context_conclude(aeron_cluster_context_t *ctx);
int aeron_cluster_context_duplicate(aeron_cluster_context_t **dest, aeron_cluster_context_t *src);

// Setters — return int (0=ok, -1=error), matching archive pattern
int aeron_cluster_context_set_aeron(aeron_cluster_context_t *ctx, aeron_t *aeron);
int aeron_cluster_context_set_aeron_directory_name(aeron_cluster_context_t *ctx, const char *dir);
int aeron_cluster_context_set_ingress_channel(aeron_cluster_context_t *ctx, const char *channel);
int aeron_cluster_context_set_ingress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int aeron_cluster_context_set_egress_channel(aeron_cluster_context_t *ctx, const char *channel);
int aeron_cluster_context_set_egress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int aeron_cluster_context_set_message_timeout_ns(aeron_cluster_context_t *ctx, uint64_t timeout_ns);
int aeron_cluster_context_set_idle_strategy(aeron_cluster_context_t *ctx, aeron_idle_strategy_func_t func, void *state);
int aeron_cluster_context_set_on_message(aeron_cluster_context_t *ctx, aeron_cluster_egress_listener_func_t handler, void *clientd);
int aeron_cluster_context_set_on_new_leader(aeron_cluster_context_t *ctx, aeron_cluster_new_leader_func_t handler, void *clientd);
int aeron_cluster_context_set_error_handler(aeron_cluster_context_t *ctx, aeron_error_handler_t handler, void *clientd);
int aeron_cluster_context_set_delegating_invoker(aeron_cluster_context_t *ctx, aeron_cluster_delegating_invoker_func_t func, void *clientd);
int aeron_cluster_context_set_ingress_endpoints(aeron_cluster_context_t *ctx, const char *endpoints);
int aeron_cluster_context_set_new_leader_timeout_ns(aeron_cluster_context_t *ctx, uint64_t timeout_ns);
int aeron_cluster_context_set_client_name(aeron_cluster_context_t *ctx, const char *name);
int aeron_cluster_context_set_is_ingress_exclusive(aeron_cluster_context_t *ctx, bool exclusive);
int aeron_cluster_context_set_on_controlled_message(aeron_cluster_context_t *ctx, aeron_cluster_controlled_egress_listener_func_t handler, void *clientd);
// Used by the C++ wrapper to relinquish aeron_t ownership after context extraction
int aeron_cluster_context_set_owns_aeron_client(aeron_cluster_context_t *ctx, bool owns);

// Getters — needed by C++ wrapper to implement read-back methods
aeron_t *aeron_cluster_context_get_aeron(aeron_cluster_context_t *ctx);
const char *aeron_cluster_context_get_aeron_directory_name(aeron_cluster_context_t *ctx);
const char *aeron_cluster_context_get_ingress_channel(aeron_cluster_context_t *ctx);
int32_t aeron_cluster_context_get_ingress_stream_id(aeron_cluster_context_t *ctx);
const char *aeron_cluster_context_get_egress_channel(aeron_cluster_context_t *ctx);
int32_t aeron_cluster_context_get_egress_stream_id(aeron_cluster_context_t *ctx);
uint64_t aeron_cluster_context_get_message_timeout_ns(aeron_cluster_context_t *ctx);
uint64_t aeron_cluster_context_get_new_leader_timeout_ns(aeron_cluster_context_t *ctx);
const char *aeron_cluster_context_get_ingress_endpoints(aeron_cluster_context_t *ctx);
const char *aeron_cluster_context_get_client_name(aeron_cluster_context_t *ctx);
bool aeron_cluster_context_get_is_ingress_exclusive(aeron_cluster_context_t *ctx);
```

**Implementation** (`aeron_cluster_context.c`):
```c
int aeron_cluster_context_init(aeron_cluster_context_t **ctx)
{
    // Match archive: always check for NULL first
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cluster_context_init(NULL)");
        return -1;
    }

    aeron_cluster_context_t *_ctx = NULL;
    if (aeron_alloc((void **)&_ctx, sizeof(aeron_cluster_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_cluster_context_t");
        return -1;
    }

    _ctx->aeron = NULL;
    // Read default aeron directory (matches archive pattern)
    if (aeron_default_path(_ctx->aeron_directory_name, sizeof(_ctx->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to resolve default aeron directory path");
        aeron_free(_ctx);
        return -1;
    }

    // IMPORTANT: false, not true — conclude() sets true only if it creates the aeron client
    _ctx->owns_aeron_client = false;

    _ctx->ingress_stream_id = AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT;  // 101
    _ctx->egress_stream_id  = AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT;   // 102
    _ctx->message_timeout_ns     = AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT;      // 5s
    _ctx->new_leader_timeout_ns  = AERON_CLUSTER_NEW_LEADER_TIMEOUT_NS_DEFAULT;   // 5s
    _ctx->retry_attempts         = AERON_CLUSTER_RETRY_ATTEMPTS_DEFAULT;          // 3

    _ctx->idle_strategy_func = NULL;
    _ctx->idle_strategy_state = NULL;
    _ctx->owns_idle_strategy = false;

    _ctx->delegating_invoker_func = NULL;
    _ctx->delegating_invoker_func_clientd = NULL;

    _ctx->error_handler = NULL;
    _ctx->error_handler_clientd = NULL;

    _ctx->ingress_channel = NULL;
    _ctx->ingress_channel_length = 0;
    _ctx->egress_channel = NULL;
    _ctx->egress_channel_length = 0;
    _ctx->ingress_endpoints = NULL;
    _ctx->ingress_endpoints_length = 0;
    _ctx->client_name = NULL;
    _ctx->client_name_length = 0;
    _ctx->is_ingress_exclusive = true;   // matches Java default

    _ctx->on_controlled_message = NULL;
    _ctx->on_controlled_message_clientd = NULL;

    // Read env vars, matching archive pattern
    char *value = NULL;
    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        aeron_cluster_context_set_aeron_directory_name(_ctx, value);
    }
    if ((value = getenv(AERON_CLUSTER_INGRESS_CHANNEL_ENV_VAR)))
    {
        if (aeron_cluster_context_set_ingress_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }
    // ... repeat for other env vars ...

    *ctx = _ctx;
    return 0;

error:
    // aeron_cluster_context_close handles NULL fields safely — frees allocated
    // strings (ingress_channel, egress_channel, etc.) then frees _ctx itself.
    // Do NOT call aeron_free(_ctx) directly here: that leaks any heap fields
    // already allocated before the error.
    aeron_cluster_context_close(_ctx);
    return -1;
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
// ... repeat pattern for every string setter

int aeron_cluster_context_conclude(aeron_cluster_context_t *ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cluster_context_conclude(NULL)");
        return -1;
    }

    // If no ingress/egress channel was set, apply defaults from configuration
    if (NULL == ctx->ingress_channel)
    {
        if (aeron_cluster_context_set_ingress_channel(ctx, AERON_CLUSTER_INGRESS_CHANNEL_DEFAULT) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }
    if (NULL == ctx->egress_channel)
    {
        if (aeron_cluster_context_set_egress_channel(ctx, AERON_CLUSTER_EGRESS_CHANNEL_DEFAULT) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    // If caller did not inject an aeron_t, create one — and mark that we own it
    if (NULL == ctx->aeron)
    {
        aeron_context_t *aeron_ctx = NULL;
        if (aeron_context_init(&aeron_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        aeron_context_set_dir(aeron_ctx, ctx->aeron_directory_name);
        if (aeron_init(&ctx->aeron, aeron_ctx) < 0)
        {
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        if (aeron_start(ctx->aeron) < 0)
        {
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        // CRITICAL: only set true here, never in init()
        ctx->owns_aeron_client = true;
    }

    // Apply a default idle strategy if none was set
    if (NULL == ctx->idle_strategy_func)
    {
        aeron_sleeping_idle_strategy_state_t *state = NULL;
        if (aeron_alloc((void **)&state, sizeof(*state)) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        aeron_sleeping_idle_strategy_init(state, 1000);  // 1 µs sleep
        ctx->idle_strategy_func  = aeron_sleeping_idle_strategy_idle;
        ctx->idle_strategy_state = state;
        ctx->owns_idle_strategy  = true;
    }

    return 0;
}
```

### 4.2 aeron_cluster_async_connect  (`≈ aeron_archive_async_connect`)

**CRITICAL: the struct definition lives in the `.c` file, not the `.h`.**
The `.h` exposes only the opaque forward declaration and the step accessor:

```c
// aeron_cluster_async_connect.h — keep minimal
#ifndef AERON_CLUSTER_ASYNC_CONNECT_H
#define AERON_CLUSTER_ASYNC_CONNECT_H
#include "aeron_cluster.h"

// Step accessor for debugging (mirrors aeron_archive_async_connect_step)
uint8_t aeron_cluster_async_connect_step(aeron_cluster_async_connect_t *async);

#endif
```

The full state machine and struct go **inside** `aeron_cluster_async_connect.c`:

```c
// aeron_cluster_async_connect.c (internal — not exposed in header)

typedef enum aeron_cluster_async_connect_state_en
{
    ADD_PUBLICATION           = 0,  // initiate async pub add — do NOT skip this
    AWAIT_PUBLICATION_CONNECTED = 1,
    SEND_CONNECT_REQUEST      = 2,
    AWAIT_SUBSCRIPTION_CONNECTED = 3,
    AWAIT_CONNECT_RESPONSE    = 4,
    DONE                      = 5,
    SEND_CHALLENGE_RESPONSE   = 6,  // auth challenge handling
    AWAIT_CHALLENGE_RESPONSE  = 7
}
aeron_cluster_async_connect_state_t;

struct aeron_cluster_async_connect_stct
{
    aeron_cluster_async_connect_state_t state;
    aeron_cluster_context_t *ctx;
    aeron_t *aeron;

    aeron_async_add_subscription_t      *async_add_subscription;
    aeron_subscription_t                *subscription;           // egress

    aeron_async_add_exclusive_publication_t *async_add_pub;
    aeron_exclusive_publication_t           *publication;        // ingress

    aeron_cluster_ingress_proxy_t *ingress_proxy;
    aeron_cluster_egress_poller_t *egress_poller;

    // Credentials from a challenge response; freed after use
    aeron_cluster_encoded_credentials_t *encoded_credentials_from_challenge;

    int64_t deadline_ns;
    int64_t correlation_id;
    int64_t cluster_session_id;
    int32_t leader_member_id;
    int32_t member_count;
    char    detail[256];
};
```

**`aeron_cluster_async_connect_poll`** steps through states. Note `ADD_PUBLICATION`
is state 0 — async_connect must initiate `aeron_async_add_exclusive_publication`
and return immediately, completing it in the next poll call:

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
        case ADD_PUBLICATION:
            return aeron_cluster_async_connect_state_add_pub(async);

        case AWAIT_PUBLICATION_CONNECTED:
            return aeron_cluster_async_connect_state_await_pub(async);

        case SEND_CONNECT_REQUEST:
            return aeron_cluster_async_connect_state_send_connect(async);

        case AWAIT_SUBSCRIPTION_CONNECTED:
            return aeron_cluster_async_connect_state_await_sub(async);

        case AWAIT_CONNECT_RESPONSE:
            return aeron_cluster_async_connect_state_await_response(client, async);

        case SEND_CHALLENGE_RESPONSE:
            return aeron_cluster_async_connect_state_send_challenge_response(async);

        case AWAIT_CHALLENGE_RESPONSE:
            return aeron_cluster_async_connect_state_await_challenge_response(client, async);

        case DONE:
            return 0;
    }
    return 0;
}
```

### 4.3 aeron_cluster_ingress_proxy  (`≈ aeron_archive_proxy`)

```c
// Buffer matches archive scale (8 KB); cluster messages are simpler but keep headroom
#define AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH (8 * 1024)

typedef struct aeron_cluster_ingress_proxy_stct
{
    aeron_cluster_context_t       *ctx;
    aeron_exclusive_publication_t *publication;
    int64_t                        cluster_session_id;
    int64_t                        leadership_term_id;  // updated on every NEW_LEADER_EVENT;
                                                        // required in SessionMessageHeader and keepalive encodes
    int                            retry_attempts;
    uint8_t buffer[AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH];
}
aeron_cluster_ingress_proxy_t;

// create (heap alloc) + init (in-place) — mirror archive proxy pattern
int aeron_cluster_ingress_proxy_create(
    aeron_cluster_ingress_proxy_t **proxy,
    aeron_cluster_context_t *ctx,
    aeron_exclusive_publication_t *publication,
    int retry_attempts);

int aeron_cluster_ingress_proxy_init(
    aeron_cluster_ingress_proxy_t *proxy,
    aeron_cluster_context_t *ctx,
    aeron_exclusive_publication_t *publication,
    int retry_attempts);

int aeron_cluster_ingress_proxy_close(aeron_cluster_ingress_proxy_t *proxy);
int aeron_cluster_ingress_proxy_delete(aeron_cluster_ingress_proxy_t *proxy);
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
bool aeron_cluster_ingress_proxy_close_session(aeron_cluster_ingress_proxy_t *proxy);
```

Encoding pattern (identical to archive proxy):
```c
bool aeron_cluster_ingress_proxy_connect(...)
{
    struct aeron_cluster_client_sessionConnectRequest codec;
    aeron_cluster_client_messageHeader hdr;

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

    char    *detail;            // heap, grown as needed (≈ error_message in archive poller)
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

**`SessionEvent.detail` carries ingress endpoints on OK response** — this is
how the client learns the multi-member endpoint map:

```c
// Inside aeron_cluster_egress_poller_on_session_event():
if (code == SESSION_EVENT_CODE_OK)
{
    // detail field = comma-separated ingress endpoints from the leader
    // e.g. "0=localhost:9010,1=localhost:9110,2=localhost:9210"
    // Store in poller->detail, then async_connect passes it to
    // aeron_cluster_context_set_ingress_endpoints() for multi-member routing.
    // For single-node clusters detail is empty — handle both cases.
    const char *endpoints = aeron_cluster_client_sessionEvent_detail(&codec);
    uint32_t endpoints_len = aeron_cluster_client_sessionEvent_detail_length(&codec);
    if (endpoints_len > 0)
    {
        // grow detail buffer and copy
    }
}
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

// Get and take ownership of context after connect (used by C++ wrapper)
aeron_cluster_context_t *aeron_cluster_get_and_own_cluster_context(aeron_cluster_client_t *client);

int64_t aeron_cluster_offer(
    aeron_cluster_client_t *client,
    const uint8_t *buffer,
    size_t offset,
    size_t length);

int aeron_cluster_poll_egress(aeron_cluster_client_t *client);

bool aeron_cluster_send_keepalive(aeron_cluster_client_t *client);

// Zero-copy offer: reserves space in the publication and lets caller write directly.
// Caller must call aeron_exclusive_publication_buffer_claim_commit() or _abort() after.
// Returns claimed position (>0) or negative back-pressure / error code.
int64_t aeron_cluster_try_claim(
    aeron_cluster_client_t *client,
    size_t length,
    aeron_buffer_claim_t *buffer_claim);

// Controlled egress poll — stops processing after each fragment via return value.
// Use when you need per-fragment flow control (matches Java controlledPollEgress).
int aeron_cluster_controlled_poll_egress(aeron_cluster_client_t *client);

// Poll internal state transitions (leader reconnect, timeout detection).
// Java pollEgress() calls this automatically; in C you may call it explicitly.
int aeron_cluster_poll_state_changes(aeron_cluster_client_t *client);

// Must be called with the result of every aeron_cluster_offer / try_claim call.
// Detects back-pressure codes that indicate the leader has changed and triggers
// the reconnection state machine (AWAIT_NEW_LEADER_CONNECTION).
void aeron_cluster_track_ingress_result(aeron_cluster_client_t *client, int64_t result);

// Admin request: ask the cluster leader to take a snapshot.
// correlationId is echoed back in the AdminResponse egress message.
bool aeron_cluster_send_admin_request_snapshot(
    aeron_cluster_client_t *client,
    int64_t correlation_id);

bool aeron_cluster_is_closed(aeron_cluster_client_t *client);

int64_t aeron_cluster_cluster_session_id(aeron_cluster_client_t *client);
int64_t aeron_cluster_leadership_term_id(aeron_cluster_client_t *client);
int32_t aeron_cluster_leader_member_id(aeron_cluster_client_t *client);

// Expose underlying Aeron objects — needed by C++ wrapper and advanced callers.
aeron_exclusive_publication_t *aeron_cluster_ingress_publication(aeron_cluster_client_t *client);
aeron_subscription_t          *aeron_cluster_egress_subscription(aeron_cluster_client_t *client);
```

> **`leadershipTermId` is load-bearing**: it must be included in every
> `SessionMessageHeader` and `SessionKeepAlive` encode.  The guide's earlier
> ingress_proxy examples omit it — add it as a field of
> `aeron_cluster_ingress_proxy_t` (updated during `onNewLeader`) and pass it
> to every SBE encode call.

### 4.6 aeron_cluster_credentials_supplier  (`≈ aeron_archive_credentials_supplier`)

The credentials supplier is a struct of callbacks, not a standalone object.
Mirror the archive pattern exactly.

```c
// aeron_cluster_credentials_supplier.h

typedef struct aeron_cluster_encoded_credentials_stct
{
    const uint8_t *data;
    size_t         length;
}
aeron_cluster_encoded_credentials_t;

// Called once when async_connect reaches SEND_CONNECT_REQUEST.
// Return NULL or zero-length credentials for unauthenticated clusters.
typedef aeron_cluster_encoded_credentials_t *(*aeron_cluster_credentials_encoded_credentials_supplier_func_t)(
    void *clientd);

// Called when the cluster returns a CHALLENGE session event.
// `challenge` is valid only for the duration of this call — copy if needed.
typedef aeron_cluster_encoded_credentials_t *(*aeron_cluster_credentials_challenge_supplier_func_t)(
    aeron_cluster_encoded_credentials_t *challenge,
    void *clientd);

// Called when async_connect is finished — supplier may free resources here.
typedef void (*aeron_cluster_credentials_free_func_t)(
    aeron_cluster_encoded_credentials_t *credentials,
    void *clientd);

typedef struct aeron_cluster_credentials_supplier_stct
{
    aeron_cluster_credentials_encoded_credentials_supplier_func_t encoded_credentials;
    aeron_cluster_credentials_challenge_supplier_func_t           on_challenge;
    aeron_cluster_credentials_free_func_t                         free_credentials;
    void *clientd;
}
aeron_cluster_credentials_supplier_t;

int aeron_cluster_credentials_supplier_init(
    aeron_cluster_credentials_supplier_t *supplier,
    aeron_cluster_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_cluster_credentials_challenge_supplier_func_t on_challenge,
    aeron_cluster_credentials_free_func_t free_credentials,
    void *clientd);
```

**Usage in async_connect**: at `SEND_CONNECT_REQUEST` state, call
`ctx->credentials_supplier.encoded_credentials(clientd)` to obtain credentials
and pass them to `aeron_cluster_ingress_proxy_connect`.  At `SEND_CHALLENGE_RESPONSE`
state, call `ctx->credentials_supplier.on_challenge(challenge, clientd)`.
After the connect completes (or fails), call `free_credentials` to let the
supplier clean up any heap data it returned.

**No-auth default**: `context_conclude` must set a no-op supplier if none is provided:
```c
// In context_conclude, after idle strategy setup:
if (NULL == ctx->credentials_supplier.encoded_credentials)
{
    static aeron_cluster_encoded_credentials_t s_empty = { NULL, 0 };
    ctx->credentials_supplier.encoded_credentials =
        aeron_cluster_credentials_supplier_encoded_credentials_null_func;
    ctx->credentials_supplier.on_challenge        = NULL;
    ctx->credentials_supplier.free_credentials    = NULL;
    ctx->credentials_supplier.clientd             = &s_empty;
}
```

---

## 5. C++ Wrapper Pattern

After the C layer is done, the C++ wrapper becomes mostly mechanical.
The critical pattern to follow is `ArchiveContext.h` and `AeronArchive.h` exactly —
the guide below corrects common simplification mistakes.

### 5.0 Exception infrastructure  (must exist before any other C++ header compiles)

Create `ClusterException.h` mirroring `ArchiveException.h`:

```cpp
// aeron-cluster/src/main/cpp_wrapper/cluster/client/ClusterException.h
#pragma once
#include "util/SourcedException.h"

namespace aeron { namespace cluster { namespace client
{

AERON_DECLARE_SOURCED_EXCEPTION(ClusterException, SourcedException);

}}}

// Map C errno + aeron_errmsg() to a ClusterException with file/line info.
// Mirror ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW from ArchiveException.h.
#define CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW \
    throw ::aeron::cluster::client::ClusterException( \
        std::string(::aeron_errmsg()), SOURCEINFO)

// Wrap an already-known error code + message (used in egress/challenge error paths).
#define CLUSTER_MAP_TO_SOURCED_EXCEPTION_AND_THROW(code, msg) \
    throw ::aeron::cluster::client::ClusterException( \
        std::string(msg) + " (" + std::to_string(code) + ")", SOURCEINFO)
```

`AERON_DECLARE_SOURCED_EXCEPTION` is a macro defined in
`aeron-client/src/main/cpp/util/SourcedException.h`; it generates the class
body and `SOURCEINFO` captures `__FILE__` / `__LINE__`.  All other C++ files in
the wrapper `#include "ClusterException.h"` before including any C header.

### 5.1 ClusterContext.h

The C++ Context must bridge C++ lambdas to C function pointers via `setupContext()`.
Storing the C++ closures as members is mandatory because the C layer holds only
raw `void *clientd` pointers.

```cpp
// ClusterContext.h
namespace aeron { namespace cluster { namespace client
{

typedef std::function<void(/* egress args */)> egress_listener_t;
typedef std::function<void()> delegating_invoker_t;

class ClusterContext
{
    friend class AeronCluster;

public:
    ClusterContext()
    {
        if (aeron_cluster_context_init(&m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;  // NOT throw std::runtime_error
        }
        setupContext();
    }

    ~ClusterContext()
    {
        aeron_cluster_context_close(m_ctx);
        m_ctx = nullptr;
    }

    inline ClusterContext &aeron(std::shared_ptr<Aeron> aeron)
    {
        if (aeron_cluster_context_set_aeron(m_ctx, aeron->aeron()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_aeronW = std::move(aeron);
        return *this;
    }

    inline std::shared_ptr<Aeron> aeron() const { return m_aeronW; }

    inline ClusterContext &ingressChannel(const std::string &channel)
    {
        aeron_cluster_context_set_ingress_channel(m_ctx, channel.c_str());
        return *this;
    }

    inline std::string ingressChannel() const
    {
        return { aeron_cluster_context_get_ingress_channel(m_ctx) };
    }

    // ... one setter+getter pair per field, all returning *this for chaining ...

    template<typename IdleStrategy>
    inline ClusterContext &idleStrategy(IdleStrategy &strategy)
    {
        m_idleFunc = [&strategy](int work_count){ strategy.idle(work_count); };
        return *this;
    }

    inline ClusterContext &delegatingInvoker(const delegating_invoker_t &invoker)
    {
        m_delegatingInvoker = invoker;
        return *this;
    }

    inline ClusterContext &errorHandler(const exception_handler_t &handler)
    {
        if (aeron_cluster_context_set_error_handler(m_ctx, error_handler_func, (void *)this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_errorHandler = handler;
        return *this;
    }

    aeron_cluster_context_t *get() const { return m_ctx; }

private:
    aeron_cluster_context_t *m_ctx = nullptr;

    std::shared_ptr<Aeron> m_aeronW = nullptr;
    std::function<void(int)> m_idleFunc;
    YieldingIdleStrategy m_defaultIdleStrategy;

    exception_handler_t m_errorHandler = nullptr;
    delegating_invoker_t m_delegatingInvoker = nullptr;

    // Private constructor used after connect() succeeds — takes ownership of
    // the context extracted from the newly created aeron_cluster_client_t.
    explicit ClusterContext(aeron_cluster_context_t *ctx) : m_ctx(ctx)
    {
        setupContext();
    }

    // Wire C++ closures into C callbacks. Called from both constructors.
    void setupContext()
    {
        if (aeron_cluster_context_set_idle_strategy(m_ctx, idle_func, (void *)this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_cluster_context_set_delegating_invoker(m_ctx, delegating_invoker_func, (void *)this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        // Set default idle strategy (overridable by user)
        this->idleStrategy(m_defaultIdleStrategy);
    }

    static void idle_func(void *clientd, int work_count)
    {
        ((ClusterContext *)clientd)->m_idleFunc(work_count);
    }

    static void error_handler_func(void *clientd, int errcode, const char *message)
    {
        auto *ctx = (ClusterContext *)clientd;
        if (nullptr != ctx->m_errorHandler)
        {
            try { CLUSTER_MAP_TO_SOURCED_EXCEPTION_AND_THROW(errcode, message); }
            catch (SourcedException &ex) { ctx->m_errorHandler(ex); }
        }
    }

    static void delegating_invoker_func(void *clientd)
    {
        auto *ctx = (ClusterContext *)clientd;
        if (nullptr != ctx->m_delegatingInvoker)
        {
            ctx->m_delegatingInvoker();
        }
    }
};

}}}
```

### 5.2 AeronCluster.h

The outer class wraps `aeron_cluster_client_t *`. Its inner `AsyncConnect` class
mirrors `AeronArchive::AsyncConnect` exactly and holds C++ callbacks copied from
the Context so they can be re-injected after the C connect completes.

```cpp
// AeronCluster.h
class AeronCluster
{
public:
    using Context_t = aeron::cluster::client::ClusterContext;

    // Inner class for async connection — mirrors AeronArchive::AsyncConnect
    class AsyncConnect
    {
        friend class AeronCluster;
    public:
        std::shared_ptr<AeronCluster> poll()
        {
            if (nullptr == m_async)
            {
                throw ClusterException("AsyncConnect already complete", SOURCEINFO);
            }

            aeron_cluster_client_t *client = nullptr;
            if (aeron_cluster_async_connect_poll(&client, m_async) < 0)
            {
                CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            if (nullptr == client)
            {
                return {};
            }

            m_async = nullptr;  // poll() freed this

            return std::shared_ptr<AeronCluster>(
                new AeronCluster(client, m_aeronW, m_errorHandler, m_delegatingInvoker));
        }

    private:
        explicit AsyncConnect(ClusterContext &ctx)
            : m_async(nullptr),
              m_aeronW(ctx.aeron()),
              m_errorHandler(ctx.m_errorHandler),
              m_delegatingInvoker(ctx.m_delegatingInvoker)
        {
            // async_connect duplicates the underlying aeron_cluster_context_t
            if (aeron_cluster_async_connect(&m_async, ctx.get()) < 0)
            {
                CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        aeron_cluster_async_connect_t *m_async;
        std::shared_ptr<Aeron> m_aeronW;
        const exception_handler_t m_errorHandler;
        const delegating_invoker_t m_delegatingInvoker;
    };

    static std::shared_ptr<AsyncConnect> asyncConnect(ClusterContext &ctx)
    {
        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    static std::shared_ptr<AeronCluster> connect(ClusterContext &ctx)
    {
        aeron_cluster_client_t *client = nullptr;
        if (aeron_cluster_connect(&client, ctx.get()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return std::shared_ptr<AeronCluster>(
            new AeronCluster(client, ctx.aeron(), ctx.m_errorHandler, ctx.m_delegatingInvoker));
    }

    ~AeronCluster()
    {
        m_egressSubscription = nullptr;  // release before closing client
        aeron_cluster_close(m_client);
    }

    std::int64_t offer(const AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        return aeron_cluster_offer(m_client,
            buffer.buffer() + offset,
            static_cast<size_t>(length));
    }

    int pollEgress() { return aeron_cluster_poll_egress(m_client); }

    bool sendKeepAlive() { return aeron_cluster_send_keepalive(m_client); }

    std::int64_t clusterSessionId() const
    {
        return aeron_cluster_cluster_session_id(m_client);
    }

    std::int32_t leaderMemberId() const
    {
        return aeron_cluster_leader_member_id(m_client);
    }

    const ClusterContext &context() { return m_ctxW; }

private:
    // Private constructor — called after C connect/poll succeeds.
    // Takes ownership of the context extracted from the client and re-wires
    // C++ callbacks (errorHandler, delegatingInvoker) into the new context.
    explicit AeronCluster(
        aeron_cluster_client_t *client,
        const std::shared_ptr<Aeron> &originalAeron,
        const exception_handler_t &errorHandler,
        const delegating_invoker_t &delegatingInvoker)
        : m_client(client),
          m_ctxW(aeron_cluster_get_and_own_cluster_context(client))
          // ^^^ transfers context ownership out of client; do NOT call
          //     aeron_cluster_get_cluster_context(m_client) after this point —
          //     the client no longer owns the context.
    {
        // Divorce the aeron_t from the cluster context so the C++ wrapper manages lifetime
        aeron_cluster_context_set_owns_aeron_client(m_ctxW.get(), false);

        // Read aeron from the *already-owned* m_ctxW, NOT from m_client.
        // Using aeron_cluster_get_cluster_context(m_client) here would be
        // use-after-transfer and is undefined behaviour.
        auto *aeron = aeron_cluster_context_get_aeron(m_ctxW.get());

        m_ctxW.aeron(nullptr == originalAeron ? std::make_shared<Aeron>(aeron) : originalAeron)
              .delegatingInvoker(delegatingInvoker);

        if (nullptr != errorHandler)
        {
            m_ctxW.errorHandler(errorHandler);
        }

        m_egressSubscription = std::make_unique<Subscription>(
            aeron,
            aeron_cluster_get_and_own_egress_subscription(m_client),
            nullptr);
    }

    aeron_cluster_client_t *m_client = nullptr;
    ClusterContext m_ctxW;
    std::unique_ptr<Subscription> m_egressSubscription = nullptr;
};
```

### 5.3 EgressPoller.h  (C++ wrapper for `aeron_cluster_egress_poller_t`)

Mirror `AeronArchive`'s `ControlResponsePoller` — thin delegation only, no
business logic.  The C poller already decoded all fields; the C++ wrapper just
exposes them with idiomatic types.

```cpp
// EgressPoller.h
#pragma once
#include "aeron_cluster_egress_poller.h"
#include "ClusterException.h"

namespace aeron { namespace cluster { namespace client
{

class EgressPoller
{
public:
    // Takes a non-owning pointer to an already-initialised C poller.
    // The C layer (aeron_cluster_client) owns the poller's lifetime.
    explicit EgressPoller(aeron_cluster_egress_poller_t *poller)
        : m_poller(poller)
    {}

    int poll()
    {
        int fragments = aeron_cluster_egress_poller_poll(m_poller);
        if (fragments < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return fragments;
    }

    bool isPollComplete()   const { return m_poller->is_poll_complete; }
    bool isSessionEvent()   const { return m_poller->is_session_event; }
    bool isNewLeader()      const { return m_poller->is_new_leader; }
    bool isAdminResponse()  const { return m_poller->is_admin_response; }
    bool wasChallenged()    const { return m_poller->was_challenged; }
    bool isCodeOk()         const { return m_poller->is_code_ok; }
    bool isCodeError()      const { return m_poller->is_code_error; }

    std::int64_t clusterSessionId() const { return m_poller->cluster_session_id; }
    std::int64_t correlationId()    const { return m_poller->correlation_id; }
    std::int64_t timestamp()        const { return m_poller->timestamp; }
    std::int32_t leaderMemberId()   const { return m_poller->leader_member_id; }
    std::int32_t templateId()       const { return m_poller->template_id; }

    // detail() is valid only during the callback that triggered the event.
    // Copy if you need it beyond that scope.
    const char *detail() const
    {
        return (m_poller->detail != nullptr) ? m_poller->detail : "";
    }

    aeron_cluster_egress_poller_t *get() const { return m_poller; }

private:
    aeron_cluster_egress_poller_t *m_poller;
};

}}}
```

Add `EgressPoller.h` to the C++ wrapper install target in CMake (see §6).

---

## 6. CMakeLists.txt

Mirror `aeron-archive/src/main/c/CMakeLists.txt` exactly.  The key pattern is
`add_custom_command` → `c_codecs` target → library `add_dependencies`.
Library target name follows the archive convention: `aeron_cluster_c_client`
(note `_c_`, not just `_client`).

```cmake
# aeron-cluster/src/main/c/CMakeLists.txt

find_package(Java REQUIRED)

set(CODEC_SCHEMA ${CLUSTER_CODEC_SCHEMA_DIR}/aeron-cluster-codecs.xml)

# List every header that generateCCodecs will produce for the client-facing
# messages.  CMake uses this list to detect when re-generation is needed.
# Run generateCCodecs once and copy the actual filenames from the output dir.
set(GENERATED_C_CODECS
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminRequestType.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/adminResponseCode.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/challenge.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/challengeResponse.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/eventCode.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/messageHeader.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/newLeaderEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionCloseRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionConnectRequest.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionEvent.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionKeepAlive.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/sessionMessageHeader.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/varAsciiEncoding.h
    ${CLUSTER_C_CODEC_TARGET_DIR}/aeron_cluster_client/varDataEncoding.h)
# NOTE: SBE generates headers for ALL messages in the XML, not just client-facing
# ones.  After first generation, extend this list to include every produced file
# so CMake can track changes correctly — see archive's CMakeLists.txt for
# the full 40-entry pattern.

add_custom_command(OUTPUT ${GENERATED_C_CODECS}
    COMMAND ${CMAKE_COMMAND} -E env
        JAVA_HOME=$ENV{JAVA_HOME}
        BUILD_JAVA_HOME=$ENV{BUILD_JAVA_HOME}
        BUILD_JAVA_VERSION=$ENV{BUILD_JAVA_VERSION}
        ${GRADLE_WRAPPER}
        -Dcodec.target.dir=${CLUSTER_C_CODEC_TARGET_DIR}
        :aeron-cluster:generateCCodecs
        --no-daemon --console=plain -q
    DEPENDS ${CODEC_SCHEMA} aeron-all-jar
    WORKING_DIRECTORY ${CLUSTER_CODEC_WORKING_DIR}
    COMMENT "Generating C Cluster codecs")

add_custom_target(cluster_c_codecs DEPENDS ${GENERATED_C_CODECS})

set(SOURCE
    client/aeron_cluster_async_connect.c
    client/aeron_cluster_client.c
    client/aeron_cluster_client_version.c
    client/aeron_cluster_configuration.c
    client/aeron_cluster_context.c
    client/aeron_cluster_credentials_supplier.c
    client/aeron_cluster_egress_poller.c
    client/aeron_cluster_ingress_proxy.c
)

set(HEADERS
    client/aeron_cluster.h
    client/aeron_cluster_async_connect.h
    client/aeron_cluster_client.h
    client/aeron_cluster_client_version.h
    client/aeron_cluster_configuration.h
    client/aeron_cluster_context.h
    client/aeron_cluster_credentials_supplier.h
    client/aeron_cluster_egress_poller.h
    client/aeron_cluster_ingress_proxy.h
)

# shared library — target name follows archive convention: _c_ in the name
add_library(aeron_cluster_c_client SHARED ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_client ALIAS aeron_cluster_c_client)

add_dependencies(aeron_cluster_c_client cluster_c_codecs)

target_include_directories(aeron_cluster_c_client
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_client
    aeron
    ${CMAKE_THREAD_LIBS_INIT})

# static library
add_library(aeron_cluster_c_client_static STATIC ${SOURCE} ${HEADERS})
add_library(aeron::aeron_cluster_c_client_static ALIAS aeron_cluster_c_client_static)

add_dependencies(aeron_cluster_c_client_static cluster_c_codecs)

target_include_directories(aeron_cluster_c_client_static
    PUBLIC  "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
            "$<INSTALL_INTERFACE:include/aeron>"
    PRIVATE ${CLUSTER_C_CODEC_TARGET_DIR})

target_link_libraries(aeron_cluster_c_client_static
    aeron_static
    ${CMAKE_THREAD_LIBS_INIT})

# C++ wrapper — header-only INTERFACE target
# Headers live in src/main/cpp_wrapper/cluster/client/
set(AERON_CLUSTER_WRAPPER_SOURCE_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/../../../../cpp_wrapper/cluster/client")

add_library(aeron_cluster_c_client_wrapper INTERFACE)
add_library(aeron::aeron_cluster_c_client_wrapper ALIAS aeron_cluster_c_client_wrapper)

target_include_directories(aeron_cluster_c_client_wrapper INTERFACE
    "$<BUILD_INTERFACE:${AERON_CLUSTER_WRAPPER_SOURCE_PATH}>"
    "$<INSTALL_INTERFACE:include/aeron>")

target_link_libraries(aeron_cluster_c_client_wrapper INTERFACE
    aeron_cluster_c_client
    aeron_client_wrapper)   # brings in Aeron.h, Subscription.h, AtomicBuffer.h

if (AERON_INSTALL_TARGETS)
    install(
        TARGETS aeron_cluster_c_client aeron_cluster_c_client_static
        EXPORT  aeron-targets
        RUNTIME DESTINATION lib
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
    install(DIRECTORY ./ DESTINATION include/aeron FILES_MATCHING PATTERN "aeron_cluster.h")
endif ()
```

---

## 7. Implementation Order

Work in this sequence — each step is independently testable:

```
Step 1  aeron_cluster_configuration      (constants + env var names — no I/O)
Step 2  aeron_cluster_context            (setters/getters + conclude, no I/O)
Step 3  aeron_cluster_egress_poller      (decode only, test with raw bytes)
Step 4  aeron_cluster_ingress_proxy      (encode only, test with buffer comparison)
Step 5  aeron_cluster_async_connect      (state machine, test each state transition)
Step 6  aeron_cluster_client             (glues 1-5 together)
Step 7  C++ wrapper                      (mechanical after 1-6 are solid)
Step 8  Tests against Java cluster       (aeron-cluster-system-tests equivalent)
```

For Step 8, the existing `ClusterClientSmokeTest.cpp` in the old fork
(`aeron-old`) provides the smoke-test scenario.  But that alone is insufficient:
the PR must include the **same breadth of test coverage that archive shipped**,
plus cluster-specific cases.  The complete required test list is below.

### Required test cases — `ClusterClientTest.cpp`

Mirror `aeron_archive_test.cpp` class structure: one base fixture (`AeronCClusterTestBase`)
that starts/stops a Java cluster node, plus derived `AeronCClusterTest` and
`AeronCClusterIdTest`.

**Context / configuration tests** (`AeronCClusterIdTest`):
```
shouldInitializeContextWithDefaultValues
shouldInitializeContextWithValuesSpecifiedViaEnvironment
shouldFailWithErrorIfRetryAttemptsIsZero
shouldFailWithErrorIfAeronClientFailsToConnect
shouldApplyDefaultParametersToIngressAndEgressChannels
shouldNotApplyDefaultParametersIfChannelsAreSetExplicitly
shouldDuplicateContext
```

**Connection tests** (`AeronCClusterTest`):
```
shouldAsyncConnectToCluster
shouldAsyncConnectToClusterWithPrebuiltAeron
shouldConnectToCluster
shouldConnectToClusterWithPrebuiltAeron
shouldConnectToClusterAndCallDelegatingInvoker
shouldConnectFromTwoClientsUsingIpc
shouldHandleNullCredentialsSupplier
shouldHandleNullDataInTheEncodedCredentials
shouldFailForIncorrectInitialCredentials
shouldBeAbleToHandleBeingChallenged
shouldFailForIncorrectChallengeCredentials
shouldObserveErrorOnBadDataOnEgressChannel
shouldCallErrorHandlerOnError
shouldSetClientName
```

**Messaging tests** (`AeronCClusterTest`):
```
shouldSendMessageToClusterAndReceiveEgress
shouldSendMultipleMessagesAndReceiveEgress
shouldSendKeepAlive
shouldTryClaimAndSendMessage
shouldSendAdminRequestToTakeSnapshot
shouldReceiveAdminResponse
```

**Leader / reconnect tests** (`AeronCClusterTest`, requires multi-node):
```
shouldHandleLeaderFailoverAndReconnect
shouldConnectToMultiMemberCluster
shouldSendMessageAfterLeaderChange
```

**C++ wrapper test** — `AeronClusterWrapperTest.cpp` (mirrors `AeronArchiveWrapperTest.cpp`):
```
shouldConnectViaWrapper
shouldOfferAndReceiveEgressViaWrapper
shouldAsyncConnectViaWrapper
shouldSendKeepAliveViaWrapper
shouldHandleExceptionOnConnectError
```

> **Rule**: every public method of the Java `AeronCluster` class must have
> corresponding C/C++ test coverage.  `aeron_archive_test.cpp` is used only as
> a **structural template** (fixture setup, test naming style, helper patterns)
> — not as a feature checklist.  The feature checklist is `AeronCluster.java`.

---

## 8. Key Constants (Java → C mapping)

```c
// aeron_cluster_configuration.h
#define AERON_CLUSTER_INGRESS_CHANNEL_ENV_VAR           "AERON_CLUSTER_INGRESS_CHANNEL"
#define AERON_CLUSTER_INGRESS_STREAM_ID_ENV_VAR         "AERON_CLUSTER_INGRESS_STREAM_ID"
#define AERON_CLUSTER_EGRESS_CHANNEL_ENV_VAR            "AERON_CLUSTER_EGRESS_CHANNEL"
#define AERON_CLUSTER_EGRESS_STREAM_ID_ENV_VAR          "AERON_CLUSTER_EGRESS_STREAM_ID"
#define AERON_CLUSTER_MESSAGE_TIMEOUT_ENV_VAR           "AERON_CLUSTER_MESSAGE_TIMEOUT"

#define AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT         101
#define AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT          102
#define AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT        (5 * INT64_C(1000000000))
#define AERON_CLUSTER_RETRY_ATTEMPTS_DEFAULT            3
// MessageHeader(8) + SessionMessageHeader block(24) = 32 bytes.
// Must be a macro expression using SBE-generated inline functions, NOT C++ scope
// syntax (MessageHeader::ENCODED_LENGTH is invalid in a C translation unit).
#define AERON_CLUSTER_SESSION_HEADER_LENGTH \
    (aeron_cluster_client_messageHeader_encodedLength() + \
     aeron_cluster_client_sessionMessageHeader_sbeBlockLength())

// SBE template IDs — verified against aeron-cluster-codecs.xml (schemaId=111 version=16)
// CRITICAL: these are the exact id= values in the XML; any mismatch silently breaks
// the egress_poller switch and ingress_proxy message header encode.
#define AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID     1  // egress: prefix on every app message
#define AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID              2  // egress: connect response / errors
#define AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID    3  // ingress: sent during handshake
#define AERON_CLUSTER_SESSION_CLOSE_REQUEST_TEMPLATE_ID      4  // ingress: clean session close
#define AERON_CLUSTER_SESSION_KEEPALIVE_TEMPLATE_ID          5  // ingress: periodic keepalive
#define AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID           6  // egress: leader changed
#define AERON_CLUSTER_CHALLENGE_TEMPLATE_ID                  7  // egress: auth challenge from cluster
#define AERON_CLUSTER_CHALLENGE_RESPONSE_TEMPLATE_ID         8  // ingress: response to challenge
#define AERON_CLUSTER_ADMIN_REQUEST_TEMPLATE_ID             23  // ingress: e.g. snapshot request
#define AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID            24  // egress: response to admin request

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
| Multi-member routing | no | **yes** — see below |
| Keepalive | no | yes — `aeron_cluster_send_keepalive()` must be called periodically |
| Challenge/Response | yes (same pattern) | yes |
| Session message wrapping | not applicable | `SessionMessageHeader` prefix on every offer |
| Admin requests | no | yes (template_id 23/24) |
| Archive ID handshake | yes (`SEND_ARCHIVE_ID_REQUEST` state) | no equivalent — remove those states |

### Multi-member ingress routing (the only conceptually new piece)

A cluster can have multiple members. The client sends ingress to the **leader only**.
When a `NEW_LEADER_EVENT` arrives, the client must switch to the new leader's publication.

**`aeron_cluster_ingress_proxy_t` additions needed:**

```c
// Inside aeron_cluster_ingress_proxy_stct (in the .c file)
struct
{
    int32_t                        member_id;
    aeron_exclusive_publication_t *publication;
} members[AERON_CLUSTER_MAX_MEMBER_COUNT];  // fixed-size array; size from context

int32_t  member_count;
int32_t  leader_member_id;
aeron_exclusive_publication_t *leader_publication;  // pointer into members[]
```

**Lifecycle rules:**

1. **On connect**: `async_connect` receives `member_count` and per-member ingress
   endpoints from the `SESSION_EVENT` response. For a single-node cluster
   (`member_count == 1`) only one publication is created; `leader_publication`
   points to it directly.

2. **On `NEW_LEADER_EVENT`**: `egress_poller` decodes the new `leaderMemberId`.
   `aeron_cluster_client` (or the ingress proxy) looks up the publication for
   that member and sets `leader_publication`. The old publication is **not closed**
   immediately — it may receive a trailing response; close it after a grace period
   or on explicit `aeron_cluster_close`.

3. **Publication creation**: create all member publications during `async_connect`
   (state `ADD_PUBLICATION` iterates over each endpoint). Do not create them lazily
   on demand — this avoids back-pressure stalls during leader switches.

4. **Thread safety**: `leader_member_id` and `leader_publication` can be read and
   written from the poll thread and the application thread. Guard them with the
   `aeron_mutex_t lock` inside `aeron_cluster_client_stct`, same as the archive
   client guards `is_connected`.

5. **Single-node shortcut**: for the common single-node development case, omit
   the members array and keep a single `publication` field — but document this
   limitation so it is not shipped to production multi-node deployments.

---

## 10. Common Mistakes That Will Block PR Acceptance

1. **`owns_aeron_client = true` in init** — must be `false`; `conclude()` sets it
   `true` only if it creates the aeron client itself. Getting this wrong causes
   double-free in tests.

2. **Exposing `aeron_cluster_async_connect_stct` in the `.h`** — the struct must
   be defined in the `.c` file. The `.h` exposes only the opaque typedef and
   `aeron_cluster_async_connect_step()`. Maintainers enforce this encapsulation.

3. **Skipping `ADD_PUBLICATION` state** — the aeron C API for adding publications
   is async; you must start it in state 0 and await completion in state 1. Making
   it synchronous either blocks or races.

4. **C++ `throw std::runtime_error(aeron_errmsg())`** — must use the project macro
   `CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW` (define it analogously to
   `ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW` in `ArchiveExceptions.h`).

5. **C++ Context missing `setupContext()`** — without wiring C++ lambdas to C
   callbacks in the constructor, idle strategy and delegating invoker silently do
   nothing, breaking blocking `connect()` in multi-threaded environments.

6. **Missing getter API** — every `set_*` function needs a `get_*` counterpart.
   The C++ wrapper needs these to implement read-back methods like
   `ingressChannel()`. Without them the wrapper cannot be made complete.

7. **Missing env var support in `context_init`** — `AERON_DIR`, `AERON_CLUSTER_*`
   env vars must be read and applied in `context_init`, same as archive.
