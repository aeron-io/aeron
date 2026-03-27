/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef AERON_CM_CONTEXT_H
#define AERON_CM_CONTEXT_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_consensus_module_configuration.h"
#include "aeron_cluster_mark_file.h"

/* -----------------------------------------------------------------------
 * Counter type IDs (match Java AeronCounters constants)
 * ----------------------------------------------------------------------- */
#define AERON_CM_COUNTER_CONSENSUS_MODULE_STATE_TYPE_ID    200
#define AERON_CM_COUNTER_NODE_ROLE_TYPE_ID                 201
#define AERON_CM_COUNTER_CONTROL_TOGGLE_TYPE_ID            202
#define AERON_CM_COUNTER_COMMIT_POSITION_TYPE_ID           203
#define AERON_CM_COUNTER_SNAPSHOT_TYPE_ID                  205
#define AERON_CM_COUNTER_ELECTION_STATE_TYPE_ID            207
#define AERON_CM_COUNTER_ERROR_COUNT_TYPE_ID               212
#define AERON_CM_COUNTER_CLIENT_TIMEOUT_TYPE_ID            213
#define AERON_CM_COUNTER_NODE_CONTROL_TOGGLE_TYPE_ID       233
#define AERON_CM_COUNTER_ELECTION_COUNT_TYPE_ID            238
#define AERON_CM_COUNTER_LEADERSHIP_TERM_ID_TYPE_ID        239

/**
 * Lightweight mock/injectable counter for unit testing.
 * In production code, a real aeron_counter_t provides the value pointer;
 * for unit tests, inject type_id directly to validate conclude() logic.
 */
typedef struct aeron_cm_counter_stct
{
    int32_t  type_id;   /* -1 = not set */
    int64_t  value;
    bool     is_set;    /* true if explicitly provided (to validate type_id) */
}
aeron_cm_counter_t;
#include "aeron_archive.h"
#include "aeron_archive_context.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef int64_t (*aeron_cluster_clock_func_t)(void *clientd);

typedef struct aeron_cm_context_stct
{
    aeron_t  *aeron;
    char      aeron_directory_name[AERON_MAX_PATH];
    bool      owns_aeron_client;

    int32_t   member_id;
    int32_t   appointed_leader_id;
    int       service_count;
    int32_t   app_version;

    /* Channels and stream IDs */
    char     *log_channel;
    int32_t   log_stream_id;
    char     *ingress_channel;
    int32_t   ingress_stream_id;
    char     *consensus_channel;
    int32_t   consensus_stream_id;
    char     *control_channel;       /* IPC: CM ↔ service */
    int32_t   consensus_module_stream_id;  /* CM ← service */
    int32_t   service_stream_id;           /* CM → service */
    char     *snapshot_channel;
    int32_t   snapshot_stream_id;

    /* Cluster topology */
    char     *cluster_members;    /* "id,ep:ep:ep:ep:ep|..." */
    char      cluster_dir[AERON_MAX_PATH];

    /* Timeouts */
    int64_t   session_timeout_ns;
    int64_t   leader_heartbeat_timeout_ns;
    int64_t   leader_heartbeat_interval_ns;
    int64_t   startup_canvass_timeout_ns;
    int64_t   election_timeout_ns;
    int64_t   election_status_interval_ns;
    int64_t   termination_timeout_ns;

    /* Optional cluster clock (defaults to aeron_nano_clock) */
    aeron_cluster_clock_func_t cluster_clock_ns;
    void                      *cluster_clock_clientd;

    /* Archive (IPC, same host) — for log recording and snapshot */
    aeron_archive_context_t   *archive_ctx;
    bool                       owns_archive_ctx;

    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    bool                       owns_idle_strategy;

    aeron_error_handler_t error_handler;
    void                 *error_handler_clientd;

    /* Injectable counters for validation in conclude().
     * Set type_id to the expected value; conclude() checks it matches. */
    aeron_cm_counter_t  module_state_counter;
    aeron_cm_counter_t  election_state_counter;
    aeron_cm_counter_t  election_counter;
    aeron_cm_counter_t  leadership_term_id_counter;
    aeron_cm_counter_t  cluster_node_role_counter;
    aeron_cm_counter_t  commit_position_counter;
    aeron_cm_counter_t  control_toggle_counter;
    aeron_cm_counter_t  node_control_toggle_counter;
    aeron_cm_counter_t  snapshot_counter;
    aeron_cm_counter_t  timed_out_client_counter;

    /* max concurrent sessions (0 = unlimited) */
    int32_t  max_concurrent_sessions;

    /* Agent role name (e.g. "consensus-module-<clusterId>-<memberId>") */
    char     agent_role_name[256];

    /* Mark file (created/checked in conclude()) */
    aeron_cluster_mark_file_t *mark_file;
    bool                       owns_mark_file;
    char                       mark_file_dir[AERON_MAX_PATH];
    int64_t                    mark_file_timeout_ms;

    /**
     * Authenticator function pointers.
     * Default: NULL = accept all (NullAuthenticator).
     * Equivalent to Java's AuthenticatorSupplier → Authenticator.
     */
    bool (*authenticate)(
        void *clientd,
        int64_t cluster_session_id,
        const uint8_t *encoded_credentials,
        size_t credentials_length);
    void (*on_challenge_response)(
        void *clientd,
        int64_t cluster_session_id,
        const uint8_t *encoded_response,
        size_t response_length);
    void *authenticator_clientd;
    char  authenticator_supplier_class_name[256];

    /**
     * ConsensusModuleExtension — optional plugin hooks.
     * All function pointers are NULL by default (no extension).
     * Mirrors Java ConsensusModuleExtension interface.
     */
    struct
    {
        void (*on_start)(void *clientd);
        int  (*do_work)(void *clientd, int64_t now_ns);
        int  (*slow_tick_work)(void *clientd, int64_t now_ns);
        void (*on_election_complete)(void *clientd);
        void (*on_new_leadership_term)(void *clientd);
        void (*on_session_opened)(void *clientd, int64_t cluster_session_id);
        void (*on_session_closed)(void *clientd, int64_t cluster_session_id, int32_t close_reason);
        void (*on_prepare_for_new_leadership)(void *clientd);
        void (*on_take_snapshot)(void *clientd, aeron_exclusive_publication_t *snapshot_pub);
        void  *clientd;
    }
    extension;
}
aeron_cm_context_t;

int  aeron_cm_context_init(aeron_cm_context_t **ctx);
int  aeron_cm_context_close(aeron_cm_context_t *ctx);
int  aeron_cm_context_conclude(aeron_cm_context_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CM_CONTEXT_H */
