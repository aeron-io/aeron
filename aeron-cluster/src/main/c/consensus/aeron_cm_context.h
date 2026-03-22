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

    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    bool                       owns_idle_strategy;

    aeron_error_handler_t error_handler;
    void                 *error_handler_clientd;
}
aeron_cm_context_t;

int  aeron_cm_context_init(aeron_cm_context_t **ctx);
int  aeron_cm_context_close(aeron_cm_context_t *ctx);
int  aeron_cm_context_conclude(aeron_cm_context_t *ctx);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CM_CONTEXT_H */
