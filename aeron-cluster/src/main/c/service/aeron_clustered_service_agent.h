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

#ifndef AERON_CLUSTERED_SERVICE_AGENT_H
#define AERON_CLUSTERED_SERVICE_AGENT_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_cluster_service.h"
#include "aeron_cluster_service_context.h"
#include "aeron_cluster_client_session.h"
#include "aeron_cluster_consensus_module_proxy.h"
#include "aeron_cluster_service_adapter.h"
#include "aeron_cluster_bounded_log_adapter.h"
#include "aeron_cluster_service_snapshot_taker.h"
#include "aeron_cluster_service_snapshot_loader.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Session-array growth constants */
#define AERON_CLUSTER_SESSIONS_INITIAL_CAPACITY 8

struct aeron_clustered_service_agent_stct
{
    aeron_cluster_service_context_t        *ctx;
    aeron_t                                *aeron;

    aeron_subscription_t                   *control_subscription;
    aeron_exclusive_publication_t          *service_publication;

    aeron_cluster_consensus_module_proxy_t  consensus_module_proxy;
    aeron_cluster_service_adapter_t        *service_adapter;
    aeron_cluster_bounded_log_adapter_t    *log_adapter;

    /* Client sessions */
    aeron_cluster_client_session_t        **sessions;
    size_t                                  sessions_count;
    size_t                                  sessions_capacity;

    /* Cluster state */
    int32_t  member_id;
    int64_t  cluster_time;
    int64_t  log_position;
    int64_t  max_log_position;
    int64_t  leadership_term_id;
    int64_t  ack_id;
    int64_t  termination_position;
    int32_t  role;                    /* aeron_cluster_role_t */
    bool     is_service_active;

    /* Commit position counter */
    aeron_counters_reader_t               *counters_reader;
    int32_t                                commit_position_counter_id;

    /* Snapshot state (transient during on_service_action) */
    int64_t  snapshot_log_position;
    int64_t  snapshot_leadership_term_id;
};

typedef struct aeron_clustered_service_agent_stct aeron_clustered_service_agent_t;

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int  aeron_clustered_service_agent_create(
    aeron_clustered_service_agent_t **agent,
    aeron_cluster_service_context_t *ctx);

int  aeron_clustered_service_agent_close(aeron_clustered_service_agent_t *agent);

int  aeron_clustered_service_agent_on_start(aeron_clustered_service_agent_t *agent);

/**
 * Main duty cycle.  Returns > 0 if work was done, 0 if idle.
 */
int  aeron_clustered_service_agent_do_work(aeron_clustered_service_agent_t *agent,
                                            int64_t now_ns);

/* -----------------------------------------------------------------------
 * Cluster API — called by user via aeron_cluster_t* (which is the agent)
 * ----------------------------------------------------------------------- */
int32_t aeron_cluster_member_id(aeron_cluster_t *cluster);
int64_t aeron_cluster_log_position(aeron_cluster_t *cluster);
int64_t aeron_cluster_time(aeron_cluster_t *cluster);
int32_t aeron_cluster_role(aeron_cluster_t *cluster);

aeron_cluster_client_session_t *aeron_cluster_get_client_session(
    aeron_cluster_t *cluster, int64_t cluster_session_id);

bool    aeron_cluster_close_client_session(aeron_cluster_t *cluster,
            int64_t cluster_session_id);

bool    aeron_cluster_schedule_timer(aeron_cluster_t *cluster,
            int64_t correlation_id, int64_t deadline);

bool    aeron_cluster_cancel_timer(aeron_cluster_t *cluster,
            int64_t correlation_id);

int64_t aeron_cluster_offer(aeron_cluster_t *cluster,
            int64_t cluster_session_id,
            const uint8_t *buffer, size_t length);

/* -----------------------------------------------------------------------
 * Internal callbacks from ServiceAdapter and BoundedLogAdapter
 * ----------------------------------------------------------------------- */
void aeron_clustered_service_agent_on_join_log(
    aeron_clustered_service_agent_t *agent,
    int64_t log_position, int64_t max_log_position,
    int32_t member_id, int32_t log_session_id, int32_t log_stream_id,
    bool is_startup, aeron_cluster_role_t role,
    const char *log_channel);

void aeron_clustered_service_agent_on_service_termination_position(
    aeron_clustered_service_agent_t *agent, int64_t log_position);

void aeron_clustered_service_agent_on_request_service_ack(
    aeron_clustered_service_agent_t *agent, int64_t log_position);

void aeron_clustered_service_agent_on_session_open(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t correlation_id,
    int64_t log_position, int64_t timestamp,
    int32_t response_stream_id, const char *response_channel,
    const uint8_t *encoded_principal, size_t principal_length);

void aeron_clustered_service_agent_on_session_close(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t timestamp,
    aeron_cluster_close_reason_t close_reason);

void aeron_clustered_service_agent_on_session_message(
    aeron_clustered_service_agent_t *agent,
    int64_t cluster_session_id, int64_t timestamp,
    const uint8_t *buffer, size_t length);

void aeron_clustered_service_agent_on_timer_event(
    aeron_clustered_service_agent_t *agent,
    int64_t correlation_id, int64_t timestamp);

void aeron_clustered_service_agent_on_service_action(
    aeron_clustered_service_agent_t *agent,
    int64_t log_position, int64_t timestamp,
    int32_t action, int32_t flags);

void aeron_clustered_service_agent_on_new_leadership_term_event(
    aeron_clustered_service_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int64_t term_base_log_position,
    int32_t leader_member_id, int32_t log_session_id,
    int32_t app_version);

void aeron_clustered_service_agent_on_membership_change(
    aeron_clustered_service_agent_t *agent,
    int64_t leadership_term_id, int64_t log_position,
    int64_t timestamp, int32_t member_id, int32_t change_type);

/* Snapshot header length constant used by log adapter */
#define AERON_CLUSTER_SESSION_HEADER_LENGTH 32

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTERED_SERVICE_AGENT_H */
