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

#ifndef AERON_CLUSTER_SERVICE_H
#define AERON_CLUSTER_SERVICE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* Forward declarations */
typedef struct aeron_clustered_service_agent_stct aeron_cluster_t;
typedef struct aeron_cluster_client_session_stct  aeron_cluster_client_session_t;
typedef struct aeron_cluster_snapshot_image_stct  aeron_cluster_snapshot_image_t;

/* -----------------------------------------------------------------------
 * Cluster role — matches Java Cluster.Role
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_role_en
{
    AERON_CLUSTER_ROLE_FOLLOWER  = 0,
    AERON_CLUSTER_ROLE_CANDIDATE = 1,
    AERON_CLUSTER_ROLE_LEADER    = 2,
}
aeron_cluster_role_t;

/* -----------------------------------------------------------------------
 * CloseReason — matches Java CloseReason
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_close_reason_en
{
    AERON_CLUSTER_CLOSE_REASON_CLIENT_ACTION = 0,
    AERON_CLUSTER_CLOSE_REASON_TIMEOUT       = 1,
    AERON_CLUSTER_CLOSE_REASON_SERVICE_ACTION = 2,
    AERON_CLUSTER_CLOSE_REASON_NULL_VALUE    = INT32_MIN
}
aeron_cluster_close_reason_t;

/* -----------------------------------------------------------------------
 * ClusteredService — function pointer interface that the user implements.
 *
 * All callbacks are invoked on the duty-cycle thread; must not block.
 * ----------------------------------------------------------------------- */
typedef struct aeron_clustered_service_stct
{
    /**
     * Called once when the service starts (or restarts after failover).
     * If snapshot_image is non-NULL, the service must load its state from it.
     */
    void (*on_start)(void *clientd, aeron_cluster_t *cluster,
                     aeron_cluster_snapshot_image_t *snapshot_image);

    void (*on_session_open)(void *clientd,
                            aeron_cluster_client_session_t *session,
                            int64_t timestamp);

    void (*on_session_close)(void *clientd,
                             aeron_cluster_client_session_t *session,
                             int64_t timestamp,
                             aeron_cluster_close_reason_t close_reason);

    void (*on_session_message)(void *clientd,
                               aeron_cluster_client_session_t *session,
                               int64_t timestamp,
                               const uint8_t *buffer,
                               size_t length);

    void (*on_timer_event)(void *clientd,
                           int64_t correlation_id,
                           int64_t timestamp);

    /**
     * Take a snapshot.  Write service state to snapshot_publication.
     * Use aeron_cluster_service_snapshot_taker_* helpers for framing.
     */
    void (*on_take_snapshot)(void *clientd,
                             aeron_exclusive_publication_t *snapshot_publication);

    void (*on_role_change)(void *clientd, aeron_cluster_role_t new_role);

    void (*on_terminate)(void *clientd, aeron_cluster_t *cluster);

    /* Optional — set to NULL if not needed */
    void (*on_new_leadership_term_event)(void *clientd,
                                         int64_t leadership_term_id,
                                         int64_t log_position,
                                         int64_t timestamp,
                                         int64_t term_base_log_position,
                                         int32_t leader_member_id,
                                         int32_t log_session_id,
                                         int32_t app_version);

    int  (*do_background_work)(void *clientd, int64_t now_ns);

    void *clientd;
}
aeron_clustered_service_t;

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_H */
