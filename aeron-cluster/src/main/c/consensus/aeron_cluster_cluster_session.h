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

#ifndef AERON_CLUSTER_CLUSTER_SESSION_H
#define AERON_CLUSTER_CLUSTER_SESSION_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef enum aeron_cluster_session_state_en
{
    AERON_CLUSTER_SESSION_STATE_INIT        = 0,
    AERON_CLUSTER_SESSION_STATE_CONNECTED   = 1,
    AERON_CLUSTER_SESSION_STATE_CHALLENGED  = 2,
    AERON_CLUSTER_SESSION_STATE_AUTHENTICATED = 3,
    AERON_CLUSTER_SESSION_STATE_OPEN        = 4,
    AERON_CLUSTER_SESSION_STATE_CLOSING     = 5,
    AERON_CLUSTER_SESSION_STATE_CLOSED      = 6,
}
aeron_cluster_session_state_t;

typedef struct aeron_cluster_cluster_session_stct
{
    int64_t  id;               /* clusterSessionId */
    int64_t  correlation_id;
    int64_t  opened_log_position;
    int64_t  time_of_last_activity_ns;
    int32_t  response_stream_id;
    char    *response_channel;
    uint8_t *encoded_principal;
    size_t   encoded_principal_length;

    aeron_cluster_session_state_t state;
    int32_t  close_reason;    /* aeron_cluster_close_reason_t */

    /* Egress publication to reply to this client */
    aeron_exclusive_publication_t *response_publication;
    aeron_t                       *aeron;
}
aeron_cluster_cluster_session_t;

int  aeron_cluster_cluster_session_create(
    aeron_cluster_cluster_session_t **session,
    int64_t id, int64_t correlation_id,
    int32_t response_stream_id, const char *response_channel,
    const uint8_t *encoded_principal, size_t principal_length,
    aeron_t *aeron);

int  aeron_cluster_cluster_session_close_and_free(aeron_cluster_cluster_session_t *session);

/** Open the response publication (called when session is authenticated). */
int  aeron_cluster_cluster_session_connect(aeron_cluster_cluster_session_t *session);

bool aeron_cluster_cluster_session_is_timed_out(
    aeron_cluster_cluster_session_t *session,
    int64_t now_ns, int64_t session_timeout_ns);

int64_t aeron_cluster_cluster_session_send_event(
    aeron_cluster_cluster_session_t *session,
    int64_t correlation_id, int64_t leadership_term_id,
    int32_t leader_member_id, int32_t event_code,
    int64_t leader_heartbeat_timeout_ns,
    const char *detail, size_t detail_length);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CLUSTER_SESSION_H */
