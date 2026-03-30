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
    AERON_CLUSTER_SESSION_STATE_CONNECTING  = 1,
    AERON_CLUSTER_SESSION_STATE_CONNECTED   = 2,
    AERON_CLUSTER_SESSION_STATE_CHALLENGED  = 3,
    AERON_CLUSTER_SESSION_STATE_AUTHENTICATED = 4,
    AERON_CLUSTER_SESSION_STATE_OPEN        = 5,
    AERON_CLUSTER_SESSION_STATE_CLOSING     = 6,
    AERON_CLUSTER_SESSION_STATE_CLOSED      = 7,
    AERON_CLUSTER_SESSION_STATE_REJECTED    = 8,
    AERON_CLUSTER_SESSION_STATE_INVALID     = 9,
}
aeron_cluster_session_state_t;

typedef enum aeron_cluster_session_action_en
{
    AERON_CLUSTER_SESSION_ACTION_CLIENT           = 0,
    AERON_CLUSTER_SESSION_ACTION_BACKUP           = 1,
    AERON_CLUSTER_SESSION_ACTION_HEARTBEAT        = 2,
    AERON_CLUSTER_SESSION_ACTION_STANDBY_SNAPSHOT = 3,
}
aeron_cluster_session_action_t;

typedef struct aeron_cluster_cluster_session_stct
{
    int64_t  id;               /* clusterSessionId */
    int64_t  correlation_id;
    int64_t  opened_log_position;
    int64_t  closed_log_position;
    int64_t  time_of_last_activity_ns;
    int32_t  response_stream_id;
    char    *response_channel;
    uint8_t *encoded_principal;
    size_t   encoded_principal_length;

    aeron_cluster_session_state_t  state;
    aeron_cluster_session_action_t action;
    int32_t  close_reason;    /* aeron_cluster_close_reason_t */
    int32_t  event_code;      /* EventCode for rejection/redirect */
    char     response_detail[256];

    int64_t  request_input;            /* for BACKUP sessions: log position from backup query */

    bool     has_open_event_pending;
    bool     has_new_leader_event_pending;
    bool     has_challenge_pending;       /* true when challenge data needs sending */

    /* Challenge data — set by aeron_cluster_cluster_session_challenge(),
     * sent by process_pending_sessions, then freed. */
    uint8_t *encoded_challenge;
    size_t   encoded_challenge_length;

    /* Egress publication to reply to this client */
    aeron_exclusive_publication_t            *response_publication;
    aeron_async_add_exclusive_publication_t  *async_response_pub; /* in-flight registration */
    aeron_t                                  *aeron;
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

bool aeron_cluster_cluster_session_is_response_pub_connected(
    aeron_cluster_cluster_session_t *session);

/** Mark session as rejected with an event code and detail string. */
void aeron_cluster_cluster_session_reject(
    aeron_cluster_cluster_session_t *session,
    int32_t event_code,
    const char *detail);

/** Mark session as needing a redirect (non-leader). */
void aeron_cluster_cluster_session_set_redirect(
    aeron_cluster_cluster_session_t *session,
    const char *ingress_endpoints);

/** Mark session as authenticated (no-challenge case). */
void aeron_cluster_cluster_session_authenticate(
    aeron_cluster_cluster_session_t *session);

/** Issue a challenge: stores the challenge data and transitions to CHALLENGED state.
 *  The challenge is sent to the client by process_pending_sessions. */
void aeron_cluster_cluster_session_challenge(
    aeron_cluster_cluster_session_t *session,
    const uint8_t *encoded_challenge,
    size_t challenge_length);

/** Transition to OPEN after log commit, record log position. */
void aeron_cluster_cluster_session_open(
    aeron_cluster_cluster_session_t *session,
    int64_t log_position);

/** Transition to CLOSING with a close reason. */
void aeron_cluster_cluster_session_closing(
    aeron_cluster_cluster_session_t *session,
    int32_t close_reason);

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
