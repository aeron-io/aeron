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

#include <string.h>
#include <errno.h>
#include <stdint.h>

#include "aeron_cluster_cluster_session.h"
#include "aeron_cluster_egress_publisher.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_cluster_cluster_session_create(
    aeron_cluster_cluster_session_t **session,
    int64_t id, int64_t correlation_id,
    int32_t response_stream_id, const char *response_channel,
    const uint8_t *encoded_principal, size_t principal_length,
    aeron_t *aeron)
{
    aeron_cluster_cluster_session_t *s = NULL;
    if (aeron_alloc((void **)&s, sizeof(aeron_cluster_cluster_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate cluster session");
        return -1;
    }

    s->id                            = id;
    s->correlation_id                = correlation_id;
    s->opened_log_position           = 0;
    s->closed_log_position           = -1;
    s->time_of_last_activity_ns      = 0;
    s->response_stream_id            = response_stream_id;
    s->state                         = AERON_CLUSTER_SESSION_STATE_INIT;
    s->action                        = AERON_CLUSTER_SESSION_ACTION_CLIENT;
    s->close_reason                  = 0;
    s->event_code                    = 0;
    s->response_detail[0]            = '\0';
    s->request_input                 = INT64_MAX;
    s->has_open_event_pending        = false;
    s->has_new_leader_event_pending  = false;
    s->has_challenge_pending         = false;
    s->encoded_challenge             = NULL;
    s->encoded_challenge_length      = 0;
    s->response_publication          = NULL;
    s->async_response_pub            = NULL;
    s->aeron                         = aeron;

    const size_t ch_len = response_channel != NULL ? strlen(response_channel) + 1 : 1;
    if (aeron_alloc((void **)&s->response_channel, ch_len) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate session response_channel");
        aeron_free(s);
        return -1;
    }
    if (response_channel != NULL) { memcpy(s->response_channel, response_channel, ch_len); }
    else { s->response_channel[0] = '\0'; }

    if (principal_length > 0 && NULL != encoded_principal)
    {
        if (aeron_alloc((void **)&s->encoded_principal, principal_length) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to allocate session encoded_principal");
            aeron_free(s->response_channel);
            aeron_free(s);
            return -1;
        }
        memcpy(s->encoded_principal, encoded_principal, principal_length);
        s->encoded_principal_length = principal_length;
    }
    else
    {
        s->encoded_principal        = NULL;
        s->encoded_principal_length = 0;
    }

    *session = s;
    return 0;
}

int aeron_cluster_cluster_session_connect(aeron_cluster_cluster_session_t *session)
{
    if (NULL != session->response_publication) { return 0; }

    aeron_async_add_exclusive_publication_t *async_pub = NULL;
    if (aeron_async_add_exclusive_publication(
        &async_pub, session->aeron,
        session->response_channel, session->response_stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start add session response publication");
        return -1;
    }
    int rc = 0;
    do { rc = aeron_async_add_exclusive_publication_poll(&session->response_publication, async_pub); }
    while (0 == rc);
    if (rc < 0) { AERON_APPEND_ERR("%s", "failed to add session response publication"); return -1; }

    return 0;
}

int aeron_cluster_cluster_session_close_and_free(aeron_cluster_cluster_session_t *session)
{
    if (NULL != session)
    {
        if (NULL != session->response_publication)
        {
            aeron_exclusive_publication_close(session->response_publication, NULL, NULL);
        }
        aeron_free(session->response_channel);
        aeron_free(session->encoded_principal);
        aeron_free(session->encoded_challenge);
        aeron_free(session);
    }
    return 0;
}

bool aeron_cluster_cluster_session_is_timed_out(
    aeron_cluster_cluster_session_t *session,
    int64_t now_ns, int64_t session_timeout_ns)
{
    return session->state == AERON_CLUSTER_SESSION_STATE_OPEN &&
           (now_ns - session->time_of_last_activity_ns) > session_timeout_ns;
}

bool aeron_cluster_cluster_session_is_response_pub_connected(
    aeron_cluster_cluster_session_t *session)
{
    if (NULL == session->response_publication) { return false; }
    return aeron_exclusive_publication_is_connected(session->response_publication);
}

void aeron_cluster_cluster_session_reject(
    aeron_cluster_cluster_session_t *session,
    int32_t event_code,
    const char *detail)
{
    session->state      = AERON_CLUSTER_SESSION_STATE_REJECTED;
    session->event_code = event_code;
    if (NULL != detail)
    {
        strncpy(session->response_detail, detail, sizeof(session->response_detail) - 1);
        session->response_detail[sizeof(session->response_detail) - 1] = '\0';
    }
    else
    {
        session->response_detail[0] = '\0';
    }
}

void aeron_cluster_cluster_session_set_redirect(
    aeron_cluster_cluster_session_t *session,
    const char *ingress_endpoints)
{
    /* EventCode: REDIRECT = 2 */
    session->event_code = 2;
    if (NULL != ingress_endpoints)
    {
        strncpy(session->response_detail, ingress_endpoints, sizeof(session->response_detail) - 1);
        session->response_detail[sizeof(session->response_detail) - 1] = '\0';
    }
    else
    {
        session->response_detail[0] = '\0';
    }
}

void aeron_cluster_cluster_session_authenticate(
    aeron_cluster_cluster_session_t *session)
{
    session->state = AERON_CLUSTER_SESSION_STATE_AUTHENTICATED;
}

void aeron_cluster_cluster_session_challenge(
    aeron_cluster_cluster_session_t *session,
    const uint8_t *encoded_challenge,
    size_t challenge_length)
{
    /* Free any previous challenge data */
    aeron_free(session->encoded_challenge);
    session->encoded_challenge        = NULL;
    session->encoded_challenge_length = 0;

    if (challenge_length > 0 && NULL != encoded_challenge)
    {
        if (aeron_alloc((void **)&session->encoded_challenge, challenge_length) < 0)
        {
            return;
        }
        memcpy(session->encoded_challenge, encoded_challenge, challenge_length);
        session->encoded_challenge_length = challenge_length;
    }

    session->state                = AERON_CLUSTER_SESSION_STATE_CHALLENGED;
    session->has_challenge_pending = true;
}

void aeron_cluster_cluster_session_open(
    aeron_cluster_cluster_session_t *session,
    int64_t log_position)
{
    session->state               = AERON_CLUSTER_SESSION_STATE_OPEN;
    session->opened_log_position = log_position;
}

void aeron_cluster_cluster_session_closing(
    aeron_cluster_cluster_session_t *session,
    int32_t close_reason)
{
    session->state        = AERON_CLUSTER_SESSION_STATE_CLOSING;
    session->close_reason = close_reason;
}

int64_t aeron_cluster_cluster_session_send_event(
    aeron_cluster_cluster_session_t *session,
    int64_t correlation_id, int64_t leadership_term_id,
    int32_t leader_member_id, int32_t event_code,
    int64_t leader_heartbeat_timeout_ns,
    const char *detail, size_t detail_length)
{
    if (NULL == session->response_publication) { return AERON_PUBLICATION_ERROR; }

    bool sent = aeron_cluster_egress_publisher_send_session_event(
        session->response_publication,
        session->id,
        correlation_id,
        leadership_term_id,
        leader_member_id,
        event_code,
        leader_heartbeat_timeout_ns,
        detail,
        detail_length);

    return sent ? 1 : AERON_PUBLICATION_BACK_PRESSURED;
}
