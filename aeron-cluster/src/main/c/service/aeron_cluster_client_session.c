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

#include "aeron_cluster_client_session.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* Client-side codec for session header (same as client ingress) */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

#define SESSION_HEADER_LENGTH 32  /* same as AERON_CLUSTER_SESSION_HEADER_LENGTH */

int aeron_cluster_client_session_create(
    aeron_cluster_client_session_t **session,
    int64_t cluster_session_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t encoded_principal_length,
    aeron_t *aeron)
{
    aeron_cluster_client_session_t *s = NULL;
    if (aeron_alloc((void **)&s, sizeof(aeron_cluster_client_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate client session");
        return -1;
    }

    s->cluster_session_id = cluster_session_id;
    s->response_stream_id = response_stream_id;
    s->response_publication = NULL;
    s->is_closing = false;
    s->is_open    = true;
    s->aeron      = aeron;

    const size_t ch_len = response_channel != NULL ? strlen(response_channel) + 1 : 1;
    if (aeron_alloc((void **)&s->response_channel, ch_len) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate session response_channel");
        aeron_free(s);
        return -1;
    }
    if (response_channel != NULL)
    {
        memcpy(s->response_channel, response_channel, ch_len);
    }
    else
    {
        s->response_channel[0] = '\0';
    }

    if (encoded_principal_length > 0 && NULL != encoded_principal)
    {
        if (aeron_alloc((void **)&s->encoded_principal, encoded_principal_length) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to allocate session encoded_principal");
            aeron_free(s->response_channel);
            aeron_free(s);
            return -1;
        }
        memcpy(s->encoded_principal, encoded_principal, encoded_principal_length);
        s->encoded_principal_length = encoded_principal_length;
    }
    else
    {
        s->encoded_principal = NULL;
        s->encoded_principal_length = 0;
    }

    *session = s;
    return 0;
}

int aeron_cluster_client_session_connect(aeron_cluster_client_session_t *session)
{
    if (NULL != session->response_publication)
    {
        return 0;
    }

    aeron_async_add_publication_t *async = NULL;
    if (aeron_async_add_publication(
        &async,
        session->aeron,
        session->response_channel,
        session->response_stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start add session response publication");
        return -1;
    }

    int rc = 0;
    do
    {
        rc = aeron_async_add_publication_poll(&session->response_publication, async);
    }
    while (0 == rc);

    if (rc < 0)
    {
        AERON_APPEND_ERR("%s", "failed to add session response publication");
        return -1;
    }

    return 0;
}

int aeron_cluster_client_session_close_and_free(aeron_cluster_client_session_t *session)
{
    if (NULL != session)
    {
        if (NULL != session->response_publication)
        {
            aeron_publication_close(session->response_publication, NULL, NULL);
        }
        aeron_free(session->response_channel);
        aeron_free(session->encoded_principal);
        aeron_free(session);
    }
    return 0;
}

int64_t aeron_cluster_client_session_offer(
    aeron_cluster_client_session_t *session,
    int64_t leadership_term_id,
    const uint8_t *buffer,
    size_t length)
{
    if (NULL == session->response_publication)
    {
        if (aeron_cluster_client_session_connect(session) < 0)
        {
            return AERON_PUBLICATION_ERROR;
        }
    }

    /* Build the 32-byte session header */
    uint8_t hdr_buf[SESSION_HEADER_LENGTH];
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;

    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, sizeof(hdr_buf), &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, session->cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);

    aeron_iovec_t vectors[2];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = SESSION_HEADER_LENGTH;
    vectors[1].iov_base = (uint8_t *)buffer;
    vectors[1].iov_len  = length;

    return aeron_publication_offerv(session->response_publication, vectors, 2, NULL, NULL);
}

int64_t aeron_cluster_client_session_id(aeron_cluster_client_session_t *s)         { return s->cluster_session_id; }
int32_t aeron_cluster_client_session_response_stream_id(aeron_cluster_client_session_t *s) { return s->response_stream_id; }
const char *aeron_cluster_client_session_response_channel(aeron_cluster_client_session_t *s) { return s->response_channel; }
const uint8_t *aeron_cluster_client_session_encoded_principal(aeron_cluster_client_session_t *s) { return s->encoded_principal; }
size_t aeron_cluster_client_session_encoded_principal_length(aeron_cluster_client_session_t *s) { return s->encoded_principal_length; }
bool aeron_cluster_client_session_is_closing(aeron_cluster_client_session_t *s) { return s->is_closing; }
void aeron_cluster_client_session_mark_closing(aeron_cluster_client_session_t *s) { s->is_closing = true; }
void aeron_cluster_client_session_reset_closing(aeron_cluster_client_session_t *s) { s->is_closing = false; }
