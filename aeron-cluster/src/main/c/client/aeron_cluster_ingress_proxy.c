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

#include "aeron_cluster.h"
#include "aeron_cluster_ingress_proxy.h"
#include "aeron_cluster_configuration.h"

/* Generated C codecs */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionConnectRequest.h"
#include "aeron_cluster_client/sessionCloseRequest.h"
#include "aeron_cluster_client/sessionKeepAlive.h"
#include "aeron_cluster_client/challengeResponse.h"
#include "aeron_cluster_client/adminRequest.h"
#include "aeron_cluster_client/adminRequestType.h"

/* -----------------------------------------------------------------------
 * Internal: offer buffer via shared or exclusive publication.
 * Retries up to retry_attempts times on BACK_PRESSURED.
 * ----------------------------------------------------------------------- */
static int64_t ingress_proxy_offer(
    aeron_cluster_ingress_proxy_t *proxy,
    const uint8_t *buffer,
    size_t length)
{
    int64_t result = AERON_PUBLICATION_CLOSED;

    for (int i = 0; i < proxy->retry_attempts; i++)
    {
        if (proxy->is_exclusive)
        {
            result = aeron_exclusive_publication_offer(
                proxy->exclusive_publication, buffer, length, NULL, NULL);
        }
        else
        {
            result = aeron_publication_offer(
                proxy->publication, buffer, length, NULL, NULL);
        }

        if (result > 0 || AERON_PUBLICATION_CLOSED == result || AERON_PUBLICATION_MAX_POSITION_EXCEEDED == result)
        {
            break;
        }
    }

    return result;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */
int aeron_cluster_ingress_proxy_init(
    aeron_cluster_ingress_proxy_t *proxy,
    aeron_publication_t *publication,
    int retry_attempts)
{
    proxy->publication           = publication;
    proxy->exclusive_publication = NULL;
    proxy->is_exclusive          = false;
    proxy->retry_attempts        = retry_attempts;
    memset(proxy->buffer, 0, sizeof(proxy->buffer));
    return 0;
}

int aeron_cluster_ingress_proxy_init_exclusive(
    aeron_cluster_ingress_proxy_t *proxy,
    aeron_exclusive_publication_t *publication,
    int retry_attempts)
{
    proxy->publication           = NULL;
    proxy->exclusive_publication = publication;
    proxy->is_exclusive          = true;
    proxy->retry_attempts        = retry_attempts;
    memset(proxy->buffer, 0, sizeof(proxy->buffer));
    return 0;
}

int64_t aeron_cluster_ingress_proxy_send_connect_request(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const aeron_cluster_encoded_credentials_t *encoded_credentials,
    const char *client_name)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionConnectRequest msg;

    if (NULL == aeron_cluster_client_sessionConnectRequest_wrap_and_apply_header(
        &msg,
        (char *)proxy->buffer,
        0,
        sizeof(proxy->buffer),
        &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionConnectRequest_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_sessionConnectRequest_set_responseStreamId(&msg, response_stream_id);
    aeron_cluster_client_sessionConnectRequest_set_version(&msg, aeron_cluster_semantic_version());

    const char *channel     = response_channel != NULL ? response_channel : "";
    const size_t channel_len = strlen(channel);
    aeron_cluster_client_sessionConnectRequest_put_responseChannel(&msg, channel, (uint32_t)channel_len);

    const char   *cred_data = (encoded_credentials != NULL && encoded_credentials->data != NULL)
        ? encoded_credentials->data : "";
    const uint32_t cred_len = (encoded_credentials != NULL) ? encoded_credentials->length : 0;
    aeron_cluster_client_sessionConnectRequest_put_encodedCredentials(&msg, cred_data, cred_len);

    const char   *name     = (client_name != NULL) ? client_name : "";
    const uint32_t name_len = (uint32_t)strlen(name);
    aeron_cluster_client_sessionConnectRequest_put_clientInfo(&msg, name, name_len);

    return ingress_proxy_offer(
        proxy,
        proxy->buffer,
        aeron_cluster_client_sessionConnectRequest_encoded_length(&msg));
}

int64_t aeron_cluster_ingress_proxy_send_close_session(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionCloseRequest msg;

    if (NULL == aeron_cluster_client_sessionCloseRequest_wrap_and_apply_header(
        &msg,
        (char *)proxy->buffer,
        0,
        sizeof(proxy->buffer),
        &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionCloseRequest_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionCloseRequest_set_clusterSessionId(&msg, cluster_session_id);

    return ingress_proxy_offer(
        proxy,
        proxy->buffer,
        aeron_cluster_client_sessionCloseRequest_encoded_length(&msg));
}

int64_t aeron_cluster_ingress_proxy_send_keep_alive(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionKeepAlive msg;

    if (NULL == aeron_cluster_client_sessionKeepAlive_wrap_and_apply_header(
        &msg,
        (char *)proxy->buffer,
        0,
        sizeof(proxy->buffer),
        &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionKeepAlive_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionKeepAlive_set_clusterSessionId(&msg, cluster_session_id);

    return ingress_proxy_offer(
        proxy,
        proxy->buffer,
        aeron_cluster_client_sessionKeepAlive_encoded_length(&msg));
}

int64_t aeron_cluster_ingress_proxy_send_challenge_response(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const aeron_cluster_encoded_credentials_t *credentials)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_challengeResponse msg;

    if (NULL == aeron_cluster_client_challengeResponse_wrap_and_apply_header(
        &msg,
        (char *)proxy->buffer,
        0,
        sizeof(proxy->buffer),
        &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_challengeResponse_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_challengeResponse_set_clusterSessionId(&msg, cluster_session_id);

    const char   *cred_data = (credentials != NULL && credentials->data != NULL) ? credentials->data : "";
    const uint32_t cred_len = (credentials != NULL) ? credentials->length : 0;
    aeron_cluster_client_challengeResponse_put_encodedCredentials(&msg, cred_data, cred_len);

    return ingress_proxy_offer(
        proxy,
        proxy->buffer,
        aeron_cluster_client_challengeResponse_encoded_length(&msg));
}

int64_t aeron_cluster_ingress_proxy_send_admin_request_snapshot(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t correlation_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_adminRequest msg;

    if (NULL == aeron_cluster_client_adminRequest_wrap_and_apply_header(
        &msg,
        (char *)proxy->buffer,
        0,
        sizeof(proxy->buffer),
        &hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_adminRequest_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_adminRequest_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_adminRequest_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_adminRequest_set_requestType(&msg, aeron_cluster_client_adminRequestType_SNAPSHOT);
    aeron_cluster_client_adminRequest_put_payload(&msg, NULL, 0);

    return ingress_proxy_offer(
        proxy,
        proxy->buffer,
        aeron_cluster_client_adminRequest_encoded_length(&msg));
}
