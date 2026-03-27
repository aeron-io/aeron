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

#ifndef AERON_CLUSTER_INGRESS_PROXY_H
#define AERON_CLUSTER_INGRESS_PROXY_H

#include "aeron_cluster.h"
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif


#define AERON_CLUSTER_INGRESS_PROXY_SEND_ATTEMPTS_DEFAULT  3
#define AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH          (8 * 1024)

typedef struct aeron_cluster_ingress_proxy_stct
{
    aeron_publication_t           *publication;
    aeron_exclusive_publication_t *exclusive_publication;
    bool                           is_exclusive;
    int                            retry_attempts;
    uint8_t                        buffer[AERON_CLUSTER_INGRESS_PROXY_BUFFER_LENGTH];
}
aeron_cluster_ingress_proxy_t;

int aeron_cluster_ingress_proxy_init(
    aeron_cluster_ingress_proxy_t *proxy,
    aeron_publication_t *publication,
    int retry_attempts);

int aeron_cluster_ingress_proxy_init_exclusive(
    aeron_cluster_ingress_proxy_t *proxy,
    aeron_exclusive_publication_t *publication,
    int retry_attempts);

int64_t aeron_cluster_ingress_proxy_send_connect_request(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const aeron_cluster_encoded_credentials_t *encoded_credentials,
    const char *client_name);

int64_t aeron_cluster_ingress_proxy_send_close_session(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id);

int64_t aeron_cluster_ingress_proxy_send_keep_alive(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id);

int64_t aeron_cluster_ingress_proxy_send_challenge_response(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const aeron_cluster_encoded_credentials_t *credentials);

int64_t aeron_cluster_ingress_proxy_send_admin_request_snapshot(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t correlation_id);

/**
 * Send a generic admin request with an explicit request type.
 * request_type is one of the aeron_cluster_client_adminRequestType_* enum values cast to int32_t.
 */
int64_t aeron_cluster_ingress_proxy_send_admin_request(
    aeron_cluster_ingress_proxy_t *proxy,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t correlation_id,
    int32_t request_type);


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_INGRESS_PROXY_H */
