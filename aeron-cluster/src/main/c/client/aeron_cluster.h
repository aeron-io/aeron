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

#ifndef AERON_CLUSTER_H
#define AERON_CLUSTER_H

#ifdef __cplusplus
extern "C"
{
#endif

#include "aeronc.h"
#include "aeron_common.h"

typedef struct aeron_cluster_stct aeron_cluster_t;
typedef struct aeron_cluster_context_stct aeron_cluster_context_t;
typedef struct aeron_cluster_async_connect_stct aeron_cluster_async_connect_t;

typedef struct aeron_cluster_encoded_credentials_stct
{
    const char *data;
    uint32_t    length;
}
aeron_cluster_encoded_credentials_t;

/**
 * Callback returning encoded credentials for the initial connect request.
 */
typedef aeron_cluster_encoded_credentials_t *(*aeron_cluster_credentials_encoded_credentials_supplier_func_t)(
    void *clientd);

/**
 * Callback returning credentials in response to a challenge from the cluster.
 */
typedef aeron_cluster_encoded_credentials_t *(*aeron_cluster_credentials_challenge_supplier_func_t)(
    aeron_cluster_encoded_credentials_t *encoded_challenge,
    void *clientd);

/**
 * Callback to free credentials returned by the supplier callbacks.
 */
typedef void (*aeron_cluster_credentials_free_func_t)(
    aeron_cluster_encoded_credentials_t *credentials,
    void *clientd);

/**
 * Callback for application egress messages received from the cluster.
 *
 * @param clientd           user-supplied context
 * @param cluster_session_id session that produced this message
 * @param leadership_term_id leadership term when the message was dispatched
 * @param timestamp          cluster time at dispatch (ns)
 * @param buffer             payload buffer (does NOT include the session header)
 * @param length             payload length in bytes
 * @param header             underlying Aeron fragment header
 */
typedef void (*aeron_cluster_egress_message_listener_func_t)(
    void *clientd,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t timestamp,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header);

/**
 * Callback for session lifecycle events (OPEN, CLOSED, TIMED_OUT).
 */
typedef void (*aeron_cluster_session_event_listener_func_t)(
    void *clientd,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int32_t event_code,
    const char *detail,
    size_t detail_length);

/**
 * Callback fired when the cluster elects a new leader and the client reconnects.
 */
typedef void (*aeron_cluster_new_leader_event_listener_func_t)(
    void *clientd,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints,
    size_t ingress_endpoints_length);

/**
 * Callback for admin response messages (e.g. snapshot acknowledgements).
 */
typedef void (*aeron_cluster_admin_response_listener_func_t)(
    void *clientd,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int32_t request_type,
    int32_t response_code,
    const char *message,
    size_t message_length,
    const uint8_t *payload,
    size_t payload_length);

/**
 * Delegating invoker — called on every idle cycle when using an agent invoker
 * instead of a background thread.
 */
typedef void (*aeron_cluster_delegating_invoker_func_t)(void *clientd);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_H */
