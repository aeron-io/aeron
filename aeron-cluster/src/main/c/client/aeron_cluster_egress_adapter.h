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

#ifndef AERON_CLUSTER_EGRESS_ADAPTER_H
#define AERON_CLUSTER_EGRESS_ADAPTER_H

#include <stdint.h>
#include <stddef.h>
#include "aeronc.h"
#include "aeron_cluster.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * EgressListener interface — callbacks dispatched by EgressAdapter.
 * Mirror of Java EgressListener.
 */
typedef struct aeron_cluster_egress_listener_stct
{
    /** Application message received from the cluster. */
    void (*on_message)(
        void *clientd,
        int64_t cluster_session_id,
        int64_t leadership_term_id,
        int64_t timestamp,
        const uint8_t *buffer, size_t length,
        aeron_header_t *header);

    /** Session lifecycle event (OPEN, CLOSED, TIMED_OUT, REDIRECT). */
    void (*on_session_event)(
        void *clientd,
        int64_t correlation_id,
        int64_t cluster_session_id,
        int64_t leadership_term_id,
        int32_t leader_member_id,
        int32_t event_code,
        const char *detail, size_t detail_length);

    /** New leader elected. */
    void (*on_new_leader)(
        void *clientd,
        int64_t cluster_session_id,
        int64_t leadership_term_id,
        int32_t leader_member_id,
        const char *ingress_endpoints, size_t ingress_endpoints_length);

    /** Admin response (e.g. snapshot ack). */
    void (*on_admin_response)(
        void *clientd,
        int64_t cluster_session_id,
        int64_t correlation_id,
        int32_t request_type,
        int32_t response_code,
        const char *message, size_t message_length,
        const uint8_t *payload, size_t payload_length);

    void *clientd;
}
aeron_cluster_egress_listener_t;

#define AERON_CLUSTER_EGRESS_ADAPTER_FRAGMENT_LIMIT 10

typedef struct aeron_cluster_egress_adapter_stct
{
    aeron_subscription_t               *subscription;
    aeron_cluster_egress_listener_t    *listener;
    aeron_fragment_assembler_t         *assembler;
    int64_t                             cluster_session_id;
}
aeron_cluster_egress_adapter_t;

/**
 * Create an EgressAdapter.
 *
 * @param adapter             out: adapter handle.
 * @param subscription        egress subscription to poll.
 * @param listener            callback table.
 * @param cluster_session_id  session ID for filtering messages.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_egress_adapter_create(
    aeron_cluster_egress_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_cluster_egress_listener_t *listener,
    int64_t cluster_session_id);

/** Poll; returns fragment count. */
int aeron_cluster_egress_adapter_poll(aeron_cluster_egress_adapter_t *adapter);

/** Close (does NOT close subscription). */
void aeron_cluster_egress_adapter_close(aeron_cluster_egress_adapter_t *adapter);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_EGRESS_ADAPTER_H */
