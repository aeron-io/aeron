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

#ifndef AERON_CLUSTER_CLIENT_H
#define AERON_CLUSTER_CLIENT_H

#include "aeron_cluster.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_ingress_proxy.h"
#include "concurrent/aeron_thread.h"

#ifdef __cplusplus
extern "C"
{
#endif


struct aeron_cluster_stct
{
    aeron_cluster_context_t         *ctx;
    aeron_mutex_t                    lock;

    aeron_cluster_ingress_proxy_t   *ingress_proxy;
    aeron_subscription_t            *subscription;       /* egress subscription */
    aeron_cluster_egress_poller_t   *egress_poller;

    int64_t  cluster_session_id;
    int64_t  leadership_term_id;
    int32_t  leader_member_id;

    bool     is_closed;
    bool     is_in_callback;
};

/**
 * Internal: called by async_connect to build the cluster object after a
 * successful handshake.  Takes ownership of proxy, subscription, and poller.
 */
int aeron_cluster_create(
    aeron_cluster_t **cluster,
    aeron_cluster_context_t *ctx,
    aeron_cluster_ingress_proxy_t *ingress_proxy,
    aeron_subscription_t *subscription,
    aeron_cluster_egress_poller_t *egress_poller,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id);

/* -----------------------------------------------------------------------
 * Public C API
 * ----------------------------------------------------------------------- */

/**
 * Connect synchronously. Blocks until connected or timeout.
 */
int aeron_cluster_connect(aeron_cluster_t **cluster, aeron_cluster_context_t *ctx);

/**
 * Begin an async connect. Poll with aeron_cluster_async_connect_poll().
 */
int aeron_cluster_async_connect(aeron_cluster_async_connect_t **async, aeron_cluster_context_t *ctx);

int aeron_cluster_close(aeron_cluster_t *cluster);
bool aeron_cluster_is_closed(aeron_cluster_t *cluster);

aeron_cluster_context_t *aeron_cluster_context(aeron_cluster_t *cluster);
int64_t aeron_cluster_cluster_session_id(aeron_cluster_t *cluster);
int64_t aeron_cluster_leadership_term_id(aeron_cluster_t *cluster);
int32_t aeron_cluster_leader_member_id(aeron_cluster_t *cluster);
aeron_subscription_t *aeron_cluster_egress_subscription(aeron_cluster_t *cluster);

/**
 * Offer a message to the cluster. Prepends the session header automatically.
 * Returns offer position (> 0) on success, or AERON_PUBLICATION_* error codes.
 */
int64_t aeron_cluster_offer(
    aeron_cluster_t *cluster,
    const uint8_t *buffer,
    size_t length);

/**
 * Send a keep-alive to prevent session timeout.
 * Returns true if sent, false if back-pressured (retry next cycle).
 */
bool aeron_cluster_send_keep_alive(aeron_cluster_t *cluster);

/**
 * Send an admin request to trigger a cluster snapshot.
 */
int64_t aeron_cluster_send_admin_request_snapshot(aeron_cluster_t *cluster, int64_t correlation_id);

/**
 * Poll egress subscription for messages and events.
 * Fires registered context callbacks. Returns number of fragments polled.
 */
int aeron_cluster_poll_egress(aeron_cluster_t *cluster);

/**
 * Track the result of an offer() call to detect leader changes.
 * Call this after every offer()/try_claim() to keep the state machine current.
 */
void aeron_cluster_track_ingress_result(aeron_cluster_t *cluster, int64_t result);


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_CLIENT_H */
