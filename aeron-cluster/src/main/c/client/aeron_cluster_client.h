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

typedef enum aeron_cluster_session_state_enum
{
    AERON_CLUSTER_SESSION_CONNECTED                  = 0,
    AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER           = 1,
    AERON_CLUSTER_SESSION_CLOSED                     = 2,
    AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER_CONNECTION = 3,
}
aeron_cluster_session_state_t;

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

    aeron_cluster_session_state_t    state;
    bool                             is_in_callback;

    /* Async leader reconnect state — mirrors Java AWAIT_NEW_LEADER_CONNECTION */
    int64_t                          state_deadline_ns;
    aeron_async_add_publication_t   *async_reconnect_pub;

    /* Last ingress endpoints for reconnect rotation */
    char                             last_endpoints[512];
    int                              reconnect_endpoint_idx;
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
 * Scatter-gather offer. The session header is prepended automatically;
 * the caller provides up to AERON_CLUSTER_OFFER_MAX_VECTORS user vectors.
 * Returns offer position on success, or AERON_PUBLICATION_* error codes.
 */
#define AERON_CLUSTER_OFFER_MAX_VECTORS 8

int64_t aeron_cluster_offer_vectors(
    aeron_cluster_t *cluster,
    const aeron_iovec_t *vectors,
    size_t vector_count);

/**
 * Zero-copy claim interface.  Reserves (length + SESSION_HEADER_LENGTH) bytes in
 * the publication and fills the session header at the start of the claim.
 * The caller fills the remaining bytes and calls aeron_buffer_claim_commit() or
 * aeron_buffer_claim_abort().
 *
 * Returns position (> 0) on success, or AERON_PUBLICATION_* error codes.
 * The session header starts at claim->data; payload starts at claim->data + AERON_CLUSTER_SESSION_HEADER_LENGTH.
 */
int64_t aeron_cluster_try_claim(
    aeron_cluster_t *cluster,
    int32_t length,
    aeron_buffer_claim_t *claim);

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
 * Send a generic admin request.
 * request_type is one of the aeron_cluster_client_adminRequestType_* enum values cast to int32_t.
 */
int64_t aeron_cluster_send_admin_request(
    aeron_cluster_t *cluster,
    int64_t correlation_id,
    int32_t request_type);

/**
 * Return the current session state.
 */
aeron_cluster_session_state_t aeron_cluster_state(aeron_cluster_t *cluster);

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

#ifdef AERON_CLUSTER_TESTING
/**
 * Test helper: dispatch a pre-built egress SBE buffer directly into the cluster's
 * event-processing logic, bypassing the real subscription poll.
 * Equivalent to feeding a fragment into the egress poller and then running the
 * cluster's state-machine update.  NOT for production use.
 */
void aeron_cluster_on_egress_for_test(
    aeron_cluster_t *cluster,
    const uint8_t *buffer,
    size_t length);
#endif /* AERON_CLUSTER_TESTING */


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_CLIENT_H */
