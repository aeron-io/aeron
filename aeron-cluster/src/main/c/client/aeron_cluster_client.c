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

#include <errno.h>
#include <string.h>
#include <stdio.h>

#include "aeron_cluster.h"
#include "aeron_cluster_client.h"
#include "aeron_cluster_async_connect.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_ingress_proxy.h"
#include "aeron_cluster_configuration.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* Generated C codecs for session header prepend */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

/* -----------------------------------------------------------------------
 * Internal: create — called from async_connect on successful handshake.
 * ----------------------------------------------------------------------- */
int aeron_cluster_create(
    aeron_cluster_t **cluster,
    aeron_cluster_context_t *ctx,
    aeron_cluster_ingress_proxy_t *ingress_proxy,
    aeron_subscription_t *subscription,
    aeron_cluster_egress_poller_t *egress_poller,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int32_t leader_member_id)
{
    aeron_cluster_t *_cluster = NULL;

    if (aeron_alloc((void **)&_cluster, sizeof(aeron_cluster_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster");
        return -1;
    }

    if (aeron_mutex_init(&_cluster->lock) < 0)
    {
        aeron_free(_cluster);
        AERON_SET_ERR(errno, "%s", "unable to init cluster mutex");
        return -1;
    }

    _cluster->ctx               = ctx;
    _cluster->ingress_proxy     = ingress_proxy;
    _cluster->subscription      = subscription;
    _cluster->egress_poller     = egress_poller;
    _cluster->cluster_session_id = cluster_session_id;
    _cluster->leadership_term_id = leadership_term_id;
    _cluster->leader_member_id   = leader_member_id;
    _cluster->state              = AERON_CLUSTER_SESSION_CONNECTED;
    _cluster->is_in_callback     = false;
    _cluster->state_deadline_ns  = INT64_MAX;
    _cluster->async_reconnect_pub = NULL;
    _cluster->last_endpoints[0]  = '\0';
    _cluster->reconnect_endpoint_idx = 0;

    /* Wire context callbacks into the poller */
    aeron_cluster_egress_poller_set_context(egress_poller, ctx);

    *cluster = _cluster;
    return 0;
}

/* -----------------------------------------------------------------------
 * Synchronous connect — convenience wrapper around async.
 * ----------------------------------------------------------------------- */
int aeron_cluster_connect(aeron_cluster_t **cluster, aeron_cluster_context_t *ctx)
{
    aeron_cluster_async_connect_t *async = NULL;

    if (aeron_cluster_async_connect(&async, ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int rc;
    while (0 == (rc = aeron_cluster_async_connect_poll(cluster, async)))
    {
        aeron_cluster_context_idle(ctx);
    }

    return rc < 0 ? -1 : 0;
}

/* -----------------------------------------------------------------------
 * close
 * ----------------------------------------------------------------------- */
int aeron_cluster_close(aeron_cluster_t *cluster)
{
    if (NULL == cluster)
    {
        return 0;
    }

    if (cluster->state != AERON_CLUSTER_SESSION_CLOSED)
    {
        cluster->state = AERON_CLUSTER_SESSION_CLOSED;

        /* Send close session so the cluster can clean up server-side */
        if (NULL != cluster->ingress_proxy)
        {
            aeron_cluster_ingress_proxy_send_close_session(
                cluster->ingress_proxy,
                cluster->cluster_session_id,
                cluster->leadership_term_id);
        }
    }

    if (NULL != cluster->egress_poller)
    {
        aeron_cluster_egress_poller_close(cluster->egress_poller);
        cluster->egress_poller = NULL;
    }

    if (NULL != cluster->subscription)
    {
        aeron_subscription_close(cluster->subscription, NULL, NULL);
        cluster->subscription = NULL;
    }

    if (NULL != cluster->ingress_proxy)
    {
        if (cluster->ingress_proxy->is_exclusive && NULL != cluster->ingress_proxy->exclusive_publication)
        {
            aeron_exclusive_publication_close(cluster->ingress_proxy->exclusive_publication, NULL, NULL);
        }
        else if (!cluster->ingress_proxy->is_exclusive && NULL != cluster->ingress_proxy->publication)
        {
            aeron_publication_close(cluster->ingress_proxy->publication, NULL, NULL);
        }
        aeron_free(cluster->ingress_proxy);
        cluster->ingress_proxy = NULL;
    }

    if (NULL != cluster->ctx)
    {
        aeron_cluster_context_close(cluster->ctx);
        cluster->ctx = NULL;
    }

    aeron_mutex_destroy(&cluster->lock);
    aeron_free(cluster);
    return 0;
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */
bool aeron_cluster_is_closed(aeron_cluster_t *cluster)
{
    return cluster->state == AERON_CLUSTER_SESSION_CLOSED;
}

aeron_cluster_context_t *aeron_cluster_context(aeron_cluster_t *cluster)
{
    return cluster->ctx;
}

int64_t aeron_cluster_cluster_session_id(aeron_cluster_t *cluster)
{
    return cluster->cluster_session_id;
}

int64_t aeron_cluster_leadership_term_id(aeron_cluster_t *cluster)
{
    return cluster->leadership_term_id;
}

int32_t aeron_cluster_leader_member_id(aeron_cluster_t *cluster)
{
    return cluster->leader_member_id;
}

aeron_subscription_t *aeron_cluster_egress_subscription(aeron_cluster_t *cluster)
{
    return cluster->subscription;
}

/* -----------------------------------------------------------------------
 * offer — prepends the 32-byte session header then delivers the payload.
 * ----------------------------------------------------------------------- */
int64_t aeron_cluster_offer(
    aeron_cluster_t *cluster,
    const uint8_t *buffer,
    size_t length)
{
    /* Build session header into a stack buffer */
    uint8_t hdr_buf[AERON_CLUSTER_SESSION_HEADER_LENGTH];

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;
    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr,
        (char *)hdr_buf,
        0,
        sizeof(hdr_buf),
        &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }

    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, cluster->leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster->cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);  /* cluster sets actual timestamp */

    aeron_iovec_t vectors[2];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = AERON_CLUSTER_SESSION_HEADER_LENGTH;
    vectors[1].iov_base = (uint8_t *)buffer;
    vectors[1].iov_len  = length;

    int64_t result;
    if (cluster->ingress_proxy->is_exclusive)
    {
        result = aeron_exclusive_publication_offerv(
            cluster->ingress_proxy->exclusive_publication, vectors, 2, NULL, NULL);
    }
    else
    {
        result = aeron_publication_offerv(
            cluster->ingress_proxy->publication, vectors, 2, NULL, NULL);
    }

    aeron_cluster_track_ingress_result(cluster, result);
    return result;
}

/* -----------------------------------------------------------------------
 * offer_vectors — scatter-gather offer with session header prepended.
 * ----------------------------------------------------------------------- */
int64_t aeron_cluster_offer_vectors(
    aeron_cluster_t *cluster,
    const aeron_iovec_t *user_vectors,
    size_t user_vector_count)
{
    if (user_vector_count > AERON_CLUSTER_OFFER_MAX_VECTORS)
    {
        return AERON_PUBLICATION_ERROR;
    }

    uint8_t hdr_buf[AERON_CLUSTER_SESSION_HEADER_LENGTH];
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;
    if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, sizeof(hdr_buf), &msg_hdr))
    {
        return AERON_PUBLICATION_ERROR;
    }
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, cluster->leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster->cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);

    /* Build combined vector array: [header, user_vectors...] */
    aeron_iovec_t vectors[1 + AERON_CLUSTER_OFFER_MAX_VECTORS];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = AERON_CLUSTER_SESSION_HEADER_LENGTH;
    for (size_t i = 0; i < user_vector_count; i++)
    {
        vectors[1 + i] = user_vectors[i];
    }

    int64_t result;
    if (cluster->ingress_proxy->is_exclusive)
    {
        result = aeron_exclusive_publication_offerv(
            cluster->ingress_proxy->exclusive_publication,
            vectors, 1 + user_vector_count, NULL, NULL);
    }
    else
    {
        result = aeron_publication_offerv(
            cluster->ingress_proxy->publication,
            vectors, 1 + user_vector_count, NULL, NULL);
    }

    aeron_cluster_track_ingress_result(cluster, result);
    return result;
}

/* -----------------------------------------------------------------------
 * try_claim — zero-copy claim with session header written at start.
 * ----------------------------------------------------------------------- */
int64_t aeron_cluster_try_claim(
    aeron_cluster_t *cluster,
    int32_t length,
    aeron_buffer_claim_t *claim)
{
    int64_t result;
    int32_t total = length + AERON_CLUSTER_SESSION_HEADER_LENGTH;

    if (cluster->ingress_proxy->is_exclusive)
    {
        result = aeron_exclusive_publication_try_claim(
            cluster->ingress_proxy->exclusive_publication, total, claim);
    }
    else
    {
        result = aeron_publication_try_claim(
            cluster->ingress_proxy->publication, total, claim);
    }

    if (result > 0)
    {
        /* Write session header into claim buffer */
        struct aeron_cluster_client_messageHeader msg_hdr;
        struct aeron_cluster_client_sessionMessageHeader hdr;
        aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
            &hdr, (char *)claim->data, 0, (uint64_t)total, &msg_hdr);
        aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, cluster->leadership_term_id);
        aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster->cluster_session_id);
        aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);
    }

    aeron_cluster_track_ingress_result(cluster, result);
    return result;
}
bool aeron_cluster_send_keep_alive(aeron_cluster_t *cluster)
{
    int64_t result = aeron_cluster_ingress_proxy_send_keep_alive(
        cluster->ingress_proxy,
        cluster->cluster_session_id,
        cluster->leadership_term_id);

    aeron_cluster_track_ingress_result(cluster, result);
    return result > 0;
}

/* -----------------------------------------------------------------------
 * send_admin_request_snapshot
 * ----------------------------------------------------------------------- */
int64_t aeron_cluster_send_admin_request_snapshot(aeron_cluster_t *cluster, int64_t correlation_id)
{
    int64_t result = aeron_cluster_ingress_proxy_send_admin_request_snapshot(
        cluster->ingress_proxy,
        cluster->cluster_session_id,
        cluster->leadership_term_id,
        correlation_id);

    aeron_cluster_track_ingress_result(cluster, result);
    return result;
}

/* -----------------------------------------------------------------------
 * send_admin_request — generic variant
 * ----------------------------------------------------------------------- */
int64_t aeron_cluster_send_admin_request(
    aeron_cluster_t *cluster,
    int64_t correlation_id,
    int32_t request_type)
{
    int64_t result = aeron_cluster_ingress_proxy_send_admin_request(
        cluster->ingress_proxy,
        cluster->cluster_session_id,
        cluster->leadership_term_id,
        correlation_id,
        request_type);

    aeron_cluster_track_ingress_result(cluster, result);
    return result;
}

/* -----------------------------------------------------------------------
 * state accessor
 * ----------------------------------------------------------------------- */
aeron_cluster_session_state_t aeron_cluster_state(aeron_cluster_t *cluster)
{
    return cluster->state;
}

#include "aeron_cluster_client/eventCode.h"

/* -----------------------------------------------------------------------
 * Internal helper: begin async reconnect to new leader endpoint.
 * Mirrors Java AeronCluster.addNewLeaderIngressPublication().
 * Does NOT block — sets up async publication add and transitions to
 * AWAIT_NEW_LEADER_CONNECTION.  The poll loop drives completion.
 * ----------------------------------------------------------------------- */
static void cluster_begin_leader_reconnect(aeron_cluster_t *cluster, const char *endpoints)
{
    if (NULL == endpoints || endpoints[0] == '\0') { return; }

    aeron_cluster_context_t *ctx = cluster->ctx;
    if (NULL == ctx) { return; }

    /* Store endpoints for rotation on retry */
    snprintf(cluster->last_endpoints, sizeof(cluster->last_endpoints), "%s", endpoints);

    /* Parse Nth endpoint from "memberId=host:port,memberId=host:port,..." format.
     * Rotates through endpoints on each reconnect attempt. */
    char endpoint_buf[256];
    endpoint_buf[0] = '\0';

    const char *p = endpoints;
    int idx = 0;
    int target = cluster->reconnect_endpoint_idx;

    while (p && *p)
    {
        const char *eq = strchr(p, '=');
        if (NULL == eq) { break; }
        const char *ep_start = eq + 1;
        const char *comma = strchr(ep_start, ',');
        size_t ep_len = comma ? (size_t)(comma - ep_start) : strlen(ep_start);

        if (idx == target)
        {
            if (ep_len >= sizeof(endpoint_buf)) { ep_len = sizeof(endpoint_buf) - 1; }
            memcpy(endpoint_buf, ep_start, ep_len);
            endpoint_buf[ep_len] = '\0';
            break;
        }
        idx++;
        p = comma ? comma + 1 : NULL;
    }

    /* If target index exceeded, wrap to 0 */
    if (endpoint_buf[0] == '\0')
    {
        cluster->reconnect_endpoint_idx = 0;
        /* Retry with first endpoint */
        const char *eq = strchr(endpoints, '=');
        if (NULL != eq)
        {
            const char *ep_start = eq + 1;
            const char *comma = strchr(ep_start, ',');
            size_t ep_len = comma ? (size_t)(comma - ep_start) : strlen(ep_start);
            if (ep_len >= sizeof(endpoint_buf)) { ep_len = sizeof(endpoint_buf) - 1; }
            memcpy(endpoint_buf, ep_start, ep_len);
            endpoint_buf[ep_len] = '\0';
        }
        else
        {
            snprintf(endpoint_buf, sizeof(endpoint_buf), "%s", endpoints);
        }
    }

    /* Advance index for next attempt */
    cluster->reconnect_endpoint_idx++;

    /* Build a new ingress channel with the parsed endpoint */
    char new_channel[512];
    snprintf(new_channel, sizeof(new_channel), "aeron:udp?endpoint=%s", endpoint_buf);

    /* Start async publication add — non-blocking */
    aeron_async_add_publication_t *async_pub = NULL;
    if (aeron_async_add_publication(&async_pub, ctx->aeron, new_channel,
        ctx->ingress_stream_id) < 0)
    {
        cluster->state = AERON_CLUSTER_SESSION_CLOSED;
        return;
    }

    cluster->async_reconnect_pub = async_pub;
    cluster->state = AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER_CONNECTION;
    cluster->state_deadline_ns = aeron_nano_clock() +
        (ctx->new_leader_timeout_ns > 0 ? ctx->new_leader_timeout_ns : ctx->message_timeout_ns);
}

/* -----------------------------------------------------------------------
 * Internal: process decoded poller events — shared by poll_egress and test helper.
 * ----------------------------------------------------------------------- */
static void cluster_process_poller_events(aeron_cluster_t *cluster)
{
    aeron_cluster_egress_poller_t *poller = cluster->egress_poller;

    /* Update leadership tracking from any event that carries new term/member */
    if (poller->leadership_term_id > cluster->leadership_term_id)
    {
        cluster->leadership_term_id = poller->leadership_term_id;
        cluster->leader_member_id   = poller->leader_member_id;
    }

    if (poller->template_id == AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID)
    {
        if (poller->event_code == (int32_t)aeron_cluster_client_eventCode_REDIRECT &&
            poller->detail_length > 0)
        {
            cluster_begin_leader_reconnect(cluster, poller->detail);
        }
        else if (poller->event_code == (int32_t)aeron_cluster_client_eventCode_CLOSED)
        {
            cluster->state = AERON_CLUSTER_SESSION_CLOSED;
        }
    }
    else if (poller->template_id == AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID &&
             poller->detail_length > 0)
    {
        /* Transition to AWAIT_NEW_LEADER, then begin async reconnect.
         * The state machine in poll_egress drives completion. */
        cluster->state = AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER;
        cluster_begin_leader_reconnect(cluster, poller->detail);
    }
}

/* -----------------------------------------------------------------------
 * poll_egress — with leader redirect, new-leader reconnect, and CLOSED detection.
 * Also drives the AWAIT_NEW_LEADER_CONNECTION async state machine.
 * ----------------------------------------------------------------------- */
int aeron_cluster_poll_egress(aeron_cluster_t *cluster)
{
    if (cluster->state == AERON_CLUSTER_SESSION_CLOSED)
    {
        return 0;
    }

    /* Drive async reconnect: poll for the new publication to be established */
    if (cluster->state == AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER_CONNECTION)
    {
        if (NULL != cluster->async_reconnect_pub)
        {
            aeron_publication_t *new_pub = NULL;
            int rc = aeron_async_add_publication_poll(&new_pub, cluster->async_reconnect_pub);
            if (rc > 0 && NULL != new_pub)
            {
                /* Swap publications */
                if (NULL != cluster->ingress_proxy &&
                    !cluster->ingress_proxy->is_exclusive &&
                    NULL != cluster->ingress_proxy->publication)
                {
                    aeron_publication_close(cluster->ingress_proxy->publication, NULL, NULL);
                }
                if (NULL != cluster->ingress_proxy)
                {
                    cluster->ingress_proxy->publication  = new_pub;
                    cluster->ingress_proxy->is_exclusive  = false;
                }
                cluster->async_reconnect_pub = NULL;
                cluster->state               = AERON_CLUSTER_SESSION_CONNECTED;
                cluster->state_deadline_ns   = INT64_MAX;
            }
            else if (rc < 0)
            {
                /* Registration failed — close the session */
                cluster->async_reconnect_pub = NULL;
                cluster->state = AERON_CLUSTER_SESSION_CLOSED;
                return 0;
            }
            /* else rc == 0: still pending, check timeout below */
        }

        /* Timeout check: if deadline has passed, try next endpoint or close.
         * Mirrors Java's endpoint rotation on reconnect failure. */
        if (cluster->state == AERON_CLUSTER_SESSION_AWAIT_NEW_LEADER_CONNECTION &&
            aeron_nano_clock() > cluster->state_deadline_ns)
        {
            cluster->async_reconnect_pub = NULL;
            /* Try next endpoint if we have stored endpoints */
            if (cluster->last_endpoints[0] != '\0')
            {
                cluster_begin_leader_reconnect(cluster, cluster->last_endpoints);
                /* If begin_leader_reconnect succeeded, state is AWAIT_NEW_LEADER_CONNECTION again.
                 * If it failed (no more endpoints), state is CLOSED. */
                if (cluster->state != AERON_CLUSTER_SESSION_CLOSED)
                {
                    return 0; /* retry with next endpoint */
                }
            }
            else
            {
                cluster->state = AERON_CLUSTER_SESSION_CLOSED;
            }
            return 0;
        }
    }

    int fragments = aeron_cluster_egress_poller_poll(cluster->egress_poller);

    if (fragments > 0 && cluster->egress_poller->is_poll_complete)
    {
        cluster_process_poller_events(cluster);
    }

    return fragments;
}

/* -----------------------------------------------------------------------
 * on_egress_for_test — bypass real subscription; dispatch buffer directly.
 * ----------------------------------------------------------------------- */
void aeron_cluster_on_egress_for_test(
    aeron_cluster_t *cluster,
    const uint8_t *buffer,
    size_t length)
{
    aeron_cluster_egress_poller_on_fragment_for_test(cluster->egress_poller, buffer, length);
    if (cluster->egress_poller->is_poll_complete)
    {
        cluster_process_poller_events(cluster);
    }
}

/* -----------------------------------------------------------------------
 * track_ingress_result — close the session if the publication signals it.
 * ----------------------------------------------------------------------- */
void aeron_cluster_track_ingress_result(aeron_cluster_t *cluster, int64_t result)
{
    if (AERON_PUBLICATION_CLOSED == result)
    {
        cluster->state = AERON_CLUSTER_SESSION_CLOSED;
    }
}
