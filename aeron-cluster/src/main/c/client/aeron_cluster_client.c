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
    _cluster->is_closed          = false;
    _cluster->is_in_callback     = false;

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

    if (!cluster->is_closed)
    {
        cluster->is_closed = true;

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
    return cluster->is_closed;
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
 * send_keep_alive
 * ----------------------------------------------------------------------- */
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
 * poll_egress
 * ----------------------------------------------------------------------- */
int aeron_cluster_poll_egress(aeron_cluster_t *cluster)
{
    if (cluster->is_closed)
    {
        return 0;
    }

    return aeron_cluster_egress_poller_poll(cluster->egress_poller);
}

/* -----------------------------------------------------------------------
 * track_ingress_result — close the session if the publication signals it.
 * ----------------------------------------------------------------------- */
void aeron_cluster_track_ingress_result(aeron_cluster_t *cluster, int64_t result)
{
    if (AERON_PUBLICATION_CLOSED == result)
    {
        cluster->is_closed = true;
    }
}
