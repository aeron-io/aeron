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

#include "aeron_cluster.h"
#include "aeron_cluster_async_connect.h"
#include "aeron_cluster_client.h"
#include "aeron_cluster_configuration.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_egress_poller.h"
#include "aeron_cluster_ingress_proxy.h"
#include "aeron_cluster_credentials_supplier.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* -----------------------------------------------------------------------
 * State machine states — defined in the .c to keep the struct opaque.
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_async_connect_state_en
{
    ADD_PUBLICATION          = 0,
    AWAIT_PUBLICATION_CONNECTED = 1,
    SEND_CONNECT_REQUEST     = 2,
    AWAIT_SUBSCRIPTION_CONNECTED = 3,
    AWAIT_CONNECT_RESPONSE   = 4,
    SEND_CHALLENGE_RESPONSE  = 5,
    AWAIT_CHALLENGE_RESPONSE = 6,
    DONE                     = 7
}
aeron_cluster_async_connect_state_t;

struct aeron_cluster_async_connect_stct
{
    aeron_cluster_async_connect_state_t state;
    aeron_cluster_context_t            *ctx;
    aeron_t                            *aeron;

    aeron_async_add_subscription_t            *async_add_subscription;
    aeron_subscription_t                      *subscription;

    aeron_async_add_publication_t             *async_add_publication;
    aeron_publication_t                       *publication;

    aeron_async_add_exclusive_publication_t   *async_add_exclusive_publication;
    aeron_exclusive_publication_t             *exclusive_publication;

    aeron_cluster_egress_poller_t    *egress_poller;
    aeron_cluster_ingress_proxy_t    *ingress_proxy;

    aeron_cluster_encoded_credentials_t *encoded_credentials_from_challenge;

    int64_t  deadline_ns;
    int64_t  correlation_id;
    int64_t  cluster_session_id;
    int64_t  leadership_term_id;
    int32_t  leader_member_id;
};

/* -----------------------------------------------------------------------
 * Forward declarations
 * ----------------------------------------------------------------------- */
static int aeron_cluster_async_connect_transition_to_done(
    aeron_cluster_t **cluster,
    aeron_cluster_async_connect_t *async);

static int aeron_cluster_async_connect_delete(aeron_cluster_async_connect_t *async);

/* -----------------------------------------------------------------------
 * Public: step accessor
 * ----------------------------------------------------------------------- */
uint8_t aeron_cluster_async_connect_step(aeron_cluster_async_connect_t *async)
{
    return (uint8_t)async->state;
}

/* -----------------------------------------------------------------------
 * aeron_cluster_async_connect  — called once to begin the handshake.
 * ----------------------------------------------------------------------- */
int aeron_cluster_async_connect(
    aeron_cluster_async_connect_t **async,
    aeron_cluster_context_t *ctx)
{
    *async = NULL;

    if (aeron_cluster_context_conclude(ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_t *aeron = ctx->aeron;

    /* Start adding the egress subscription */
    aeron_async_add_subscription_t *async_add_subscription = NULL;
    if (aeron_async_add_subscription(
        &async_add_subscription,
        aeron,
        ctx->egress_channel,
        ctx->egress_stream_id,
        NULL, NULL, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    /* Start adding the ingress publication (exclusive or shared based on context) */
    aeron_async_add_publication_t           *async_add_pub  = NULL;
    aeron_async_add_exclusive_publication_t *async_add_excl = NULL;

    if (ctx->is_ingress_exclusive)
    {
        if (aeron_async_add_exclusive_publication(
            &async_add_excl,
            aeron,
            ctx->ingress_channel,
            ctx->ingress_stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }
    else
    {
        if (aeron_async_add_publication(
            &async_add_pub,
            aeron,
            ctx->ingress_channel,
            ctx->ingress_stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    aeron_cluster_async_connect_t *_async = NULL;
    if (aeron_alloc((void **)&_async, sizeof(aeron_cluster_async_connect_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster_async_connect");
        return -1;
    }

    _async->state = ADD_PUBLICATION;

    if (aeron_cluster_context_duplicate(&_async->ctx, ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(_async);
        return -1;
    }

    /* Transfer ownership of aeron client to the async object */
    aeron_cluster_context_set_owns_aeron_client(ctx, false);

    _async->aeron                           = aeron;
    _async->async_add_subscription          = async_add_subscription;
    _async->subscription                    = NULL;
    _async->async_add_publication           = async_add_pub;
    _async->publication                     = NULL;
    _async->async_add_exclusive_publication = async_add_excl;
    _async->exclusive_publication           = NULL;
    _async->egress_poller                   = NULL;
    _async->ingress_proxy                   = NULL;
    _async->encoded_credentials_from_challenge = NULL;
    _async->deadline_ns                     = aeron_nano_clock() + (int64_t)ctx->message_timeout_ns;
    _async->correlation_id                  = AERON_NULL_VALUE;
    _async->cluster_session_id              = AERON_NULL_VALUE;
    _async->leadership_term_id              = AERON_NULL_VALUE;
    _async->leader_member_id                = AERON_NULL_VALUE;

    *async = _async;
    return 0;
}

/* -----------------------------------------------------------------------
 * aeron_cluster_async_connect_poll  — call repeatedly until returns 1 or -1.
 *   0  = not done yet, call again
 *   1  = done, *cluster is valid
 *  -1  = error, aeron_errmsg() has details
 * ----------------------------------------------------------------------- */
int aeron_cluster_async_connect_poll(
    aeron_cluster_t **cluster,
    aeron_cluster_async_connect_t *async)
{
    *cluster = NULL;

    if (aeron_nano_clock() > async->deadline_ns)
    {
        AERON_SET_ERR(-1, "%s", "cluster connect timeout");
        goto cleanup;
    }

    /* -----------------------------------------------------------------
     * ADD_PUBLICATION: wait for both publication and subscription ready.
     * ----------------------------------------------------------------- */
    if (ADD_PUBLICATION == async->state)
    {
        /* Poll publication */
        if (NULL == async->publication && NULL == async->exclusive_publication)
        {
            if (async->ctx->is_ingress_exclusive)
            {
                int rc = aeron_async_add_exclusive_publication_poll(
                    &async->exclusive_publication, async->async_add_exclusive_publication);
                if (rc < 0)
                {
                    async->async_add_exclusive_publication = NULL;
                    AERON_APPEND_ERR("%s", "");
                    goto cleanup;
                }
                else if (rc == 1)
                {
                    async->async_add_exclusive_publication = NULL;
                }
            }
            else
            {
                int rc = aeron_async_add_publication_poll(&async->publication, async->async_add_publication);
                if (rc < 0)
                {
                    async->async_add_publication = NULL;
                    AERON_APPEND_ERR("%s", "");
                    goto cleanup;
                }
                else if (rc == 1)
                {
                    async->async_add_publication = NULL;
                }
            }
        }

        /* Build ingress proxy once publication is ready */
        bool pub_ready = (async->ctx->is_ingress_exclusive)
            ? (NULL != async->exclusive_publication)
            : (NULL != async->publication);

        if (pub_ready && NULL == async->ingress_proxy)
        {
            if (aeron_alloc((void **)&async->ingress_proxy, sizeof(aeron_cluster_ingress_proxy_t)) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto cleanup;
            }

            if (async->ctx->is_ingress_exclusive)
            {
                aeron_cluster_ingress_proxy_init_exclusive(
                    async->ingress_proxy,
                    async->exclusive_publication,
                    (int)async->ctx->message_retry_attempts);
            }
            else
            {
                aeron_cluster_ingress_proxy_init(
                    async->ingress_proxy,
                    async->publication,
                    (int)async->ctx->message_retry_attempts);
            }
        }

        /* Poll subscription */
        if (NULL == async->subscription)
        {
            int rc = aeron_async_add_subscription_poll(&async->subscription, async->async_add_subscription);
            if (rc < 0)
            {
                async->async_add_subscription = NULL;
                AERON_APPEND_ERR("%s", "");
                goto cleanup;
            }
            else if (rc == 1)
            {
                async->async_add_subscription = NULL;
            }
        }

        /* Build egress poller once subscription is ready */
        if (NULL != async->subscription && NULL == async->egress_poller)
        {
            if (aeron_cluster_egress_poller_create(
                &async->egress_poller,
                async->subscription,
                AERON_CLUSTER_EGRESS_POLLER_FRAGMENT_LIMIT_DEFAULT) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto cleanup;
            }
        }

        if (NULL != async->ingress_proxy && NULL != async->egress_poller)
        {
            async->state = AWAIT_PUBLICATION_CONNECTED;
        }
    }

    /* -----------------------------------------------------------------
     * AWAIT_PUBLICATION_CONNECTED
     * ----------------------------------------------------------------- */
    if (AWAIT_PUBLICATION_CONNECTED == async->state)
    {
        bool connected = async->ctx->is_ingress_exclusive
            ? aeron_exclusive_publication_is_connected(async->exclusive_publication)
            : aeron_publication_is_connected(async->publication);

        if (connected)
        {
            async->state = SEND_CONNECT_REQUEST;
        }
        else
        {
            aeron_cluster_context_invoke_aeron_client(async->ctx);
            return 0;
        }
    }

    /* -----------------------------------------------------------------
     * SEND_CONNECT_REQUEST
     * ----------------------------------------------------------------- */
    if (SEND_CONNECT_REQUEST == async->state)
    {
        /* Resolve the actual egress channel endpoint (may have port 0) */
        aeron_subscription_constants_t constants;
        aeron_subscription_constants(async->subscription, &constants);
        size_t egress_channel_len = strlen(constants.channel) + AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN;
        char *resolved_egress_channel = NULL;
        if (aeron_alloc((void **)&resolved_egress_channel, egress_channel_len) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }

        if (aeron_subscription_try_resolve_channel_endpoint_port(
            async->subscription, resolved_egress_channel, egress_channel_len) < 0)
        {
            aeron_free(resolved_egress_channel);
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }

        if ('\0' == resolved_egress_channel[0])
        {
            /* Port not yet resolved, try again next poll */
            aeron_free(resolved_egress_channel);
            aeron_cluster_context_invoke_aeron_client(async->ctx);
            return 0;
        }

        aeron_cluster_encoded_credentials_t *credentials =
            aeron_cluster_credentials_supplier_encoded_credentials(&async->ctx->credentials_supplier);

        async->correlation_id = aeron_next_correlation_id(async->aeron);

        int64_t result = aeron_cluster_ingress_proxy_send_connect_request(
            async->ingress_proxy,
            async->correlation_id,
            async->ctx->egress_stream_id,
            resolved_egress_channel,
            credentials,
            async->ctx->client_name);

        aeron_cluster_credentials_supplier_on_free(&async->ctx->credentials_supplier, credentials);
        aeron_free(resolved_egress_channel);

        if (result > 0)
        {
            async->state = AWAIT_SUBSCRIPTION_CONNECTED;
        }
        else
        {
            aeron_cluster_context_invoke_aeron_client(async->ctx);
            return 0;
        }
    }

    /* -----------------------------------------------------------------
     * AWAIT_SUBSCRIPTION_CONNECTED
     * ----------------------------------------------------------------- */
    if (AWAIT_SUBSCRIPTION_CONNECTED == async->state)
    {
        if (aeron_subscription_is_connected(async->subscription))
        {
            async->state = AWAIT_CONNECT_RESPONSE;
        }
        else
        {
            aeron_cluster_context_invoke_aeron_client(async->ctx);
            return 0;
        }
    }

    /* -----------------------------------------------------------------
     * SEND_CHALLENGE_RESPONSE
     * ----------------------------------------------------------------- */
    if (SEND_CHALLENGE_RESPONSE == async->state)
    {
        int64_t result = aeron_cluster_ingress_proxy_send_challenge_response(
            async->ingress_proxy,
            async->correlation_id,
            async->cluster_session_id,
            async->encoded_credentials_from_challenge);

        aeron_cluster_credentials_supplier_on_free(
            &async->ctx->credentials_supplier,
            async->encoded_credentials_from_challenge);
        async->encoded_credentials_from_challenge = NULL;

        if (result > 0)
        {
            async->state = AWAIT_CHALLENGE_RESPONSE;
        }
        else
        {
            aeron_cluster_context_invoke_aeron_client(async->ctx);
            return 0;
        }
    }

    /* -----------------------------------------------------------------
     * AWAIT_CONNECT_RESPONSE / AWAIT_CHALLENGE_RESPONSE
     * ----------------------------------------------------------------- */
    if (NULL != async->egress_poller)
    {
        if (aeron_cluster_egress_poller_poll(async->egress_poller) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto cleanup;
        }

        aeron_cluster_egress_poller_t *poller = async->egress_poller;

        if (poller->is_poll_complete &&
            poller->template_id == AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID &&
            poller->correlation_id == async->correlation_id)
        {
            async->cluster_session_id = poller->cluster_session_id;
            async->leadership_term_id = poller->leadership_term_id;
            async->leader_member_id   = poller->leader_member_id;

            if (poller->event_code == AERON_CLUSTER_EVENT_CODE_OK)
            {
                return aeron_cluster_async_connect_transition_to_done(cluster, async);
            }
            else if (poller->event_code == AERON_CLUSTER_EVENT_CODE_REDIRECT)
            {
                /* Multi-member: detail contains the new ingress endpoints.
                 * Update context and restart from SEND_CONNECT_REQUEST. */
                if (NULL != poller->detail && poller->detail_length > 0)
                {
                    if (aeron_cluster_context_set_ingress_endpoints(
                        async->ctx, poller->detail) < 0)
                    {
                        AERON_APPEND_ERR("%s", "");
                        goto cleanup;
                    }
                }
                async->state = SEND_CONNECT_REQUEST;
            }
            else if (poller->event_code == AERON_CLUSTER_EVENT_CODE_AUTHENTICATION_REJECTED)
            {
                AERON_SET_ERR(EINVAL, "cluster authentication rejected: %.*s",
                    (int)poller->detail_length,
                    poller->detail != NULL ? poller->detail : "");
                goto cleanup;
            }
            else
            {
                AERON_SET_ERR(EINVAL, "unexpected session event code: %d", poller->event_code);
                goto cleanup;
            }
        }
        else if (poller->was_challenged &&
                 poller->cluster_session_id == async->cluster_session_id)
        {
            async->encoded_credentials_from_challenge =
                aeron_cluster_credentials_supplier_on_challenge(
                    &async->ctx->credentials_supplier,
                    &poller->encoded_challenge);

            async->correlation_id = aeron_next_correlation_id(async->aeron);
            async->state = SEND_CHALLENGE_RESPONSE;
        }
    }

    aeron_cluster_context_invoke_aeron_client(async->ctx);
    return 0;

cleanup:
    aeron_cluster_async_connect_delete(async);
    return -1;
}

/* -----------------------------------------------------------------------
 * Transition to DONE: build the aeron_cluster_t and transfer ownership.
 * ----------------------------------------------------------------------- */
static int aeron_cluster_async_connect_transition_to_done(
    aeron_cluster_t **cluster,
    aeron_cluster_async_connect_t *async)
{
    int rc = aeron_cluster_create(
        cluster,
        async->ctx,
        async->ingress_proxy,
        async->subscription,
        async->egress_poller,
        async->cluster_session_id,
        async->leadership_term_id,
        async->leader_member_id);

    if (rc == 0)
    {
        /* Prevent cleanup from closing resources now owned by the cluster */
        async->ingress_proxy  = NULL;
        async->subscription   = NULL;
        async->egress_poller  = NULL;
        async->ctx            = NULL;

        async->state = DONE;
        aeron_cluster_async_connect_delete(async);
        return 1;
    }

    AERON_APPEND_ERR("%s", "");
    aeron_cluster_async_connect_delete(async);
    return -1;
}

/* -----------------------------------------------------------------------
 * Cleanup helper — frees all resources not yet transferred.
 * ----------------------------------------------------------------------- */
static int aeron_cluster_async_connect_delete(aeron_cluster_async_connect_t *async)
{
    if (NULL == async)
    {
        return 0;
    }

    if (NULL != async->egress_poller)
    {
        aeron_cluster_egress_poller_close(async->egress_poller);
        async->egress_poller = NULL;
    }

    if (NULL != async->subscription)
    {
        aeron_subscription_close(async->subscription, NULL, NULL);
        async->subscription = NULL;
    }

    if (NULL != async->exclusive_publication)
    {
        aeron_exclusive_publication_close(async->exclusive_publication, NULL, NULL);
        async->exclusive_publication = NULL;
    }

    if (NULL != async->publication)
    {
        aeron_publication_close(async->publication, NULL, NULL);
        async->publication = NULL;
    }

    aeron_free(async->ingress_proxy);
    async->ingress_proxy = NULL;

    if (NULL != async->ctx)
    {
        aeron_cluster_context_close(async->ctx);
        async->ctx = NULL;
    }

    aeron_free(async);
    return 0;
}
