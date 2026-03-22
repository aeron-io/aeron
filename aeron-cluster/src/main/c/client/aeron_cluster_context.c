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
#include <stdio.h>
#include <string.h>

#include "aeron_cluster.h"
#include "aeron_cluster_context.h"
#include "aeron_cluster_configuration.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_strutil.h"
#include "aeron_agent.h"

/* -----------------------------------------------------------------------
 * Internal helper: allocate/reallocate a heap-copied string field.
 * ----------------------------------------------------------------------- */
static int aeron_cluster_context_set_string(char **target, size_t *target_length, const char *value)
{
    if (NULL == value)
    {
        aeron_free(*target);
        *target = NULL;
        *target_length = 0;
        return 0;
    }

    const size_t needed = strlen(value) + 1;
    char *tmp = *target;

    if (NULL == tmp)
    {
        if (aeron_alloc((void **)&tmp, needed) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to allocate cluster context string");
            return -1;
        }
    }
    else if (needed > *target_length)
    {
        if (aeron_reallocf((void **)&tmp, needed) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to reallocate cluster context string");
            return -1;
        }
    }

    memcpy(tmp, value, needed);
    *target = tmp;
    *target_length = needed;
    return 0;
}

/* -----------------------------------------------------------------------
 * init
 * ----------------------------------------------------------------------- */
int aeron_cluster_context_init(aeron_cluster_context_t **ctx)
{
    aeron_cluster_context_t *_ctx = NULL;

    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cluster_context_init(NULL)");
        return -1;
    }

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_cluster_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster_context");
        return -1;
    }

    _ctx->aeron = NULL;
    if (aeron_default_path(_ctx->aeron_directory_name, sizeof(_ctx->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to resolve default aeron directory path");
        goto error;
    }
    _ctx->client_name[0]      = '\0';
    _ctx->owns_aeron_client   = false;

    _ctx->ingress_channel         = NULL;
    _ctx->ingress_channel_length  = 0;
    _ctx->ingress_stream_id       = AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT;

    _ctx->ingress_endpoints        = NULL;
    _ctx->ingress_endpoints_length = 0;

    _ctx->egress_channel        = NULL;
    _ctx->egress_channel_length = 0;
    _ctx->egress_stream_id      = AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT;

    _ctx->message_timeout_ns      = AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT;
    _ctx->message_retry_attempts  = AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_DEFAULT;
    _ctx->is_ingress_exclusive    = false;

    _ctx->idle_strategy_func  = NULL;
    _ctx->idle_strategy_state = NULL;
    _ctx->owns_idle_strategy  = false;

    aeron_cluster_context_set_credentials_supplier(_ctx, NULL, NULL, NULL, NULL);

    _ctx->on_message          = NULL;
    _ctx->on_message_clientd  = NULL;

    _ctx->on_session_event         = NULL;
    _ctx->on_session_event_clientd = NULL;

    _ctx->on_new_leader_event         = NULL;
    _ctx->on_new_leader_event_clientd = NULL;

    _ctx->on_admin_response         = NULL;
    _ctx->on_admin_response_clientd = NULL;

    _ctx->error_handler         = NULL;
    _ctx->error_handler_clientd = NULL;

    _ctx->delegating_invoker_func         = NULL;
    _ctx->delegating_invoker_func_clientd = NULL;

    /* Apply environment overrides */
    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        aeron_cluster_context_set_aeron_directory_name(_ctx, value);
    }

    if ((value = getenv(AERON_CLUSTER_CLIENT_NAME_ENV_VAR)))
    {
        if (aeron_cluster_context_set_client_name(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if ((value = getenv(AERON_CLUSTER_INGRESS_CHANNEL_ENV_VAR)))
    {
        if (aeron_cluster_context_set_ingress_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->ingress_stream_id = aeron_config_parse_int32(
        AERON_CLUSTER_INGRESS_STREAM_ID_ENV_VAR,
        getenv(AERON_CLUSTER_INGRESS_STREAM_ID_ENV_VAR),
        _ctx->ingress_stream_id,
        INT32_MIN,
        INT32_MAX);

    if ((value = getenv(AERON_CLUSTER_INGRESS_ENDPOINTS_ENV_VAR)))
    {
        if (aeron_cluster_context_set_ingress_endpoints(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if ((value = getenv(AERON_CLUSTER_EGRESS_CHANNEL_ENV_VAR)))
    {
        if (aeron_cluster_context_set_egress_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->egress_stream_id = aeron_config_parse_int32(
        AERON_CLUSTER_EGRESS_STREAM_ID_ENV_VAR,
        getenv(AERON_CLUSTER_EGRESS_STREAM_ID_ENV_VAR),
        _ctx->egress_stream_id,
        INT32_MIN,
        INT32_MAX);

    _ctx->message_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CLUSTER_MESSAGE_TIMEOUT_ENV_VAR,
        getenv(AERON_CLUSTER_MESSAGE_TIMEOUT_ENV_VAR),
        _ctx->message_timeout_ns,
        1000,
        INT64_MAX);

    _ctx->message_retry_attempts = (uint32_t)aeron_config_parse_int32(
        AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_ENV_VAR,
        getenv(AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_ENV_VAR),
        (int32_t)_ctx->message_retry_attempts,
        0,
        INT32_MAX);

    *ctx = _ctx;
    return 0;

error:
    aeron_free(_ctx);
    return -1;
}

/* -----------------------------------------------------------------------
 * close
 * ----------------------------------------------------------------------- */
int aeron_cluster_context_close(aeron_cluster_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client)
        {
            aeron_context_t *aeron_ctx = aeron_context(ctx->aeron);
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;
            aeron_context_close(aeron_ctx);
        }

        aeron_free(ctx->ingress_channel);
        aeron_free(ctx->ingress_endpoints);
        aeron_free(ctx->egress_channel);

        if (ctx->owns_idle_strategy)
        {
            aeron_free(ctx->idle_strategy_state);
        }

        aeron_free(ctx);
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * conclude — validates required fields, creates aeron client if needed,
 *            loads default idle strategy.
 * ----------------------------------------------------------------------- */
int aeron_cluster_context_conclude(aeron_cluster_context_t *ctx)
{
    if (NULL == ctx->egress_channel)
    {
        AERON_SET_ERR(EINVAL, "%s", "egress channel is required");
        goto error;
    }

    if (0 == ctx->message_retry_attempts)
    {
        AERON_SET_ERR(EINVAL, "%s", "message_retry_attempts must be > 0");
        goto error;
    }

    if (NULL == ctx->aeron)
    {
        ctx->owns_aeron_client = true;

        aeron_context_t *aeron_ctx;
        if (aeron_context_init(&aeron_ctx) < 0 ||
            aeron_context_set_dir(aeron_ctx, ctx->aeron_directory_name) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }

        const char *client_name = ctx->client_name[0] != '\0' ? ctx->client_name : "cluster-client";
        if (aeron_context_set_client_name(aeron_ctx, client_name) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }

        if (aeron_init(&ctx->aeron, aeron_ctx) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if (NULL == ctx->idle_strategy_func)
    {
        ctx->owns_idle_strategy = true;
        if (NULL == (ctx->idle_strategy_func = aeron_idle_strategy_load(
            "backoff",
            &ctx->idle_strategy_state,
            "AERON_CLUSTER_IDLE_STRATEGY",
            NULL)))
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    return 0;

error:
    if (ctx->owns_aeron_client && NULL != ctx->aeron)
    {
        aeron_close(ctx->aeron);
        ctx->aeron = NULL;
    }
    return -1;
}

/* -----------------------------------------------------------------------
 * duplicate
 * ----------------------------------------------------------------------- */
int aeron_cluster_context_duplicate(aeron_cluster_context_t **dest_p, aeron_cluster_context_t *src)
{
    aeron_cluster_context_t *_ctx = NULL;

    if (aeron_alloc((void **)&_ctx, sizeof(aeron_cluster_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster_context duplicate");
        return -1;
    }

    memcpy(_ctx, src, sizeof(aeron_cluster_context_t));

    _ctx->ingress_channel = NULL;
    if (aeron_cluster_context_set_ingress_channel(_ctx, src->ingress_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    _ctx->ingress_endpoints = NULL;
    if (aeron_cluster_context_set_ingress_endpoints(_ctx, src->ingress_endpoints) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    _ctx->egress_channel = NULL;
    if (aeron_cluster_context_set_egress_channel(_ctx, src->egress_channel) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    _ctx->owns_idle_strategy = false;

    *dest_p = _ctx;
    return 0;

error:
    aeron_free(_ctx->ingress_channel);
    aeron_free(_ctx->ingress_endpoints);
    aeron_free(_ctx->egress_channel);
    aeron_free(_ctx);
    return -1;
}

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */
void aeron_cluster_context_idle(aeron_cluster_context_t *ctx)
{
    ctx->idle_strategy_func(ctx->idle_strategy_state, 0);
}

void aeron_cluster_context_invoke_aeron_client(aeron_cluster_context_t *ctx)
{
    if (aeron_context_get_use_conductor_agent_invoker(aeron_context(ctx->aeron)))
    {
        aeron_main_do_work(ctx->aeron);
    }

    if (NULL != ctx->delegating_invoker_func)
    {
        ctx->delegating_invoker_func(ctx->delegating_invoker_func_clientd);
    }
}

/* -----------------------------------------------------------------------
 * Setters / getters
 * ----------------------------------------------------------------------- */
int aeron_cluster_context_set_aeron(aeron_cluster_context_t *ctx, aeron_t *aeron)
{
    ctx->aeron = aeron;
    return 0;
}

aeron_t *aeron_cluster_context_get_aeron(aeron_cluster_context_t *ctx)
{
    return ctx->aeron;
}

int aeron_cluster_context_set_owns_aeron_client(aeron_cluster_context_t *ctx, bool owns_aeron_client)
{
    ctx->owns_aeron_client = owns_aeron_client;
    return 0;
}

bool aeron_cluster_context_get_owns_aeron_client(aeron_cluster_context_t *ctx)
{
    return ctx->owns_aeron_client;
}

int aeron_cluster_context_set_aeron_directory_name(aeron_cluster_context_t *ctx, const char *value)
{
    snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name), "%s", value);
    return 0;
}

const char *aeron_cluster_context_get_aeron_directory_name(aeron_cluster_context_t *ctx)
{
    return ctx->aeron_directory_name;
}

int aeron_cluster_context_set_client_name(aeron_cluster_context_t *ctx, const char *value)
{
    size_t copy_length = 0;
    if (!aeron_str_length(value, AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1, &copy_length))
    {
        AERON_SET_ERR(EINVAL, "client_name length must be <= %d", AERON_COUNTER_MAX_CLIENT_NAME_LENGTH);
        return -1;
    }

    memcpy(ctx->client_name, value, copy_length);
    ctx->client_name[copy_length] = '\0';
    return 0;
}

const char *aeron_cluster_context_get_client_name(aeron_cluster_context_t *ctx)
{
    return ctx->client_name;
}

int aeron_cluster_context_set_ingress_channel(aeron_cluster_context_t *ctx, const char *channel)
{
    return aeron_cluster_context_set_string(&ctx->ingress_channel, &ctx->ingress_channel_length, channel);
}

const char *aeron_cluster_context_get_ingress_channel(aeron_cluster_context_t *ctx)
{
    return ctx->ingress_channel;
}

int aeron_cluster_context_set_ingress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id)
{
    ctx->ingress_stream_id = stream_id;
    return 0;
}

int32_t aeron_cluster_context_get_ingress_stream_id(aeron_cluster_context_t *ctx)
{
    return ctx->ingress_stream_id;
}

int aeron_cluster_context_set_ingress_endpoints(aeron_cluster_context_t *ctx, const char *endpoints)
{
    return aeron_cluster_context_set_string(&ctx->ingress_endpoints, &ctx->ingress_endpoints_length, endpoints);
}

const char *aeron_cluster_context_get_ingress_endpoints(aeron_cluster_context_t *ctx)
{
    return ctx->ingress_endpoints;
}

int aeron_cluster_context_set_is_ingress_exclusive(aeron_cluster_context_t *ctx, bool value)
{
    ctx->is_ingress_exclusive = value;
    return 0;
}

bool aeron_cluster_context_get_is_ingress_exclusive(aeron_cluster_context_t *ctx)
{
    return ctx->is_ingress_exclusive;
}

int aeron_cluster_context_set_egress_channel(aeron_cluster_context_t *ctx, const char *channel)
{
    return aeron_cluster_context_set_string(&ctx->egress_channel, &ctx->egress_channel_length, channel);
}

const char *aeron_cluster_context_get_egress_channel(aeron_cluster_context_t *ctx)
{
    return ctx->egress_channel;
}

int aeron_cluster_context_set_egress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id)
{
    ctx->egress_stream_id = stream_id;
    return 0;
}

int32_t aeron_cluster_context_get_egress_stream_id(aeron_cluster_context_t *ctx)
{
    return ctx->egress_stream_id;
}

int aeron_cluster_context_set_message_timeout_ns(aeron_cluster_context_t *ctx, uint64_t timeout_ns)
{
    ctx->message_timeout_ns = timeout_ns;
    return 0;
}

uint64_t aeron_cluster_context_get_message_timeout_ns(aeron_cluster_context_t *ctx)
{
    return ctx->message_timeout_ns;
}

int aeron_cluster_context_set_message_retry_attempts(aeron_cluster_context_t *ctx, uint32_t attempts)
{
    ctx->message_retry_attempts = attempts;
    return 0;
}

uint32_t aeron_cluster_context_get_message_retry_attempts(aeron_cluster_context_t *ctx)
{
    return ctx->message_retry_attempts;
}

int aeron_cluster_context_set_idle_strategy(
    aeron_cluster_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state)
{
    ctx->idle_strategy_func  = idle_strategy_func;
    ctx->idle_strategy_state = idle_strategy_state;
    return 0;
}

int aeron_cluster_context_set_credentials_supplier(
    aeron_cluster_context_t *ctx,
    aeron_cluster_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_cluster_credentials_challenge_supplier_func_t on_challenge,
    aeron_cluster_credentials_free_func_t on_free,
    void *clientd)
{
    ctx->credentials_supplier.encoded_credentials = encoded_credentials;
    ctx->credentials_supplier.on_challenge        = on_challenge;
    ctx->credentials_supplier.on_free             = on_free;
    ctx->credentials_supplier.clientd             = clientd;
    return 0;
}

int aeron_cluster_context_set_on_message(
    aeron_cluster_context_t *ctx,
    aeron_cluster_egress_message_listener_func_t on_message,
    void *clientd)
{
    ctx->on_message         = on_message;
    ctx->on_message_clientd = clientd;
    return 0;
}

int aeron_cluster_context_set_on_session_event(
    aeron_cluster_context_t *ctx,
    aeron_cluster_session_event_listener_func_t on_session_event,
    void *clientd)
{
    ctx->on_session_event         = on_session_event;
    ctx->on_session_event_clientd = clientd;
    return 0;
}

int aeron_cluster_context_set_on_new_leader_event(
    aeron_cluster_context_t *ctx,
    aeron_cluster_new_leader_event_listener_func_t on_new_leader_event,
    void *clientd)
{
    ctx->on_new_leader_event         = on_new_leader_event;
    ctx->on_new_leader_event_clientd = clientd;
    return 0;
}

int aeron_cluster_context_set_on_admin_response(
    aeron_cluster_context_t *ctx,
    aeron_cluster_admin_response_listener_func_t on_admin_response,
    void *clientd)
{
    ctx->on_admin_response         = on_admin_response;
    ctx->on_admin_response_clientd = clientd;
    return 0;
}

int aeron_cluster_context_set_error_handler(
    aeron_cluster_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd)
{
    ctx->error_handler         = error_handler;
    ctx->error_handler_clientd = clientd;
    return 0;
}

int aeron_cluster_context_set_delegating_invoker(
    aeron_cluster_context_t *ctx,
    aeron_cluster_delegating_invoker_func_t delegating_invoker_func,
    void *clientd)
{
    ctx->delegating_invoker_func         = delegating_invoker_func;
    ctx->delegating_invoker_func_clientd = clientd;
    return 0;
}
