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

#include "aeron_cluster_service_context.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

static int svc_ctx_set_string(char **target, size_t *len, const char *value)
{
    if (NULL == value)
    {
        aeron_free(*target);
        *target = NULL;
        *len = 0;
        return 0;
    }
    const size_t needed = strlen(value) + 1;
    char *tmp = *target;
    if (NULL == tmp)
    {
        if (aeron_alloc((void **)&tmp, needed) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "unable to allocate service context string");
            return -1;
        }
    }
    else if (needed > *len)
    {
        if (aeron_reallocf((void **)&tmp, needed) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to reallocate service context string");
            return -1;
        }
    }
    memcpy(tmp, value, needed);
    *target = tmp;
    *len = needed;
    return 0;
}

int aeron_cluster_service_context_init(aeron_cluster_service_context_t **ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cluster_service_context_init(NULL)");
        return -1;
    }

    aeron_cluster_service_context_t *_ctx = NULL;
    if (aeron_alloc((void **)&_ctx, sizeof(aeron_cluster_service_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate aeron_cluster_service_context");
        return -1;
    }

    _ctx->aeron = NULL;
    if (aeron_default_path(_ctx->aeron_directory_name, sizeof(_ctx->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to resolve default aeron directory path");
        goto error;
    }
    _ctx->owns_aeron_client = false;

    _ctx->service_id = AERON_CLUSTER_SERVICE_ID_DEFAULT;

    _ctx->control_channel  = NULL; _ctx->control_channel_length  = 0;
    _ctx->consensus_module_stream_id = AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_DEFAULT;

    _ctx->service_channel  = NULL; _ctx->service_channel_length  = 0;
    _ctx->service_stream_id = AERON_CLUSTER_SERVICE_STREAM_ID_DEFAULT;

    _ctx->snapshot_channel = NULL; _ctx->snapshot_channel_length = 0;
    _ctx->snapshot_stream_id = AERON_CLUSTER_SNAPSHOT_STREAM_ID_DEFAULT;

    _ctx->cluster_dir[0] = '\0';
    _ctx->app_version    = 0;
    _ctx->cluster_id     = 0;
    _ctx->archive        = NULL;

    _ctx->idle_strategy_func  = NULL;
    _ctx->idle_strategy_state = NULL;
    _ctx->owns_idle_strategy  = false;

    _ctx->error_handler         = NULL;
    _ctx->error_handler_clientd = NULL;
    _ctx->mark_file_update_fn      = NULL;
    _ctx->mark_file_update_clientd = NULL;
    _ctx->service = NULL;

    /* Apply env vars */
    char *value;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        aeron_cluster_service_context_set_aeron_directory_name(_ctx, value);
    }

    if ((value = getenv(AERON_CLUSTER_CONTROL_CHANNEL_ENV_VAR)))
    {
        if (aeron_cluster_service_context_set_control_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }
    else
    {
        if (aeron_cluster_service_context_set_control_channel(
            _ctx, AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->consensus_module_stream_id = aeron_config_parse_int32(
        AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_ENV_VAR,
        getenv(AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_ENV_VAR),
        _ctx->consensus_module_stream_id, INT32_MIN, INT32_MAX);

    if ((value = getenv(AERON_CLUSTER_SERVICE_STREAM_ID_ENV_VAR_SVC)) ||
        /* also accept the existing client-side INGRESS env var for compatibility */
        (value = getenv("AERON_CLUSTER_SERVICE_STREAM_ID")))
    {
        if (aeron_cluster_service_context_set_service_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }
    else
    {
        if (aeron_cluster_service_context_set_service_channel(
            _ctx, AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->service_stream_id = aeron_config_parse_int32(
        AERON_CLUSTER_SERVICE_STREAM_ID_ENV_VAR_SVC,
        getenv(AERON_CLUSTER_SERVICE_STREAM_ID_ENV_VAR_SVC),
        _ctx->service_stream_id, INT32_MIN, INT32_MAX);

    if ((value = getenv(AERON_CLUSTER_SNAPSHOT_CHANNEL_ENV_VAR)))
    {
        if (aeron_cluster_service_context_set_snapshot_channel(_ctx, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }
    else
    {
        if (aeron_cluster_service_context_set_snapshot_channel(
            _ctx, AERON_CLUSTER_SNAPSHOT_CHANNEL_DEFAULT) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    _ctx->snapshot_stream_id = aeron_config_parse_int32(
        AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR,
        getenv(AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR),
        _ctx->snapshot_stream_id, INT32_MIN, INT32_MAX);

    if ((value = getenv(AERON_CLUSTER_DIR_ENV_VAR)))
    {
        aeron_cluster_service_context_set_cluster_dir(_ctx, value);
    }

    _ctx->service_id = aeron_config_parse_int32(
        AERON_CLUSTER_SERVICE_ID_ENV_VAR,
        getenv(AERON_CLUSTER_SERVICE_ID_ENV_VAR),
        _ctx->service_id, 0, INT32_MAX);

    *ctx = _ctx;
    return 0;

error:
    aeron_free(_ctx->control_channel);
    aeron_free(_ctx->service_channel);
    aeron_free(_ctx->snapshot_channel);
    aeron_free(_ctx);
    return -1;
}

int aeron_cluster_service_context_close(aeron_cluster_service_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client && NULL != ctx->aeron)
        {
            aeron_context_t *aeron_ctx = aeron_context(ctx->aeron);
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;
            aeron_context_close(aeron_ctx);
        }
        aeron_free(ctx->control_channel);
        aeron_free(ctx->service_channel);
        aeron_free(ctx->snapshot_channel);
        if (ctx->owns_idle_strategy)
        {
            aeron_free(ctx->idle_strategy_state);
        }
        aeron_free(ctx);
    }
    return 0;
}

int aeron_cluster_service_context_conclude(aeron_cluster_service_context_t *ctx)
{
    if (NULL == ctx->service)
    {
        AERON_SET_ERR(EINVAL, "%s", "service callback struct is required");
        return -1;
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
            "backoff", &ctx->idle_strategy_state,
            "AERON_CLUSTER_SERVICE_IDLE_STRATEGY", NULL)))
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

void aeron_cluster_service_context_idle(aeron_cluster_service_context_t *ctx)
{
    ctx->idle_strategy_func(ctx->idle_strategy_state, 0);
}

void aeron_cluster_service_context_invoke_aeron_client(aeron_cluster_service_context_t *ctx)
{
    if (aeron_context_get_use_conductor_agent_invoker(aeron_context(ctx->aeron)))
    {
        aeron_main_do_work(ctx->aeron);
    }
}

/* Setters/getters */

int aeron_cluster_service_context_set_aeron(aeron_cluster_service_context_t *ctx, aeron_t *aeron)
{ ctx->aeron = aeron; return 0; }

aeron_t *aeron_cluster_service_context_get_aeron(aeron_cluster_service_context_t *ctx)
{ return ctx->aeron; }

int aeron_cluster_service_context_set_aeron_directory_name(aeron_cluster_service_context_t *ctx, const char *dir)
{ snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name), "%s", dir); return 0; }

const char *aeron_cluster_service_context_get_aeron_directory_name(aeron_cluster_service_context_t *ctx)
{ return ctx->aeron_directory_name; }

int aeron_cluster_service_context_set_service_id(aeron_cluster_service_context_t *ctx, int32_t id)
{ ctx->service_id = id; return 0; }

int32_t aeron_cluster_service_context_get_service_id(aeron_cluster_service_context_t *ctx)
{ return ctx->service_id; }

int aeron_cluster_service_context_set_control_channel(aeron_cluster_service_context_t *ctx, const char *ch)
{ return svc_ctx_set_string(&ctx->control_channel, &ctx->control_channel_length, ch); }

const char *aeron_cluster_service_context_get_control_channel(aeron_cluster_service_context_t *ctx)
{ return ctx->control_channel; }

int aeron_cluster_service_context_set_consensus_module_stream_id(aeron_cluster_service_context_t *ctx, int32_t id)
{ ctx->consensus_module_stream_id = id; return 0; }

int32_t aeron_cluster_service_context_get_consensus_module_stream_id(aeron_cluster_service_context_t *ctx)
{ return ctx->consensus_module_stream_id; }

int aeron_cluster_service_context_set_service_channel(aeron_cluster_service_context_t *ctx, const char *ch)
{ return svc_ctx_set_string(&ctx->service_channel, &ctx->service_channel_length, ch); }

const char *aeron_cluster_service_context_get_service_channel(aeron_cluster_service_context_t *ctx)
{ return ctx->service_channel; }

int aeron_cluster_service_context_set_service_stream_id(aeron_cluster_service_context_t *ctx, int32_t id)
{ ctx->service_stream_id = id; return 0; }

int32_t aeron_cluster_service_context_get_service_stream_id(aeron_cluster_service_context_t *ctx)
{ return ctx->service_stream_id; }

int aeron_cluster_service_context_set_snapshot_channel(aeron_cluster_service_context_t *ctx, const char *ch)
{ return svc_ctx_set_string(&ctx->snapshot_channel, &ctx->snapshot_channel_length, ch); }

const char *aeron_cluster_service_context_get_snapshot_channel(aeron_cluster_service_context_t *ctx)
{ return ctx->snapshot_channel; }

int aeron_cluster_service_context_set_snapshot_stream_id(aeron_cluster_service_context_t *ctx, int32_t id)
{ ctx->snapshot_stream_id = id; return 0; }

int32_t aeron_cluster_service_context_get_snapshot_stream_id(aeron_cluster_service_context_t *ctx)
{ return ctx->snapshot_stream_id; }

int aeron_cluster_service_context_set_cluster_dir(aeron_cluster_service_context_t *ctx, const char *dir)
{ snprintf(ctx->cluster_dir, sizeof(ctx->cluster_dir), "%s", dir); return 0; }

const char *aeron_cluster_service_context_get_cluster_dir(aeron_cluster_service_context_t *ctx)
{ return ctx->cluster_dir; }

int aeron_cluster_service_context_set_app_version(aeron_cluster_service_context_t *ctx, int32_t v)
{ ctx->app_version = v; return 0; }

int32_t aeron_cluster_service_context_get_app_version(aeron_cluster_service_context_t *ctx)
{ return ctx->app_version; }

int aeron_cluster_service_context_set_idle_strategy(aeron_cluster_service_context_t *ctx,
    aeron_idle_strategy_func_t func, void *state)
{ ctx->idle_strategy_func = func; ctx->idle_strategy_state = state; return 0; }

int aeron_cluster_service_context_set_error_handler(aeron_cluster_service_context_t *ctx,
    aeron_error_handler_t handler, void *clientd)
{ ctx->error_handler = handler; ctx->error_handler_clientd = clientd; return 0; }

int aeron_cluster_service_context_set_service(aeron_cluster_service_context_t *ctx,
    aeron_clustered_service_t *service)
{ ctx->service = service; return 0; }

int aeron_cluster_service_context_set_cluster_id(aeron_cluster_service_context_t *ctx, int32_t cluster_id)
{ ctx->cluster_id = cluster_id; return 0; }

int32_t aeron_cluster_service_context_get_cluster_id(aeron_cluster_service_context_t *ctx)
{ return ctx->cluster_id; }

int aeron_cluster_service_context_set_archive(aeron_cluster_service_context_t *ctx, aeron_archive_t *archive)
{ ctx->archive = (struct aeron_archive_stct *)archive; return 0; }

aeron_archive_t *aeron_cluster_service_context_get_archive(aeron_cluster_service_context_t *ctx)
{ return ctx->archive; }
