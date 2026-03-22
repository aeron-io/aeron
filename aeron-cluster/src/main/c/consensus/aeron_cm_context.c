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

#include "aeron_cm_context.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

static int ctx_set_str(char **dst, const char *src)
{
    if (NULL != *dst) { aeron_free(*dst); *dst = NULL; }
    if (NULL == src) { return 0; }
    const size_t n = strlen(src) + 1;
    if (aeron_alloc((void **)dst, n) < 0) { return -1; }
    memcpy(*dst, src, n);
    return 0;
}

int aeron_cm_context_init(aeron_cm_context_t **ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cm_context_init(NULL)");
        return -1;
    }

    aeron_cm_context_t *c = NULL;
    if (aeron_alloc((void **)&c, sizeof(aeron_cm_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate CM context");
        return -1;
    }

    c->aeron              = NULL;
    c->owns_aeron_client  = false;
    if (aeron_default_path(c->aeron_directory_name, sizeof(c->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    c->member_id          = AERON_CM_MEMBER_ID_DEFAULT;
    c->appointed_leader_id = AERON_CM_APPOINTED_LEADER_DEFAULT;
    c->service_count      = AERON_CM_SERVICE_COUNT_DEFAULT;
    c->app_version        = 0;

    c->log_channel         = NULL;
    c->log_stream_id       = AERON_CM_LOG_STREAM_ID_DEFAULT;
    c->ingress_channel     = NULL;
    c->ingress_stream_id   = AERON_CM_INGRESS_STREAM_ID_DEFAULT;
    c->consensus_channel   = NULL;
    c->consensus_stream_id = AERON_CM_CONSENSUS_STREAM_ID_DEFAULT;
    c->control_channel     = NULL;
    c->consensus_module_stream_id = AERON_CM_CONSENSUS_MODULE_STREAM_ID_DEFAULT;
    c->service_stream_id   = AERON_CM_SERVICE_STREAM_ID_DEFAULT;
    c->snapshot_channel    = NULL;
    c->snapshot_stream_id  = AERON_CM_SNAPSHOT_STREAM_ID_DEFAULT;

    c->cluster_members = NULL;
    c->cluster_dir[0]  = '\0';

    c->session_timeout_ns            = AERON_CM_SESSION_TIMEOUT_NS_DEFAULT;
    c->leader_heartbeat_timeout_ns   = AERON_CM_LEADER_HEARTBEAT_TIMEOUT_NS_DEFAULT;
    c->leader_heartbeat_interval_ns  = AERON_CM_LEADER_HEARTBEAT_INTERVAL_NS_DEFAULT;
    c->startup_canvass_timeout_ns    = AERON_CM_STARTUP_CANVASS_TIMEOUT_NS_DEFAULT;
    c->election_timeout_ns           = AERON_CM_ELECTION_TIMEOUT_NS_DEFAULT;
    c->election_status_interval_ns   = AERON_CM_ELECTION_STATUS_INTERVAL_NS_DEFAULT;
    c->termination_timeout_ns        = AERON_CM_TERMINATION_TIMEOUT_NS_DEFAULT;

    c->cluster_clock_ns     = NULL;
    c->cluster_clock_clientd = NULL;
    c->idle_strategy_func   = NULL;
    c->idle_strategy_state  = NULL;
    c->owns_idle_strategy   = false;
    c->error_handler        = NULL;
    c->error_handler_clientd = NULL;

    /* Apply env vars */
    char *v;

    if ((v = getenv(AERON_DIR_ENV_VAR)))
    { snprintf(c->aeron_directory_name, sizeof(c->aeron_directory_name), "%s", v); }

    if ((v = getenv(AERON_CM_MEMBER_ID_ENV_VAR)))
    { c->member_id = (int32_t)atoi(v); }

    if ((v = getenv(AERON_CM_LOG_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->log_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->log_channel, AERON_CM_LOG_CHANNEL_DEFAULT) < 0) goto error; }

    if ((v = getenv(AERON_CM_INGRESS_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->ingress_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->ingress_channel, AERON_CM_INGRESS_CHANNEL_DEFAULT) < 0) goto error; }

    if ((v = getenv(AERON_CM_CONSENSUS_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->consensus_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->consensus_channel, AERON_CM_CONSENSUS_CHANNEL_DEFAULT) < 0) goto error; }

    if (ctx_set_str(&c->control_channel, AERON_CM_CONTROL_CHANNEL_DEFAULT) < 0) goto error;
    if (ctx_set_str(&c->snapshot_channel, AERON_CM_SNAPSHOT_CHANNEL_DEFAULT) < 0) goto error;

    if ((v = getenv(AERON_CM_CLUSTER_MEMBERS_ENV_VAR)))
    { if (ctx_set_str(&c->cluster_members, v) < 0) goto error; }

    if ((v = getenv(AERON_CM_CLUSTER_DIR_ENV_VAR)))
    { snprintf(c->cluster_dir, sizeof(c->cluster_dir), "%s", v); }

    if ((v = getenv(AERON_CM_SERVICE_COUNT_ENV_VAR)))
    { c->service_count = atoi(v); }

    c->consensus_stream_id = aeron_config_parse_int32(
        AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR,
        getenv(AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR),
        c->consensus_stream_id, INT32_MIN, INT32_MAX);

    if ((v = getenv(AERON_CM_SESSION_TIMEOUT_ENV_VAR)))
    { c->session_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CM_SESSION_TIMEOUT_ENV_VAR, v, c->session_timeout_ns, 1000, INT64_MAX); }

    if ((v = getenv(AERON_CM_ELECTION_TIMEOUT_ENV_VAR)))
    { c->election_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CM_ELECTION_TIMEOUT_ENV_VAR, v, c->election_timeout_ns, 1000, INT64_MAX); }

    *ctx = c;
    return 0;

error:
    aeron_free(c->log_channel);
    aeron_free(c->ingress_channel);
    aeron_free(c->consensus_channel);
    aeron_free(c->control_channel);
    aeron_free(c->snapshot_channel);
    aeron_free(c->cluster_members);
    aeron_free(c);
    return -1;
}

int aeron_cm_context_close(aeron_cm_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client && NULL != ctx->aeron)
        {
            aeron_context_t *ac = aeron_context(ctx->aeron);
            aeron_close(ctx->aeron);
            aeron_context_close(ac);
        }
        aeron_free(ctx->log_channel);
        aeron_free(ctx->ingress_channel);
        aeron_free(ctx->consensus_channel);
        aeron_free(ctx->control_channel);
        aeron_free(ctx->snapshot_channel);
        aeron_free(ctx->cluster_members);
        if (ctx->owns_idle_strategy) { aeron_free(ctx->idle_strategy_state); }
        aeron_free(ctx);
    }
    return 0;
}

int aeron_cm_context_conclude(aeron_cm_context_t *ctx)
{
    if (NULL == ctx->cluster_members || '\0' == *ctx->cluster_members)
    {
        AERON_SET_ERR(EINVAL, "%s", "cluster_members is required");
        return -1;
    }

    if (NULL == ctx->aeron)
    {
        ctx->owns_aeron_client = true;
        aeron_context_t *ac;
        if (aeron_context_init(&ac) < 0 ||
            aeron_context_set_dir(ac, ctx->aeron_directory_name) < 0 ||
            aeron_init(&ctx->aeron, ac) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    if (NULL == ctx->idle_strategy_func)
    {
        ctx->owns_idle_strategy = true;
        ctx->idle_strategy_func = aeron_idle_strategy_load(
            "backoff", &ctx->idle_strategy_state,
            "AERON_CM_IDLE_STRATEGY", NULL);
        if (NULL == ctx->idle_strategy_func) { AERON_APPEND_ERR("%s", ""); return -1; }
    }

    return 0;
}
