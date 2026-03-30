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
#include <stdlib.h>
#include <sys/stat.h>

#include "aeron_cluster_service_container.h"
#include "aeron_cluster_service_context.h"
#include "aeron_clustered_service_agent.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define AERON_CLUSTER_MAX_SERVICE_COUNT 127

/* -----------------------------------------------------------------------
 * conclude: validate & fill defaults — mirrors Java ClusteredServiceContainer.Context.conclude()
 * ----------------------------------------------------------------------- */
int aeron_cluster_service_container_conclude(aeron_cluster_service_context_t *ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "ctx must not be NULL");
        return -1;
    }
    if (NULL == ctx->aeron)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron client must be set on context before conclude");
        return -1;
    }
    if (NULL == ctx->service)
    {
        AERON_SET_ERR(EINVAL, "%s", "clustered service must be set on context before conclude");
        return -1;
    }

    /* Validate service ID range — mirrors Java: serviceId in [0, MAX_SERVICE_COUNT) */
    if (ctx->service_id < 0 || ctx->service_id >= AERON_CLUSTER_MAX_SERVICE_COUNT)
    {
        AERON_SET_ERR(EINVAL, "service_id %d out of range [0, %d)",
            ctx->service_id, AERON_CLUSTER_MAX_SERVICE_COUNT);
        return -1;
    }

    /* Fill channel defaults if not already set by the user */
    if (NULL == ctx->control_channel)
    {
        aeron_cluster_service_context_set_control_channel(ctx,
            AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT);
    }
    if (NULL == ctx->service_channel)
    {
        aeron_cluster_service_context_set_service_channel(ctx,
            AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT);
    }
    if (NULL == ctx->snapshot_channel)
    {
        aeron_cluster_service_context_set_snapshot_channel(ctx,
            AERON_CLUSTER_SNAPSHOT_CHANNEL_DEFAULT);
    }
    if (ctx->consensus_module_stream_id == 0)
    {
        ctx->consensus_module_stream_id = AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_DEFAULT;
    }
    if (ctx->service_stream_id == 0)
    {
        ctx->service_stream_id = AERON_CLUSTER_SERVICE_STREAM_ID_DEFAULT;
    }
    if (ctx->snapshot_stream_id == 0)
    {
        ctx->snapshot_stream_id = AERON_CLUSTER_SNAPSHOT_STREAM_ID_DEFAULT;
    }

    /* Fill cluster_dir from env var if not explicitly set */
    if (ctx->cluster_dir[0] == '\0')
    {
        const char *env_dir = getenv(AERON_CLUSTER_DIR_ENV_VAR);
        if (NULL != env_dir && env_dir[0] != '\0')
        {
            aeron_cluster_service_context_set_cluster_dir(ctx, env_dir);
        }
    }

    /* Ensure cluster directory exists */
    if (ctx->cluster_dir[0] != '\0')
    {
        struct stat st;
        if (stat(ctx->cluster_dir, &st) != 0)
        {
            if (mkdir(ctx->cluster_dir, 0755) != 0 && errno != EEXIST)
            {
                AERON_SET_ERR(errno, "failed to create cluster dir: %s", ctx->cluster_dir);
                return -1;
            }
        }
    }

    /* Fill app_version from env var if not set */
    if (ctx->app_version == 0)
    {
        const char *env_ver = getenv(AERON_CLUSTER_SERVICE_APP_VERSION_ENV_VAR);
        if (NULL != env_ver)
        {
            ctx->app_version = (int32_t)strtol(env_ver, NULL, 10);
        }
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * create
 * ----------------------------------------------------------------------- */
int aeron_cluster_service_container_create(
    aeron_cluster_service_container_t **container,
    aeron_cluster_service_context_t *ctx)
{
    aeron_cluster_service_container_t *c = NULL;
    if (aeron_alloc((void **)&c, sizeof(*c)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate ClusteredServiceContainer");
        return -1;
    }

    c->ctx        = ctx;
    c->agent      = NULL;
    c->is_running = false;

    if (aeron_clustered_service_agent_create(&c->agent, ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to create ClusteredServiceAgent");
        aeron_free(c);
        return -1;
    }

    *container = c;
    return 0;
}

/* -----------------------------------------------------------------------
 * start
 * ----------------------------------------------------------------------- */
int aeron_cluster_service_container_start(aeron_cluster_service_container_t *container)
{
    if (aeron_clustered_service_agent_on_start(container->agent) < 0)
    {
        AERON_APPEND_ERR("%s", "ClusteredServiceContainer: on_start failed");
        return -1;
    }
    container->is_running = true;
    return 0;
}

/* -----------------------------------------------------------------------
 * do_work
 * ----------------------------------------------------------------------- */
int aeron_cluster_service_container_do_work(
    aeron_cluster_service_container_t *container,
    int64_t now_ns)
{
    if (!container->is_running) { return 0; }

    int work = aeron_clustered_service_agent_do_work(container->agent, now_ns);

    /* Stop when agent signals termination */
    if (!container->agent->is_service_active)
    {
        container->is_running = false;
    }

    return work;
}

bool aeron_cluster_service_container_is_running(
    const aeron_cluster_service_container_t *container)
{
    return container->is_running;
}

/* -----------------------------------------------------------------------
 * close
 * ----------------------------------------------------------------------- */
int aeron_cluster_service_container_close(aeron_cluster_service_container_t *container)
{
    if (NULL == container) { return 0; }
    aeron_clustered_service_agent_close(container->agent);
    aeron_free(container);
    return 0;
}
