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

#include "aeron_cluster_backup_media_driver.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_cluster_backup_media_driver_launch(
    aeron_cluster_backup_media_driver_t **driver,
    aeron_cluster_backup_context_t *backup_ctx,
    const char *cluster_dir)
{
    aeron_cluster_backup_media_driver_t *d = NULL;
    if (aeron_alloc((void **)&d, sizeof(*d)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate ClusterBackupMediaDriver");
        return -1;
    }

    d->aeron_ctx    = NULL;
    d->aeron        = NULL;
    d->owns_aeron   = false;
    d->archive      = NULL;
    d->backup_agent = NULL;
    d->backup_ctx   = backup_ctx;
    d->is_closed    = false;

    /* Create Aeron client if not already provided */
    if (NULL == backup_ctx->aeron)
    {
        if (aeron_context_init(&d->aeron_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to init aeron context");
            goto error;
        }

        if (aeron_init(&d->aeron, d->aeron_ctx) < 0 ||
            aeron_start(d->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to start aeron client");
            goto error;
        }

        backup_ctx->aeron = d->aeron;
        d->owns_aeron = true;
    }
    else
    {
        d->aeron = backup_ctx->aeron;
    }

    /* Connect backup archive if not provided */
    if (NULL == backup_ctx->backup_archive)
    {
        aeron_archive_context_t *arch_ctx = NULL;
        if (aeron_archive_context_init(&arch_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to init archive context");
            goto error;
        }
        aeron_archive_context_set_aeron(arch_ctx, d->aeron);

        if (aeron_archive_connect(&d->archive, arch_ctx) < 0)
        {
            aeron_archive_context_close(arch_ctx);
            AERON_APPEND_ERR("%s", "failed to connect backup archive");
            goto error;
        }
        aeron_archive_context_close(arch_ctx);
        backup_ctx->backup_archive = d->archive;
    }
    else
    {
        d->archive = backup_ctx->backup_archive;
    }

    /* Launch backup agent */
    if (aeron_cluster_backup_launch(
        &d->backup_agent, backup_ctx, cluster_dir,
        aeron_epoch_clock()) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to launch backup agent");
        goto error;
    }

    *driver = d;
    return 0;

error:
    aeron_cluster_backup_media_driver_close(d);
    return -1;
}

int aeron_cluster_backup_media_driver_do_work(
    aeron_cluster_backup_media_driver_t *driver,
    int64_t now_ms)
{
    if (NULL == driver || driver->is_closed) { return 0; }
    return aeron_cluster_backup_agent_do_work(driver->backup_agent, now_ms);
}

void aeron_cluster_backup_media_driver_close(
    aeron_cluster_backup_media_driver_t *driver)
{
    if (NULL == driver || driver->is_closed) { return; }
    driver->is_closed = true;

    if (NULL != driver->backup_agent)
    {
        aeron_cluster_backup_agent_close(driver->backup_agent);
        driver->backup_agent = NULL;
    }

    if (NULL != driver->archive)
    {
        aeron_archive_close(driver->archive);
        driver->archive = NULL;
    }

    if (driver->owns_aeron)
    {
        if (NULL != driver->aeron)
        {
            aeron_close(driver->aeron);
            driver->aeron = NULL;
        }
        if (NULL != driver->aeron_ctx)
        {
            aeron_context_close(driver->aeron_ctx);
            driver->aeron_ctx = NULL;
        }
    }

    aeron_free(driver);
}
