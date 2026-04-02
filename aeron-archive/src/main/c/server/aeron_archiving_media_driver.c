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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "aeron_archiving_media_driver.h"
#include "aeronmd.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "concurrent/aeron_thread.h"

static void *archive_duty_cycle_thread(void *arg)
{
    aeron_archiving_media_driver_t *amd = (aeron_archiving_media_driver_t *)arg;
    while (amd->running)
    {
        int work = aeron_archive_server_do_work(amd->archive);
        if (work <= 0)
        {
            aeron_micro_sleep(1000); /* 1ms idle */
        }
    }
    return NULL;
}

int aeron_archiving_media_driver_launch(
    aeron_archiving_media_driver_t **archiving_driver,
    aeron_driver_context_t *driver_ctx,
    aeron_archive_server_context_t *archive_ctx)
{
    aeron_driver_context_t *d_ctx = driver_ctx;
    aeron_archive_server_context_t *a_ctx = archive_ctx;
    aeron_driver_t *driver = NULL;
    aeron_archive_server_t *archive = NULL;
    bool owns_driver_ctx = false;
    bool owns_archive_ctx = false;

    /* Initialise driver context if not provided */
    if (NULL == d_ctx)
    {
        if (aeron_driver_context_init(&d_ctx) < 0)
        {
            return -1;
        }
        owns_driver_ctx = true;
    }

    /* Start the media driver with background thread (not manual) */
    if (aeron_driver_init(&driver, d_ctx) < 0)
    {
        goto error_cleanup;
    }

    if (aeron_driver_start(driver, false) < 0)
    {
        goto error_cleanup;
    }

    /* Initialise archive context if not provided */
    if (NULL == a_ctx)
    {
        if (aeron_archive_server_context_init(&a_ctx) < 0)
        {
            goto error_cleanup;
        }
        owns_archive_ctx = true;
    }

    /* Configure archive to use the driver's aeron directory */
    const char *aeron_dir = aeron_driver_context_get_dir(d_ctx);
    if (NULL != aeron_dir)
    {
        snprintf(a_ctx->aeron_directory_name, sizeof(a_ctx->aeron_directory_name), "%s", aeron_dir);
    }

    /* Conclude and create the archive server */
    if (!a_ctx->is_concluded)
    {
        if (aeron_archive_server_context_conclude(a_ctx) < 0)
        {
            goto error_cleanup;
        }
    }

    if (aeron_archive_server_create(&archive, a_ctx) < 0)
    {
        goto error_cleanup;
    }

    /* Allocate the aggregate */
    aeron_archiving_media_driver_t *amd = NULL;
    if (aeron_alloc((void **)&amd, sizeof(aeron_archiving_media_driver_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archiving media driver");
        goto error_cleanup;
    }

    amd->driver = driver;
    amd->driver_ctx = d_ctx;
    amd->archive = archive;
    amd->archive_ctx = a_ctx;
    amd->running = true;
    amd->archive_thread = NULL;

    /* Start archive duty cycle on background thread */
    aeron_thread_t *thread = NULL;
    if (aeron_alloc((void **)&thread, sizeof(aeron_thread_t)) < 0)
    {
        aeron_free(amd);
        goto error_cleanup;
    }
    if (aeron_thread_create(thread, NULL, archive_duty_cycle_thread, amd) != 0)
    {
        AERON_SET_ERR(errno, "%s", "failed to create archive thread");
        aeron_free(thread);
        aeron_free(amd);
        goto error_cleanup;
    }
    amd->archive_thread = thread;

    *archiving_driver = amd;
    return 0;

error_cleanup:
    if (NULL != archive)
    {
        aeron_archive_server_close(archive);
        archive = NULL;
        a_ctx = NULL;
    }
    else if (owns_archive_ctx && NULL != a_ctx)
    {
        aeron_archive_server_context_close(a_ctx);
    }

    if (NULL != driver)
    {
        aeron_driver_close(driver);
        driver = NULL;
        d_ctx = NULL;
    }
    else if (owns_driver_ctx && NULL != d_ctx)
    {
        aeron_driver_context_close(d_ctx);
    }

    return -1;
}

int aeron_archiving_media_driver_do_work(aeron_archiving_media_driver_t *archiving_driver)
{
    if (NULL == archiving_driver)
    {
        return 0;
    }

    int work_count = 0;

    /* Drive the media driver main loop (manual mode) */
    if (NULL != archiving_driver->driver)
    {
        int result = aeron_driver_main_do_work(archiving_driver->driver);
        if (result < 0)
        {
            return -1;
        }
        work_count += result;
    }

    /* Drive the archive conductor */
    if (NULL != archiving_driver->archive)
    {
        int result = aeron_archive_server_do_work(archiving_driver->archive);
        if (result < 0)
        {
            return -1;
        }
        work_count += result;
    }

    return work_count;
}

int aeron_archiving_media_driver_close(aeron_archiving_media_driver_t *archiving_driver)
{
    if (NULL == archiving_driver)
    {
        return 0;
    }

    /* Stop archive background thread */
    archiving_driver->running = false;
    if (NULL != archiving_driver->archive_thread)
    {
        aeron_thread_t *thread = (aeron_thread_t *)archiving_driver->archive_thread;
        aeron_thread_join(*thread, NULL);
        aeron_free(thread);
        archiving_driver->archive_thread = NULL;
    }

    /* Close archive first, then driver (reverse order of creation) */
    if (NULL != archiving_driver->archive)
    {
        aeron_archive_server_close(archiving_driver->archive);
        archiving_driver->archive = NULL;
        archiving_driver->archive_ctx = NULL;
    }

    if (NULL != archiving_driver->driver)
    {
        aeron_driver_close(archiving_driver->driver);
        archiving_driver->driver = NULL;
        archiving_driver->driver_ctx = NULL;
    }

    aeron_free(archiving_driver);
    return 0;
}

aeron_archive_server_t *aeron_archiving_media_driver_archive(
    aeron_archiving_media_driver_t *archiving_driver)
{
    return (NULL != archiving_driver) ? archiving_driver->archive : NULL;
}

aeron_driver_t *aeron_archiving_media_driver_driver(
    aeron_archiving_media_driver_t *archiving_driver)
{
    return (NULL != archiving_driver) ? archiving_driver->driver : NULL;
}
