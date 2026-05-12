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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <inttypes.h>

#include "aeron_common.h"
#include "util/aeron_fileutil.h"
#include "aeronc.h"
#include "aeron_context.h"
#include "util/aeron_error.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_mpsc_rb.h"

#include <stdio.h>

int aeron_fprintf(const char* src_, uint64_t line_, void *stream, const char *format, ...)
{
    va_list list;
    va_start(list, format);
    int ret = aeron_get_fprintf_handler()(src_, line_, (FILE*)stream, format, list);
    va_end(list);
    return ret;
}

static int aeron_default_fprintf(const char* src_, uint64_t line_, void *stream, const char *format, va_list list)
{
    return vfprintf(stream, format, list);
}

static aeron_fprintf_handler_t* aeron_get_fprintf_handler_impl()
{
    static aeron_fprintf_handler_t ret = &aeron_default_fprintf;
    return &ret;
}

aeron_fprintf_handler_t aeron_set_fprintf_handler(aeron_fprintf_handler_t fn)
{
    aeron_fprintf_handler_t* ptr = aeron_get_fprintf_handler_impl();
    aeron_fprintf_handler_t  ret = *ptr;

    *ptr = fn;
    return ret;
}

aeron_fprintf_handler_t aeron_get_fprintf_handler()
{
    return *aeron_get_fprintf_handler_impl();
}

int aeron_client_connect_to_driver(aeron_mapped_file_t *cnc_mmap, aeron_context_t *context)
{
    long long start_ms = context->epoch_clock();
    long long deadline_ms = start_ms + (long long)context->driver_timeout_ms;
    char filename[AERON_MAX_PATH];
    if (aeron_cnc_resolve_filename(context->aeron_dir, filename, sizeof(filename)) < 0)
    {
        AERON_APPEND_ERR("Failed to resolve CnC file path: dir=%s, filename=%s", context->aeron_dir, filename);
        return -1;
    }

    while (true)
    {
        aeron_cnc_metadata_t *metadata;
        aeron_cnc_load_result_t result = aeron_cnc_map_file_and_load_metadata(context->aeron_dir, cnc_mmap, &metadata);
        switch (result)
        {
            case AERON_CNC_LOAD_FAILED:
                AERON_APPEND_ERR("%s", "");
                return -1;

            case AERON_CNC_LOAD_AWAIT_FILE:
                if (context->epoch_clock() > deadline_ms)
                {
                    AERON_SET_ERR(-AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "CnC file not created: %s", filename);
                    return -1;
                }
                aeron_micro_sleep(16 * 1000);
                continue;

            case AERON_CNC_LOAD_AWAIT_MMAP:
                aeron_micro_sleep(1000);
                continue;

            case AERON_CNC_LOAD_AWAIT_VERSION:
                if (context->epoch_clock() > deadline_ms)
                {
                    AERON_SET_ERR(
                        -AERON_CLIENT_ERROR_CLIENT_TIMEOUT, "CnC file is created but not initialised: %s", filename);
                    aeron_unmap(cnc_mmap);
                    return -1;
                }
                aeron_micro_sleep(1000);
                continue;

            case AERON_CNC_LOAD_AWAIT_CNC_DATA:
                aeron_micro_sleep(1000);
                continue;

            case AERON_CNC_LOAD_SUCCESS:
            default:
                break;
        }

        aeron_mpsc_rb_t rb;

        if (aeron_mpsc_rb_init(
            &rb, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) != 0)
        {
            AERON_APPEND_ERR("%s", "Unable to initialise to_driver_buffer");
            aeron_unmap(cnc_mmap);
            return -1;
        }

        while (0 == aeron_mpsc_rb_consumer_heartbeat_time_value(&rb))
        {
            if (context->epoch_clock() > deadline_ms)
            {
                AERON_SET_ERR(
                    -AERON_CLIENT_ERROR_DRIVER_TIMEOUT,
                    "no driver heartbeat detected after: %" PRIu64 "ms",
                    context->driver_timeout_ms);
                aeron_unmap(cnc_mmap);
                return -1;
            }

            aeron_micro_sleep(1000);
        }

        long long now_ms = context->epoch_clock();
        if (aeron_mpsc_rb_consumer_heartbeat_time_value(&rb) < (now_ms - (long long)context->driver_timeout_ms))
        {
            if (now_ms > deadline_ms)
            {
                AERON_SET_ERR(
                    -AERON_CLIENT_ERROR_DRIVER_TIMEOUT,
                    "no driver heartbeat detected after: %" PRIu64 "ms",
                    context->driver_timeout_ms);
                aeron_unmap(cnc_mmap);
                return -1;
            }

            aeron_unmap(cnc_mmap);

            aeron_micro_sleep(100 * 1000);
            continue;
        }

        break;
    }

    return 0;
}
