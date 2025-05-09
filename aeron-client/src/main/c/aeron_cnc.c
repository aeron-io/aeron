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
#include <inttypes.h>

#include "aeron_alloc.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_distinct_error_log.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_counters_manager.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "reports/aeron_loss_reporter.h"
#include "aeron_cnc_file_descriptor.h"

typedef struct aeron_cnc_stct
{
    aeron_mapped_file_t cnc_mmap;
    aeron_cnc_metadata_t *metadata;
    aeron_counters_reader_t counters_reader;
    char base_path[AERON_MAX_PATH];
    char filename[AERON_MAX_PATH];
}
aeron_cnc_t;

int aeron_cnc_init(aeron_cnc_t **aeron_cnc, const char *base_path, int64_t timeout_ms)
{
    aeron_cnc_t *_aeron_cnc;

    if (aeron_alloc((void **)&_aeron_cnc, sizeof(aeron_cnc_t)) < 0)
    {
        AERON_APPEND_ERR("Failed to allocate aeron_cnc, cnc filename: %s", base_path);
        return AERON_CNC_LOAD_FAILED;
    }

    strncpy(_aeron_cnc->base_path, base_path, sizeof(_aeron_cnc->base_path) - 1);
    aeron_cnc_resolve_filename(base_path, _aeron_cnc->filename, sizeof(_aeron_cnc->filename));

    int64_t deadline_ms = aeron_epoch_clock() + timeout_ms;
    while (true)
    {
        aeron_cnc_load_result_t result = aeron_cnc_map_file_and_load_metadata(
            base_path, &_aeron_cnc->cnc_mmap, &_aeron_cnc->metadata);

        if (AERON_CNC_LOAD_SUCCESS == result)
        {
            break;
        }
        else if (AERON_CNC_LOAD_FAILED == result)
        {
            AERON_APPEND_ERR("%s", "Failed to load aeron_cnc_t");
            goto error;
        }
        else
        {
            if (deadline_ms <= aeron_epoch_clock())
            {
                AERON_SET_ERR(
                    AERON_CLIENT_ERROR_DRIVER_TIMEOUT,
                    "Timed out waiting for CnC file to become available after %" PRId64 "ms",
                    timeout_ms);
                goto error;
            }

            aeron_micro_sleep(16 * 1000);
        }
    }

    aeron_counters_reader_init(
        &_aeron_cnc->counters_reader,
        aeron_cnc_counters_metadata_buffer(_aeron_cnc->metadata),
        _aeron_cnc->metadata->counter_metadata_buffer_length,
        aeron_cnc_counters_values_buffer(_aeron_cnc->metadata),
        _aeron_cnc->metadata->counter_values_buffer_length);

    *aeron_cnc = _aeron_cnc;
    return 0;

error:
    aeron_free(_aeron_cnc);
    return -1;
}

const char *aeron_cnc_filename(aeron_cnc_t *aeron_cnc)
{
    return aeron_cnc->filename;
}

int aeron_cnc_constants(aeron_cnc_t *aeron_cnc, aeron_cnc_constants_t *constants)
{
    if (NULL == aeron_cnc || NULL == constants)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, aeron_cnc: %s, constants: %s",
            AERON_NULL_STR(aeron_cnc),
            AERON_NULL_STR(constants));
        return -1;
    }

    // A memcpy would be faster, but this allows the metadata to evolve independently of
    // the constants. This is also not a critical performance path.
    constants->cnc_version = aeron_cnc->metadata->cnc_version;
    constants->to_driver_buffer_length = aeron_cnc->metadata->to_driver_buffer_length;
    constants->to_clients_buffer_length = aeron_cnc->metadata->to_clients_buffer_length;
    constants->counter_metadata_buffer_length = aeron_cnc->metadata->counter_metadata_buffer_length;
    constants->counter_values_buffer_length = aeron_cnc->metadata->counter_values_buffer_length;
    constants->error_log_buffer_length = aeron_cnc->metadata->error_log_buffer_length;
    constants->client_liveness_timeout = aeron_cnc->metadata->client_liveness_timeout;
    constants->start_timestamp = aeron_cnc->metadata->start_timestamp;
    constants->pid = aeron_cnc->metadata->pid;
    constants->file_page_size = aeron_cnc->metadata->file_page_size;

    return 0;
}

int64_t aeron_cnc_to_driver_heartbeat(aeron_cnc_t *aeron_cnc)
{
    uint8_t *to_driver_buffer = aeron_cnc_to_driver_buffer(aeron_cnc->metadata);
    aeron_mpsc_rb_t to_driver_rb = { 0 };
    if (aeron_mpsc_rb_init(&to_driver_rb, to_driver_buffer, aeron_cnc->metadata->to_driver_buffer_length) < 0)
    {
        return -1;
    }

    return aeron_mpsc_rb_consumer_heartbeat_time_value(&to_driver_rb);
}

size_t aeron_cnc_error_log_read(
    aeron_cnc_t *aeron_cnc,
    aeron_error_log_reader_func_t callback,
    void *clientd,
    int64_t since_timestamp)
{
    uint8_t *error_buffer = aeron_cnc_error_log_buffer(aeron_cnc->metadata);

    return aeron_error_log_read(
        error_buffer, aeron_cnc->metadata->error_log_buffer_length, callback, clientd, since_timestamp);
}

aeron_counters_reader_t *aeron_cnc_counters_reader(aeron_cnc_t *aeron_cnc)
{
    return &aeron_cnc->counters_reader;
}

int aeron_cnc_loss_reporter_read(
    aeron_cnc_t *aeron_cnc,
    aeron_loss_reporter_read_entry_func_t entry_func,
    void *clientd)
{
    char loss_report_filename[AERON_MAX_PATH];

    if (aeron_loss_reporter_resolve_filename(
        aeron_cnc->base_path, loss_report_filename, sizeof(loss_report_filename)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to resolve loss report file name");
        return -1;
    }

    aeron_mapped_file_t loss_mmap;
    if (aeron_map_existing_file(&loss_mmap, loss_report_filename) < 0) 
    {
        AERON_APPEND_ERR("%s", "Failed to map loss report");
        return -1;
    }

    int result = (int)aeron_loss_reporter_read(loss_mmap.addr, loss_mmap.length, entry_func, clientd);

    aeron_unmap(&loss_mmap);

    return result;
}

void aeron_cnc_close(aeron_cnc_t *aeron_cnc)
{
    if (NULL != aeron_cnc)
    {
        aeron_unmap(&aeron_cnc->cnc_mmap);
        aeron_free(aeron_cnc);
    }
}
