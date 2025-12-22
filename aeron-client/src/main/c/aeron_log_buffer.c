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

#include "aeron_log_buffer.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_log_buffer_create(
    aeron_log_buffer_t **log_buffer, const char *log_file, int64_t correlation_id, bool pre_touch)
{
    aeron_log_buffer_t *_log_buffer = NULL;

    *log_buffer = NULL;
    if (aeron_alloc((void **)&_log_buffer, sizeof(aeron_log_buffer_t)) < 0)
    {
        AERON_APPEND_ERR(
            "Unable to allocate log buffer, log_file: %s, correlation_id: %" PRId64, log_file, correlation_id);
        return -1;
    }

    if (aeron_raw_log_map_existing(&_log_buffer->mapped_raw_log, log_file, pre_touch) < 0)
    {
        AERON_APPEND_ERR("Unable to map raw log for log buffer, correlation_id: %" PRId64, correlation_id);
        aeron_free(_log_buffer);
        return -1;
    }

    _log_buffer->correlation_id = correlation_id;
    _log_buffer->refcnt = 0;

    *log_buffer = _log_buffer;
    return 0;
}

int aeron_log_buffer_delete(aeron_log_buffer_t *log_buffer)
{
    if (NULL != log_buffer)
    {
        aeron_raw_log_close(&log_buffer->mapped_raw_log, NULL);
        aeron_free(log_buffer);
    }

    return 0;
}
