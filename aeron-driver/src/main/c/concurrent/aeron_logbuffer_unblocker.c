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
#endif

#include "concurrent/aeron_logbuffer_unblocker.h"

bool aeron_logbuffer_unblocker_unblock(
    aeron_mapped_buffer_t *term_buffers, aeron_logbuffer_metadata_t *log_meta_data, int64_t blocked_position)
{
    const size_t term_length = term_buffers[0].length;
    const size_t position_bits_to_shift = (size_t)aeron_number_of_trailing_zeroes((int32_t)term_length);
    const int32_t blocked_term_count = (int32_t)(blocked_position >> position_bits_to_shift);
    const size_t blocked_index = aeron_logbuffer_index_by_term_count(blocked_term_count);

    int32_t active_term_count;
    AERON_GET_ACQUIRE(active_term_count, log_meta_data->active_term_count);

    int64_t raw_tail;
    AERON_GET_ACQUIRE(raw_tail, log_meta_data->term_tail_counters[blocked_index]);

    const int32_t term_id = aeron_logbuffer_term_id(raw_tail);
    const int32_t tail_offset = aeron_logbuffer_term_offset(raw_tail, (int32_t)term_length);
    const int32_t blocked_offset = aeron_logbuffer_compute_term_offset_from_position(
        blocked_position, position_bits_to_shift);

    if (active_term_count == (blocked_term_count - 1) && 0 == blocked_offset)
    {
        int64_t current_raw_tail;
        AERON_GET_ACQUIRE(
            current_raw_tail,
            log_meta_data->term_tail_counters[aeron_logbuffer_index_by_term_count(active_term_count)]);

        const int32_t current_term_id = aeron_logbuffer_term_id(current_raw_tail);
        aeron_logbuffer_rotate_log(log_meta_data, active_term_count, current_term_id);

        return true;
    }

    switch (aeron_term_unblocker_unblock(
        log_meta_data, term_buffers[blocked_index].addr, term_length, blocked_offset, tail_offset, term_id))
    {
        case AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED_TO_END:
            aeron_logbuffer_rotate_log(log_meta_data, blocked_term_count, term_id);
            return true;

        case AERON_TERM_UNBLOCKER_STATUS_UNBLOCKED:
            return true;

        case AERON_TERM_UNBLOCKER_STATUS_NO_ACTION:
            break;
    }

    return false;
}
