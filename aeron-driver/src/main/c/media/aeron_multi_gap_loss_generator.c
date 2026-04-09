/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
#include <stdbool.h>
#include <stdint.h>

#include "aeron_alloc.h"
#include "aeron_driver_common.h"
#include "collections/aeron_int64_counter_map.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "media/aeron_multi_gap_loss_generator.h"

typedef struct aeron_multi_gap_loss_generator_state_stct
{
    int32_t term_id;
    int32_t gap_radix_bits;
    int32_t gap_radix_mask;
    int32_t gap_length;
    int32_t last_gap_limit;
    aeron_int64_counter_map_t stream_and_session_id_to_offset_map;
}
aeron_multi_gap_loss_generator_state_t;

static bool aeron_multi_gap_loss_generator_should_drop_frame_detailed(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    aeron_multi_gap_loss_generator_state_t *s = (aeron_multi_gap_loss_generator_state_t *)state;

    if (s->term_id != term_id)
    {
        return false;
    }

    if (term_offset > s->last_gap_limit)
    {
        return false;
    }

    const int64_t key = aeron_map_compound_key(stream_id, session_id);
    int64_t maximum_dropped_offset = aeron_int64_counter_map_get(&s->stream_and_session_id_to_offset_map, key);

    if (maximum_dropped_offset == AERON_LOSS_GENERATOR_NULL_OFFSET)
    {
        maximum_dropped_offset = term_offset;
        if (aeron_int64_counter_map_put(
            &s->stream_and_session_id_to_offset_map, key, maximum_dropped_offset, NULL) != 0)
        {
            return false;
        }
    }

    if (maximum_dropped_offset > term_offset)
    {
        return false;
    }

    const int32_t frame_limit = term_offset + length;

    const int32_t previous_gap_offset = term_offset & s->gap_radix_mask;
    const int32_t previous_gap_limit = previous_gap_offset + s->gap_length;

    if (previous_gap_offset > 0 && term_offset < previous_gap_limit)
    {
        aeron_int64_counter_map_put(&s->stream_and_session_id_to_offset_map, key, frame_limit, NULL);
        return true;
    }

    const int32_t next_gap_offset = ((term_offset >> s->gap_radix_bits) + 1) << s->gap_radix_bits;
    const int32_t next_gap_limit = next_gap_offset + s->gap_length;

    if (frame_limit > next_gap_offset && term_offset < next_gap_limit)
    {
        aeron_int64_counter_map_put(&s->stream_and_session_id_to_offset_map, key, frame_limit, NULL);
        return true;
    }

    return false;
}

int aeron_multi_gap_loss_generator_create(
    aeron_loss_generator_t **generator,
    int32_t term_id,
    int32_t gap_radix,
    int32_t gap_length,
    int32_t total_gaps)
{
    const int32_t actual_gap_radix = aeron_find_next_power_of_two(gap_radix);
    if (gap_length >= actual_gap_radix)
    {
        AERON_SET_ERR(EINVAL, "gap_length (%d) must be smaller than gap_radix (%d)", gap_length, actual_gap_radix);
        return -1;
    }

    aeron_multi_gap_loss_generator_state_t *state = NULL;
    if (aeron_loss_generator_alloc(generator, sizeof(*state), (void **)&state) < 0)
    {
        return -1;
    }

    if (aeron_int64_counter_map_init(
        &state->stream_and_session_id_to_offset_map,
        AERON_LOSS_GENERATOR_NULL_OFFSET,
        16,
        AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        aeron_free(*generator);
        return -1;
    }

    state->term_id = term_id;
    state->gap_radix_bits = aeron_number_of_trailing_zeroes(actual_gap_radix);
    state->gap_radix_mask = ~(actual_gap_radix - 1);
    state->gap_length = gap_length;
    state->last_gap_limit = (total_gaps * actual_gap_radix) + gap_length;

    (*generator)->should_drop_frame_detailed = aeron_multi_gap_loss_generator_should_drop_frame_detailed;
    return 0;
}

void aeron_multi_gap_loss_generator_delete(aeron_loss_generator_t *generator)
{
    if (NULL != generator)
    {
        aeron_multi_gap_loss_generator_state_t *state =
            (aeron_multi_gap_loss_generator_state_t *)generator->state;
        aeron_int64_counter_map_delete(&state->stream_and_session_id_to_offset_map);
        aeron_free(generator);
    }
}
