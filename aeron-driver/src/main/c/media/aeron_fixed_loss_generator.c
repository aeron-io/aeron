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

#include <stdbool.h>
#include <stdint.h>

#include "aeron_alloc.h"
#include "aeron_driver_common.h"
#include "collections/aeron_int64_counter_map.h"
#include "media/aeron_fixed_loss_generator.h"

typedef struct aeron_fixed_loss_generator_state_stct
{
    int32_t term_id;
    int32_t term_offset;
    int32_t length;
    aeron_int64_counter_map_t stream_and_session_id_to_offset_map;
}
aeron_fixed_loss_generator_state_t;

static bool aeron_fixed_loss_generator_should_drop_frame_detailed(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    aeron_fixed_loss_generator_state_t *s = (aeron_fixed_loss_generator_state_t *)state;

    if (s->term_id != term_id)
    {
        return false;
    }

    const int32_t drop_region_offset = s->term_offset;
    const int32_t drop_region_limit = drop_region_offset + s->length;
    if (term_offset >= drop_region_limit)
    {
        return false;
    }

    const int64_t key = aeron_map_compound_key(stream_id, session_id);
    int64_t tracking_offset = aeron_int64_counter_map_get(&s->stream_and_session_id_to_offset_map, key);

    if (tracking_offset == AERON_LOSS_GENERATOR_NULL_OFFSET)
    {
        tracking_offset = term_offset;
        if (aeron_int64_counter_map_put(&s->stream_and_session_id_to_offset_map, key, tracking_offset, NULL) != 0)
        {
            return false;
        }
    }

    if (tracking_offset > term_offset)
    {
        return false;
    }

    const int32_t frame_limit = term_offset + length;
    if (frame_limit <= drop_region_offset)
    {
        return false;
    }

    if (aeron_int64_counter_map_put(
        &s->stream_and_session_id_to_offset_map, key, (int64_t)frame_limit, NULL) != 0)
    {
        return false;
    }
    return true;
}

int aeron_fixed_loss_generator_create(
    aeron_loss_generator_t **generator,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    aeron_fixed_loss_generator_state_t *state = NULL;
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
    state->term_offset = term_offset;
    state->length = length;

    (*generator)->should_drop_frame_detailed = aeron_fixed_loss_generator_should_drop_frame_detailed;
    return 0;
}

void aeron_fixed_loss_generator_delete(aeron_loss_generator_t *generator)
{
    if (NULL != generator)
    {
        aeron_fixed_loss_generator_state_t *state =
            (aeron_fixed_loss_generator_state_t *)generator->state;
        aeron_int64_counter_map_delete(&state->stream_and_session_id_to_offset_map);
        aeron_free(generator);
    }
}
