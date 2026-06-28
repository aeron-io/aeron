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
#include "concurrent/aeron_atomic.h"
#include "aeron_stream_id_frame_data_loss_generator.h"

typedef struct aeron_stream_id_frame_data_loss_generator_state_stct
{
    int32_t stream_id;
    aeron_test_frame_data_predicate_func_t predicate;
    void *clientd;
    volatile bool enabled;
}
aeron_stream_id_frame_data_loss_generator_state_t;

static bool aeron_stream_id_frame_data_loss_generator_should_drop_frame_detailed(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    aeron_stream_id_frame_data_loss_generator_state_t *s =
        (aeron_stream_id_frame_data_loss_generator_state_t *)state;
    bool enabled;
    AERON_GET_ACQUIRE(enabled, s->enabled);

    if (!enabled || stream_id != s->stream_id || NULL == s->predicate)
    {
        return false;
    }

    return s->predicate(buffer, (size_t)length, s->clientd);
}

int aeron_stream_id_frame_data_loss_generator_create(aeron_loss_generator_t **generator)
{
    aeron_stream_id_frame_data_loss_generator_state_t *state = NULL;
    if (aeron_loss_generator_alloc(generator, sizeof(*state), (void **)&state) < 0)
    {
        return -1;
    }

    (*generator)->should_drop_frame_detailed = aeron_stream_id_frame_data_loss_generator_should_drop_frame_detailed;
    return 0;
}

void aeron_stream_id_frame_data_loss_generator_enable(
    aeron_loss_generator_t *generator,
    int32_t stream_id,
    aeron_test_frame_data_predicate_func_t predicate,
    void *clientd)
{
    aeron_stream_id_frame_data_loss_generator_state_t *state =
        (aeron_stream_id_frame_data_loss_generator_state_t *)generator->state;
    state->stream_id = stream_id;
    state->predicate = predicate;
    state->clientd = clientd;
    AERON_SET_RELEASE(state->enabled, true);
}

void aeron_stream_id_frame_data_loss_generator_disable(aeron_loss_generator_t *generator)
{
    aeron_stream_id_frame_data_loss_generator_state_t *state =
        (aeron_stream_id_frame_data_loss_generator_state_t *)generator->state;
    AERON_SET_RELEASE(state->enabled, false);
}

void aeron_stream_id_frame_data_loss_generator_delete(aeron_loss_generator_t *generator)
{
    aeron_free(generator);
}
