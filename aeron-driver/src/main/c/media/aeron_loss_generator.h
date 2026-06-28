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

#ifndef AERON_LOSS_GENERATOR_H
#define AERON_LOSS_GENERATOR_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "aeron_alloc.h"
#include "aeron_socket.h"

#define AERON_LOSS_GENERATOR_NULL_OFFSET (INT64_C(-1))

typedef bool (*aeron_loss_generator_should_drop_frame_simple_func_t)(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t length);

typedef bool (*aeron_loss_generator_should_drop_frame_detailed_func_t)(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length);

typedef struct aeron_loss_generator_stct
{
    void *state;
    aeron_loss_generator_should_drop_frame_simple_func_t should_drop_frame_simple;
    aeron_loss_generator_should_drop_frame_detailed_func_t should_drop_frame_detailed;
    void (*close)(struct aeron_loss_generator_stct *generator);
}
aeron_loss_generator_t;

// Single allocation for the vtable and a trailing state block. Zero-initialized,
// so unused vtable slots may be left NULL — the dispatchers treat NULL as no-drop.
static inline int aeron_loss_generator_alloc(
    aeron_loss_generator_t **generator_out,
    size_t state_size,
    void **state_out)
{
    aeron_loss_generator_t *gen = NULL;
    if (aeron_alloc((void **)&gen, sizeof(aeron_loss_generator_t) + state_size) < 0)
    {
        return -1;
    }
    gen->state = (uint8_t *)gen + sizeof(aeron_loss_generator_t);
    *state_out = gen->state;
    *generator_out = gen;
    return 0;
}

static inline bool aeron_loss_generator_should_drop_frame(
    const aeron_loss_generator_t *generator,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t length)
{
    return NULL != generator->should_drop_frame_simple &&
        generator->should_drop_frame_simple(generator->state, address, buffer, length);
}

static inline bool aeron_loss_generator_should_drop_frame_detailed(
    const aeron_loss_generator_t *generator,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    if (NULL != generator->should_drop_frame_detailed)
    {
        return generator->should_drop_frame_detailed(
            generator->state, address, buffer, stream_id, session_id, term_id, term_offset, length);
    }

    return aeron_loss_generator_should_drop_frame(generator, address, buffer, length);
}

#endif //AERON_LOSS_GENERATOR_H
