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
#include <stdlib.h>

#include "aeronc.h"
#include "aeron_alloc.h"
#include "aeron_windows.h"
#include "media/aeron_random_loss_generator.h"

typedef struct aeron_random_loss_generator_state_stct
{
    double rate;
    unsigned short xsubi[3];
}
aeron_random_loss_generator_state_t;

static bool aeron_random_loss_generator_should_drop_frame_simple(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t length)
{
    aeron_random_loss_generator_state_t *s = (aeron_random_loss_generator_state_t *)state;
    return aeron_erand48(s->xsubi) <= s->rate;
}

int aeron_random_loss_generator_create(
    aeron_loss_generator_t **generator, double rate, int64_t seed)
{
    aeron_random_loss_generator_state_t *state = NULL;
    if (aeron_loss_generator_alloc(generator, sizeof(*state), (void **)&state) < 0)
    {
        return -1;
    }

    state->rate = rate;

    const int64_t seed_value = (seed == -1) ?
        aeron_nano_clock() ^ (int64_t)(uintptr_t)state : seed;
    state->xsubi[2] = (unsigned short)(seed_value & 0xFFFF);
    state->xsubi[1] = (unsigned short)((seed_value >> 16) & 0xFFFF);
    state->xsubi[0] = (unsigned short)((seed_value >> 32) & 0xFFFF);

    (*generator)->should_drop_frame_simple = aeron_random_loss_generator_should_drop_frame_simple;
    return 0;
}

void aeron_random_loss_generator_delete(aeron_loss_generator_t *generator)
{
    aeron_free(generator);
}
