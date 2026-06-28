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

#include "aeronc.h"
#include "aeron_alloc.h"
#include "aeron_socket.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_port_loss_generator.h"

typedef struct aeron_port_loss_generator_state_stct
{
    int port;
    int64_t drop_until_ns;
    volatile bool drop;
}
aeron_port_loss_generator_state_t;

static int aeron_port_loss_generator_port_of(const struct sockaddr_storage *address)
{
    if (AF_INET == address->ss_family)
    {
        return ntohs(((const struct sockaddr_in *)address)->sin_port);
    }
    if (AF_INET6 == address->ss_family)
    {
        return ntohs(((const struct sockaddr_in6 *)address)->sin6_port);
    }
    return -1;
}

// Assumes a single harness thread drives start_dropping and the frame check. A racing
// start_dropping with the self-disable path below can lose the new enable if the expiry
// SET_RELEASE runs after the new start_dropping.
static bool aeron_port_loss_generator_should_drop_frame_simple(
    void *state,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t length)
{
    aeron_port_loss_generator_state_t *s = (aeron_port_loss_generator_state_t *)state;
    bool drop;
    AERON_GET_ACQUIRE(drop, s->drop);

    if (drop && s->port == aeron_port_loss_generator_port_of(address))
    {
        if (aeron_nano_clock() < s->drop_until_ns)
        {
            return true;
        }
        AERON_SET_RELEASE(s->drop, false);
    }

    return false;
}

int aeron_port_loss_generator_create(aeron_loss_generator_t **generator)
{
    aeron_port_loss_generator_state_t *state = NULL;
    if (aeron_loss_generator_alloc(generator, sizeof(*state), (void **)&state) < 0)
    {
        return -1;
    }

    (*generator)->should_drop_frame_simple = aeron_port_loss_generator_should_drop_frame_simple;
    return 0;
}

void aeron_port_loss_generator_start_dropping(
    aeron_loss_generator_t *generator, int port, int64_t duration_ns)
{
    aeron_port_loss_generator_state_t *state = (aeron_port_loss_generator_state_t *)generator->state;
    state->port = port;
    state->drop_until_ns = aeron_nano_clock() + duration_ns;
    AERON_SET_RELEASE(state->drop, true);
}

void aeron_port_loss_generator_delete(aeron_loss_generator_t *generator)
{
    aeron_free(generator);
}
