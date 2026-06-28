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
#include <string.h>

#include "aeron_alloc.h"
#include "concurrent/aeron_atomic.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "media/aeron_udp_channel.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_setup_at_position_loss_generator.h"

typedef struct aeron_setup_at_position_loss_generator_state_stct
{
    aeron_setup_at_position_loss_config_t *config;
    struct aeron_receive_channel_endpoint_stct *endpoint;
}
aeron_setup_at_position_loss_generator_state_t;

void aeron_setup_at_position_loss_config_init(aeron_setup_at_position_loss_config_t *config)
{
    memset(config, 0, sizeof(*config));
}

void aeron_setup_at_position_loss_config_set_target(
    aeron_setup_at_position_loss_config_t *config,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t term_offset)
{
    config->stream_id = stream_id;
    config->initial_term_id = initial_term_id;
    config->active_term_id = active_term_id;
    config->term_offset = term_offset;
}

void aeron_setup_at_position_loss_config_set_endpoint_skip_substring(
    aeron_setup_at_position_loss_config_t *config,
    const char *substring)
{
    if (NULL == substring)
    {
        config->endpoint_skip_substring[0] = '\0';
        return;
    }
    strncpy(config->endpoint_skip_substring, substring, sizeof(config->endpoint_skip_substring) - 1);
    config->endpoint_skip_substring[sizeof(config->endpoint_skip_substring) - 1] = '\0';
}

void aeron_setup_at_position_loss_config_enable(aeron_setup_at_position_loss_config_t *config)
{
    AERON_SET_RELEASE(config->enabled, true);
}

void aeron_setup_at_position_loss_config_disable(aeron_setup_at_position_loss_config_t *config)
{
    AERON_SET_RELEASE(config->enabled, false);
}

int aeron_setup_at_position_loss_config_setups_dropped(aeron_setup_at_position_loss_config_t *config)
{
    int dropped;
    AERON_GET_ACQUIRE(dropped, config->setups_dropped);
    return dropped;
}

static bool aeron_setup_at_position_loss_generator_should_drop_frame_simple(
    void *state_ptr,
    const struct sockaddr_storage *address,
    const uint8_t *buffer,
    int32_t length)
{
    (void)address;

    aeron_setup_at_position_loss_generator_state_t *state =
        (aeron_setup_at_position_loss_generator_state_t *)state_ptr;
    aeron_setup_at_position_loss_config_t *config = state->config;

    bool enabled;
    AERON_GET_ACQUIRE(enabled, config->enabled);
    if (!enabled)
    {
        return false;
    }

    if (length < (int32_t)sizeof(aeron_setup_header_t))
    {
        return false;
    }

    const aeron_frame_header_t *frame = (const aeron_frame_header_t *)buffer;
    if (AERON_HDR_TYPE_SETUP != frame->type)
    {
        return false;
    }

    const aeron_setup_header_t *setup = (const aeron_setup_header_t *)buffer;
    if (setup->stream_id != config->stream_id ||
        setup->initial_term_id != config->initial_term_id ||
        setup->active_term_id != config->active_term_id ||
        setup->term_offset != config->term_offset)
    {
        return false;
    }

    if ('\0' != config->endpoint_skip_substring[0])
    {
        // udp_channel is set by the driver after the supplier callback returns and before
        // any frame can arrive, so this dereference is safe at frame-check time.
        aeron_udp_channel_t *channel = state->endpoint->conductor_fields.udp_channel;
        if (NULL != channel && NULL != strstr(channel->original_uri, config->endpoint_skip_substring))
        {
            return false;
        }
    }

    int prev;
    AERON_GET_ACQUIRE(prev, config->setups_dropped);
    AERON_SET_RELEASE(config->setups_dropped, prev + 1);

    return true;
}

static void aeron_setup_at_position_loss_generator_close(aeron_loss_generator_t *generator)
{
    // The state was allocated as a trailing block by aeron_loss_generator_alloc,
    // so freeing the generator frees both. The shared config is owned by the caller.
    aeron_free(generator);
}

int aeron_setup_at_position_loss_generator_create(
    aeron_loss_generator_t **generator,
    aeron_setup_at_position_loss_config_t *config,
    struct aeron_receive_channel_endpoint_stct *endpoint)
{
    aeron_setup_at_position_loss_generator_state_t *state = NULL;
    if (aeron_loss_generator_alloc(generator, sizeof(*state), (void **)&state) < 0)
    {
        return -1;
    }

    state->config = config;
    state->endpoint = endpoint;
    (*generator)->should_drop_frame_simple = aeron_setup_at_position_loss_generator_should_drop_frame_simple;
    (*generator)->should_drop_frame_detailed = NULL;
    (*generator)->close = aeron_setup_at_position_loss_generator_close;

    return 0;
}
