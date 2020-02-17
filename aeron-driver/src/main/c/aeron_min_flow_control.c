/*
 * Copyright 2014-2020 Real Logic Limited.
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

#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_error.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_flow_control.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"
#include <aeronmd.h>
#include <media/aeron_udp_channel.h>
#include <uri/aeron_uri.h>

typedef struct aeron_min_flow_control_strategy_receiver_stct
{
    int64_t last_position;
    int64_t last_position_plus_window;
    int64_t time_of_last_status_message;
    int64_t receiver_id;
}
aeron_min_flow_control_strategy_receiver_t;

typedef struct aeron_min_flow_control_strategy_state_stct
{
    struct receiver_stct
    {
        size_t length;
        size_t capacity;
        aeron_min_flow_control_strategy_receiver_t *array;
    }
    receivers;

    int64_t receiver_timeout_ns;
}
aeron_min_flow_control_strategy_state_t;

int64_t aeron_min_flow_control_strategy_on_idle(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    int64_t min_limit_position = INT64_MAX;

    for (int last_index = (int) strategy_state->receivers.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if ((receiver->time_of_last_status_message + strategy_state->receiver_timeout_ns) - now_ns < 0)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *) strategy_state->receivers.array,
                sizeof(aeron_min_flow_control_strategy_receiver_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            strategy_state->receivers.length--;
        }
        else
        {
            min_limit_position = receiver->last_position_plus_window < min_limit_position ?
                receiver->last_position_plus_window : min_limit_position;
        }
    }

    return strategy_state->receivers.length > 0 ? min_limit_position : snd_lmt;
}


int64_t aeron_min_flow_control_strategy_on_sm(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns)
{
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    const int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);

    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    bool is_existing = false;
    int64_t min_position = INT64_MAX;

    for (size_t i = 0; i < strategy_state->receivers.length; i++)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if (status_message_header->receiver_id == receiver->receiver_id)
        {
            receiver->last_position = position > receiver->last_position ? position : receiver->last_position;
            receiver->last_position_plus_window = position + status_message_header->receiver_window;
            receiver->time_of_last_status_message = now_ns;
            is_existing = true;
        }

        min_position = receiver->last_position_plus_window < min_position ?
            receiver->last_position_plus_window : min_position;
    }

    if (!is_existing)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, strategy_state->receivers, aeron_min_flow_control_strategy_receiver_t);

        if (ensure_capacity_result >= 0)
        {
            aeron_min_flow_control_strategy_receiver_t *receiver =
                &strategy_state->receivers.array[strategy_state->receivers.length++];

            receiver->last_position = position;
            receiver->last_position_plus_window = position + status_message_header->receiver_window;
            receiver->time_of_last_status_message = now_ns;
            receiver->receiver_id = status_message_header->receiver_id;

            min_position = (position + status_message_header->receiver_window) < min_position ?
                (position + status_message_header->receiver_window) : min_position;
        }
        else
        {
            min_position = 0 == strategy_state->receivers.length ? snd_lmt : min_position;
        }
    }

    return snd_lmt > min_position ? snd_lmt : min_position;
}

int aeron_min_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_min_flow_control_strategy_state_t *strategy_state =
        (aeron_min_flow_control_strategy_state_t *)strategy->state;

    aeron_free(strategy_state->receivers.array);
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

int aeron_min_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    aeron_flow_control_strategy_t *_strategy;
    aeron_flow_control_tagged_options_t options;

    const char* fc_options = aeron_uri_find_param_value(&channel->uri.params.udp.additional_params, AERON_URI_FC_KEY);
    size_t fc_options_length = NULL != fc_options ? strlen(fc_options) : 0;
    if (aeron_flow_control_parse_tagged_options(fc_options_length, fc_options, &options) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0 ||
        aeron_alloc((void **)&_strategy->state, sizeof(aeron_min_flow_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->on_idle = aeron_min_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_min_flow_control_strategy_on_sm;
    _strategy->fini = aeron_min_flow_control_strategy_fini;

    aeron_min_flow_control_strategy_state_t *state = (aeron_min_flow_control_strategy_state_t *)_strategy->state;

    state->receivers.array = NULL;
    state->receivers.capacity = 0;
    state->receivers.length = 0;

    state->receiver_timeout_ns = options.timeout_ns.is_present ?
        options.timeout_ns.value : context->flow_control.receiver_timeout_ns;

    *strategy = _strategy;

    return 0;
}

typedef struct aeron_tagged_flow_control_strategy_state_stct
{
    aeron_min_flow_control_strategy_state_t min_flow_control_state;
    int64_t group_receiver_tag;
    int32_t group_min_size;
    aeron_distinct_error_log_t *error_log;
}
aeron_tagged_flow_control_strategy_state_t;

int64_t aeron_tagged_flow_control_strategy_on_idle(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream)
{
    aeron_tagged_flow_control_strategy_state_t *strategy_state = (aeron_tagged_flow_control_strategy_state_t *)state;
    aeron_min_flow_control_strategy_state_t *min_strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    int64_t min_limit_position = INT64_MAX;

    for (int last_index = (int) min_strategy_state->receivers.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &min_strategy_state->receivers.array[i];

        if ((receiver->time_of_last_status_message + min_strategy_state->receiver_timeout_ns) - now_ns < 0)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *) min_strategy_state->receivers.array,
                sizeof(aeron_min_flow_control_strategy_receiver_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            min_strategy_state->receivers.length--;
        }
        else
        {
            min_limit_position = receiver->last_position_plus_window < min_limit_position ?
                receiver->last_position_plus_window : min_limit_position;
        }
    }

    return (int32_t)min_strategy_state->receivers.length < strategy_state->group_min_size ||
        min_strategy_state->receivers.length == 0 ? snd_lmt : min_limit_position;
}

int64_t aeron_tagged_flow_control_strategy_on_sm(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns)
{
    aeron_tagged_flow_control_strategy_state_t *strategy_state =
        (aeron_tagged_flow_control_strategy_state_t *)state;
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    const int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);
    const int64_t window_length = status_message_header->receiver_window;
    const int64_t receiver_id = status_message_header->receiver_id;

    int64_t sm_receiver_tag;
    const int bytes_read = aeron_udp_protocol_sm_receiver_tag(status_message_header, &sm_receiver_tag);
    const bool was_present = bytes_read == sizeof(sm_receiver_tag);
    int64_t position_plus_window = position + window_length;

    if (0 != bytes_read && !was_present)
    {
        aeron_distinct_error_log_record(
            strategy_state->error_log,
            EINVAL,
            "invalid receiver tag on status message",
            "Received a status message for tagged flow control that did not have 0 or 8 bytes for the receiver_tag");
    }

    const bool is_tagged = was_present && sm_receiver_tag == strategy_state->group_receiver_tag;

    bool is_existing = false;
    int64_t min_position = INT64_MAX;

    for (size_t i = 0; i < strategy_state->min_flow_control_state.receivers.length; i++)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver =
            &strategy_state->min_flow_control_state.receivers.array[i];

        if (is_tagged && receiver_id == receiver->receiver_id)
        {
            receiver->last_position = position > receiver->last_position ? position : receiver->last_position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message = now_ns;
            is_existing = true;
        }

        min_position = receiver->last_position_plus_window < min_position ?
            receiver->last_position_plus_window : min_position;
    }

    if (is_tagged && !is_existing)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result,
            strategy_state->min_flow_control_state.receivers,
            aeron_min_flow_control_strategy_receiver_t);

        if (ensure_capacity_result >= 0)
        {
            aeron_min_flow_control_strategy_receiver_t *receiver =
                &strategy_state->min_flow_control_state.receivers.array[strategy_state->min_flow_control_state.receivers.length++];

            receiver->last_position = position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message = now_ns;
            receiver->receiver_id = receiver_id;

            min_position = (position + window_length) < min_position ? (position + window_length) : min_position;
        }
        else
        {
            min_position = 0 == strategy_state->min_flow_control_state.receivers.length ? snd_lmt : min_position;
        }
    }

    if ((int32_t)strategy_state->min_flow_control_state.receivers.length < strategy_state->group_min_size)
    {
        return snd_lmt;
    }
    else if (strategy_state->min_flow_control_state.receivers.length == 0)
    {
        return snd_lmt > position_plus_window ? snd_lmt : position_plus_window;
    }
    else
    {
        return snd_lmt > min_position ? snd_lmt : min_position;
    }
}

int aeron_tagged_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_tagged_flow_control_strategy_state_t *strategy_state =
        (aeron_tagged_flow_control_strategy_state_t *)strategy->state;

    aeron_free(strategy_state->min_flow_control_state.receivers.array);
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

bool aeron_tagged_flow_control_strategy_has_required_receivers(aeron_flow_control_strategy_t *strategy)
{
    aeron_tagged_flow_control_strategy_state_t *strategy_state =
        (aeron_tagged_flow_control_strategy_state_t *)strategy->state;

    return strategy_state->group_min_size <= (int32_t)strategy_state->min_flow_control_state.receivers.length;
}

int aeron_tagged_flow_control_strategy_to_string(
    aeron_flow_control_strategy_t *strategy,
    char *buffer,
    size_t buffer_len)
{
    aeron_tagged_flow_control_strategy_state_t *strategy_state =
        (aeron_tagged_flow_control_strategy_state_t *)strategy->state;

    buffer[buffer_len - 1] = '\0';

    return snprintf(
        buffer,
        buffer_len - 1,
        "group_receiver_tag: %" PRId64 ", group_min_size: %" PRId32 ", receiver_count: %zu",
        strategy_state->group_receiver_tag,
        strategy_state->group_min_size,
        strategy_state->min_flow_control_state.receivers.length);
}

int aeron_tagged_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    aeron_flow_control_strategy_t *_strategy;
    aeron_flow_control_tagged_options_t options;

    const char *fc_options = aeron_uri_find_param_value(&channel->uri.params.udp.additional_params, "fc");
    if (aeron_flow_control_parse_tagged_options(NULL != fc_options ? strlen(fc_options) : 0, fc_options, &options) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0 ||
        aeron_alloc((void **)&_strategy->state, sizeof(aeron_tagged_flow_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->on_idle = aeron_tagged_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_tagged_flow_control_strategy_on_sm;
    _strategy->fini = aeron_tagged_flow_control_strategy_fini;
    _strategy->has_required_receivers = aeron_tagged_flow_control_strategy_has_required_receivers;

    aeron_tagged_flow_control_strategy_state_t *state =
        (aeron_tagged_flow_control_strategy_state_t *)_strategy->state;

    state->min_flow_control_state.receivers.array = NULL;
    state->min_flow_control_state.receivers.capacity = 0;
    state->min_flow_control_state.receivers.length = 0;
    state->group_receiver_tag = options.receiver_tag.is_present ?
        options.receiver_tag.value : context->flow_control.group_receiver_tag;
    state->group_min_size = options.group_min_size.is_present ?
        options.group_min_size.value : context->flow_control.receiver_group_min_size;

    state->error_log = context->error_log;

    state->min_flow_control_state.receiver_timeout_ns = options.timeout_ns.is_present ?
        options.timeout_ns.value : context->flow_control.receiver_timeout_ns;

    *strategy = _strategy;

    return 0;
}
