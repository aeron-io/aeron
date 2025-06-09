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

#include <errno.h>
#include <inttypes.h>
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "util/aeron_arrayutil.h"
#include "aeron_flow_control.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"
#include "media/aeron_udp_channel.h"
#include "aeron_counters.h"
#include "aeron_position.h"

typedef struct aeron_min_flow_control_strategy_receiver_stct
{
    uint8_t padding_before[AERON_CACHE_LINE_LENGTH];
    int64_t last_position;
    int64_t last_position_plus_window;
    int64_t time_of_last_status_message_ns;
    int64_t receiver_id;
    int32_t session_id;
    int32_t stream_id;
    bool eos_flagged;
    uint8_t padding_after[AERON_CACHE_LINE_LENGTH];
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

    volatile bool has_required_receivers;
    int64_t receiver_timeout_ns;
    int64_t time_of_last_setup_ns;
    int64_t last_setup_snd_lmt;
    int64_t group_tag;
    int32_t group_min_size;
    bool has_tagged_status_message_triggered_setup;
    const aeron_udp_channel_t *channel;
    aeron_counters_manager_t *counters_manager;

    aeron_distinct_error_log_t *error_log;
    struct
    {
        aeron_driver_flow_control_strategy_on_receiver_change_func_t receiver_added;
        aeron_driver_flow_control_strategy_on_receiver_change_func_t receiver_removed;
    } log;
    aeron_position_t receivers_counter;
    size_t retransmit_receiver_window_multiple;
}
aeron_min_flow_control_strategy_state_t;

int64_t aeron_min_flow_control_strategy_last_setup_snd_lmt(
    aeron_min_flow_control_strategy_state_t *strategy_state,
    int64_t now_ns)
{
    if (-1 != strategy_state->last_setup_snd_lmt)
    {
        if ((strategy_state->time_of_last_setup_ns + strategy_state->receiver_timeout_ns) - now_ns < 0)
        {
            strategy_state->last_setup_snd_lmt = -1;
        }
        else
        {
            return strategy_state->last_setup_snd_lmt;
        }
    }

    return INT64_MAX;
}

int64_t aeron_min_flow_control_strategy_on_idle(
    void *state, int64_t now_ns, int64_t snd_lmt, int64_t snd_pos, bool is_end_of_stream)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    int64_t min_limit_position = aeron_min_flow_control_strategy_last_setup_snd_lmt(strategy_state, now_ns);
    size_t receiver_count = strategy_state->receivers.length;

    for (int last_index = (int)receiver_count - 1, i = last_index; i >= 0; i--)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if ((receiver->time_of_last_status_message_ns + strategy_state->receiver_timeout_ns) - now_ns < 0 ||
            receiver->eos_flagged)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)strategy_state->receivers.array,
                sizeof(aeron_min_flow_control_strategy_receiver_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            receiver_count--;

            aeron_driver_flow_control_strategy_on_receiver_change_func_t receiver_removed =
                strategy_state->log.receiver_removed;
            if (NULL != receiver_removed)
            {
                receiver_removed(
                    receiver->receiver_id,
                    receiver->session_id,
                    receiver->stream_id,
                    strategy_state->channel->uri_length,
                    strategy_state->channel->original_uri,
                    receiver_count);
            }
        }
        else
        {
            min_limit_position = receiver->last_position_plus_window < min_limit_position ?
                receiver->last_position_plus_window : min_limit_position;
        }
    }

    if (receiver_count != strategy_state->receivers.length)
    {
        strategy_state->receivers.length = receiver_count;
        bool has_required_receivers = receiver_count >= (size_t)strategy_state->group_min_size;
        AERON_SET_RELEASE(strategy_state->has_required_receivers, has_required_receivers);
        aeron_counter_set_release(
            strategy_state->receivers_counter.value_addr, (int64_t) strategy_state->receivers.length);
    }

    return strategy_state->receivers.length < (size_t)strategy_state->group_min_size ||
        strategy_state->receivers.length == 0 ? snd_lmt : min_limit_position;
}

int64_t aeron_min_flow_control_strategy_process_sm(
    aeron_min_flow_control_strategy_state_t *strategy_state,
    aeron_status_message_header_t *status_message_header,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns,
    bool matches_tag)
{
    const int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);
    const int64_t window_length = status_message_header->receiver_window;
    const int64_t receiver_id = status_message_header->receiver_id;
    const bool eos_flagged = status_message_header->frame_header.flags & AERON_STATUS_MESSAGE_HEADER_EOS_FLAG;
    int64_t position_plus_window = position + window_length;

    bool is_existing = false;
    int64_t min_position = aeron_min_flow_control_strategy_last_setup_snd_lmt(strategy_state, now_ns);

    for (size_t i = 0; i < strategy_state->receivers.length; i++)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if (matches_tag && receiver_id == receiver->receiver_id)
        {
            receiver->eos_flagged = eos_flagged;
            receiver->last_position = position > receiver->last_position ? position : receiver->last_position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message_ns = now_ns;
            is_existing = true;
        }

        min_position = receiver->last_position_plus_window < min_position ?
            receiver->last_position_plus_window : min_position;
    }

    if (!is_existing &&
        !eos_flagged &&
        matches_tag &&
        (0 == strategy_state->receivers.length || position_plus_window >= min_position - window_length))
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(
            ensure_capacity_result, strategy_state->receivers, aeron_min_flow_control_strategy_receiver_t)

        if (ensure_capacity_result >= 0)
        {
            const size_t receivers_length = strategy_state->receivers.length;
            aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[receivers_length];
            strategy_state->receivers.length = receivers_length + 1;

            receiver->last_position = position;
            receiver->last_position_plus_window = position + window_length;
            receiver->time_of_last_status_message_ns = now_ns;
            receiver->receiver_id = receiver_id;
            receiver->session_id = status_message_header->session_id;
            receiver->stream_id = status_message_header->stream_id;
            receiver->eos_flagged = false;

            min_position = position_plus_window < min_position ? position_plus_window : min_position;

            bool has_required_receivers = strategy_state->receivers.length >= (size_t)strategy_state->group_min_size;
            AERON_SET_RELEASE(strategy_state->has_required_receivers, has_required_receivers);

            strategy_state->last_setup_snd_lmt = -1;

            aeron_driver_flow_control_strategy_on_receiver_change_func_t receiver_added =
                strategy_state->log.receiver_added;
            if (NULL != receiver_added)
            {
                receiver_added(
                    receiver->receiver_id,
                    receiver->session_id,
                    receiver->stream_id,
                    strategy_state->channel->uri_length,
                    strategy_state->channel->original_uri,
                    strategy_state->receivers.length);
            }

            aeron_counter_set_release(
                strategy_state->receivers_counter.value_addr, (int64_t) strategy_state->receivers.length);
        }
    }

    if (strategy_state->receivers.length < (size_t)strategy_state->group_min_size)
    {
        return snd_lmt;
    }
    else if (0 == strategy_state->receivers.length)
    {
        return snd_lmt > position_plus_window ? snd_lmt : position_plus_window;
    }
    else
    {
        return snd_lmt > min_position ? snd_lmt : min_position;
    }
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
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    return aeron_min_flow_control_strategy_process_sm(
        strategy_state,
        status_message_header,
        snd_lmt,
        initial_term_id,
        position_bits_to_shift,
        now_ns,
        true);
}

int64_t aeron_min_flow_control_strategy_on_setup(
    void *state,
    const uint8_t *setup,
    size_t length,
    int64_t now_ns,
    int64_t snd_lmt,
    size_t position_bits_to_shift,
    int64_t snd_pos)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;

    if (strategy_state->has_tagged_status_message_triggered_setup && strategy_state->receivers.length > 0)
    {
        strategy_state->time_of_last_setup_ns = now_ns;
        strategy_state->last_setup_snd_lmt = snd_lmt;
    }

    strategy_state->has_tagged_status_message_triggered_setup = false;

    return snd_lmt;
}

void aeron_min_flow_control_strategy_on_error(
    void *state,
    const uint8_t *error,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    aeron_error_t *error_header = (aeron_error_t *)error;

    for (size_t i = 0; i < strategy_state->receivers.length; i++)
    {
        aeron_min_flow_control_strategy_receiver_t *receiver = &strategy_state->receivers.array[i];

        if (error_header->receiver_id == receiver->receiver_id)
        {
            receiver->eos_flagged = true;
        }
    }
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
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    int64_t receiver_group_tag;
    const int bytes_read = aeron_udp_protocol_group_tag(status_message_header, &receiver_group_tag);
    const bool was_present = bytes_read == sizeof(receiver_group_tag);

    if (0 != bytes_read && !was_present)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s",
            "Received a status message for tagged flow control that did not have 0 or 8 bytes for the group_tag");
        aeron_distinct_error_log_record(strategy_state->error_log, aeron_errcode(), aeron_errmsg());
        aeron_err_clear();
    }

    const bool matches_tag = was_present && receiver_group_tag == strategy_state->group_tag;

    return aeron_min_flow_control_strategy_process_sm(
        strategy_state, status_message_header, snd_lmt, initial_term_id, position_bits_to_shift, now_ns, matches_tag);
}

int64_t aeron_tagged_flow_control_strategy_on_setup(
    void *state,
    const uint8_t *setup,
    size_t length,
    int64_t now_ns,
    int64_t snd_lmt,
    size_t position_bits_to_shift,
    int64_t snd_pos)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;

    if (strategy_state->has_tagged_status_message_triggered_setup && strategy_state->receivers.length > 0)
    {
        strategy_state->time_of_last_setup_ns = now_ns;
        strategy_state->last_setup_snd_lmt = snd_lmt;
    }

    strategy_state->has_tagged_status_message_triggered_setup = false;

    return snd_lmt;
}

void aeron_min_flow_control_strategy_process_on_trigger_send_setup(
    aeron_min_flow_control_strategy_state_t *strategy_state,
    aeron_status_message_header_t *status_message_header,
    size_t length,
    int64_t now_ns,
    bool has_matching_tag)
{
    if (!strategy_state->has_tagged_status_message_triggered_setup)
    {
        strategy_state->has_tagged_status_message_triggered_setup = has_matching_tag;
    }
}

void aeron_tagged_flow_control_strategy_on_trigger_send_setup(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    int64_t receiver_group_tag;
    const int bytes_read = aeron_udp_protocol_group_tag(status_message_header, &receiver_group_tag);
    const bool is_tag_present = bytes_read == sizeof(receiver_group_tag);

    if (0 != bytes_read && !is_tag_present)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s",
            "Received a status message for tagged flow control that did not have 0 or 8 bytes for the group_tag");
        aeron_distinct_error_log_record(strategy_state->error_log, aeron_errcode(), aeron_errmsg());
        aeron_err_clear();
    }

    const bool has_matching_tag = is_tag_present && receiver_group_tag == strategy_state->group_tag;

    aeron_min_flow_control_strategy_process_on_trigger_send_setup(
        strategy_state, status_message_header, length, now_ns, has_matching_tag);
}

size_t aeron_min_flow_control_strategy_max_retransmission_length(
    void *state,
    size_t term_offset,
    size_t resend_length,
    size_t term_buffer_length,
    size_t mtu_length)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;

    return aeron_flow_control_calculate_retransmission_length(
        resend_length,
        term_buffer_length,
        term_offset,
        strategy_state->retransmit_receiver_window_multiple);
}

void aeron_min_flow_control_strategy_on_trigger_send_setup(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns)
{
    aeron_min_flow_control_strategy_state_t *strategy_state = (aeron_min_flow_control_strategy_state_t *)state;
    aeron_status_message_header_t *status_message_header = (aeron_status_message_header_t *)sm;

    aeron_min_flow_control_strategy_process_on_trigger_send_setup(
        strategy_state, status_message_header, length, now_ns, true);
}

int aeron_min_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_min_flow_control_strategy_state_t *strategy_state =
        (aeron_min_flow_control_strategy_state_t *)strategy->state;

    if (NULL != strategy_state->counters_manager)
    {
        aeron_counters_manager_free(
            strategy_state->counters_manager, strategy_state->receivers_counter.counter_id);
    }

    aeron_free(strategy_state->receivers.array);
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

bool aeron_min_flow_control_strategy_has_required_receivers(aeron_flow_control_strategy_t *strategy)
{
    aeron_min_flow_control_strategy_state_t *strategy_state =
        (aeron_min_flow_control_strategy_state_t *)strategy->state;

    bool has_required_receivers;
    AERON_GET_ACQUIRE(has_required_receivers, strategy_state->has_required_receivers);

    return has_required_receivers;
}

int aeron_tagged_flow_control_strategy_allocate_receiver_counter(
    aeron_min_flow_control_strategy_state_t *strategy_state,
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    const aeron_udp_channel_t *channel)
{
    const int32_t counter_id = aeron_stream_counter_allocate(
        counters_manager,
        AERON_MIN_FLOW_CONTROL_RECEIVERS_COUNTER_NAME,
        AERON_COUNTER_FC_NUM_RECEIVERS_TYPE_ID,
        registration_id,
        session_id,
        stream_id,
        channel->uri_length,
        channel->original_uri,
        "");

    if (counter_id < 0)
    {
        return -1;
    }

    strategy_state->receivers_counter.counter_id = counter_id;
    strategy_state->receivers_counter.value_addr = aeron_counters_manager_addr(counters_manager, counter_id);
    aeron_counter_set_release(strategy_state->receivers_counter.value_addr, 0);

    return 0;
}

int aeron_tagged_flow_control_strategy_supplier_init(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity,
    bool is_group_tag_aware)
{
    aeron_flow_control_strategy_t *_strategy;
    aeron_flow_control_tagged_options_t options;

    options.multicast_flow_control_rrwm = context->multicast_flow_control_rrwm;
    const char *fc_options = aeron_uri_find_param_value(&channel->uri.params.udp.additional_params, AERON_URI_FC_KEY);
    if (aeron_flow_control_parse_tagged_options(NULL != fc_options ? strlen(fc_options) : 0, fc_options, &options) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc((void**)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc(&_strategy->state, sizeof(aeron_min_flow_control_strategy_state_t)) < 0)
    {
        aeron_free(_strategy);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _strategy->on_idle = aeron_min_flow_control_strategy_on_idle;
    _strategy->on_status_message = is_group_tag_aware ?
        aeron_tagged_flow_control_strategy_on_sm : aeron_min_flow_control_strategy_on_sm;
    _strategy->on_setup = is_group_tag_aware ?
        aeron_tagged_flow_control_strategy_on_setup : aeron_min_flow_control_strategy_on_setup;
    _strategy->on_error = aeron_min_flow_control_strategy_on_error;
    _strategy->fini = aeron_min_flow_control_strategy_fini;
    _strategy->has_required_receivers = aeron_min_flow_control_strategy_has_required_receivers;
    _strategy->on_trigger_send_setup = is_group_tag_aware ?
        aeron_tagged_flow_control_strategy_on_trigger_send_setup :
        aeron_min_flow_control_strategy_on_trigger_send_setup;
    _strategy->max_retransmission_length = aeron_min_flow_control_strategy_max_retransmission_length;

    aeron_min_flow_control_strategy_state_t *state = (aeron_min_flow_control_strategy_state_t *)_strategy->state;

    state->receivers.array = NULL;
    state->receivers.capacity = 0;
    state->receivers.length = 0;

    state->channel = channel;

    state->receiver_timeout_ns = (int64_t)(options.timeout_ns.is_present ?
        options.timeout_ns.value : context->flow_control.receiver_timeout_ns);
    state->group_min_size = options.group_min_size.is_present ?
        options.group_min_size.value : context->flow_control.group_min_size;
    state->group_tag = options.group_tag.is_present ? options.group_tag.value : context->flow_control.group_tag;
    state->has_tagged_status_message_triggered_setup = false;

    state->error_log = context->error_log;
    state->time_of_last_setup_ns = 0;
    state->last_setup_snd_lmt = -1;

    state->log.receiver_added = context->log.flow_control_on_receiver_added;
    state->log.receiver_removed = context->log.flow_control_on_receiver_removed;
    state->counters_manager = counters_manager;
    state->receivers_counter.value_addr = NULL;
    state->receivers_counter.counter_id = -1;

    if (NULL != counters_manager &&
        aeron_tagged_flow_control_strategy_allocate_receiver_counter(
            state, counters_manager, registration_id, session_id, stream_id, channel) < 0)
    {
        return -1;
    }

    bool has_required_receivers = state->receivers.length >= (size_t)state->group_min_size;
    AERON_SET_RELEASE(state->has_required_receivers, has_required_receivers);
    state->retransmit_receiver_window_multiple = options.multicast_flow_control_rrwm;
    *strategy = _strategy;

    return 0;
}

int aeron_min_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    return aeron_tagged_flow_control_strategy_supplier_init(
        strategy, context, counters_manager, channel, stream_id, session_id,
        registration_id, initial_term_id, term_buffer_capacity, false);
}

int aeron_tagged_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity)
{
    return aeron_tagged_flow_control_strategy_supplier_init(
        strategy, context, counters_manager, channel, stream_id, session_id,
        registration_id, initial_term_id, term_buffer_capacity, true);
}

int aeron_tagged_flow_control_strategy_to_string(
    aeron_flow_control_strategy_t *strategy, char *buffer, size_t buffer_len)
{
    aeron_min_flow_control_strategy_state_t *strategy_state =
        (aeron_min_flow_control_strategy_state_t *)strategy->state;

    buffer[buffer_len - 1] = '\0';

    return snprintf(
        buffer,
        buffer_len - 1,
        "group_tag: %" PRId64 ", group_min_size: %" PRId32 ", receiver_count: %" PRIu64,
        strategy_state->group_tag,
        strategy_state->group_min_size,
        (uint64_t)strategy_state->receivers.length);
}
