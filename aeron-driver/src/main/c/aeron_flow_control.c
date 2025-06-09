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

#include <stdlib.h>
#include <errno.h>
#include "media/aeron_udp_channel.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_symbol_table.h"
#include "aeron_alloc.h"
#include "aeron_flow_control.h"

typedef struct aeron_max_flow_control_strategy_state_stct
{
    size_t retransmit_receiver_window_multiple;
}
aeron_max_flow_control_strategy_state_t;

typedef struct aeron_unicast_flow_control_strategy_state_stct
{
    size_t retransmit_receiver_window_multiple;
}
aeron_unicast_flow_control_strategy_state_t;

aeron_symbol_table_func_t aeron_flow_control_strategy_table[] =
    {
        {
            AERON_UNICAST_MAX_FLOW_CONTROL_STRATEGY_NAME,
            "aeron_unicast_flow_control_strategy_supplier",
            (aeron_fptr_t)aeron_unicast_flow_control_strategy_supplier
        },
        {
            AERON_MULTICAST_MAX_FLOW_CONTROL_STRATEGY_NAME,
            "aeron_max_multicast_flow_control_strategy_supplier",
            (aeron_fptr_t)aeron_max_multicast_flow_control_strategy_supplier
        },
        {
            AERON_MULTICAST_MIN_FLOW_CONTROL_STRATEGY_NAME,
            "aeron_min_flow_control_strategy_supplier",
            (aeron_fptr_t)aeron_min_flow_control_strategy_supplier
        },
        {
            AERON_MULTICAST_TAGGED_FLOW_CONTROL_STRATEGY_NAME,
            "aeron_tagged_flow_control_strategy_supplier",
            (aeron_fptr_t)aeron_tagged_flow_control_strategy_supplier
        }
    };

static const size_t aeron_flow_control_strategy_table_length =
    sizeof(aeron_flow_control_strategy_table) / sizeof(aeron_symbol_table_func_t);

aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_load(const char *strategy_name)
{
    return (aeron_flow_control_strategy_supplier_func_t)aeron_symbol_table_func_load(
        aeron_flow_control_strategy_table, aeron_flow_control_strategy_table_length, strategy_name, "flow control");
}

bool aeron_flow_control_strategy_has_required_receivers_default(aeron_flow_control_strategy_t *strategy)
{
    return true;
}

int64_t aeron_max_flow_control_strategy_on_idle(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream)
{
    return snd_lmt;
}

int64_t aeron_max_flow_control_strategy_on_setup(
    void *state,
    const uint8_t *setup,
    size_t length,
    int64_t now_ns,
    int64_t snd_lmt,
    size_t position_bits_to_shift,
    int64_t snd_pos)
{
    return snd_lmt;
}

int64_t aeron_max_flow_control_strategy_on_sm(
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

    int64_t position = aeron_logbuffer_compute_position(
        status_message_header->consumption_term_id,
        status_message_header->consumption_term_offset,
        position_bits_to_shift,
        initial_term_id);
    int64_t window_edge = position + status_message_header->receiver_window;

    return snd_lmt > window_edge ? snd_lmt : window_edge;
}

void aeron_max_flow_control_strategy_on_error(
    void *state,
    const uint8_t *error,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns)
{
}

size_t aeron_flow_control_calculate_retransmission_length(
    size_t resend_length,
    size_t term_buffer_length,
    size_t term_offset,
    size_t retransmit_receiver_window_multiple)
{
    size_t length_to_end_of_term = term_buffer_length - term_offset;
    size_t initial_window_length = aeron_driver_context_get_rcv_initial_window_length(NULL);
    size_t receiver_window_length = aeron_receiver_window_length(initial_window_length, term_buffer_length);
    size_t estimated_retransmit_length = receiver_window_length * retransmit_receiver_window_multiple;

    return (length_to_end_of_term < estimated_retransmit_length) ?
        AERON_MIN(length_to_end_of_term, resend_length) :
        AERON_MIN(estimated_retransmit_length, resend_length);
}

size_t aeron_max_flow_control_strategy_max_retransmission_length(
    void *state,
    size_t term_offset,
    size_t resend_length,
    size_t term_buffer_length,
    size_t mtu_length)
{
    aeron_max_flow_control_strategy_state_t *strategy_state = (aeron_max_flow_control_strategy_state_t *)state;

    return aeron_flow_control_calculate_retransmission_length(
        resend_length,
        term_buffer_length,
        term_offset,
        strategy_state->retransmit_receiver_window_multiple);
}

size_t aeron_unicast_flow_control_strategy_max_retransmission_length(
    void *state,
    size_t term_offset,
    size_t resend_length,
    size_t term_buffer_length,
    size_t mtu_length)
{
    aeron_unicast_flow_control_strategy_state_t *strategy_state = (aeron_unicast_flow_control_strategy_state_t *)state;

    return aeron_flow_control_calculate_retransmission_length(
        resend_length,
        term_buffer_length,
        term_offset,
        strategy_state->retransmit_receiver_window_multiple);
}

void aeron_max_flow_control_strategy_on_trigger_send_setup(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns)
{
}

int aeron_max_flow_control_strategy_fini(aeron_flow_control_strategy_t *strategy)
{
    aeron_free(strategy->state);
    aeron_free(strategy);
    return 0;
}

int aeron_flow_control_parse_max_options(
    size_t options_length, const char *options, aeron_flow_control_max_options_t *flow_control_options)
{
    if (0 == options_length || NULL == options)
    {
        return 0;
    }

    const char *current_option = options;
    size_t remaining = options_length;

    const char *next_option;
    do
    {
        next_option = (const char *)memchr(current_option, ',', remaining);

        ptrdiff_t current_option_length;

        if (NULL == next_option)
        {
            current_option_length = remaining;
        }
        else
        {
            current_option_length = next_option - current_option;

            // Skip the comma.
            next_option++;
            remaining -= (current_option_length + 1);
        }

        if (strncmp(current_option, "max", 3) == 0)
        {
            // skip over the strategy declaration
        }
        else if (strncmp(current_option, "rrwm:", 5) == 0)
        {
            char *endptr;
            errno = 0;
            int64_t signedRrwm = strtol(current_option + 5, &endptr, 10);
            if (0 == errno && signedRrwm > 0)
            {
                flow_control_options->multicast_flow_control_rrwm = (size_t)signedRrwm;
            }
            else
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Flow control options - invalid flow control retransmit receiver window multiple, field: %.*s, options: %.*s",
                    (int)current_option_length,
                    current_option,
                    (int)options_length,
                    options);
                return -1;
            }
        }
        else
        {
            AERON_SET_ERR(
                EINVAL,
                "Flow control options - unrecognised option, field: %.*s, options: %.*s",
                (int)current_option_length,
                current_option,
                (int)options_length,
                options);

            return -1;
        }

        current_option = next_option;
    }
    while (NULL != current_option && 0 < remaining);

    return 1;
}

int aeron_max_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length)
{
    aeron_flow_control_strategy_t *_strategy;

    aeron_flow_control_max_options_t options;

    options.multicast_flow_control_rrwm = context->multicast_flow_control_rrwm;
    const char *fc_options = aeron_uri_find_param_value(&channel->uri.params.udp.additional_params, AERON_URI_FC_KEY);
    if (aeron_flow_control_parse_max_options(NULL != fc_options ? strlen(fc_options) : 0, fc_options, &options) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc((void**)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc(&_strategy->state, sizeof(aeron_max_flow_control_strategy_state_t)) < 0)
    {
        aeron_free(_strategy);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _strategy->on_idle = aeron_max_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_max_flow_control_strategy_on_sm;
    _strategy->on_setup = aeron_max_flow_control_strategy_on_setup;
    _strategy->on_error = aeron_max_flow_control_strategy_on_error;
    _strategy->fini = aeron_max_flow_control_strategy_fini;
    _strategy->has_required_receivers = aeron_flow_control_strategy_has_required_receivers_default;
    _strategy->on_trigger_send_setup = aeron_max_flow_control_strategy_on_trigger_send_setup;
    _strategy->max_retransmission_length = aeron_max_flow_control_strategy_max_retransmission_length;
    aeron_max_flow_control_strategy_state_t *state = _strategy->state;
    state->retransmit_receiver_window_multiple = options.multicast_flow_control_rrwm;

    *strategy = _strategy;

    return 0;
}

int aeron_unicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length)
{
    aeron_flow_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_flow_control_strategy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_alloc(&_strategy->state, sizeof(aeron_unicast_flow_control_strategy_state_t)) < 0)
    {
        aeron_free(_strategy);
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _strategy->on_idle = aeron_max_flow_control_strategy_on_idle;
    _strategy->on_status_message = aeron_max_flow_control_strategy_on_sm;
    _strategy->on_setup = aeron_max_flow_control_strategy_on_setup;
    _strategy->on_error = aeron_max_flow_control_strategy_on_error;
    _strategy->fini = aeron_max_flow_control_strategy_fini;
    _strategy->has_required_receivers = aeron_flow_control_strategy_has_required_receivers_default;
    _strategy->on_trigger_send_setup = aeron_max_flow_control_strategy_on_trigger_send_setup;
    _strategy->max_retransmission_length = aeron_unicast_flow_control_strategy_max_retransmission_length;
    aeron_unicast_flow_control_strategy_state_t *state = _strategy->state;
    state->retransmit_receiver_window_multiple = context->unicast_flow_control_rrwm;
    *strategy = _strategy;

    return 0;
}

aeron_flow_control_strategy_supplier_func_table_entry_t aeron_flow_control_strategy_supplier_table[] =
{
    { AERON_UNICAST_MAX_FLOW_CONTROL_STRATEGY_NAME, aeron_unicast_flow_control_strategy_supplier },
    { AERON_MULTICAST_MAX_FLOW_CONTROL_STRATEGY_NAME, aeron_max_multicast_flow_control_strategy_supplier },
    { AERON_MULTICAST_MIN_FLOW_CONTROL_STRATEGY_NAME, aeron_min_flow_control_strategy_supplier },
    { AERON_MULTICAST_TAGGED_FLOW_CONTROL_STRATEGY_NAME, aeron_tagged_flow_control_strategy_supplier }
};

aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_by_name(const char *name)
{
    size_t entries = sizeof(aeron_flow_control_strategy_supplier_table) /
        sizeof(aeron_flow_control_strategy_supplier_func_table_entry_t);

    for (size_t i = 0; i < entries; i++)
    {
        aeron_flow_control_strategy_supplier_func_table_entry_t *entry = &aeron_flow_control_strategy_supplier_table[i];

        if (strncmp(entry->name, name, strlen(entry->name)) == 0)
        {
            return entry->supplier_func;
        }
    }

    return NULL;
}

void aeron_flow_control_extract_strategy_name_length(
    const size_t options_length, const char *options, size_t *strategy_length)
{
    const char *next_option = (const char *)memchr(options, ',', options_length);
    *strategy_length = NULL == next_option ? options_length : (size_t)labs((long)(next_option - options));
}

int aeron_default_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length)
{
    aeron_flow_control_strategy_supplier_func_t flow_control_strategy_supplier_func;

    if (aeron_udp_channel_is_multi_destination(channel) ||
        channel->is_multicast)
    {
        const char *flow_control_options = aeron_uri_find_param_value(
            &channel->uri.params.udp.additional_params, AERON_URI_FC_KEY);
        if (NULL != flow_control_options)
        {
            const char *strategy_name = flow_control_options;
            size_t strategy_name_length = 0;
            aeron_flow_control_extract_strategy_name_length(
                strlen(flow_control_options), flow_control_options, &strategy_name_length);

            if (0 == strategy_name_length)
            {
                AERON_SET_ERR(
                    EINVAL,
                    "No flow control strategy name specified, URI: %.*s",
                    (int)channel->uri_length,
                    channel->original_uri);
                return -1;
            }

            if (strlen(AERON_MAX_FLOW_CONTROL_STRATEGY_NAME) == strategy_name_length &&
                0 == strncmp(AERON_MAX_FLOW_CONTROL_STRATEGY_NAME, strategy_name, strategy_name_length))
            {
                flow_control_strategy_supplier_func = aeron_max_multicast_flow_control_strategy_supplier;
            }
            else if (strlen(AERON_MIN_FLOW_CONTROL_STRATEGY_NAME) == strategy_name_length &&
                0 == strncmp(AERON_MIN_FLOW_CONTROL_STRATEGY_NAME, strategy_name, strategy_name_length))
            {
                flow_control_strategy_supplier_func = aeron_min_flow_control_strategy_supplier;
            }
            else if (strlen(AERON_TAGGED_FLOW_CONTROL_STRATEGY_NAME) == strategy_name_length &&
                0 == strncmp(AERON_TAGGED_FLOW_CONTROL_STRATEGY_NAME, strategy_name, strategy_name_length))
            {
                flow_control_strategy_supplier_func = aeron_tagged_flow_control_strategy_supplier;
            }
            else
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Invalid flow control strategy name: %.*s from URI: %.*s",
                    (int)strategy_name_length,
                    strategy_name,
                    (int)channel->uri_length,
                    channel->original_uri);

                return -1;
            }
        }
        else
        {
            flow_control_strategy_supplier_func = context->multicast_flow_control_supplier_func;
        }
    }
    else
    {
        flow_control_strategy_supplier_func = context->unicast_flow_control_supplier_func;
    }

    int rc = flow_control_strategy_supplier_func(
        strategy, context, counters_manager, channel, stream_id, session_id,
        registration_id, initial_term_id, term_length);

    if (0 <= rc && NULL != *strategy && NULL == (*strategy)->has_required_receivers)
    {
        (*strategy)->has_required_receivers = aeron_flow_control_strategy_has_required_receivers_default;
    }

    return rc;
}

#define AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN (64)

int aeron_flow_control_parse_tagged_options(
    size_t options_length, const char *options, aeron_flow_control_tagged_options_t *flow_control_options)
{
    flow_control_options->strategy_name = NULL;
    flow_control_options->strategy_name_length = 0;
    flow_control_options->timeout_ns.is_present = false;
    flow_control_options->timeout_ns.value = 0;
    flow_control_options->group_tag.is_present = false;
    flow_control_options->group_tag.value = -1;
    flow_control_options->group_min_size.is_present = false;
    flow_control_options->group_min_size.value = 0;

    char number_buffer[AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN];

    if (0 == options_length || NULL == options)
    {
        return 0;
    }

    const char *current_option = options;
    size_t remaining = options_length;

    const char *next_option;
    do
    {
        next_option = (const char *)memchr(current_option, ',', remaining);

        ptrdiff_t current_option_length;

        if (NULL == next_option)
        {
            current_option_length = remaining;
        }
        else
        {
            current_option_length = next_option - current_option;

            // Skip the comma.
            next_option++;
            remaining -= (current_option_length + 1);
        }

        if (NULL == flow_control_options->strategy_name)
        {
            flow_control_options->strategy_name = current_option;
            flow_control_options->strategy_name_length = current_option_length;
        }
        else if (current_option_length > 2 &&
            ('g' == current_option[0] || 't' == current_option[0]) &&
            ':' == current_option[1])
        {
            const size_t value_length = current_option_length - 2;
            const char *value = current_option + 2;

            if (AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN <= value_length)
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Flow control options - number field too long (found %d, max %d), field: %.*s, options: %.*s",
                    (int)value_length,
                    (AERON_FLOW_CONTROL_NUMBER_BUFFER_LEN - 1),
                    (int)value_length,
                    value,
                    (int)options_length,
                    options);

                return -1;
            }
            strncpy(number_buffer, value, value_length);
            number_buffer[value_length] = '\0';

            if ('g' == current_option[0])
            {
                char *end_ptr = "";
                errno = 0;

                const long long group_tag = strtoll(number_buffer, &end_ptr, 10);
                const bool has_group_min_size = '/' == *end_ptr;

                if (0 == errno && number_buffer != end_ptr && ('\0' == *end_ptr || has_group_min_size))
                {
                    flow_control_options->group_tag.is_present = true;
                    flow_control_options->group_tag.value = (int64_t)group_tag;
                }
                else if (number_buffer != end_ptr && !has_group_min_size) // Allow empty values if we have a group count
                {
                    AERON_SET_ERR(
                        EINVAL,
                        "Flow control options - invalid group, field: %.*s, options: %.*s",
                        (int)current_option_length,
                        current_option,
                        (int)options_length,
                        options);

                    return -1;
                }

                if (has_group_min_size)
                {
                    const char *group_min_size_ptr = end_ptr + 1;
                    end_ptr = "";
                    errno = 0;

                    const long group_min_size = strtol(group_min_size_ptr, &end_ptr, 10);

                    if (0 == errno &&
                        '\0' == *end_ptr &&
                        group_min_size_ptr != end_ptr &&
                        0 <= group_min_size && group_min_size <= INT32_MAX)
                    {
                        flow_control_options->group_min_size.is_present = true;
                        flow_control_options->group_min_size.value = (int32_t)group_min_size;
                    }
                    else
                    {
                        AERON_SET_ERR(
                            EINVAL,
                            "Group count invalid, field: %.*s, options: %.*s",
                            (int)current_option_length,
                            current_option,
                            (int)options_length,
                            options);

                        return -1;
                    }
                }
            }
            else if ('t' == current_option[0])
            {
                uint64_t timeout_ns;
                if (0 <= aeron_parse_duration_ns(number_buffer, &timeout_ns))
                {
                    flow_control_options->timeout_ns.is_present = true;
                    flow_control_options->timeout_ns.value = timeout_ns;
                }
                else
                {
                    AERON_SET_ERR(
                        EINVAL,
                        "Flow control options - invalid timeout, field: %.*s, options: %.*s",
                        (int)current_option_length,
                        current_option,
                        (int)options_length,
                        options);

                    return -1;
                }
            }
        }
        else if (strncmp(current_option, "rrwm:", 5) == 0)
        {
            char *endptr;
            errno = 0;
            int64_t signedRrwm = strtol(current_option + 5, &endptr, 10);
            if (0 == errno && signedRrwm > 0)
            {
                flow_control_options->multicast_flow_control_rrwm = (size_t)signedRrwm;
            }
            else
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Flow control options - invalid flow control retransmit receiver window multiple, field: %.*s, options: %.*s",
                    (int)current_option_length,
                    current_option,
                    (int)options_length,
                    options);
                return -1;
            }
        }
        else
        {
            AERON_SET_ERR(
                EINVAL,
                "Flow control options - unrecognised option, field: %.*s, options: %.*s",
                (int)current_option_length,
                current_option,
                (int)options_length,
                options);

            return -1;
        }

        current_option = next_option;
    }
    while (NULL != current_option && 0 < remaining);

    return 1;
}
