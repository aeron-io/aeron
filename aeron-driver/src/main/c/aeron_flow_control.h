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

#ifndef AERON_FLOW_CONTROL_H
#define AERON_FLOW_CONTROL_H

#include "aeron_socket.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

#define AERON_MAX_FLOW_CONTROL_STRATEGY_NAME "max"
#define AERON_MIN_FLOW_CONTROL_STRATEGY_NAME "min"
#define AERON_TAGGED_FLOW_CONTROL_STRATEGY_NAME "tagged"

#define AERON_MIN_FLOW_CONTROL_RECEIVERS_COUNTER_NAME ("fc-receivers")

#define AERON_MULTICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE UINT32_C(4)
#define AERON_UNICAST_FLOW_CONTROL_RETRANSMIT_RECEIVER_WINDOW_MULTIPLE UINT32_C(16)

typedef int64_t (*aeron_flow_control_strategy_on_idle_func_t)(
    void *state,
    int64_t now_ns,
    int64_t snd_lmt,
    int64_t snd_pos,
    bool is_end_of_stream);

typedef int64_t (*aeron_flow_control_strategy_on_sm_func_t)(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t snd_lmt,
    int32_t initial_term_id,
    size_t position_bits_to_shift,
    int64_t now_ns);

typedef int64_t (*aeron_flow_control_strategy_on_setup_func_t)(
    void *state,
    const uint8_t *setup,
    size_t length,
    int64_t now_ns,
    int64_t snd_lmt,
    size_t position_bits_to_shift,
    int64_t snd_pos);

typedef void (*aeron_flow_control_strategy_on_error_func_t)(
    void *state,
    const uint8_t *error,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns);

typedef void (*aeron_flow_control_strategy_on_trigger_send_setup_func_t)(
    void *state,
    const uint8_t *sm,
    size_t length,
    struct sockaddr_storage *recv_addr,
    int64_t now_ns);

typedef size_t (*aeron_flow_control_strategy_max_retransmission_length_func_t)(
    void *state,
    size_t term_offset,
    size_t resend_length,
    size_t term_buffer_length,
    size_t mtu_length);

typedef int (*aeron_flow_control_strategy_fini_func_t)(aeron_flow_control_strategy_t *strategy);

typedef bool (*aeron_flow_control_strategy_has_required_receivers_func_t)(aeron_flow_control_strategy_t *strategy);

typedef struct aeron_flow_control_strategy_stct
{
    aeron_flow_control_strategy_on_sm_func_t on_status_message;
    aeron_flow_control_strategy_on_idle_func_t on_idle;
    aeron_flow_control_strategy_on_setup_func_t on_setup;
    aeron_flow_control_strategy_on_error_func_t on_error;
    aeron_flow_control_strategy_fini_func_t fini;
    aeron_flow_control_strategy_has_required_receivers_func_t has_required_receivers;
    aeron_flow_control_strategy_on_trigger_send_setup_func_t on_trigger_send_setup;
    aeron_flow_control_strategy_max_retransmission_length_func_t max_retransmission_length;
    void *state;
}
aeron_flow_control_strategy_t;

bool aeron_flow_control_strategy_has_required_receivers_default(aeron_flow_control_strategy_t *strategy);

typedef struct aeron_flow_control_tagged_options_stct
{
    size_t strategy_name_length;
    const char *strategy_name;
    struct
    {
        bool is_present;
        int64_t value;
    }
    group_tag;
    struct
    {
        bool is_present;
        uint64_t value;
    }
    timeout_ns;
    struct
    {
        bool is_present;
        int32_t value;
    }
    group_min_size;
    size_t multicast_flow_control_rrwm;
}
aeron_flow_control_tagged_options_t;

typedef struct aeron_flow_control_max_options_stct
{
    size_t multicast_flow_control_rrwm;
}
aeron_flow_control_max_options_t;

aeron_flow_control_strategy_supplier_func_t aeron_flow_control_strategy_supplier_load(const char *strategy_name);

int aeron_max_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length);

int aeron_unicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length);

int aeron_min_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity);

int aeron_tagged_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_buffer_capacity);

int aeron_tagged_flow_control_strategy_to_string(
    aeron_flow_control_strategy_t *strategy, char *buffer, size_t buffer_len);

int aeron_default_multicast_flow_control_strategy_supplier(
    aeron_flow_control_strategy_t **strategy,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    const aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t initial_term_id,
    size_t term_length);

typedef struct aeron_flow_control_strategy_supplier_func_table_entry_stct
{
    const char *name;
    aeron_flow_control_strategy_supplier_func_t supplier_func;
}
aeron_flow_control_strategy_supplier_func_table_entry_t;

int aeron_flow_control_parse_tagged_options(
    size_t options_length, const char *options, aeron_flow_control_tagged_options_t *flow_control_options);

size_t aeron_flow_control_calculate_retransmission_length(
    size_t resend_length,
    size_t term_buffer_length,
    size_t term_offset,
    size_t retransmit_receiver_window_multiple);

#endif //AERON_FLOW_CONTROL_H
