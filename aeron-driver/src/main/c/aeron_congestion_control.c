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
#include <math.h>
#include "util/aeron_parse_util.h"
#include "util/aeron_error.h"
#include "util/aeron_symbol_table.h"
#include "aeron_congestion_control.h"
#include "aeron_alloc.h"
#include "aeron_driver_context.h"
#include "aeron_position.h"
#include "media/aeron_udp_channel.h"

#define AERON_CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT (100 * 1000LL)
#define AERON_CUBICCONGESTIONCONTROL_SECOND_IN_NS (1 * 1000 * 1000 * 1000LL)

#define AERON_CUBICCONGESTIONCONTROL_INITCWND (10)
#define AERON_CUBICCONGESTIONCONTROL_C (0.4)
#define AERON_CUBICCONGESTIONCONTROL_B (0.2)
#define AERON_CUBICCONGESTIONCONTROL_RTT_TIMEOUT_MULTIPLE (4)

#define AERON_CUBICCONGESTIONCONTROL_MEASURERTT_DEFAULT (false)
#define AERON_CUBICCONGESTIONCONTROL_TCPMODE_DEFAULT (false)

static const aeron_symbol_table_func_t aeron_congestion_control_table[] =
    {
        {
            "default",
            "aeron_congestion_control_default_strategy_supplier",
            (aeron_fptr_t)aeron_congestion_control_default_strategy_supplier
        },
        {
            "static",
            "aeron_static_window_congestion_control_strategy_supplier",
            (aeron_fptr_t)aeron_static_window_congestion_control_strategy_supplier
        },
        {
            "cubic",
            "aeron_cubic_congestion_control_strategy_supplier",
            (aeron_fptr_t)aeron_cubic_congestion_control_strategy_supplier
        },
    };

static const size_t aeron_congestion_control_table_length =
    sizeof(aeron_congestion_control_table) / sizeof(aeron_symbol_table_func_t);

aeron_congestion_control_strategy_supplier_func_t aeron_congestion_control_strategy_supplier_load(
    const char *strategy_name)
{
    return (aeron_congestion_control_strategy_supplier_func_t)aeron_symbol_table_func_load(
        aeron_congestion_control_table, aeron_congestion_control_table_length, strategy_name, "congestion control");
}

struct aeron_static_window_congestion_control_strategy_state_stct
{
    int32_t window_length;
};

typedef struct aeron_static_window_congestion_control_strategy_state_stct
    aeron_static_window_congestion_control_strategy_state_t;

bool aeron_static_window_congestion_control_strategy_should_measure_rtt(void *state, int64_t now_ns)
{
    return false;
}

void aeron_static_window_congestion_control_strategy_on_rttm_sent(void *state, int64_t now_ns)
{
}

void aeron_static_window_congestion_control_strategy_on_rttm(
    void *state, int64_t now_ns, int64_t rtt_ns, struct sockaddr_storage *source_address)
{
}

int32_t aeron_static_window_congestion_control_strategy_on_track_rebuild(
    void *state,
    bool *should_force_sm,
    int64_t now_ns,
    int64_t new_consumption_position,
    int64_t last_sm_position,
    int64_t hwm_position,
    int64_t starting_rebuild_position,
    int64_t ending_rebuild_position,
    bool loss_occurred)
{
    *should_force_sm = false;
    return ((aeron_static_window_congestion_control_strategy_state_t *)state)->window_length;
}

int32_t aeron_static_window_congestion_control_strategy_initial_window_length(void *state)
{
    return ((aeron_static_window_congestion_control_strategy_state_t *)state)->window_length;
}

int32_t aeron_static_window_congestion_control_strategy_max_window_length(void *state)
{
    return ((aeron_static_window_congestion_control_strategy_state_t *)state)->window_length;
}

int aeron_congestion_control_strategy_fini(aeron_congestion_control_strategy_t *strategy)
{
    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

int aeron_static_window_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_congestion_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_congestion_control_strategy_t)) < 0 ||
        aeron_alloc(&_strategy->state, sizeof(aeron_static_window_congestion_control_strategy_state_t)) < 0)
    {
        return -1;
    }

    _strategy->should_measure_rtt = aeron_static_window_congestion_control_strategy_should_measure_rtt;
    _strategy->on_rttm_sent = aeron_static_window_congestion_control_strategy_on_rttm_sent;
    _strategy->on_rttm = aeron_static_window_congestion_control_strategy_on_rttm;
    _strategy->on_track_rebuild = aeron_static_window_congestion_control_strategy_on_track_rebuild;
    _strategy->initial_window_length = aeron_static_window_congestion_control_strategy_initial_window_length;
    _strategy->max_window_length = aeron_static_window_congestion_control_strategy_max_window_length;
    _strategy->fini = aeron_congestion_control_strategy_fini;

    aeron_static_window_congestion_control_strategy_state_t *state = _strategy->state;
    const int32_t initial_window_length = (int32_t)aeron_udp_channel_receiver_window(
        channel, context->initial_window_length);

    state->window_length = (int32_t)aeron_receiver_window_length(initial_window_length, term_length);

    *strategy = _strategy;

    return 0;
}

int aeron_congestion_control_default_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    const char *cc_str = aeron_uri_find_param_value(&channel->uri.params.udp.additional_params, AERON_URI_CC_KEY);
    size_t scc_length = sizeof(AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE) + 1;
    size_t ccc_length = sizeof(AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE) + 1;
    int result = -1;

    if (NULL == cc_str || 0 == strncmp(cc_str, AERON_STATICWINDOWCONGESTIONCONTROL_CC_PARAM_VALUE, scc_length))
    {
        result = aeron_static_window_congestion_control_strategy_supplier(
            strategy,
            channel,
            stream_id,
            session_id,
            registration_id,
            term_length,
            sender_mtu_length,
            control_address,
            src_address,
            context,
            counters_manager);
    }
    else if (0 == strncmp(cc_str, AERON_CUBICCONGESTIONCONTROL_CC_PARAM_VALUE, ccc_length))
    {
        result = aeron_cubic_congestion_control_strategy_supplier(
            strategy,
            channel,
            stream_id,
            session_id,
            registration_id,
            term_length,
            sender_mtu_length,
            control_address,
            src_address,
            context,
            counters_manager);
    }

    return result;
}

struct aeron_cubic_congestion_control_strategy_state_stct
{
    bool tcp_mode;
    bool measure_rtt;

    int32_t initial_window_length;
    int32_t max_window_length;
    int32_t mtu;
    int32_t max_cwnd;
    int32_t cwnd;
    int32_t w_max;
    double k;

    uint64_t initial_rtt_ns;
    int64_t rtt_ns;
    int64_t rtt_timeout_ns;
    int64_t window_update_timeout_ns;
    int64_t last_loss_timestamp_ns;
    int64_t last_update_timestamp_ns;
    int64_t last_rtt_timestamp_ns;

    aeron_position_t rtt_indicator;
    aeron_position_t window_indicator;

    aeron_counters_manager_t *counters_manager;
};

typedef struct aeron_cubic_congestion_control_strategy_state_stct aeron_cubic_congestion_control_strategy_state_t;

bool aeron_cubic_congestion_control_strategy_should_measure_rtt(void *state, int64_t now_ns)
{
    aeron_cubic_congestion_control_strategy_state_t *cubic_state =
        (aeron_cubic_congestion_control_strategy_state_t *)state;

    return cubic_state->measure_rtt &&
        ((cubic_state->last_rtt_timestamp_ns + cubic_state->rtt_timeout_ns) - now_ns < 0);
}

void aeron_cubic_congestion_control_strategy_on_rttm_sent(void *state, int64_t now_ns)
{
    aeron_cubic_congestion_control_strategy_state_t *cubic_state =
        (aeron_cubic_congestion_control_strategy_state_t *)state;

    cubic_state->last_rtt_timestamp_ns = now_ns;
}

void aeron_cubic_congestion_control_strategy_on_rttm(
    void *state, int64_t now_ns, int64_t rtt_ns, struct sockaddr_storage *source_address)
{
    aeron_cubic_congestion_control_strategy_state_t *cubic_state =
        (aeron_cubic_congestion_control_strategy_state_t *)state;

    cubic_state->last_rtt_timestamp_ns = now_ns;
    cubic_state->rtt_ns = rtt_ns;
    aeron_counter_set_release(cubic_state->rtt_indicator.value_addr, rtt_ns);
    cubic_state->rtt_timeout_ns =
        (rtt_ns > (int64_t)cubic_state->initial_rtt_ns ? rtt_ns : (int64_t)cubic_state->initial_rtt_ns) *
        AERON_CUBICCONGESTIONCONTROL_RTT_TIMEOUT_MULTIPLE;
}

int32_t aeron_cubic_congestion_control_strategy_on_track_rebuild(
    void *state,
    bool *should_force_sm,
    int64_t now_ns,
    int64_t new_consumption_position,
    int64_t last_sm_position,
    int64_t hwm_position,
    int64_t starting_rebuild_position,
    int64_t ending_rebuild_position,
    bool loss_occurred)
{
    aeron_cubic_congestion_control_strategy_state_t *cubic_state =
        (aeron_cubic_congestion_control_strategy_state_t *)state;
    *should_force_sm = false;

    if (loss_occurred)
    {
        *should_force_sm = true;
        cubic_state->w_max = cubic_state->cwnd;
        cubic_state->k = cbrt(
            (double)cubic_state->w_max * AERON_CUBICCONGESTIONCONTROL_B / AERON_CUBICCONGESTIONCONTROL_C);

        const int32_t cwnd = (int32_t)(cubic_state->cwnd * (1.0 - AERON_CUBICCONGESTIONCONTROL_B));
        cubic_state->cwnd = cwnd > 1 ? cwnd : 1;
        cubic_state->last_loss_timestamp_ns = now_ns;
    }
    else if (cubic_state->cwnd < cubic_state->max_cwnd &&
        ((cubic_state->last_update_timestamp_ns + cubic_state->window_update_timeout_ns) - now_ns < 0))
    {
        // W_cubic = C(T - K)^3 + w_max
        const double duration_since_decr =
            (double)(now_ns - cubic_state->last_loss_timestamp_ns) / (double)AERON_CUBICCONGESTIONCONTROL_SECOND_IN_NS;
        const double diff_to_k = duration_since_decr - cubic_state->k;
        const double incr = AERON_CUBICCONGESTIONCONTROL_C * diff_to_k * diff_to_k * diff_to_k;

        const int32_t cwnd = cubic_state->w_max + (int32_t)incr;
        cubic_state->cwnd = cwnd < cubic_state->max_cwnd ? cwnd : cubic_state->max_cwnd;

        // if using TCP mode, then check to see if we are in the TCP region
        if (cubic_state->tcp_mode && cubic_state->cwnd < cubic_state->w_max)
        {
            // W_tcp(t) = w_max * (1 - B) + 3 * B / (2 - B) * t / RTT
            const double rtt_in_seconds =
                (double)cubic_state->rtt_ns / (double)AERON_CUBICCONGESTIONCONTROL_SECOND_IN_NS;
            const double w_tcp = (double)cubic_state->w_max * (1.0 - AERON_CUBICCONGESTIONCONTROL_B) +
                ((3.0 * AERON_CUBICCONGESTIONCONTROL_B / (2.0 - AERON_CUBICCONGESTIONCONTROL_B)) *
                    (duration_since_decr / rtt_in_seconds));

            const int32_t new_cwnd = (int32_t)w_tcp;
            cubic_state->cwnd = new_cwnd > cubic_state->cwnd ? new_cwnd : cubic_state->cwnd;
        }

        cubic_state->last_update_timestamp_ns = now_ns;
    }
    else if (1 == cubic_state->cwnd && new_consumption_position > last_sm_position)
    {
        *should_force_sm = true;
    }

    const int32_t window = cubic_state->cwnd * cubic_state->mtu;
    aeron_counter_set_release(cubic_state->window_indicator.value_addr, window);

    return window;
}

int32_t aeron_cubic_congestion_control_strategy_initial_window_length(void *state)
{
    return ((aeron_cubic_congestion_control_strategy_state_t *)state)->initial_window_length;
}

int32_t aeron_cubic_congestion_control_strategy_max_window_length(void *state)
{
    return ((aeron_cubic_congestion_control_strategy_state_t *)state)->max_window_length;
}

int aeron_cubic_congestion_control_strategy_fini(aeron_congestion_control_strategy_t *strategy)
{
    aeron_cubic_congestion_control_strategy_state_t *state = strategy->state;
    aeron_counters_manager_free(state->counters_manager, state->rtt_indicator.counter_id);
    aeron_counters_manager_free(state->counters_manager, state->window_indicator.counter_id);

    aeron_free(strategy->state);
    aeron_free(strategy);

    return 0;
}

int aeron_cubic_congestion_control_strategy_supplier(
    aeron_congestion_control_strategy_t **strategy,
    aeron_udp_channel_t *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t registration_id,
    int32_t term_length,
    int32_t sender_mtu_length,
    struct sockaddr_storage *control_address,
    struct sockaddr_storage *src_address,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager)
{
    aeron_congestion_control_strategy_t *_strategy;

    if (aeron_alloc((void **)&_strategy, sizeof(aeron_congestion_control_strategy_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc(&_strategy->state, sizeof(aeron_cubic_congestion_control_strategy_state_t)) < 0)
    {
        aeron_free(strategy);
        return -1;
    }

    _strategy->should_measure_rtt = aeron_cubic_congestion_control_strategy_should_measure_rtt;
    _strategy->on_rttm_sent = aeron_cubic_congestion_control_strategy_on_rttm_sent;
    _strategy->on_rttm = aeron_cubic_congestion_control_strategy_on_rttm;
    _strategy->on_track_rebuild = aeron_cubic_congestion_control_strategy_on_track_rebuild;
    _strategy->initial_window_length = aeron_cubic_congestion_control_strategy_initial_window_length;
    _strategy->max_window_length = aeron_cubic_congestion_control_strategy_max_window_length;
    _strategy->fini = aeron_cubic_congestion_control_strategy_fini;

    aeron_cubic_congestion_control_strategy_state_t *state = _strategy->state;

    // Config values
    state->tcp_mode = aeron_parse_bool(
        getenv(AERON_CUBICCONGESTIONCONTROL_TCPMODE_ENV_VAR),
        AERON_CUBICCONGESTIONCONTROL_TCPMODE_DEFAULT);
    state->measure_rtt = aeron_parse_bool(
        getenv(AERON_CUBICCONGESTIONCONTROL_MEASURERTT_ENV_VAR),
        AERON_CUBICCONGESTIONCONTROL_MEASURERTT_DEFAULT);
    state->initial_rtt_ns = AERON_CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT;
    char *const rtt_ns = getenv(AERON_CUBICCONGESTIONCONTROL_INITIALRTT_ENV_VAR);
    if (NULL != rtt_ns)
    {
        if (-1 == aeron_parse_duration_ns(rtt_ns, &state->initial_rtt_ns))
        {
            goto error_cleanup;
        }
    }

    state->mtu = sender_mtu_length;
    const int32_t initial_window_length = (int32_t)aeron_udp_channel_receiver_window(
        channel, context->initial_window_length);
    state->max_window_length = (int32_t)aeron_receiver_window_length(initial_window_length, term_length);

    state->max_cwnd = state->max_window_length / sender_mtu_length;
    state->cwnd = state->max_cwnd > AERON_CUBICCONGESTIONCONTROL_INITCWND ?
        AERON_CUBICCONGESTIONCONTROL_INITCWND : state->max_cwnd;
    state->initial_window_length = state->cwnd * sender_mtu_length;

    // initially set w_max to max window and act in the TCP and concave region initially
    state->w_max = state->max_cwnd;
    state->k = cbrt((double)state->w_max * AERON_CUBICCONGESTIONCONTROL_B / AERON_CUBICCONGESTIONCONTROL_C);

    // determine interval for adjustment based on heuristic of MTU, max window, and/or RTT estimate
    state->rtt_ns = (int64_t)state->initial_rtt_ns;
    state->window_update_timeout_ns = state->rtt_ns;
    state->rtt_timeout_ns = state->rtt_ns * AERON_CUBICCONGESTIONCONTROL_RTT_TIMEOUT_MULTIPLE;

    const int32_t rtt_indicator_counter_id = aeron_stream_counter_allocate(
        counters_manager,
        AERON_CUBICCONGESTIONCONTROL_RTT_INDICATOR_COUNTER_NAME,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_NULL_VALUE,
        registration_id,
        session_id,
        stream_id,
        channel->uri_length,
        channel->original_uri,
        "");
    if (rtt_indicator_counter_id < 0)
    {
        goto error_cleanup;
    }

    const int32_t window_indicator_counter_id = aeron_stream_counter_allocate(
        counters_manager,
        AERON_CUBICCONGESTIONCONTROL_WINDOW_INDICATOR_COUNTER_NAME,
        AERON_COUNTER_PER_IMAGE_TYPE_ID,
        AERON_NULL_VALUE,
        registration_id,
        session_id,
        stream_id,
        channel->uri_length,
        channel->original_uri,
        "");
    if (window_indicator_counter_id < 0)
    {
        aeron_counters_manager_free(counters_manager, rtt_indicator_counter_id);
        goto error_cleanup;
    }

    state->counters_manager = counters_manager;

    state->rtt_indicator.counter_id = rtt_indicator_counter_id;
    state->rtt_indicator.value_addr = aeron_counters_manager_addr(counters_manager, rtt_indicator_counter_id);
    aeron_counter_set_release(state->rtt_indicator.value_addr, 0);

    state->window_indicator.counter_id = window_indicator_counter_id;
    state->window_indicator.value_addr = aeron_counters_manager_addr(counters_manager, window_indicator_counter_id);
    aeron_counter_set_release(state->window_indicator.value_addr, state->initial_window_length);

    state->last_rtt_timestamp_ns = 0;

    state->last_loss_timestamp_ns = aeron_clock_cached_nano_time(context->receiver_cached_clock);
    state->last_update_timestamp_ns = state->last_loss_timestamp_ns;

    *strategy = _strategy;
    return 0;

error_cleanup:
    aeron_congestion_control_strategy_fini(_strategy);
    return -1;
}

int32_t aeron_cubic_congestion_control_strategy_get_max_cwnd(void *state)
{
    return ((aeron_cubic_congestion_control_strategy_state_t *)state)->max_cwnd;
}
