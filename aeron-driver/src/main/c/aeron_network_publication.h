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

#ifndef AERON_NETWORK_PUBLICATION_H
#define AERON_NETWORK_PUBLICATION_H

#include "util/aeron_bitutil.h"
#include "util/aeron_fileutil.h"
#include "uri/aeron_driver_uri.h"
#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_system_counters.h"
#include "aeron_retransmit_handler.h"

typedef enum aeron_network_publication_state_enum
{
    AERON_NETWORK_PUBLICATION_STATE_ACTIVE,
    AERON_NETWORK_PUBLICATION_STATE_DRAINING,
    AERON_NETWORK_PUBLICATION_STATE_LINGER,
    AERON_NETWORK_PUBLICATION_STATE_DONE
}
aeron_network_publication_state_t;

#define AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS (100 * 1000 * 1000LL)
#define AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS (100 * 1000 * 1000LL)

typedef struct aeron_send_channel_endpoint_stct aeron_send_channel_endpoint_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

typedef struct aeron_network_publication_stct
{
    struct aeron_network_publication_conductor_fields_stct
    {
        bool has_reached_end_of_life;
        aeron_network_publication_state_t state;
        int32_t refcnt;
        aeron_driver_managed_resource_t managed_resource;
        aeron_subscribable_t subscribable;
        int64_t clean_position;
        int64_t time_of_last_activity_ns;
        int64_t last_snd_pos;
    }
    conductor_fields;

    uint8_t conductor_fields_pad[
        (4 * AERON_CACHE_LINE_LENGTH) - sizeof(struct aeron_network_publication_conductor_fields_stct)];

    aeron_mapped_raw_log_t mapped_raw_log;
    aeron_position_t pub_pos_position;
    aeron_position_t pub_lmt_position;
    aeron_position_t snd_pos_position;
    aeron_position_t snd_lmt_position;
    aeron_atomic_counter_t snd_bpe_counter;
    aeron_atomic_counter_t snd_naks_received_counter;
    aeron_retransmit_handler_t retransmit_handler;
    aeron_logbuffer_metadata_t *log_meta_data;
    aeron_send_channel_endpoint_t *endpoint;
    aeron_flow_control_strategy_t *flow_control;
    aeron_clock_cache_t *cached_clock;

    uint8_t sender_fields_pad_lhs[AERON_CACHE_LINE_LENGTH];
    bool has_initial_connection;
    bool track_sender_limits;
    int64_t time_of_last_data_or_heartbeat_ns;
    size_t current_messages_per_send;
    int64_t status_message_deadline_ns;
    int64_t time_of_last_setup_ns;
    uint8_t sender_fields_pad_rhs[AERON_CACHE_LINE_LENGTH];

    struct sockaddr_storage endpoint_address;

    char *log_file_name;
    int64_t term_buffer_length;
    int64_t term_window_length;
    int64_t trip_gain;
    int64_t linger_timeout_ns;
    int64_t unblock_timeout_ns;
    int64_t connection_timeout_ns;
    int64_t untethered_window_limit_timeout_ns;
    int64_t untethered_linger_timeout_ns;
    int64_t untethered_resting_timeout_ns;

    int64_t tag;
    int64_t response_correlation_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t starting_term_id;
    int32_t term_length_mask;
    size_t starting_term_offset;
    size_t log_file_name_length;
    size_t position_bits_to_shift;
    size_t mtu_length;
    size_t max_messages_per_send;
    bool spies_simulate_connection;
    bool signal_eos;
    bool is_setup_elicited;
    bool is_exclusive;
    bool is_response;
    volatile bool has_receivers;
    volatile bool has_spies;
    volatile bool is_connected;
    volatile bool is_end_of_stream;
    volatile bool has_sender_released;
    volatile bool has_received_unicast_eos;
    aeron_raw_log_close_func_t raw_log_close_func;
    aeron_raw_log_free_func_t raw_log_free_func;
    struct
    {
        aeron_untethered_subscription_state_change_func_t untethered_subscription_state_change;
        aeron_driver_resend_func_t resend;
        aeron_driver_publication_revoke_func_t publication_revoke;
    } log;

    volatile int64_t *short_sends_counter;
    volatile int64_t *heartbeats_sent_counter;
    volatile int64_t *sender_flow_control_limits_counter;
    volatile int64_t *retransmits_sent_counter;
    volatile int64_t *retransmitted_bytes_counter;
    volatile int64_t *unblocked_publications_counter;
    volatile int64_t *publications_revoked_counter;
    volatile int64_t *mapped_bytes_counter;

    aeron_int64_counter_map_t receiver_liveness_tracker;
}
aeron_network_publication_t;

int aeron_network_publication_create(
    aeron_network_publication_t **publication,
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_context_t *context,
    int64_t registration_id,
    int32_t session_id,
    int32_t stream_id,
    int32_t initial_term_id,
    aeron_position_t *pub_pos_position,
    aeron_position_t *pub_lmt_position,
    aeron_position_t *snd_pos_position,
    aeron_position_t *snd_lmt_position,
    aeron_atomic_counter_t *snd_bpe_counter,
    aeron_atomic_counter_t *snd_naks_received_counter,
    aeron_flow_control_strategy_t *flow_control_strategy,
    aeron_driver_uri_publication_params_t *params,
    bool is_exclusive,
    aeron_system_counters_t *system_counters);

void aeron_network_publication_close(
    aeron_counters_manager_t *counters_manager, aeron_network_publication_t *publication);

bool aeron_network_publication_free(aeron_network_publication_t *publication);

void aeron_network_publication_on_time_event(
    aeron_driver_conductor_t *conductor, aeron_network_publication_t *publication, int64_t now_ns, int64_t now_ms);

int aeron_network_publication_send(aeron_network_publication_t *publication, int64_t now_ns);
int aeron_network_publication_resend(void *clientd, int32_t term_id, int32_t term_offset, size_t length);

int aeron_network_publication_send_data(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t snd_pos, int32_t term_offset);

int aeron_network_publication_on_nak(
    aeron_network_publication_t *publication, int32_t term_id, int32_t term_offset, int32_t length);

void aeron_network_publication_on_status_message(
    aeron_network_publication_t *publication,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

void aeron_network_publication_on_error(
    aeron_network_publication_t *publication,
    int64_t destination_registration_id,
    const uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *src_address,
    aeron_driver_conductor_proxy_t *pStct);

void aeron_network_publication_on_rttm(
    aeron_network_publication_t *publication, const uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_network_publication_clean_buffer(aeron_network_publication_t *publication, int64_t position);

int aeron_network_publication_update_pub_pos_and_lmt(aeron_network_publication_t *publication);

void aeron_network_publication_check_for_blocked_publisher(
    aeron_network_publication_t *publication, int64_t now_ns, int64_t producer_position, int64_t snd_pos);

void aeron_network_publication_add_subscriber_hook(void *clientd, volatile int64_t *value_addr);

void aeron_network_publication_remove_subscriber_hook(void *clientd, volatile int64_t *value_addr);

inline bool aeron_network_publication_is_possibly_blocked(
    aeron_network_publication_t *publication, int64_t producer_position, int64_t consumer_position)
{
    int32_t producer_term_count;

    AERON_GET_ACQUIRE(producer_term_count, publication->log_meta_data->active_term_count);
    const int32_t expected_term_count = (int32_t)(consumer_position >> publication->position_bits_to_shift);

    if (producer_term_count != expected_term_count)
    {
        return true;
    }

    return producer_position > consumer_position;
}

inline int64_t aeron_network_publication_producer_position(aeron_network_publication_t *publication)
{
    int64_t raw_tail;

    AERON_LOGBUFFER_RAWTAIL_VOLATILE(raw_tail, publication->log_meta_data);

    return aeron_logbuffer_compute_position(
        aeron_logbuffer_term_id(raw_tail),
        aeron_logbuffer_term_offset(raw_tail, (int32_t)publication->mapped_raw_log.term_length),
        publication->position_bits_to_shift,
        publication->initial_term_id);
}

inline int64_t aeron_network_publication_join_position(aeron_network_publication_t *publication)
{
    return aeron_counter_get_acquire(publication->snd_pos_position.value_addr);
}

inline void aeron_network_publication_trigger_send_setup_frame(
    aeron_network_publication_t *publication,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    const int64_t time_ns = aeron_clock_cached_nano_time(publication->cached_clock);
    bool is_end_of_stream;
    AERON_GET_ACQUIRE(is_end_of_stream, publication->is_end_of_stream);

    if (!is_end_of_stream)
    {
        publication->is_setup_elicited = true;
        publication->flow_control->on_trigger_send_setup(
            publication->flow_control->state,
            buffer,
            length,
            addr,
            time_ns);

        if (publication->is_response)
        {
            if (AF_INET == addr->ss_family)
            {
                memcpy(&publication->endpoint_address, addr, sizeof(struct sockaddr_in));
            }
            else if (AF_INET6 == addr->ss_family)
            {
                memcpy(&publication->endpoint_address, addr, sizeof(struct sockaddr_in6));
            }
        }
    }
}

inline void aeron_network_publication_sender_release(aeron_network_publication_t *publication)
{
    AERON_SET_RELEASE(publication->has_sender_released, true);
}

inline bool aeron_network_publication_has_sender_released(aeron_network_publication_t *publication)
{
    bool has_sender_released;
    AERON_GET_ACQUIRE(has_sender_released, publication->has_sender_released);

    return has_sender_released;
}

inline int64_t aeron_network_publication_max_spy_position(aeron_network_publication_t *publication, int64_t snd_pos)
{
    int64_t position = snd_pos;

    for (size_t i = 0, length = publication->conductor_fields.subscribable.length; i < length; i++)
    {
        aeron_tetherable_position_t *tetherable_position = &publication->conductor_fields.subscribable.array[i];
        int64_t spy_position = aeron_counter_get_acquire(tetherable_position->value_addr);

        if (AERON_SUBSCRIPTION_TETHER_RESTING != tetherable_position->state)
        {
            position = spy_position > position ? spy_position : position;
        }
    }

    return position;
}

inline bool aeron_network_publication_is_accepting_subscriptions(aeron_network_publication_t *publication)
{
    return AERON_NETWORK_PUBLICATION_STATE_ACTIVE == publication->conductor_fields.state ||
        (AERON_NETWORK_PUBLICATION_STATE_DRAINING == publication->conductor_fields.state &&
            aeron_driver_subscribable_has_working_positions(&publication->conductor_fields.subscribable) &&
         aeron_network_publication_producer_position(publication) >
             aeron_counter_get_acquire(publication->snd_pos_position.value_addr));
}

inline int64_t aeron_network_publication_registration_id(aeron_network_publication_t *publication)
{
    return publication->log_meta_data->correlation_id;
}

#endif //AERON_NETWORK_PUBLICATION_H
