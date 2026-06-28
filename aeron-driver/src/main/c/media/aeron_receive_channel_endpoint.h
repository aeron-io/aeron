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

#ifndef AERON_RECEIVE_CHANNEL_ENDPOINT_H
#define AERON_RECEIVE_CHANNEL_ENDPOINT_H

#include "collections/aeron_int64_counter_map.h"
#include "aeron_data_packet_dispatcher.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_driver_context.h"
#include "aeron_system_counters.h"
#include "media/aeron_receive_destination.h"
#include "aeron_loss_generator.h"

typedef enum aeron_receive_channel_endpoint_status_enum
{
    AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE,
    AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING,
    AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED
}
aeron_receive_channel_endpoint_status_t;

typedef struct aeron_receive_destination_entry_stct
{
    aeron_receive_destination_t *destination;
}
aeron_receive_destination_entry_t;

typedef struct aeron_receive_channel_endpoint_stct
{
    struct aeron_receive_channel_endpoint_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        aeron_udp_channel_t *udp_channel;
        aeron_receive_channel_endpoint_status_t status;
        int64_t image_ref_count;
    }
    conductor_fields;

    struct destination_stct
    {
        size_t length;
        size_t capacity;
        aeron_receive_destination_entry_t *array;
    }
    destinations;

    aeron_data_packet_dispatcher_t dispatcher;
    aeron_int64_counter_map_t stream_id_to_refcnt_map;
    aeron_int64_counter_map_t stream_and_session_id_to_refcnt_map;
    aeron_int64_counter_map_t response_stream_id_to_refcnt_map;
    aeron_atomic_counter_t channel_status;
    aeron_driver_receiver_proxy_t *receiver_proxy;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_clock_cache_t *cached_clock;

    aeron_driver_nak_message_func_t send_nak_message;

    int64_t receiver_id;
    volatile bool has_receiver_released;
    struct
    {
        bool is_present;
        int64_t value;
    }
    group_tag;

    int64_t *short_sends_counter;
    int64_t *possible_ttl_asymmetry_counter;
    int64_t *errors_frames_sent_counter;

    const aeron_loss_generator_t *data_loss_generator;
    const aeron_loss_generator_t *control_loss_generator;
}
aeron_receive_channel_endpoint_t;

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_receive_destination_t *straight_through_destination,
    aeron_atomic_counter_t *status_indicator,
    aeron_system_counters_t *system_counters,
    aeron_driver_context_t *context);

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *endpoint);

int aeron_receive_channel_endpoint_close(aeron_receive_channel_endpoint_t *endpoint);

int aeron_receive_channel_endpoint_send(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *address,
    struct iovec *iov);

int aeron_receive_channel_endpoint_elicit_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id);

int aeron_receive_channel_endpoint_send_sm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *control_addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t receiver_window,
    uint8_t flags);

int aeron_receive_channel_endpoint_send_nak(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length);

int aeron_receive_channel_endpoint_send_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int64_t echo_timestamp,
    int64_t reception_delta,
    bool is_reply);

int aeron_receive_channel_endpoint_send_response_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t response_session_id);

int aeron_receiver_channel_endpoint_send_error_frame(
    aeron_receive_channel_endpoint_t *channel_endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *control_addr,
    int32_t session_id,
    int32_t stream_id,
    int32_t error_code,
    const char *invalidation_reason);

void aeron_receive_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp);

int aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp);

int aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_on_unconnected_stream(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_receive_channel_endpoint_matches_tag(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    bool *has_match);

void aeron_receive_channel_endpoint_try_remove_endpoint(aeron_receive_channel_endpoint_t *endpoint);

int aeron_receive_channel_endpoint_incref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

int aeron_receive_channel_endpoint_decref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

int aeron_receive_channel_endpoint_incref_to_stream_and_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id);

int aeron_receive_channel_endpoint_decref_to_stream_and_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id);

int aeron_receive_channel_endpoint_incref_to_response_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

int aeron_receive_channel_endpoint_decref_to_response_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);

int aeron_receive_channel_endpoint_on_add_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int aeron_receive_channel_endpoint_on_remove_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id);
int aeron_receive_channel_endpoint_on_add_subscription_by_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id);
int aeron_receive_channel_endpoint_on_remove_subscription_by_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id);

int aeron_receive_channel_endpoint_add_destination(
    aeron_receive_channel_endpoint_t *endpoint, aeron_receive_destination_t *destination);
int aeron_receive_channel_endpoint_remove_destination(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    aeron_receive_destination_t **destination_out);

int aeron_receive_channel_endpoint_on_add_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image);
int aeron_receive_channel_endpoint_on_remove_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image);

int aeron_receiver_channel_endpoint_validate_sender_mtu_length(
    aeron_receive_channel_endpoint_t *endpoint,
    size_t sender_mtu_length,
    size_t window_max_length,
    aeron_driver_context_t *ctx);

void aeron_receive_channel_endpoint_check_for_re_resolution(
    aeron_receive_channel_endpoint_t *endpoint,
    int64_t now_ns,
    aeron_driver_conductor_proxy_t *conductor_proxy);

void aeron_receive_channel_endpoint_update_control_address(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *address);

int aeron_receive_channel_endpoint_add_poll_transports(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_transport_poller_t *poller);

int aeron_receive_channel_endpoint_remove_poll_transports(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_transport_poller_t *poller);

int aeron_receive_channel_endpoint_add_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_driver_receiver_t *receiver,
    int32_t session_id,
    int32_t stream_id);

int aeron_receive_channel_endpoint_add_pending_setup_destination(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_driver_receiver_t *receiver,
    aeron_receive_destination_t *destination,
    int32_t session_id,
    int32_t stream_id);

inline void aeron_receive_channel_endpoint_on_remove_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id)
{
    aeron_data_packet_dispatcher_remove_pending_setup(&endpoint->dispatcher, session_id, stream_id);
}

static inline void aeron_receive_channel_endpoint_on_remove_matching_state(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id, uint32_t image_state)
{
    aeron_data_packet_dispatcher_remove_matching_state(&endpoint->dispatcher, session_id, stream_id, image_state);
}

inline void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint)
{
    AERON_SET_RELEASE(endpoint->has_receiver_released, true);
}

inline bool aeron_receive_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint)
{
    bool has_receiver_released;
    AERON_GET_ACQUIRE(has_receiver_released, endpoint->has_receiver_released);

    return has_receiver_released;
}

inline bool aeron_receive_channel_endpoint_should_elicit_setup_message(aeron_receive_channel_endpoint_t *endpoint)
{
    return aeron_data_packet_dispatcher_should_elicit_setup_message(&endpoint->dispatcher);
}

inline int aeron_receive_channel_endpoint_bind_addr_and_port(
    aeron_receive_channel_endpoint_t *endpoint, char *buffer, size_t length)
{
    if (0 < endpoint->destinations.length)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[0].destination;
        return endpoint->transport_bindings->bind_addr_and_port_func(&destination->transport, buffer, length);
    }
    else
    {
        buffer[0] = '\0';
    }

    return 0;
}

inline void aeron_receive_channel_endpoint_inc_image_ref_count(aeron_receive_channel_endpoint_t *endpoint)
{
    endpoint->conductor_fields.image_ref_count++;
}

inline void aeron_receive_channel_endpoint_dec_image_ref_count(aeron_receive_channel_endpoint_t *endpoint)
{
    endpoint->conductor_fields.image_ref_count--;
}

#endif //AERON_RECEIVE_CHANNEL_ENDPOINT_H
