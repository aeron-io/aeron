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

#ifndef AERON_SEND_CHANNEL_ENDPOINT_H
#define AERON_SEND_CHANNEL_ENDPOINT_H

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "util/aeron_netutil.h"
#include "aeron_network_publication.h"
#include "aeron_driver_context.h"
#include "aeron_udp_channel.h"
#include "aeron_udp_channel_transport.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_udp_destination_tracker.h"
#include "aeron_driver_sender_proxy.h"

#define AERON_SEND_CHANNEL_ENDPOINT_DESTINATION_TIMEOUT_NS (5 * 1000 * 1000 * 1000LL)

typedef enum aeron_send_channel_endpoint_status_enum
{
    AERON_SEND_CHANNEL_ENDPOINT_STATUS_ACTIVE,
    AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSING,
    AERON_SEND_CHANNEL_ENDPOINT_STATUS_CLOSED
}
aeron_send_channel_endpoint_status_t;

typedef struct aeron_send_channel_endpoint_stct
{
    struct aeron_send_channel_endpoint_conductor_fields_stct
    {
        aeron_driver_managed_resource_t managed_resource;
        int32_t refcnt;
        bool has_reached_end_of_life;
        aeron_udp_channel_t *udp_channel;
        aeron_send_channel_endpoint_status_t status;
        size_t socket_sndbuf;
        size_t socket_rcvbuf;
    }
    conductor_fields;

    volatile bool has_sender_released;
    aeron_udp_channel_transport_t transport;
    aeron_atomic_counter_t channel_status;
    aeron_atomic_counter_t local_sockaddr_indicator;
    aeron_atomic_counter_t tracker_num_destinations;
    aeron_udp_destination_tracker_t *destination_tracker;
    aeron_driver_sender_proxy_t *sender_proxy;
    aeron_int64_to_ptr_hash_map_t publication_dispatch_map;
    aeron_udp_channel_transport_bindings_t *transport_bindings;
    aeron_udp_channel_data_paths_t *data_paths;
    struct sockaddr_storage current_data_addr;
    struct sockaddr_storage bind_addr;
    aeron_port_manager_t *port_manager;
    aeron_clock_cache_t *cached_clock;
    int64_t time_of_last_sm_ns;

    aeron_driver_nak_message_func_t on_nak_message;

    uint8_t padding[AERON_CACHE_LINE_LENGTH];
}
aeron_send_channel_endpoint_t;

int aeron_send_channel_endpoint_create(
    aeron_send_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_driver_uri_publication_params_t *params,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id);

int aeron_send_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_send_channel_endpoint_t *endpoint);

int aeron_send_channel_endpoint_close(aeron_send_channel_endpoint_t *endpoint);

int aeron_send_channel_send(
    aeron_send_channel_endpoint_t *endpoint,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent);

int aeron_send_channel_send_endpoint_address(
    aeron_send_channel_endpoint_t *endpoint,
    struct sockaddr_storage* endpoint_address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent);

int aeron_send_channel_endpoint_add_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

int aeron_send_channel_endpoint_remove_publication(
    aeron_send_channel_endpoint_t *endpoint, aeron_network_publication_t *publication);

void aeron_send_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *sender_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp);

int aeron_send_channel_endpoint_on_nak(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

int aeron_send_channel_endpoint_on_status_message(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_send_channel_endpoint_on_error(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_rttm(
    aeron_send_channel_endpoint_t *endpoint, uint8_t *buffer, size_t length, struct sockaddr_storage *addr);

void aeron_send_channel_endpoint_on_response_setup(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_driver_conductor_proxy_t *conductor_proxy,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr);

int aeron_send_channel_endpoint_check_for_re_resolution(
    aeron_send_channel_endpoint_t *endpoint, int64_t now_ns, aeron_driver_conductor_proxy_t *conductor_proxy);

int aeron_send_channel_endpoint_resolution_change(
    aeron_driver_context_t *context,
    aeron_send_channel_endpoint_t *endpoint,
    const char *endpoint_name,
    struct sockaddr_storage *new_addr);

int aeron_send_channel_endpoint_matches_tag(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    bool *has_match);

inline void aeron_send_channel_endpoint_sender_release(aeron_send_channel_endpoint_t *endpoint)
{
    AERON_SET_RELEASE(endpoint->has_sender_released, true);
}

inline bool aeron_send_channel_endpoint_has_sender_released(aeron_send_channel_endpoint_t *endpoint)
{
    bool has_sender_released;
    AERON_GET_ACQUIRE(has_sender_released, endpoint->has_sender_released);

    return has_sender_released;
}

inline int aeron_send_channel_endpoint_add_destination(
    aeron_send_channel_endpoint_t *endpoint,
    aeron_uri_t *uri,
    struct sockaddr_storage *addr,
    int64_t destination_registration_id)
{
    const int64_t now_ns = aeron_clock_cached_nano_time(endpoint->destination_tracker->cached_clock);
    return aeron_udp_destination_tracker_manual_add_destination(
        endpoint->destination_tracker, now_ns, uri, addr, destination_registration_id);
}

inline int aeron_send_channel_endpoint_remove_destination(
    aeron_send_channel_endpoint_t *endpoint, struct sockaddr_storage *addr, aeron_uri_t **removed_uri)
{
    return aeron_udp_destination_tracker_remove_destination(endpoint->destination_tracker, addr, removed_uri);
}

inline int aeron_send_channel_endpoint_remove_destination_by_id(
    aeron_send_channel_endpoint_t *endpoint, int64_t registration_destination_id, aeron_uri_t **removed_uri)
{
    return aeron_udp_destination_tracker_remove_destination_by_id(
        endpoint->destination_tracker, registration_destination_id, removed_uri);
}

inline int aeron_send_channel_endpoint_bind_addr_and_port(
    aeron_send_channel_endpoint_t *endpoint, char *buffer, size_t length)
{
    return endpoint->transport_bindings->bind_addr_and_port_func(&endpoint->transport, buffer, length);
}

inline bool aeron_send_channel_is_unicast(aeron_send_channel_endpoint_t *endpoint)
{
    return NULL == endpoint->destination_tracker && !endpoint->conductor_fields.udp_channel->is_multicast;
}

#endif //AERON_SEND_CHANNEL_ENDPOINT_H
