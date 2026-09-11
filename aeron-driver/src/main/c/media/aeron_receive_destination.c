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

#include "util/aeron_error.h"
#include "aeron_driver_receiver.h"
#include "aeron_position.h"
#include "media/aeron_receive_destination.h"

int aeron_receive_destination_create(
    aeron_receive_destination_t **destination,
    aeron_udp_channel_t *destination_channel,
    aeron_udp_channel_t *endpoint_channel,
    aeron_driver_context_t *context,
    aeron_counters_manager_t *counters_manager,
    int64_t registration_id,
    int32_t channel_status_counter_id)
{
    aeron_receive_destination_t *_destination = NULL;
    const size_t socket_rcvbuf = aeron_udp_channel_socket_so_rcvbuf(endpoint_channel, context->socket_rcvbuf);
    const size_t socket_sndbuf = aeron_udp_channel_socket_so_sndbuf(endpoint_channel, context->socket_sndbuf);
    bool is_media_timestamping = aeron_udp_channel_is_media_rcv_timestamps_enabled(endpoint_channel);

    if (aeron_alloc((void **)&_destination, sizeof(aeron_receive_destination_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "could not allocate receive_channel_endpoint");
        return -1;
    }

    _destination->transport.fd = -1;
    _destination->data_paths = &context->receiver_proxy->receiver->data_paths;
    _destination->transport.data_paths = _destination->data_paths;
    _destination->local_sockaddr_indicator.counter_id = AERON_NULL_COUNTER_ID;

    if (context->receiver_port_manager->get_managed_port(
        context->receiver_port_manager->state,
        &_destination->bind_addr,
        destination_channel,
        &destination_channel->remote_data) < 0)
    {
        AERON_APPEND_ERR("uri = %s", destination_channel->original_uri);
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    _destination->port_manager = context->receiver_port_manager;

    aeron_udp_channel_transport_params_t transport_params = {
        socket_rcvbuf,
        socket_sndbuf,
        context->mtu_length,
        destination_channel->interface_index,
        0 != destination_channel->multicast_ttl ? destination_channel->multicast_ttl : context->multicast_ttl,
        is_media_timestamping,
    };

    if (context->udp_channel_transport_bindings->init_func(
        &_destination->transport,
        &_destination->bind_addr,
        &destination_channel->local_data,
        NULL,
        &transport_params,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        AERON_APPEND_ERR("uri = %s", destination_channel->original_uri);
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    if (aeron_udp_channel_is_channel_rcv_timestamps_enabled(endpoint_channel))
    {
        _destination->transport.timestamp_flags |= AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP;
    }

    char local_sockaddr[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
    if (context->udp_channel_transport_bindings->bind_addr_and_port_func(
        &_destination->transport, local_sockaddr, sizeof(local_sockaddr)) < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    _destination->local_sockaddr_indicator.counter_id = aeron_counter_local_sockaddr_indicator_allocate(
        counters_manager,
        AERON_COUNTER_RCV_LOCAL_SOCKADDR_NAME,
        registration_id,
        channel_status_counter_id,
        local_sockaddr);
    _destination->local_sockaddr_indicator.value_addr = aeron_counters_manager_addr(
        counters_manager, _destination->local_sockaddr_indicator.counter_id);

    if (_destination->local_sockaddr_indicator.counter_id < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    if (context->udp_channel_transport_bindings->get_so_rcvbuf_func(&_destination->transport, &_destination->so_rcvbuf) < 0)
    {
        aeron_receive_destination_delete(_destination, counters_manager);
        return -1;
    }

    _destination->transport.destination_clientd = _destination;
    _destination->time_of_last_activity_ns = aeron_clock_cached_nano_time(context->receiver_cached_clock);

    if (destination_channel->is_multicast)
    {
        memcpy(&_destination->current_control_addr, &destination_channel->remote_control, sizeof(_destination->current_control_addr));
    }
    else if (destination_channel->has_explicit_control)
    {
        memcpy(&_destination->current_control_addr, &destination_channel->local_control, sizeof(_destination->current_control_addr));
    }

    _destination->has_control_addr = destination_channel->is_multicast || destination_channel->has_explicit_control;

    aeron_counter_set_release(
        _destination->local_sockaddr_indicator.value_addr, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_ACTIVE);

    // Only take ownership of the destination_channel if the receive destination is successfully created.
    _destination->conductor_fields.udp_channel = destination_channel;
    *destination = _destination;

    return 0;
}

void aeron_receive_destination_delete(
    aeron_receive_destination_t *destination, aeron_counters_manager_t *counters_manager)
{
    if (NULL != counters_manager && AERON_NULL_COUNTER_ID != destination->local_sockaddr_indicator.counter_id)
    {
        aeron_counter_set_release(
            destination->local_sockaddr_indicator.value_addr, AERON_COUNTER_CHANNEL_ENDPOINT_STATUS_CLOSING);
        aeron_counters_manager_free(counters_manager, destination->local_sockaddr_indicator.counter_id);
        destination->local_sockaddr_indicator.counter_id = AERON_NULL_COUNTER_ID;
    }

    if (NULL != destination->port_manager)
    {
        destination->port_manager->free_managed_port(
            destination->port_manager->state,
            &destination->bind_addr);
    }

    aeron_udp_channel_delete(destination->conductor_fields.udp_channel);
    aeron_free(destination);
}

extern void aeron_receive_destination_update_last_activity_ns(aeron_receive_destination_t *destination, int64_t now_ns);

extern bool aeron_receive_destination_re_resolution_required(aeron_receive_destination_t *destination, int64_t now_ns);
