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

#include <inttypes.h>

#include "aeron_driver_context.h"
#include "aeron_socket.h"
#include "aeron_system_counters.h"
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "util/aeron_arrayutil.h"
#include "aeron_alloc.h"
#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_receiver.h"
#include "aeron_timestamps.h"

int aeron_receive_channel_endpoint_set_group_tag(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    aeron_driver_context_t *context)
{
    int64_t group_tag = 0;
    int rc = aeron_uri_get_int64(&channel->uri.params.udp.additional_params, AERON_URI_GTAG_KEY, 0, &group_tag);
    if (rc < 0)
    {
        return -1;
    }

    if (0 == rc)
    {
        endpoint->group_tag.is_present = context->receiver_group_tag.is_present;
        endpoint->group_tag.value = context->receiver_group_tag.value;
    }
    else
    {
        endpoint->group_tag.is_present = true;
        endpoint->group_tag.value = group_tag;
    }

    return 0;
}

int aeron_receive_channel_endpoint_create(
    aeron_receive_channel_endpoint_t **endpoint,
    aeron_udp_channel_t *channel,
    aeron_receive_destination_t *straight_through_destination,
    aeron_atomic_counter_t *status_indicator,
    aeron_system_counters_t *system_counters,
    aeron_driver_context_t *context)
{
    aeron_receive_channel_endpoint_t *_endpoint = NULL;

    if (aeron_alloc((void **)&_endpoint, sizeof(aeron_receive_channel_endpoint_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "could not allocate receive_channel_endpoint");
        return -1;
    }

    if (aeron_data_packet_dispatcher_init(
        &_endpoint->dispatcher, context->conductor_proxy, context->receiver_proxy->receiver) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to initialise data packet dispatcher");
        return -1;
    }

    if (aeron_int64_counter_map_init(
        &_endpoint->stream_id_to_refcnt_map, 0, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "could not init stream_id_to_refcnt_map");
        return -1;
    }

    if (aeron_int64_counter_map_init(
        &_endpoint->stream_and_session_id_to_refcnt_map, 0, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "could not init stream_and_session_id_to_refcnt_map");
        return -1;
    }

    if (aeron_int64_counter_map_init(
        &_endpoint->response_stream_id_to_refcnt_map, 0, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "could not init response_stream_id_to_refcnt_map");
        return -1;
    }

    _endpoint->conductor_fields.managed_resource.clientd = _endpoint;
    _endpoint->conductor_fields.managed_resource.registration_id = -1;
    _endpoint->conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_ACTIVE;
    _endpoint->conductor_fields.image_ref_count = 0;
    _endpoint->channel_status.counter_id = -1;
    _endpoint->transport_bindings = context->udp_channel_transport_bindings;

    _endpoint->has_receiver_released = false;

    _endpoint->channel_status.counter_id = status_indicator->counter_id;
    _endpoint->channel_status.value_addr = status_indicator->value_addr;

    _endpoint->receiver_id = context->next_receiver_id++;
    _endpoint->receiver_proxy = context->receiver_proxy;

    if (aeron_receive_channel_endpoint_set_group_tag(_endpoint, channel, context) < 0)
    {
        aeron_receive_channel_endpoint_delete(NULL, _endpoint);
        return -1;
    }

    _endpoint->short_sends_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    _endpoint->possible_ttl_asymmetry_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_POSSIBLE_TTL_ASYMMETRY);
    _endpoint->errors_frames_sent_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_ERROR_FRAMES_SENT);

    _endpoint->cached_clock = context->receiver_cached_clock;

    _endpoint->send_nak_message = context->log.send_nak_message;

    if (NULL != straight_through_destination)
    {
        if (aeron_receive_channel_endpoint_add_destination(_endpoint, straight_through_destination) < 0)
        {
            return -1;
        }
    }

    // Only take ownership on successful construction.
    _endpoint->conductor_fields.udp_channel = channel;
    *endpoint = _endpoint;
    return 0;
}

int aeron_receive_channel_endpoint_delete(
    aeron_counters_manager_t *counters_manager, aeron_receive_channel_endpoint_t *endpoint)
{
    if (NULL != counters_manager && -1 != endpoint->channel_status.counter_id)
    {
        aeron_counters_manager_free(counters_manager, endpoint->channel_status.counter_id);
    }

    aeron_int64_counter_map_delete(&endpoint->stream_id_to_refcnt_map);
    aeron_int64_counter_map_delete(&endpoint->stream_and_session_id_to_refcnt_map);
    aeron_int64_counter_map_delete(&endpoint->response_stream_id_to_refcnt_map);
    aeron_data_packet_dispatcher_close(&endpoint->dispatcher);
    bool delete_this_channel = false;

    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (endpoint->conductor_fields.status != AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED)
        {
            endpoint->transport_bindings->close_func(&destination->transport);
        }

        // The endpoint will be deleted by the destination, for simple endpoints, i.e. non-mds the channel is shared.
        delete_this_channel |= destination->conductor_fields.udp_channel == endpoint->conductor_fields.udp_channel;
        aeron_receive_destination_delete(destination, counters_manager);
    }

    if (!delete_this_channel)
    {
        aeron_udp_channel_delete(endpoint->conductor_fields.udp_channel);
    }

    aeron_free(endpoint->destinations.array);
    aeron_free(endpoint);

    return 0;
}

int aeron_receive_channel_endpoint_close(aeron_receive_channel_endpoint_t *endpoint)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;
        endpoint->transport_bindings->close_func(&destination->transport);
    }

    endpoint->conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSED;

    return 0;
}

int aeron_receive_channel_endpoint_send(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *address,
    struct iovec *iov)
{
    int64_t min_bytes_sent = (int64_t)iov->iov_len;
    int64_t bytes_sent = 0;

    const int sendmsg_result = destination->data_paths->send_func(
        destination->data_paths, &destination->transport, address, iov, 1, &bytes_sent);

    if (0 <= sendmsg_result)
    {
        min_bytes_sent = bytes_sent < min_bytes_sent ? bytes_sent : min_bytes_sent;
    }
    else
    {
        min_bytes_sent = sendmsg_result;
    }

    return (int)min_bytes_sent;
}

int aeron_receive_channel_endpoint_elicit_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    int work_count = 0;
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;
        int result = aeron_receive_channel_endpoint_send_sm(
            endpoint,
            destination,
            &destination->current_control_addr,
            stream_id,
            session_id,
            0,
            0,
            0,
            AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG);

        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        result += work_count;
    }

    return work_count;
}

int aeron_receive_channel_endpoint_send_sm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *control_addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t receiver_window,
    uint8_t flags)
{
    uint8_t buffer[sizeof(aeron_status_message_header_t) + sizeof(aeron_status_message_optional_header_t)];
    aeron_status_message_header_t *sm_header = (aeron_status_message_header_t *)buffer;
    aeron_status_message_optional_header_t *sm_optional_header =
        (aeron_status_message_optional_header_t *)(buffer + sizeof(aeron_status_message_header_t));

    struct iovec iov;

    const int32_t frame_length = endpoint->group_tag.is_present ?
        sizeof(aeron_status_message_header_t) + sizeof(aeron_status_message_optional_header_t) :
        sizeof(aeron_status_message_header_t);

    sm_header->frame_header.frame_length = frame_length;
    sm_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    sm_header->frame_header.flags = flags;
    sm_header->frame_header.type = AERON_HDR_TYPE_SM;
    sm_header->session_id = session_id;
    sm_header->stream_id = stream_id;
    sm_header->consumption_term_id = term_id;
    sm_header->consumption_term_offset = term_offset;
    sm_header->receiver_window = receiver_window;
    sm_header->receiver_id = endpoint->receiver_id;
    sm_optional_header->group_tag = endpoint->group_tag.value;

    iov.iov_base = buffer;
    iov.iov_len = (size_t)frame_length;

    int bytes_sent = aeron_receive_channel_endpoint_send(endpoint, destination, control_addr, &iov);
    if (bytes_sent != (int)iov.iov_len)
    {
        if (bytes_sent >= 0 && !aeron_is_acceptable_socket_error())
        {
            aeron_counter_increment(endpoint->short_sends_counter);
        }
    }

    return bytes_sent;
}

int aeron_receive_channel_endpoint_send_nak(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t term_id,
    int32_t term_offset,
    int32_t length)
{
    uint8_t buffer[sizeof(aeron_nak_header_t)];
    aeron_nak_header_t *nak_header = (aeron_nak_header_t *)buffer;
    struct iovec iov;

    nak_header->frame_header.frame_length = sizeof(aeron_nak_header_t);
    nak_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    nak_header->frame_header.flags = 0;
    nak_header->frame_header.type = AERON_HDR_TYPE_NAK;
    nak_header->session_id = session_id;
    nak_header->stream_id = stream_id;
    nak_header->term_id = term_id;
    nak_header->term_offset = term_offset;
    nak_header->length = length;

    iov.iov_base = buffer;
    iov.iov_len = sizeof(aeron_nak_header_t);

    int bytes_sent = aeron_receive_channel_endpoint_send(endpoint, destination, addr, &iov);
    if (bytes_sent != (int)iov.iov_len)
    {
        if (bytes_sent >= 0 && !aeron_is_acceptable_socket_error())
        {
            aeron_counter_increment(endpoint->short_sends_counter);
        }
    }

    aeron_driver_nak_message_func_t send_nak_message = endpoint->send_nak_message;
    if (NULL != send_nak_message)
    {
        send_nak_message(
            addr,
            session_id,
            stream_id,
            term_id,
            term_offset,
            length,
            endpoint->conductor_fields.udp_channel->uri_length,
            endpoint->conductor_fields.udp_channel->original_uri);
    }

    return bytes_sent;
}

int aeron_receive_channel_endpoint_send_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int64_t echo_timestamp,
    int64_t reception_delta,
    bool is_reply)
{
    uint8_t buffer[sizeof(aeron_rttm_header_t)];
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *)buffer;
    struct iovec iov;

    rttm_header->frame_header.frame_length = sizeof(aeron_rttm_header_t);
    rttm_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    rttm_header->frame_header.flags = is_reply ? AERON_RTTM_HEADER_REPLY_FLAG : (uint8_t)0;
    rttm_header->frame_header.type = AERON_HDR_TYPE_RTTM;
    rttm_header->session_id = session_id;
    rttm_header->stream_id = stream_id;
    rttm_header->echo_timestamp = echo_timestamp;
    rttm_header->reception_delta = reception_delta;
    rttm_header->receiver_id = endpoint->receiver_id;

    iov.iov_base = buffer;
    iov.iov_len = sizeof(aeron_rttm_header_t);

    int bytes_sent = aeron_receive_channel_endpoint_send(endpoint, destination, addr, &iov);
    if (bytes_sent != (int)iov.iov_len)
    {
        if (bytes_sent >= 0 && !aeron_is_acceptable_socket_error())
        {
            aeron_counter_increment(endpoint->short_sends_counter);
        }
    }

    return bytes_sent;
}

int aeron_receive_channel_endpoint_send_response_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *addr,
    int32_t stream_id,
    int32_t session_id,
    int32_t response_session_id)
{
    uint8_t buffer[sizeof(aeron_response_setup_header_t)];
    aeron_response_setup_header_t *res_setup_header = (aeron_response_setup_header_t *)buffer;
    struct iovec iov;

    res_setup_header->frame_header.frame_length = sizeof(aeron_response_setup_header_t);
    res_setup_header->frame_header.version = AERON_FRAME_HEADER_VERSION;
    res_setup_header->frame_header.flags = UINT8_C(0);
    res_setup_header->frame_header.type = AERON_HDR_TYPE_RSP_SETUP;
    res_setup_header->session_id = session_id;
    res_setup_header->stream_id = stream_id;
    res_setup_header->response_session_id = response_session_id;

    iov.iov_base = buffer;
    iov.iov_len = sizeof(aeron_response_setup_header_t);
    int bytes_sent = aeron_receive_channel_endpoint_send(endpoint, destination, addr, &iov);
    if (bytes_sent != (int)iov.iov_len)
    {
        if (bytes_sent >= 0 && !aeron_is_acceptable_socket_error())
        {
            aeron_counter_increment(endpoint->short_sends_counter);
        }
    }

    return bytes_sent;
}

int aeron_receiver_channel_endpoint_send_error_frame(
    aeron_receive_channel_endpoint_t *channel_endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *control_addr,
    int32_t session_id,
    int32_t stream_id,
    int32_t error_code,
    const char *invalidation_reason)
{
    uint8_t buffer[AERON_ERROR_MAX_FRAME_LENGTH];
    aeron_error_t *error = (aeron_error_t *)buffer;
    struct iovec iov;

    const size_t error_message_length = strnlen(invalidation_reason, AERON_ERROR_MAX_TEXT_LENGTH);
    const size_t frame_length = sizeof(aeron_error_t) + error_message_length;
    error->frame_header.frame_length = (int32_t)frame_length;
    error->frame_header.version = AERON_FRAME_HEADER_VERSION;
    error->frame_header.flags = channel_endpoint->group_tag.is_present ? AERON_ERROR_HAS_GROUP_TAG_FLAG : UINT8_C(0);
    error->frame_header.type = AERON_HDR_TYPE_ERR;
    error->session_id = session_id;
    error->stream_id = stream_id;
    error->receiver_id = channel_endpoint->receiver_id;
    error->group_tag = channel_endpoint->group_tag.value;
    error->error_code = error_code;
    error->error_length = (int32_t)error_message_length;
    memcpy(&buffer[sizeof(aeron_error_t)], invalidation_reason, error_message_length);

    iov.iov_base = buffer;
    iov.iov_len = (unsigned long)frame_length;
    int bytes_sent = aeron_receive_channel_endpoint_send(channel_endpoint, destination, control_addr, &iov);
    if (bytes_sent != (int)iov.iov_len)
    {
        if (bytes_sent >= 0 && !aeron_is_acceptable_socket_error())
        {
            aeron_counter_increment(channel_endpoint->short_sends_counter);
        }
    }
    else
    {
        aeron_counter_increment(channel_endpoint->errors_frames_sent_counter);
    }

    return bytes_sent;
}

void aeron_receive_channel_endpoint_dispatch(
    aeron_udp_channel_data_paths_t *data_paths,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)receiver_clientd;
    aeron_frame_header_t *frame_header = (aeron_frame_header_t *)buffer;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)endpoint_clientd;
    aeron_receive_destination_t *destination = (aeron_receive_destination_t *)destination_clientd;

    if ((length < sizeof(aeron_frame_header_t)) || (frame_header->version != AERON_FRAME_HEADER_VERSION))
    {
        aeron_counter_increment(receiver->invalid_frames_counter);
        return;
    }

    switch (frame_header->type)
    {
        case AERON_HDR_TYPE_PAD:
        case AERON_HDR_TYPE_DATA:
            if (length >= sizeof(aeron_data_header_t))
            {
                if (aeron_receive_channel_endpoint_on_data(
                    endpoint, destination, buffer, length, addr, media_receive_timestamp) < 0)
                {
                    AERON_APPEND_ERR("%s", "receiver on_data");
                    aeron_driver_receiver_log_error(receiver);
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter);
            }
            break;

        case AERON_HDR_TYPE_SETUP:
            if (length >= sizeof(aeron_setup_header_t))
            {
                if (aeron_receive_channel_endpoint_on_setup(endpoint, destination, buffer, length, addr) < 0)
                {
                    AERON_APPEND_ERR("%s", "receiver on_setup");
                    aeron_driver_receiver_log_error(receiver);
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter);
            }
            break;

        case AERON_HDR_TYPE_RTTM:
            if (length >= sizeof(aeron_rttm_header_t))
            {
                if (aeron_receive_channel_endpoint_on_rttm(endpoint, destination, buffer, length, addr) < 0)
                {
                    AERON_APPEND_ERR("%s", "receiver on_rttm");
                    aeron_driver_receiver_log_error(receiver);
                }
            }
            else
            {
                aeron_counter_increment(receiver->invalid_frames_counter);
            }
            break;

        default:
            break;
    }
}

static void aeron_receive_channel_endpoint_apply_timestamps(
    aeron_udp_channel_t *endpoint_channel,
    uint32_t timestamp_flags,
    struct timespec *media_receive_timestamp,
    uint8_t *buffer,
    size_t length)
{
    if (!aeron_publication_image_is_heartbeat(buffer, length))
    {
        if (NULL != media_receive_timestamp)
        {
            int32_t offset = endpoint_channel->media_rcv_timestamp_offset;
            aeron_timestamps_set_timestamp(media_receive_timestamp, offset, buffer, length);
        }

        if (AERON_UDP_CHANNEL_TRANSPORT_CHANNEL_RCV_TIMESTAMP & timestamp_flags)
        {
            struct timespec receive_timestamp;
            if (0 == aeron_clock_gettime_realtime(&receive_timestamp))
            {
                int32_t offset = endpoint_channel->channel_rcv_timestamp_offset;
                aeron_timestamps_set_timestamp(&receive_timestamp, offset, buffer, length);
            }
        }
    }
}

int aeron_receive_channel_endpoint_on_data(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr,
    struct timespec *media_receive_timestamp)
{
    aeron_data_header_t *data_header = (aeron_data_header_t *)buffer;

    aeron_receive_channel_endpoint_apply_timestamps(
        endpoint->conductor_fields.udp_channel,
        destination->transport.timestamp_flags,
        media_receive_timestamp,
        buffer,
        length);

    aeron_receive_destination_update_last_activity_ns(
        destination, aeron_clock_cached_nano_time(endpoint->cached_clock));

    return aeron_data_packet_dispatcher_on_data(
        &endpoint->dispatcher, endpoint, destination, data_header, buffer, length, addr);
}

int aeron_receive_channel_endpoint_on_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_setup_header_t *setup_header = (aeron_setup_header_t *)buffer;

    aeron_receive_destination_update_last_activity_ns(
        destination, aeron_clock_cached_nano_time(endpoint->cached_clock));

    return aeron_data_packet_dispatcher_on_setup(
        &endpoint->dispatcher, endpoint, destination, setup_header, buffer, length, addr);
}

int aeron_receive_channel_endpoint_on_rttm(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    aeron_rttm_header_t *rttm_header = (aeron_rttm_header_t *)buffer;
    int result = 0;

    if (endpoint->receiver_id == rttm_header->receiver_id || 0 == rttm_header->receiver_id)
    {
        int64_t now_ns = aeron_clock_cached_nano_time(endpoint->cached_clock);
        aeron_receive_destination_update_last_activity_ns(destination, now_ns);

        result = aeron_data_packet_dispatcher_on_rttm(
            &endpoint->dispatcher, endpoint, destination, rttm_header, buffer, length, addr);
    }

    return result;
}

int aeron_receive_channel_endpoint_matches_tag(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    bool *has_match)
{
    struct sockaddr_storage* current_control_addr = NULL;
    if (1 == endpoint->destinations.length &&
        endpoint->destinations.array[0].destination->conductor_fields.udp_channel == endpoint->conductor_fields.udp_channel &&
        endpoint->destinations.array[0].destination->conductor_fields.udp_channel->has_explicit_control)
    {
        current_control_addr = &endpoint->destinations.array[0].destination->current_control_addr;
    }

    if (aeron_udp_channel_matches_tag(
        channel, endpoint->conductor_fields.udp_channel, current_control_addr, NULL, has_match) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

void aeron_receive_channel_endpoint_try_remove_endpoint(aeron_receive_channel_endpoint_t *endpoint)
{
    if (0 == endpoint->stream_id_to_refcnt_map.size &&
        0 == endpoint->stream_and_session_id_to_refcnt_map.size &&
        0 == endpoint->response_stream_id_to_refcnt_map.size &&
        0 >= endpoint->conductor_fields.image_ref_count)
    {
        /* mark as CLOSING to be aware not to use again (to be receiver_released and deleted) */
        endpoint->conductor_fields.status = AERON_RECEIVE_CHANNEL_ENDPOINT_STATUS_CLOSING;
        aeron_driver_receiver_proxy_on_remove_endpoint(endpoint->receiver_proxy, endpoint);
    }
}

int aeron_receive_channel_endpoint_incref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    int64_t count;
    if (aeron_int64_counter_map_inc_and_get(&endpoint->stream_id_to_refcnt_map, stream_id, &count) < 0)
    {
        return -1;
    }

    if (1 == count)
    {
        aeron_driver_receiver_proxy_on_add_subscription(endpoint->receiver_proxy, endpoint, stream_id);
    }

    return 0;
}

int aeron_receive_channel_endpoint_decref_to_stream(aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    const int64_t count = aeron_int64_counter_map_get(&endpoint->stream_id_to_refcnt_map, stream_id);

    if (0 == count)
    {
        return 0;
    }

    int64_t count_after_dec = 0;
    int result = aeron_int64_counter_map_dec_and_get(&endpoint->stream_id_to_refcnt_map, stream_id, &count_after_dec);
    if (result < 0)
    {
        return -1;
    }

    if (0 == count_after_dec)
    {
        aeron_driver_receiver_proxy_on_remove_subscription(endpoint->receiver_proxy, endpoint, stream_id);

        aeron_receive_channel_endpoint_try_remove_endpoint(endpoint);
    }

    return result;
}

int aeron_receive_channel_endpoint_incref_to_response_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    int64_t count;
    if (aeron_int64_counter_map_inc_and_get(&endpoint->response_stream_id_to_refcnt_map, stream_id, &count) < 0)
    {
        return -1;
    }

    return 0;
}

int aeron_receive_channel_endpoint_decref_to_response_stream(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    const int64_t count = aeron_int64_counter_map_get(&endpoint->response_stream_id_to_refcnt_map, stream_id);

    if (0 == count)
    {
        return 0;
    }

    int64_t count_after_dec = 0;
    int result = aeron_int64_counter_map_dec_and_get(
        &endpoint->response_stream_id_to_refcnt_map, stream_id, &count_after_dec);
    if (result < 0)
    {
        return -1;
    }

    if (0 == count_after_dec)
    {
        aeron_receive_channel_endpoint_try_remove_endpoint(endpoint);
    }

    return result;
}

int aeron_receive_channel_endpoint_incref_to_stream_and_session(
    aeron_receive_channel_endpoint_t *endpoint,
    int32_t stream_id,
    int32_t session_id)
{
    int64_t count;
    if (aeron_int64_counter_map_inc_and_get(
        &endpoint->stream_and_session_id_to_refcnt_map,
        aeron_map_compound_key(stream_id, session_id),
        &count) < 0)
    {
        return -1;
    }

    if (1 == count)
    {
        aeron_driver_receiver_proxy_on_add_subscription_by_session(
            endpoint->receiver_proxy, endpoint, stream_id, session_id);
    }

    return 0;
}

int aeron_receive_channel_endpoint_decref_to_stream_and_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id)
{
    const int64_t stream_and_session_key = aeron_map_compound_key(stream_id, session_id);
    const int64_t count = aeron_int64_counter_map_get(
        &endpoint->stream_and_session_id_to_refcnt_map, stream_and_session_key);

    if (0 == count)
    {
        return 0;
    }

    int64_t count_after_dec = 0;
    int result = aeron_int64_counter_map_dec_and_get(
        &endpoint->stream_and_session_id_to_refcnt_map, stream_and_session_key, &count_after_dec);

    if (result < 0)
    {
        return -1;
    }

    if (0 == count_after_dec)
    {
        aeron_driver_receiver_proxy_on_remove_subscription_by_session(
            endpoint->receiver_proxy, endpoint, stream_id, session_id);

        aeron_receive_channel_endpoint_try_remove_endpoint(endpoint);
    }

    return result;
}

int aeron_receive_channel_endpoint_on_add_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_add_subscription(&endpoint->dispatcher, stream_id);
}

int aeron_receive_channel_endpoint_on_remove_subscription(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id)
{
    return aeron_data_packet_dispatcher_remove_subscription(&endpoint->dispatcher, stream_id);
}

int aeron_receive_channel_endpoint_on_add_subscription_by_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id)
{
    return aeron_data_packet_dispatcher_add_subscription_by_session(&endpoint->dispatcher, stream_id, session_id);
}

int aeron_receive_channel_endpoint_add_destination(
    aeron_receive_channel_endpoint_t *endpoint, aeron_receive_destination_t *destination)
{
    int capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(capacity_result, endpoint->destinations, aeron_receive_channel_endpoint_t)

    if (capacity_result < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate space for additional destinations");
        return -1;
    }

    endpoint->destinations.array[endpoint->destinations.length].destination = destination;
    destination->transport.dispatch_clientd = endpoint;

    endpoint->destinations.length++;

    return (int)endpoint->destinations.length;
}

int aeron_receive_channel_endpoint_remove_destination(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_udp_channel_t *channel,
    aeron_receive_destination_t **destination_out)
{
    int deleted = 0;

    for (int last_index = (int)endpoint->destinations.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;
        if (aeron_udp_channel_equals(channel, destination->conductor_fields.udp_channel))
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)endpoint->destinations.array, sizeof(aeron_receive_destination_entry_t), i, last_index);

            --endpoint->destinations.length;
            ++deleted;

            if (NULL != destination)
            {
                *destination_out = destination;
            }

            break;
        }
    }

    return deleted;
}

int aeron_receive_channel_endpoint_on_remove_subscription_by_session(
    aeron_receive_channel_endpoint_t *endpoint, int32_t stream_id, int32_t session_id)
{
    return aeron_data_packet_dispatcher_remove_subscription_by_session(&endpoint->dispatcher, stream_id, session_id);
}

int aeron_receive_channel_endpoint_on_add_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (aeron_udp_channel_interceptors_image_notifications(
            destination->data_paths,
            &destination->transport,
            image,
            AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION) < 0)
        {
            return -1;
        }
    }

    return aeron_data_packet_dispatcher_add_publication_image(&endpoint->dispatcher, image);
}

int aeron_receive_channel_endpoint_on_remove_publication_image(
    aeron_receive_channel_endpoint_t *endpoint, aeron_publication_image_t *image)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (aeron_udp_channel_interceptors_image_notifications(
            destination->data_paths,
            &destination->transport,
            image,
            AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION) < 0)
        {
            return -1;
        }
    }

    return aeron_data_packet_dispatcher_remove_publication_image(&endpoint->dispatcher, image);
}

static inline bool aeron_receive_channel_endpoint_validate_so_rcvbuf(
    size_t so_rcvbuf,
    size_t value,
    const char *msg,
    aeron_driver_context_t *ctx)
{
    if (0 != so_rcvbuf && so_rcvbuf < value)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s greater than socket SO_RCVBUF, increase '"
            AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "' to match window: value=%" PRIu64 ", SO_RCVBUF=%" PRIu64,
            msg, value, so_rcvbuf);

        return false;
    }

    if (0 == so_rcvbuf && ctx->os_buffer_lengths.default_so_rcvbuf < value)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s greater than socket SO_RCVBUF, increase '"
            AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR "' to match window: value=%" PRIu64 ", SO_RCVBUF=%" PRIu64 " (OS Default)",
            msg, value, ctx->os_buffer_lengths.default_so_rcvbuf);

        return false;
    }

    return true;
}

int aeron_receiver_channel_endpoint_validate_sender_mtu_length(
    aeron_receive_channel_endpoint_t *endpoint,
    size_t sender_mtu_length,
    size_t window_max_length,
    aeron_driver_context_t *ctx)
{
    if (sender_mtu_length < AERON_DATA_HEADER_LENGTH)
    {
        AERON_SET_ERR(
            EINVAL,
            "mtuLength=%" PRIu64 " < DATA_HEADER_LENGTH=%d",
            (uint64_t)sender_mtu_length,
            AERON_DATA_HEADER_LENGTH);
        return -1;
    }

    if (sender_mtu_length > AERON_MAX_UDP_PAYLOAD_LENGTH)
    {
        AERON_SET_ERR(
            EINVAL,
            "mtuLength=%" PRIu64 " > MAX_UDP_PAYLOAD_LENGTH=%d",
            (uint64_t)sender_mtu_length,
            AERON_MAX_UDP_PAYLOAD_LENGTH);
        return -1;
    }

    if ((sender_mtu_length & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)) != 0)
    {
        AERON_SET_ERR(
            EINVAL,
            "mtuLength=%" PRIu64 " must be a multiple of FRAME_ALIGNMENT=%d",
            (uint64_t)sender_mtu_length,
            AERON_LOGBUFFER_FRAME_ALIGNMENT);
        return -1;
    }

    if (sender_mtu_length > window_max_length)
    {
        AERON_SET_ERR(
            EINVAL,
            "mtuLength=%" PRIu64 " > initialWindowLength=%" PRIu64,
            (uint64_t)sender_mtu_length,
            (uint64_t)window_max_length);
        return -1;
    }

    const size_t socket_rcvbuf = aeron_udp_channel_socket_so_rcvbuf(
        endpoint->conductor_fields.udp_channel, ctx->socket_rcvbuf);

    if (!aeron_receive_channel_endpoint_validate_so_rcvbuf(socket_rcvbuf, window_max_length, "Max Window length", ctx))
    {
        return -1;
    }

    if (!aeron_receive_channel_endpoint_validate_so_rcvbuf(socket_rcvbuf, sender_mtu_length, "Sender MTU", ctx))
    {
        return -1;
    }

    return 0;
}

void aeron_receive_channel_endpoint_check_for_re_resolution(
    aeron_receive_channel_endpoint_t *endpoint, int64_t now_ns, aeron_driver_conductor_proxy_t *conductor_proxy)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (aeron_receive_destination_re_resolution_required(destination, now_ns))
        {
            const char *endpoint_name = destination->conductor_fields.udp_channel->uri.params.udp.control;
            struct sockaddr_storage *addr = &destination->current_control_addr;
            aeron_driver_conductor_proxy_on_re_resolve_control(
                conductor_proxy, endpoint_name, endpoint, destination, addr);
            aeron_receive_destination_update_last_activity_ns(destination, now_ns);
        }
    }
}

void aeron_receive_channel_endpoint_update_control_address(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    struct sockaddr_storage *address)
{
    if (destination->conductor_fields.udp_channel->has_explicit_control)
    {
        memcpy(&destination->current_control_addr, address, sizeof(destination->current_control_addr));
    }
}

int aeron_receive_channel_endpoint_add_poll_transports(
    aeron_receive_channel_endpoint_t *endpoint, aeron_udp_transport_poller_t *poller)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (aeron_udp_channel_interceptors_transport_notifications(
            destination->data_paths,
            &destination->transport,
            destination->conductor_fields.udp_channel,
            &endpoint->dispatcher,
            AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION) < 0)
        {
            return -1;
        }

        if (endpoint->transport_bindings->poller_add_func(poller, &destination->transport))
        {
            return -1;
        }
    }

    return 0;
}

int aeron_receive_channel_endpoint_remove_poll_transports(
    aeron_receive_channel_endpoint_t *endpoint, aeron_udp_transport_poller_t *poller)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;

        if (aeron_udp_channel_interceptors_transport_notifications(
            destination->data_paths,
            &destination->transport,
            destination->conductor_fields.udp_channel,
            &endpoint->dispatcher,
            AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION) < 0)
        {
            return -1;
        }

        if (endpoint->transport_bindings->poller_remove_func(poller, &destination->transport))
        {
            return -1;
        }
    }

    return 0;
}

int aeron_receive_channel_endpoint_add_pending_setup_destination(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_driver_receiver_t *receiver,
    aeron_receive_destination_t *destination,
    int32_t session_id,
    int32_t stream_id)
{
    aeron_udp_channel_t *udp_channel = destination->conductor_fields.udp_channel;

    if (destination->conductor_fields.udp_channel->has_explicit_control)
    {
        if (aeron_driver_receiver_add_pending_setup(
            receiver, endpoint, destination, session_id, stream_id, &udp_channel->local_control) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to add pending setup for receiver");
            return -1;
        }

        if (aeron_receive_channel_endpoint_send_sm(
            endpoint, destination, &destination->current_control_addr, stream_id, session_id, 0, 0, 0,
            AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to send sm for receiver");
            return -1;
        }

        return 1;
    }

    return 0;
}

int aeron_receive_channel_endpoint_add_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_driver_receiver_t *receiver,
    int32_t session_id,
    int32_t stream_id)
{
    for (size_t i = 0, len = endpoint->destinations.length; i < len; i++)
    {
        aeron_receive_destination_t *destination = endpoint->destinations.array[i].destination;
        if (aeron_receive_channel_endpoint_add_pending_setup_destination(
            endpoint, receiver, destination, session_id, stream_id) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            aeron_driver_receiver_log_error(receiver);
        }
    }

    return 0;
}

extern void aeron_receive_channel_endpoint_on_remove_pending_setup(
    aeron_receive_channel_endpoint_t *endpoint, int32_t session_id, int32_t stream_id);

extern void aeron_receive_channel_endpoint_receiver_release(aeron_receive_channel_endpoint_t *endpoint);

extern bool aeron_receive_channel_endpoint_has_receiver_released(aeron_receive_channel_endpoint_t *endpoint);

extern bool aeron_receive_channel_endpoint_should_elicit_setup_message(aeron_receive_channel_endpoint_t *endpoint);

extern int aeron_receive_channel_endpoint_bind_addr_and_port(
    aeron_receive_channel_endpoint_t *endpoint, char *buffer, size_t length);

extern void aeron_receive_channel_endpoint_inc_image_ref_count(aeron_receive_channel_endpoint_t *endpoint);
extern void aeron_receive_channel_endpoint_dec_image_ref_count(aeron_receive_channel_endpoint_t *endpoint);
