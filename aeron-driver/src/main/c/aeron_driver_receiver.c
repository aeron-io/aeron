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

#include <stdio.h>
#include "util/aeron_arrayutil.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "aeron_driver_receiver.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

int aeron_driver_receiver_init(
    aeron_driver_receiver_t *receiver,
    aeron_driver_context_t *context,
    aeron_system_counters_t *system_counters,
    aeron_distinct_error_log_t *error_log)
{
    if (context->udp_channel_transport_bindings->poller_init_func(
        &receiver->poller, context, AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        return -1;
    }

    receiver->recv_buffers.vector_capacity = context->receiver_io_vector_capacity;
    for (size_t i = 0; i < receiver->recv_buffers.vector_capacity; i++)
    {
        size_t offset = 0;
        if (aeron_alloc_aligned(
            (void **)&receiver->recv_buffers.buffers[i],
            &offset,
            AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH,
            AERON_CACHE_LINE_LENGTH) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to allocate receiver->recv_buffers");
            return -1;
        }

        receiver->recv_buffers.iov[i].iov_base = receiver->recv_buffers.buffers[i] + offset;
        receiver->recv_buffers.iov[i].iov_len = AERON_DRIVER_RECEIVER_MAX_UDP_PACKET_LENGTH;
    }

    if (aeron_udp_channel_data_paths_init(
        &receiver->data_paths,
        context->udp_channel_outgoing_interceptor_bindings,
        context->udp_channel_incoming_interceptor_bindings,
        context->udp_channel_transport_bindings,
        aeron_receive_channel_endpoint_dispatch,
        context,
        AERON_UDP_CHANNEL_TRANSPORT_AFFINITY_RECEIVER) < 0)
    {
        return -1;
    }

    receiver->images.array = NULL;
    receiver->images.length = 0;
    receiver->images.capacity = 0;

    receiver->pending_setups.array = NULL;
    receiver->pending_setups.length = 0;
    receiver->pending_setups.capacity = 0;

    receiver->context = context;
    receiver->poller_poll_func = context->udp_channel_transport_bindings->poller_poll_func;
    receiver->recvmmsg_func = context->udp_channel_transport_bindings->recvmmsg_func;
    receiver->error_log = error_log;

    receiver->receiver_proxy.command_queue = &context->receiver_command_queue;
    receiver->receiver_proxy.fail_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_RECEIVER_PROXY_FAILS);
    receiver->receiver_proxy.threading_mode = context->threading_mode;
    receiver->receiver_proxy.receiver = receiver;
    receiver->receiver_proxy.log.on_add_endpoint = context->log.receiver_proxy_on_add_endpoint;
    receiver->receiver_proxy.log.on_remove_endpoint = context->log.receiver_proxy_on_remove_endpoint;

    receiver->errors_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    receiver->invalid_frames_counter = aeron_system_counter_addr(system_counters, AERON_SYSTEM_COUNTER_INVALID_PACKETS);
    receiver->total_bytes_received_counter =  aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_BYTES_RECEIVED);
    receiver->resolution_changes_counter = aeron_system_counter_addr(
        system_counters, AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES);

    int64_t now_ns = context->nano_clock();
    receiver->re_resolution_deadline_ns = now_ns + (int64_t)context->re_resolution_check_interval_ns;
    aeron_duty_cycle_tracker_t *tracker = receiver->context->receiver_duty_cycle_tracker;
    tracker->update(tracker->state, now_ns);

    return 0;
}

static void aeron_driver_receiver_on_rb_command_queue(
    int32_t msg_type_id,
    const void *message,
    size_t size,
    void *clientd)
{
    aeron_command_base_t *cmd = (aeron_command_base_t *)message;
    cmd->func(clientd, cmd);
}

int aeron_driver_receiver_do_work(void *clientd)
{
    struct mmsghdr mmsghdr[AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX];
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;

    const size_t vlen = receiver->recv_buffers.vector_capacity;
    int64_t now_ns = receiver->context->nano_clock();
    aeron_clock_update_cached_nano_time(receiver->context->receiver_cached_clock, now_ns);

    aeron_duty_cycle_tracker_t *tracker = receiver->context->receiver_duty_cycle_tracker;
    tracker->measure_and_update(tracker->state, now_ns);

    int work_count = (int)aeron_mpsc_rb_read(
        receiver->receiver_proxy.command_queue,
        aeron_driver_receiver_on_rb_command_queue,
        receiver,
        AERON_COMMAND_DRAIN_LIMIT);

    for (size_t i = 0; i < vlen; i++)
    {
        mmsghdr[i].msg_hdr.msg_name = &receiver->recv_buffers.addrs[i];
        mmsghdr[i].msg_hdr.msg_namelen = sizeof(receiver->recv_buffers.addrs[i]);
        mmsghdr[i].msg_hdr.msg_iov = &receiver->recv_buffers.iov[i];
        mmsghdr[i].msg_hdr.msg_iovlen = 1;
        mmsghdr[i].msg_hdr.msg_flags = 0;
        mmsghdr[i].msg_hdr.msg_control = NULL;
        mmsghdr[i].msg_hdr.msg_controllen = 0;
        mmsghdr[i].msg_len = 0;
    }

    int64_t bytes_received = 0;
    int poll_result = receiver->poller_poll_func(
        &receiver->poller,
        mmsghdr,
        vlen,
        &bytes_received,
        receiver->data_paths.recv_func,
        receiver->recvmmsg_func,
        receiver);

    if (poll_result < 0)
    {
        AERON_APPEND_ERR("%s", "receiver poller_poll");
        aeron_driver_receiver_log_error(receiver);
    }

    work_count += (int)bytes_received;

    aeron_counter_get_and_add_release(receiver->total_bytes_received_counter, bytes_received);

    for (size_t i = 0, length = receiver->images.length; i < length; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;

        if (NULL != image->endpoint)
        {
            int send_result = aeron_publication_image_send_pending_status_message(image, now_ns);
            if (send_result < 0)
            {
                AERON_APPEND_ERR("%s", "receiver send SM");
                aeron_driver_receiver_log_error(receiver);
            }
            else
            {
                work_count += send_result;
            }

            send_result = aeron_publication_image_send_pending_loss(image);
            if (send_result < 0)
            {
                AERON_APPEND_ERR("%s", "receiver send NAK");
                aeron_driver_receiver_log_error(receiver);
            }
            else
            {
                work_count += send_result;
            }

            send_result = aeron_publication_image_initiate_rttm(image, now_ns);
            if (send_result < 0)
            {
                AERON_APPEND_ERR("%s", "receiver send RTTM");
                aeron_driver_receiver_log_error(receiver);
            }
            else
            {
                work_count += send_result;
            }
        }
    }

    for (int last_index = (int)receiver->pending_setups.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_driver_receiver_pending_setup_entry_t *entry = &receiver->pending_setups.array[i];

        if (now_ns > (entry->time_of_status_message_ns + AERON_DRIVER_RECEIVER_PENDING_SETUP_TIMEOUT_NS))
        {
            if (!entry->is_periodic)
            {
                aeron_receive_channel_endpoint_on_remove_pending_setup(
                    entry->endpoint, entry->session_id, entry->stream_id);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)receiver->pending_setups.array,
                    sizeof(aeron_driver_receiver_pending_setup_entry_t),
                    (size_t)i,
                    (size_t)last_index);
                last_index--;
                receiver->pending_setups.length--;
            }
            else if (aeron_receive_channel_endpoint_should_elicit_setup_message(entry->endpoint))
            {
                if (aeron_receive_channel_endpoint_send_sm(
                    entry->endpoint,
                    entry->destination,
                    &entry->control_addr,
                    entry->stream_id,
                    entry->session_id,
                    0,
                    0,
                    0,
                    AERON_STATUS_MESSAGE_HEADER_SEND_SETUP_FLAG) < 0)
                {
                    AERON_APPEND_ERR("%s", "receiver send periodic SM");
                    aeron_driver_receiver_log_error(receiver);
                }

                entry->time_of_status_message_ns = now_ns;
            }
        }
    }

    if (receiver->context->re_resolution_check_interval_ns > 0 && (receiver->re_resolution_deadline_ns - now_ns) < 0)
    {
        receiver->re_resolution_deadline_ns = now_ns + (int64_t)receiver->context->re_resolution_check_interval_ns;
        aeron_udp_transport_poller_check_receive_endpoint_re_resolutions(
            &receiver->poller, now_ns, receiver->context->conductor_proxy);
    }

    return work_count;
}

void aeron_driver_receiver_on_close(void *clientd)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;

    for (size_t i = 0; i < receiver->recv_buffers.vector_capacity; i++)
    {
        aeron_free(receiver->recv_buffers.buffers[i]);
    }

    aeron_free(receiver->images.array);
    aeron_free(receiver->pending_setups.array);
    aeron_udp_channel_data_paths_delete(&receiver->data_paths);

    receiver->context->udp_channel_transport_bindings->poller_close_func(&receiver->poller);
}

void aeron_driver_receiver_on_add_endpoint(void *clientd, void *command)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->item;

    if (aeron_receive_channel_endpoint_add_poll_transports(endpoint, &receiver->poller) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_add_endpoint");
        aeron_driver_receiver_log_error(receiver);
        return;
    }

    aeron_receive_channel_endpoint_add_pending_setup(endpoint, receiver, 0, 0);
}

void aeron_driver_receiver_on_remove_endpoint(void *clientd, void *command)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_base_t *cmd = (aeron_command_base_t *)command;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->item;

    if (aeron_receive_channel_endpoint_remove_poll_transports(endpoint, &receiver->poller) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_remove_endpoint");
        aeron_driver_receiver_log_error(receiver);
    }

    for (int last_index = (int)receiver->pending_setups.length - 1, i = last_index; i >= 0; i--)
    {
        aeron_driver_receiver_pending_setup_entry_t *entry = &receiver->pending_setups.array[i];

        if (entry->endpoint == endpoint)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)receiver->pending_setups.array,
                sizeof(aeron_driver_receiver_pending_setup_entry_t),
                (size_t)i,
                (size_t)last_index);
            last_index--;
            receiver->pending_setups.length--;
        }
    }

    for (size_t i = 0, len = receiver->images.length; i < len; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;
        if (endpoint == image->endpoint)
        {
            aeron_publication_image_disconnect_endpoint(image);
        }
    }

    aeron_driver_conductor_proxy_on_receive_endpoint_removed(receiver->context->conductor_proxy, endpoint);
}

void aeron_driver_receiver_on_add_subscription(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_add_subscription(endpoint, cmd->stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_add_subscription");
        aeron_driver_receiver_log_error(receiver);
    }
}

void aeron_driver_receiver_on_remove_subscription(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_remove_subscription(endpoint, cmd->stream_id) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_remove_subscription");
        aeron_driver_receiver_log_error(receiver);
    }
}

void aeron_driver_receiver_on_add_subscription_by_session(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_add_subscription_by_session(endpoint, cmd->stream_id, cmd->session_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_driver_receiver_log_error(receiver);
        return;
    }

    if (endpoint->conductor_fields.udp_channel->has_explicit_control)
    {
        if (aeron_receive_channel_endpoint_elicit_setup(endpoint, cmd->stream_id, cmd->session_id) < 0)
        {
            AERON_APPEND_ERR("streamId=%" PRId32 " sessionId=%" PRId32, cmd->stream_id, cmd->session_id);
            aeron_driver_receiver_log_error(receiver);
            return;
        }
    }
}

void aeron_driver_receiver_on_request_setup(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (endpoint->conductor_fields.udp_channel->has_explicit_control)
    {
        if (aeron_receive_channel_endpoint_elicit_setup(endpoint, cmd->stream_id, cmd->session_id) < 0)
        {
            AERON_APPEND_ERR("streamId=%" PRId32 " sessionId=%" PRId32, cmd->stream_id, cmd->session_id);
            aeron_driver_receiver_log_error(receiver);
            return;
        }
    }
}

void aeron_driver_receiver_on_remove_subscription_by_session(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_subscription_t *cmd = (aeron_command_subscription_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    if (aeron_receive_channel_endpoint_on_remove_subscription_by_session(endpoint, cmd->stream_id, cmd->session_id) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_remove_subscription");
        aeron_driver_receiver_log_error(receiver);
    }
}

void aeron_driver_receiver_on_add_destination(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_add_rcv_destination_t *command = (aeron_command_add_rcv_destination_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)command->endpoint;
    aeron_receive_destination_t *destination = (aeron_receive_destination_t *)command->destination;

    if (aeron_receive_channel_endpoint_add_destination(endpoint, destination) < 0)
    {
        AERON_APPEND_ERR("%s", "on_add_destination, add to endpoint");
        aeron_driver_receiver_log_error(receiver);
        return;
    }

    if (aeron_udp_channel_interceptors_transport_notifications(
        destination->data_paths,
        &destination->transport,
        destination->conductor_fields.udp_channel,
        &endpoint->dispatcher,
        AERON_UDP_CHANNEL_INTERCEPTOR_ADD_NOTIFICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "on_add_destination, interceptors transport notifications");
        aeron_driver_receiver_log_error(receiver);
    }

    if (endpoint->transport_bindings->poller_add_func(&receiver->poller, &destination->transport) < 0)
    {
        AERON_APPEND_ERR("%s", "on_add_destination, add to poller");
        aeron_driver_receiver_log_error(receiver);
        aeron_receive_channel_endpoint_remove_destination(endpoint, destination->conductor_fields.udp_channel, NULL);
        return;
    }

    if (destination->conductor_fields.udp_channel->has_explicit_control)
    {
        if (aeron_receive_channel_endpoint_add_pending_setup_destination(endpoint, receiver, destination, 0, 0) < 0)
        {
            AERON_APPEND_ERR("%s", "on_add_destination, pending_setup");
            aeron_driver_receiver_log_error(receiver);

            aeron_receive_channel_endpoint_remove_destination(endpoint, destination->conductor_fields.udp_channel, NULL);
            endpoint->transport_bindings->poller_remove_func(&receiver->poller, &destination->transport);
            endpoint->transport_bindings->close_func(&destination->transport);
            return;
        }
    }

    for (size_t i = 0, len = receiver->images.length; i < len; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;
        if (endpoint == image->endpoint)
        {
            aeron_publication_image_add_destination(image, destination);
        }
    }
}

void aeron_driver_receiver_on_remove_destination(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_remove_rcv_destination_t *command = (aeron_command_remove_rcv_destination_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)command->endpoint;
    aeron_udp_channel_t *channel = (aeron_udp_channel_t *)command->channel;
    aeron_receive_destination_t *destination = NULL;

    if (0 < aeron_receive_channel_endpoint_remove_destination(endpoint, channel, &destination) && NULL != destination)
    {
        if (aeron_udp_channel_interceptors_transport_notifications(
            destination->data_paths,
            &destination->transport,
            destination->conductor_fields.udp_channel,
            &endpoint->dispatcher,
            AERON_UDP_CHANNEL_INTERCEPTOR_REMOVE_NOTIFICATION) < 0)
        {
            AERON_APPEND_ERR("%s", "on_add_destination, interceptors transport notifications");
            aeron_driver_receiver_log_error(receiver);
        }

        endpoint->transport_bindings->poller_remove_func(&receiver->poller, &destination->transport);

        for (size_t i = 0, len = receiver->images.length; i < len; i++)
        {
            aeron_publication_image_t *image = receiver->images.array[i].image;
            if (endpoint == image->endpoint)
            {
                aeron_publication_image_remove_destination(image, channel);
            }
        }

        aeron_driver_conductor_proxy_on_delete_receive_destination(
            receiver->context->conductor_proxy, endpoint, destination, channel);
    }
}

void aeron_driver_receiver_disconnect_inactive_image(
    aeron_driver_receiver_t *receiver,
    const aeron_receive_channel_endpoint_t *endpoint,
    const int32_t stream_id,
    const int32_t session_id)
{
    for (size_t i = 0, length = receiver->images.length; i < length; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;
        if (endpoint == image->endpoint && stream_id == image->stream_id && session_id == image->session_id)
        {
            aeron_publication_image_stop_status_messages_if_not_active(image);
        }
    }
}

void aeron_driver_receiver_on_add_publication_image(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_publication_image_t *cmd = (aeron_command_publication_image_t *)item;
    aeron_publication_image_t *image = cmd->image;

    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, receiver->images, aeron_driver_receiver_image_entry_t)

    if (ensure_capacity_result < 0 ||
        aeron_receive_channel_endpoint_on_add_publication_image(image->endpoint, image) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_add_publication_image");
        aeron_driver_receiver_log_error(receiver);
    }
    else
    {
        aeron_driver_receiver_disconnect_inactive_image(receiver, image->endpoint, image->stream_id, image->session_id);
        receiver->images.array[receiver->images.length++].image = cmd->image;
    }
}

void aeron_driver_receiver_on_remove_publication_image(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = (aeron_driver_receiver_t *)clientd;
    aeron_command_publication_image_t *cmd = (aeron_command_publication_image_t *)item;
    aeron_publication_image_t *image = (aeron_publication_image_t *)cmd->image;

    if (NULL != image->endpoint &&
        aeron_receive_channel_endpoint_on_remove_publication_image(image->endpoint, image) < 0)
    {
        AERON_APPEND_ERR("%s", "receiver on_remove_publication_image");
        aeron_driver_receiver_log_error(receiver);
    }

    for (size_t i = 0, size = receiver->images.length, last_index = size - 1; i < size; i++)
    {
        if (image == receiver->images.array[i].image)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)receiver->images.array, sizeof(aeron_driver_receiver_image_entry_t), i, last_index);
            receiver->images.length--;
            break;
        }
    }

    aeron_driver_conductor_proxy_on_release_resource(
        receiver->context->conductor_proxy, image, AERON_DRIVER_CONDUCTOR_RESOURCE_TYPE_PUBLICATION_IMAGE);
}

void aeron_driver_receiver_on_remove_matching_state(void *clientd, void *item)
{
    aeron_command_on_remove_matching_state_t *cmd = (aeron_command_on_remove_matching_state_t *)item;
    aeron_receive_channel_endpoint_t *endpoint = (aeron_receive_channel_endpoint_t *)cmd->endpoint;

    aeron_receive_channel_endpoint_on_remove_matching_state(endpoint, cmd->session_id, cmd->stream_id, cmd->state);
}

void aeron_driver_receiver_on_resolution_change(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = clientd;
    aeron_command_receiver_resolution_change_t *cmd = item;
    aeron_receive_channel_endpoint_t *endpoint = cmd->endpoint;
    aeron_receive_destination_t *destination = cmd->destination;

    for (size_t i = 0; i < receiver->pending_setups.length; i++)
    {
        aeron_driver_receiver_pending_setup_entry_t *pending_setup = &receiver->pending_setups.array[i];

        if (pending_setup->endpoint == endpoint &&
            pending_setup->destination == destination &&
            pending_setup->is_periodic)
        {
            memcpy(&pending_setup->control_addr, &cmd->new_addr, sizeof(pending_setup->control_addr));
            aeron_counter_get_and_add_release(receiver->resolution_changes_counter, 1);
        }
    }

    aeron_receive_channel_endpoint_update_control_address(endpoint, destination, &cmd->new_addr);
}

void aeron_driver_receiver_on_invalidate_image(void *clientd, void *item)
{
    aeron_driver_receiver_t *receiver = clientd;
    aeron_command_receiver_invalidate_image_t *cmd = item;
    const int64_t correlation_id = cmd->image_correlation_id;
    const int32_t reason_length = cmd->reason_length;
    const char *reason = (const char *)cmd->reason_text;

    for (size_t i = 0, size = receiver->images.length; i < size; i++)
    {
        aeron_publication_image_t *image = receiver->images.array[i].image;
        // TODO: Should we pass the pointer to the image here instead of the correlation_id.
        if (correlation_id == aeron_publication_image_registration_id(image))
        {
            aeron_publication_image_invalidate(image, reason_length, reason);
            break;
        }
    }
}

int aeron_driver_receiver_add_pending_setup(
    aeron_driver_receiver_t *receiver,
    aeron_receive_channel_endpoint_t *endpoint,
    aeron_receive_destination_t *destination,
    int32_t session_id,
    int32_t stream_id,
    struct sockaddr_storage *control_addr)
{
    int ensure_capacity_result = 0;
    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, receiver->pending_setups, aeron_driver_receiver_pending_setup_entry_t)

    if (ensure_capacity_result < 0)
    {
        AERON_APPEND_ERR("%s", "receiver add_pending_setup");
        return ensure_capacity_result;
    }

    aeron_driver_receiver_pending_setup_entry_t *entry =
        &receiver->pending_setups.array[receiver->pending_setups.length++];

    entry->endpoint = endpoint;
    entry->destination = destination;
    entry->session_id = session_id;
    entry->stream_id = stream_id;
    entry->time_of_status_message_ns = aeron_clock_cached_nano_time(receiver->context->receiver_cached_clock);
    entry->is_periodic = false;
    if (NULL != control_addr)
    {
        memcpy(&entry->control_addr, control_addr, sizeof(entry->control_addr));
        entry->is_periodic = true;
    }

    return ensure_capacity_result;
}

extern void aeron_driver_receiver_log_error(aeron_driver_receiver_t *receiver);
